"""
Tests for the networking view and the demonstrations:
  - request tracer: the proxy records every hop/decision of one request (rate limit, cache, pool,
    selection, forward attempts, retries) when asked to trace it;
  - HTTP outcome series: per-second 200 (backend / brownout / cache), 429 and errors;
  - scenario runner: prepares a clean controller, applies the allowed mitigations, drives the phases.
"""

import json
import threading
import time
import types
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from load_balancer.balancer import LoadBalancer
from load_balancer.cache import ResponseCache
from load_balancer.metrics import MetricsCollector
from load_balancer.ratelimit import HeavyHitterLimiter
from load_balancer.scheduler import SchedulerManager
from load_balancer.server_registry import ServerRegistry
from load_balancer import scenarios as sc_mod


class Backend:
    def __init__(self, status=200):
        outer = self

        class H(BaseHTTPRequestHandler):
            def do_GET(self):
                body = json.dumps({"status": "success", "service_ms": 50.0, "queue_ms": 2.0}).encode()
                self.send_response(outer.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass
        self.status = status
        self.srv = HTTPServer(("127.0.0.1", 0), H)
        self.port = self.srv.server_address[1]
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def close(self):
        self.srv.shutdown()
        self.srv.server_close()


class TestRequestTracer(unittest.TestCase):
    def setUp(self):
        self.b1, self.b2 = Backend(200), Backend(503)
        self.reg = ServerRegistry()
        self.reg.register_server("server-1", "Server 1", "127.0.0.1", self.b1.port)
        self.reg.register_server("server-2", "Server 2", "127.0.0.1", self.b2.port)
        self.cache, self.lim = ResponseCache(), HeavyHitterLimiter()
        self.lb = LoadBalancer(self.reg, SchedulerManager(self.reg), MetricsCollector(), cache=self.cache, limiter=self.lim)

    def tearDown(self):
        self.b1.close()
        self.b2.close()

    def test_trace_lists_every_hop(self):
        trace = []
        code, body = 0, {}
        for _ in range(2):                        # round robin: one of the two requests meets the failing server first
            trace = []
            code, body = self.lb.handle_request(cache_key="7", trace=trace)
            if any(t["step"] == "retry" for t in trace):
                break
        self.assertEqual(code, 200)
        steps = [t["step"] for t in trace]
        self.assertEqual(steps[:3], ["rate_limit", "cache", "pool"])
        self.assertIn("select", steps)
        self.assertIn("forward", steps)
        self.assertEqual(steps[-1], "backend")
        self.assertEqual(trace[1]["result"], "off")
        fw = [t for t in trace if t["step"] == "forward"]
        if len(fw) == 2:                          # failed attempt on Server 2, then retried on Server 1
            self.assertEqual(fw[0]["result"], "HTTP 503")
            self.assertIn("retry", steps)
        self.assertTrue(all(t["t_ms"] >= 0 for t in trace))
        self.assertNotIn("trace", body)           # the proxy adds it to the HTTP response, not the balancer

    def test_trace_of_cache_hit_and_429(self):
        self.cache.configure(fresh=True)
        self.cache.put("7", {"status": "success", "server_id": "server-1"})
        trace = []
        code, _ = self.lb.handle_request(cache_key="7", trace=trace)
        self.assertEqual((code, trace[1]["step"], trace[1]["result"]), (200, "cache", "HIT"))
        self.assertEqual(len(trace), 2)           # no backend involved
        self.lim.configure(True, 0.5)
        for _ in range(3):
            trace = []
            code, _ = self.lb.handle_request(client_ip="10.9.9.9", trace=trace)
        self.assertEqual(code, 429)
        self.assertEqual(trace[0]["result"], "429 Too Many Requests")

    def test_no_trace_by_default(self):
        code, body = self.lb.handle_request()
        self.assertNotIn("trace", body)


class TestResponseSeries(unittest.TestCase):
    def test_outcomes_per_second(self):
        m = MetricsCollector()
        now = time.time()
        for ok in (True, True, True, False):
            m.record_e2e(5.0, ok, now)
        m.count("cache_hits")
        m.count("degraded_responses")
        m.count("rate_limited")
        r = m.response_series(10, now)
        last = {k: r[k][-1] for k in ("backend_full", "backend_light", "cache", "rejected_429", "errors")}
        self.assertEqual(last, {"backend_full": 1, "backend_light": 1, "cache": 1, "rejected_429": 1, "errors": 1})
        self.assertEqual(len(r["t"]), 10)


class FakeTraffic:
    def __init__(self):
        self.rates, self.started, self.stopped = [], None, 0

    def start(self, **kw):
        self.started = kw

    def update_rate(self, r):
        self.rates.append(r)

    def stop(self):
        self.stopped += 1

    def status(self):
        return {"sent": 10, "ok": 9, "rate_limited": 1, "failed": 0}


class TestScenarioRunner(unittest.TestCase):
    def make(self):
        from tests.test_resilience_controller import ControllerCase
        case = ControllerCase()
        ctl = case.make()
        faults = types.SimpleNamespace(active_by_server=lambda: {}, clear=lambda *a: [], injected=[])
        faults.inject = lambda sid, t: faults.injected.append((sid, t))
        cap = types.SimpleNamespace(reset=lambda: None, resume=lambda: None)
        self.traffic = FakeTraffic()
        return sc_mod.ScenarioRunner({"traffic": self.traffic, "faults": faults, "resilience": ctl, "cache": case.cache,
                                      "limiter": case.lim, "balancer": case.lb, "capacity": cap, "metrics": case.metrics}), ctl, faults

    def test_runs_three_phases_and_reports(self):
        old = sc_mod.PHASES
        sc_mod.PHASES = (("normal", 0.2), ("spike", 0.2), ("calm", 0.2))
        try:
            runner, ctl, faults = self.make()
            runner.start("scale_out")
            time.sleep(0.2)
            self.assertEqual(ctl.allowed, {"scale_out"})          # checkout: only servers allowed
            runner._thread.join(timeout=5)
            self.assertEqual(self.traffic.rates, [sc_mod.LOW, sc_mod.HIGH, sc_mod.LOW])
            self.assertEqual(self.traffic.started["keys"], 0)
            self.assertFalse(runner.status()["state"]["running"])
            self.assertEqual(runner.last["outcome"], "finished")
            runner.start("failure")
            runner._thread.join(timeout=5)
            self.assertEqual(faults.injected, [("server-2", "refused")])
        finally:
            sc_mod.PHASES = old

    def test_stop_and_unknown(self):
        runner, _, _ = self.make()
        with self.assertRaises(ValueError):
            runner.start("nope")
        runner.start("cache")
        runner.stop(wait=True)
        self.assertEqual(runner.last["outcome"], "stopped")


if __name__ == "__main__":
    unittest.main()
