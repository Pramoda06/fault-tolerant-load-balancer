"""
Tests for network-fault handling in the reverse proxy.

Circuit breaker transitions with a fake clock (12), simulated partition and packet loss (13),
failure classification, idempotency-aware retries, retry budget, admission control,
one and several backends failing (1, 2), and the fault lifecycle (inject -> detect -> clear ->
recovered) with a fake control plane.
"""

import time
import unittest
from collections import Counter
from unittest.mock import patch, MagicMock

import requests

import config
from load_balancer.server_registry import ServerRegistry
from load_balancer.scheduler import SchedulerManager
from load_balancer.metrics import MetricsCollector
from load_balancer.balancer import LoadBalancer
from load_balancer.circuit_breaker import CircuitBreakerRegistry, CLOSED, OPEN, HALF_OPEN
from load_balancer.netsim import NetworkSimulator, SimulatedPartition, SimulatedPacketLoss
from load_balancer.retry_policy import RetryBudget, failure_kind, safe_to_retry, backoff_seconds
from load_balancer.faults import FaultManager, FaultError


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def make_registry(n=3):
    reg = ServerRegistry(recovery_threshold=2)
    for i in range(1, n + 1):
        reg.register_server(f"server-{i}", f"Server {i}", "127.0.0.1", 5000 + i, weight=1)
    return reg


def ok_resp(sid="x"):
    r = MagicMock(status_code=200)
    r.json.return_value = {"status": "healthy", "server_id": sid, "service_ms": 50.0, "queue_ms": 0.0}
    r.headers = {}
    return r


def err_resp(code=503, overloaded=False):
    r = MagicMock(status_code=code)
    r.json.return_value = {"status": "error"}
    r.headers = {"X-Backend-Overloaded": "1"} if overloaded else {}
    return r


def port_of(url):
    return int(url.split(":")[2].split("/")[0])


class TestCircuitBreaker(unittest.TestCase):                                     # case 12
    def test_closed_open_half_open_closed(self):
        clock = Clock()
        events = []
        cb = CircuitBreakerRegistry(failure_threshold=3, open_seconds=5, half_open_max_trials=1,
                                    half_open_successes=2, clock=clock, on_event=lambda *e: events.append(e[0]))
        for _ in range(2):
            cb.record_failure("s1", "timeout")
        self.assertEqual(cb.state("s1"), CLOSED)
        cb.record_failure("s1", "timeout")
        self.assertEqual(cb.state("s1"), OPEN)
        self.assertFalse(cb.can_route("s1"))
        clock.t += 5
        self.assertEqual(cb.state("s1"), HALF_OPEN)
        self.assertTrue(cb.acquire("s1"))          # one trial allowed
        self.assertFalse(cb.acquire("s1"))         # second concurrent trial refused
        cb.record_success("s1")
        self.assertEqual(cb.state("s1"), HALF_OPEN)
        self.assertTrue(cb.acquire("s1"))
        cb.record_success("s1")
        self.assertEqual(cb.state("s1"), CLOSED)
        self.assertEqual(events, ["CIRCUIT_OPENED", "CIRCUIT_HALF_OPEN", "CIRCUIT_CLOSED"])

    def test_half_open_failure_reopens(self):
        clock = Clock()
        cb = CircuitBreakerRegistry(failure_threshold=1, open_seconds=2, clock=clock)
        cb.record_failure("s1", "refused")
        clock.t += 2
        self.assertTrue(cb.acquire("s1"))
        cb.record_failure("s1", "refused")
        self.assertEqual(cb.state("s1"), OPEN)
        self.assertEqual(cb.snapshot()["s1"]["opens"], 2)

    def test_success_resets_consecutive_failures(self):
        cb = CircuitBreakerRegistry(failure_threshold=3, clock=Clock())
        cb.record_failure("s1")
        cb.record_failure("s1")
        cb.record_success("s1")
        cb.record_failure("s1")
        cb.record_failure("s1")
        self.assertEqual(cb.state("s1"), CLOSED)

    def test_reset_on_readmission(self):
        cb = CircuitBreakerRegistry(failure_threshold=1, open_seconds=60, clock=Clock())
        cb.record_failure("s1")
        cb.reset("s1", reason="re-admitted by health checks")
        self.assertEqual(cb.state("s1"), CLOSED)


class TestNetworkSimulator(unittest.TestCase):                                   # case 13
    def test_partition_raises_connect_timeout_and_counts(self):
        ns = NetworkSimulator(sleep=lambda s: None)
        ns.set_partition("s2", True)
        with self.assertRaises(SimulatedPartition):
            ns.before_send("s2", timeout=3.0)
        ns.before_send("s1", timeout=3.0)          # other servers unaffected
        self.assertTrue(issubclass(SimulatedPartition, requests.exceptions.ConnectTimeout))
        self.assertEqual(ns.snapshot()["stats"]["partition_drops"], 1)

    def test_loss_rate_is_seeded_and_close_to_probability(self):
        def run(seed):
            ns = NetworkSimulator(seed=seed, sleep=lambda s: None)
            ns.set_loss("s1", 0.3)
            lost = 0
            for _ in range(2000):
                try:
                    ns.before_send("s1")
                except SimulatedPacketLoss:
                    lost += 1
            return lost
        a, b = run(7), run(7)
        self.assertEqual(a, b)                      # repeatable with the same seed
        self.assertTrue(500 <= a <= 700, a)         # ~30 % of 2000

    def test_clear(self):
        ns = NetworkSimulator(sleep=lambda s: None)
        ns.set_partition("s1", True)
        ns.set_loss("s1", 0.5)
        ns.clear("s1")
        ns.before_send("s1")
        self.assertEqual(ns.faults(), {})


class TestRetryPolicy(unittest.TestCase):
    def test_failure_classification(self):
        self.assertEqual(failure_kind(exc=SimulatedPartition("x")), "partition")
        self.assertEqual(failure_kind(exc=SimulatedPacketLoss("x")), "packet_loss")
        self.assertEqual(failure_kind(exc=requests.exceptions.ConnectTimeout()), "connect_timeout")
        self.assertEqual(failure_kind(exc=requests.exceptions.ReadTimeout()), "timeout")
        self.assertEqual(failure_kind(exc=requests.exceptions.ConnectionError()), "connection_refused")
        self.assertEqual(failure_kind(status_code=503), "http_5xx")
        self.assertEqual(failure_kind(status_code=503, overloaded=True), "backend_overloaded")

    def test_idempotency_rules(self):
        idem = config.IDEMPOTENT_METHODS
        self.assertTrue(safe_to_retry("GET", "timeout", idem))
        self.assertFalse(safe_to_retry("POST", "timeout", idem))          # may have executed
        self.assertFalse(safe_to_retry("POST", "http_5xx", idem))
        self.assertTrue(safe_to_retry("POST", "connection_refused", idem))  # never delivered
        self.assertTrue(safe_to_retry("POST", "partition", idem))

    def test_budget_limits_retries(self):
        b = RetryBudget(ratio=0.1, min_tokens=2, max_tokens=5)
        self.assertTrue(b.try_spend())
        self.assertTrue(b.try_spend())
        self.assertFalse(b.try_spend())
        for _ in range(10):
            b.on_request()                           # 10 requests earn 1 token
        self.assertTrue(b.try_spend())
        self.assertEqual(b.snapshot()["retries_denied"], 1)

    def test_backoff_is_bounded_exponential(self):
        class R:
            def random(self):
                return 1.0
        self.assertAlmostEqual(backoff_seconds(1, 20, 200, R()), 0.030)
        self.assertAlmostEqual(backoff_seconds(2, 20, 200, R()), 0.060)
        self.assertAlmostEqual(backoff_seconds(9, 20, 200, R()), 0.300)  # capped 200 ms + 50 % jitter


class TestProxyFailureHandling(unittest.TestCase):
    def make_lb(self, n=3, algo="ROUND_ROBIN", netsim=None, budget=None):
        reg = make_registry(n)
        metrics = MetricsCollector()
        cb = CircuitBreakerRegistry(failure_threshold=3, open_seconds=60)
        lb = LoadBalancer(reg, SchedulerManager(algo), metrics, breakers=cb, retry_budget=budget,
                          netsim=netsim or NetworkSimulator(sleep=lambda s: None), sleep=lambda s: None)
        return reg, metrics, cb, lb

    @patch("load_balancer.balancer.requests.get")
    def test_one_backend_fails_others_serve(self, mock_get):                    # case 1
        reg, metrics, cb, lb = self.make_lb()

        def fake(url, headers=None, timeout=None):
            if port_of(url) == 5002:
                raise requests.exceptions.ConnectionError("refused")
            return ok_resp()
        mock_get.side_effect = fake
        codes = [lb.handle_request()[0] for _ in range(12)]
        self.assertEqual(codes, [200] * 12)
        self.assertEqual(cb.state("server-2"), OPEN)      # passive health took it out
        self.assertGreaterEqual(metrics.counters["connection_refused"], 3)

    @patch("load_balancer.balancer.requests.get")
    def test_multiple_backends_fail(self, mock_get):                            # case 2
        reg, metrics, cb, lb = self.make_lb()
        reg.get_server("server-1").status = "UNHEALTHY"

        def fake(url, headers=None, timeout=None):
            if port_of(url) == 5002:
                return err_resp(503)
            return ok_resp()
        mock_get.side_effect = fake
        served = Counter()
        for _ in range(10):
            code, body = lb.handle_request()
            self.assertEqual(code, 200)
            served[body["server_id"]] += 1
        self.assertEqual(set(served), {"server-3"})

    @patch("load_balancer.balancer.requests.post")
    def test_post_not_retried_after_timeout_but_retried_after_refused(self, mock_post):
        reg, metrics, cb, lb = self.make_lb()
        mock_post.side_effect = [requests.exceptions.ReadTimeout("t"), ok_resp()]
        code, body = lb.handle_request(method="POST", payload={"a": 1})
        self.assertEqual(code, 503)
        self.assertEqual(metrics.counters["not_retried_non_idempotent"], 1)
        self.assertEqual(mock_post.call_count, 1)
        mock_post.side_effect = [requests.exceptions.ConnectionError("refused"), ok_resp()]
        code, body = lb.handle_request(method="POST", payload={"a": 1})
        self.assertEqual(code, 200)
        self.assertEqual(body["failover_retries"], 1)

    @patch("load_balancer.balancer.requests.get")
    def test_partition_bypassed_and_counted(self, mock_get):                    # case 13
        ns = NetworkSimulator(sleep=lambda s: None)
        ns.set_partition("server-2", True)
        reg, metrics, cb, lb = self.make_lb(netsim=ns)
        mock_get.side_effect = lambda url, headers=None, timeout=None: ok_resp()
        for _ in range(9):
            self.assertEqual(lb.handle_request()[0], 200)
        called_ports = {port_of(c.args[0]) for c in mock_get.call_args_list}
        self.assertNotIn(5002, called_ports)        # the partitioned backend was never contacted
        self.assertGreaterEqual(metrics.counters["simulated_partition"], 1)

    @patch("load_balancer.balancer.requests.get")
    def test_packet_loss_retried_for_get(self, mock_get):                        # case 13
        ns = NetworkSimulator(seed=1, sleep=lambda s: None)
        ns.set_loss("server-1", 1.0)                # every exchange with server-1 is lost
        reg, metrics, cb, lb = self.make_lb(netsim=ns)
        mock_get.side_effect = lambda url, headers=None, timeout=None: ok_resp()
        code, body = lb.handle_request()
        self.assertEqual(code, 200)
        self.assertNotEqual(body["server_id"], "server-1")          # retried on another backend
        self.assertEqual(body["failover_retries"], 1)
        self.assertEqual(metrics.counters["simulated_packet_loss"], 1)

    @patch("load_balancer.balancer.requests.get")
    def test_retry_budget_prevents_storm(self, mock_get):
        reg, metrics, cb, lb = self.make_lb(budget=RetryBudget(ratio=0.0, min_tokens=1, max_tokens=1))
        cb.failure_threshold = 1000
        mock_get.side_effect = requests.exceptions.ReadTimeout("t")
        results = [lb.handle_request()[0] for _ in range(5)]
        self.assertEqual(results, [503] * 5)
        self.assertEqual(metrics.counters["retries_attempted"], 1)       # only one token
        self.assertGreaterEqual(metrics.counters["retries_denied_budget"], 4)

    @patch("load_balancer.balancer.requests.get")
    def test_overloaded_backend_does_not_open_circuit(self, mock_get):
        reg, metrics, cb, lb = self.make_lb(n=1)
        mock_get.side_effect = lambda url, headers=None, timeout=None: err_resp(503, overloaded=True)
        for _ in range(6):
            lb.handle_request()
        self.assertEqual(cb.state("server-1"), CLOSED)
        self.assertEqual(metrics.counters["backend_overloaded"], 6)

    def test_admission_control_sheds(self):
        reg, metrics, cb, lb = self.make_lb()
        lb._inflight = config.PROXY_MAX_INFLIGHT
        code, body = lb.handle_request()
        self.assertEqual(code, 503)
        self.assertIn("OVERLOADED", body["message"])
        self.assertEqual(metrics.counters["shed_proxy_overload"], 1)

    def test_data_recovery_gate_blocks_routing(self):
        reg, metrics, cb, lb = self.make_lb()
        lb.routing_gate = lambda: "DATA RECOVERY IN PROGRESS"
        code, body = lb.handle_request()
        self.assertEqual((code, body["message"]), (503, "DATA RECOVERY IN PROGRESS"))

    @patch("load_balancer.balancer.requests.get")
    def test_all_circuits_open_fail_fast(self, mock_get):
        reg, metrics, cb, lb = self.make_lb(n=2)
        for sid in ("server-1", "server-2"):
            for _ in range(3):
                cb.record_failure(sid)
        code, body = lb.handle_request()
        self.assertEqual(code, 503)
        self.assertIn("OPEN circuit", body["message"])
        mock_get.assert_not_called()


class FakeControlPlane:
    """Answers the proxy's control-plane calls like a real backend would."""

    def __init__(self):
        self.calls = []

    def __call__(self, url, json=None, timeout=None):
        self.calls.append((url, json))
        r = MagicMock(status_code=200)
        r.json.return_value = {"failure_injected_at": 1000.0} if url.endswith("/control/fault") else {}
        return r


class TestFaultLifecycle(unittest.TestCase):
    def setUp(self):
        self.reg = make_registry()
        for s in self.reg.get_all_servers():
            s.control_url = f"http://127.0.0.1:{s.port + 1000}"
        self.clock = Clock()
        self.metrics = MetricsCollector()
        self.ns = NetworkSimulator(sleep=lambda s: None)
        self.cb = CircuitBreakerRegistry(clock=self.clock)
        self.post = FakeControlPlane()
        self.fm = FaultManager(self.reg, self.metrics, self.ns, breakers=self.cb, clock=self.clock, http_post=self.post)

    def events(self):
        return [e["event_type"] for e in self.metrics.get_events()]

    def test_backend_fault_through_control_plane(self):
        rec = self.fm.inject("server-2", "refused")
        self.assertEqual(self.post.calls[-1], ("http://127.0.0.1:6002/control/fault", {"type": "refused", "delay_ms": 0}))
        self.assertFalse(rec["simulated"])
        self.clock.t += 1.5
        self.fm.on_health_failure("server-2", "connection_refused")
        self.reg.get_server("server-2").status = "UNHEALTHY"
        self.fm.clear("server-2")
        self.assertTrue(self.post.calls[-1][0].endswith("/control/clear"))
        self.fm.tick()                               # still UNHEALTHY -> not recovered yet
        self.reg.get_server("server-2").status = "HEALTHY"
        self.clock.t += 4
        self.fm.tick()
        r = self.fm.records()[-1]
        self.assertEqual(r["detected_by"], "health probe (connection_refused)")
        self.assertAlmostEqual(r["detected_at"] - r["injected_at"], 1.5)
        self.assertTrue(r["outcome"].startswith("recovered"))
        self.assertEqual(self.events(), ["FAULT_INJECTED", "FAULT_DETECTED", "FAULT_CLEARED", "FAULT_RECOVERED"])

    def test_network_faults_are_simulated_and_combinable(self):
        self.fm.inject("server-1", "partition")
        self.fm.inject("server-1", "packet_loss", {"loss": 0.25})
        self.assertEqual(self.ns.faults()["server-1"], {"partition": True, "loss": 0.25})
        self.assertTrue(all(r["simulated"] for r in self.fm.records()))
        self.fm.clear("server-1", "partition")
        self.assertEqual(self.ns.faults()["server-1"], {"partition": False, "loss": 0.25})
        self.fm.clear("server-1")
        self.assertEqual(self.ns.faults(), {})

    def test_new_backend_fault_supersedes_old(self):
        self.fm.inject("server-3", "slow", {"delay_ms": 300})
        self.fm.inject("server-3", "http_5xx")
        active = self.fm.active_by_server()["server-3"]
        self.assertEqual([f["type"] for f in active], ["http_5xx"])

    def test_slow_detected_by_response_time(self):
        self.fm.inject("server-1", "slow")
        self.fm.on_response("server-1", 100)
        self.assertIsNone(self.fm.records()[-1]["detected_at"])
        self.fm.on_response("server-1", 450)
        self.assertIn("slow response", self.fm.records()[-1]["detected_by"])

    def test_invalid_requests_rejected(self):
        with self.assertRaises(FaultError):
            self.fm.inject("server-1", "meteor")
        with self.assertRaises(FaultError):
            self.fm.inject("server-9", "refused")
        with self.assertRaises(FaultError):
            self.fm.inject("server-1", "packet_loss", {"loss": 1.5})


if __name__ == "__main__":
    unittest.main()
