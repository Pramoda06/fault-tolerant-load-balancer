"""
Unit tests for the cost-aware resilience controller and its mitigations.

Deterministic: fake clocks, a fake capacity estimator and explicit metric timestamps.
Covers the response cache (fresh hits, TTL, stale-if-error), the heavy-hitter limiter
(token bucket, 429), the proxy integration (cache hit, stale answer instead of 503, 429 without
using a backend, brownout header), and the controller: one rung at a time, measured effects,
counterfactual relaxation (no oscillation), the scale-out gate, and the baseline policies.
"""

import types
import unittest

from load_balancer.cache import ResponseCache
from load_balancer.ratelimit import HeavyHitterLimiter
from load_balancer.resilience import ResilienceController, RUNGS
from load_balancer.metrics import MetricsCollector
from load_balancer.server_registry import ServerRegistry
from load_balancer.scheduler import SchedulerManager
from load_balancer.balancer import LoadBalancer


class Clock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


def settings(**over):
    import config
    s = {k: getattr(config, k) for k in dir(config) if k.isupper()}
    s.update(RESILIENCE_POLICY="ladder", RESILIENCE_SLO_P95_MS=200.0, RESILIENCE_SLO_ERROR=0.01,
             RESILIENCE_WINDOW_S=5.0, RESILIENCE_UTIL_HIGH=0.85, RESILIENCE_UTIL_LOW=0.5, RESILIENCE_RELAX_S=10.0,
             RESILIENCE_DWELL_S=5.0, RESILIENCE_CACHE_TTL_S=2.0, RESILIENCE_HEAVY_FACTOR=2.0,
             RESILIENCE_STATIC_SERVERS=4, CAPACITY_MIN_SERVERS=1, BACKEND_DEGRADED_FACTOR=0.4)
    s.update(over)
    return types.SimpleNamespace(**s)


class FakeCapacity:
    def __init__(self, routable=2, cap=20.0):
        self.routable, self.cap = routable, cap
        self.scale_out_gate = None
        self.notified = 0

    def estimate(self, now=None):
        return {"routable": self.routable, "capacity_rps_per_server": self.cap, "safe_rps_per_server": self.cap * 0.7,
                "replacements_routable": 0, "pending": 0, "recovering": 0}

    def notify(self):
        self.notified += 1


class TestCache(unittest.TestCase):
    def test_fresh_ttl_and_stale_if_error(self):
        clk = Clock()
        c = ResponseCache(ttl=2.0, stale_max=60.0, clock=clk)
        c.put("7", {"v": 1})
        self.assertIsNone(c.fresh("7"))                 # fresh caching off
        c.configure(fresh=True, stale_on_error=True)
        self.assertEqual(c.fresh("7")[0], {"v": 1})
        clk.t += 3
        self.assertIsNone(c.fresh("7"))                 # older than ttl
        self.assertEqual(c.stale("7")[0], {"v": 1})     # still usable as stale-if-error
        clk.t += 60
        self.assertIsNone(c.stale("7"))
        self.assertEqual((c.hits, c.misses, c.stale_served), (1, 1, 1))

    def test_bounded_size(self):
        c = ResponseCache(max_entries=3)
        for i in range(10):
            c.put(str(i), {"i": i})
        self.assertEqual(c.status()["entries"], 3)


class TestLimiter(unittest.TestCase):
    def test_token_bucket_and_disabled(self):
        clk = Clock()
        lim = HeavyHitterLimiter(clock=clk)
        self.assertTrue(all(lim.allow("a") for _ in range(100)))        # off: everything allowed
        lim.configure(True, 2.0)                                         # 2 req/s, burst 4
        allowed = sum(lim.allow("a") for _ in range(10))
        self.assertEqual(allowed, 4)
        clk.t += 1.0
        self.assertEqual(sum(lim.allow("a") for _ in range(10)), 2)     # refilled 2 tokens
        self.assertTrue(lim.allow("b"))                                  # other clients unaffected
        self.assertEqual(lim.rejected, 14)


class TestProxyIntegration(unittest.TestCase):
    def setUp(self):
        self.reg = ServerRegistry()                     # no backends: every routed request gets 503
        self.metrics = MetricsCollector()
        self.cache = ResponseCache(ttl=2.0, stale_max=60.0)
        self.lim = HeavyHitterLimiter()
        self.lb = LoadBalancer(self.reg, SchedulerManager(self.reg), self.metrics, cache=self.cache, limiter=self.lim)

    def test_cache_hit_answers_without_backend(self):
        self.cache.configure(fresh=True)
        self.cache.put("5", {"status": "success", "server_id": "server-1"})
        code, body = self.lb.handle_request(cache_key="5")
        self.assertEqual((code, body["cache"]), (200, "HIT"))
        self.assertEqual(self.metrics.counters.get("cache_hits"), 1)
        self.assertEqual(self.metrics.arrivals_between(0), 0)            # not counted as backend demand

    def test_stale_if_error_instead_of_503(self):
        self.cache.put("5", {"status": "success", "server_id": "server-1"})
        code, _ = self.lb.handle_request(cache_key="5")
        self.assertEqual(code, 503)                                      # stale-if-error off
        self.cache.configure(stale_on_error=True)
        code, body = self.lb.handle_request(cache_key="5")
        self.assertEqual((code, body["cache"]), (200, "STALE"))
        self.assertIn("NO HEALTHY BACKEND", body["stale_reason"])

    def test_rate_limited_client_gets_429(self):
        self.lim.configure(True, 1.0)
        codes = [self.lb.handle_request(client_ip="10.0.0.9")[0] for _ in range(5)]
        self.assertIn(429, codes)
        self.assertEqual(self.metrics.counters.get("rate_limited"), codes.count(429))

    def test_internal_writes_are_never_rate_limited(self):
        self.lim.configure(True, 0.1)
        codes = [self.lb.handle_request(method="POST", rate_limit=False)[0] for _ in range(5)]
        self.assertNotIn(429, codes)


class ControllerCase(unittest.TestCase):
    def make(self, policy="ladder", routable=2, **over):
        self.clk = Clock()
        self.metrics = MetricsCollector()
        self.cap = FakeCapacity(routable=routable)
        self.cache = ResponseCache(clock=self.clk)
        self.lim = HeavyHitterLimiter(clock=self.clk)
        self.lb = types.SimpleNamespace(degrade=False)
        self.ctl = ResilienceController(self.metrics, self.cap, self.lb, self.cache, self.lim, None,
                                        settings=settings(RESILIENCE_POLICY=policy, **over), clock=self.clk)
        return self.ctl

    def load(self, backend_rps, seconds, p95=60.0, cache_rps=0.0, signals=True):
        """Advance time; record backend arrivals, answered requests and cache hits per second.
        signals=True: requests repeat a few popular items and one client sends half of them, so the
        cache and the rate limiter have predicted relief; False: neither can help."""
        for _ in range(int(seconds)):
            t0 = self.clk.t
            for k in range(int(backend_rps)):
                t = t0 + k / max(1, backend_rps)
                self.metrics.record_arrival(t)
                self.metrics.record_e2e(p95, True, t)
                with self.metrics._lock:
                    self.metrics._counter_events.append((t, "client_requests"))
                if signals:
                    self.cache.note(str(k % 5))
                    self.lim.note("heavy" if k % 2 else f"c{k}")
                else:
                    self.cache.note(f"u{self.clk.t}-{k}")
                    self.lim.note(f"c{k}")
            for k in range(int(cache_rps)):
                self.metrics.record_e2e(1.0, True, t0 + k / max(1, cache_rps))
                with self.metrics._lock:
                    self.metrics._counter_events.append((t0 + k / max(1, cache_rps), "cache_hits"))
            self.clk.t += 1.0
            self.ctl.tick()


class TestController(ControllerCase):
    def test_escalates_one_rung_per_dwell_and_gates_scale_out(self):
        ctl = self.make()
        self.assertFalse(ctl.scale_out_allowed()[0])
        self.load(40, 5, p95=400)                         # 2 servers x 20 req/s at 100 %: SLO broken
        self.assertEqual(ctl.level, 1)
        self.assertTrue(self.cache.fresh_enabled and self.cache.stale_on_error)
        self.load(40, 5, p95=400)
        self.assertEqual(ctl.level, 2)
        self.assertTrue(self.lim.enabled)
        self.load(40, 5, p95=400)
        self.assertEqual(ctl.level, 3)
        self.assertTrue(self.lb.degrade)
        self.assertFalse(ctl.scale_out_allowed()[0])
        self.load(40, 5, p95=400)
        self.assertEqual(ctl.level, len(RUNGS))
        self.assertTrue(ctl.scale_out_allowed()[0])        # servers only as the last resort
        st = ctl.status()
        self.assertEqual([d["rung"] for d in st["decisions"]][::-1], list(RUNGS))
        self.assertIsNotNone(st["decisions"][-1]["after"])  # effect of the first decision was measured

    def test_skips_rungs_without_predicted_relief(self):
        ctl = self.make()
        self.load(40, 3, p95=400, signals=False)          # unique items, no heavy client
        self.assertEqual(ctl.level, 3)                     # straight to brownout
        d = ctl.status()["decisions"][0]
        self.assertIn("skipped cache", d["reason"])
        self.assertIn("rate_limit", d["reason"])
        self.assertTrue(self.lb.degrade)
        self.assertFalse(self.cache.fresh_enabled)           # skipped rungs are never switched on
        self.assertFalse(self.lim.enabled)
        ctl2 = self.make()                                   # fresh run: one overloaded second, then demand drops
        self.load(40, 1, p95=400, signals=False)
        self.assertEqual(ctl2.level, 3)
        self.load(8, 30, p95=50, signals=False)              # one relax, straight to rung 0
        ctl = ctl2
        self.assertEqual(ctl.level, 0)
        self.assertEqual([(d["action"], d["rung"]) for d in ctl.status()["decisions"]],
                         [("relax", "degrade"), ("escalate", "degrade")])

    def test_disallowed_rungs_are_skipped(self):
        ctl = self.make()
        ctl.set_allowed(["scale_out"])                       # e.g. checkout: no stale, rejected or degraded responses
        self.load(40, 3, p95=400)
        self.assertEqual(ctl.level, 4)
        self.assertTrue(ctl.scale_out_allowed()[0])
        self.assertFalse(self.cache.fresh_enabled or self.lim.enabled or self.lb.degrade)
        self.assertIn("cache (not allowed)", ctl.status()["decisions"][0]["reason"])

    def test_no_escalation_when_slo_met_and_utilisation_moderate(self):
        ctl = self.make()
        self.load(25, 30, p95=60)                          # 62 % utilisation, p95 fine
        self.assertEqual(ctl.level, 0)

    def test_counterfactual_relaxation_prevents_oscillation(self):
        ctl = self.make()
        self.load(40, 5, p95=400)                          # escalate to cache
        self.assertEqual(ctl.level, 1)
        # the cache now absorbs 24 req/s: backends see 16 req/s (40 %), p95 fine -> but without the
        # cache utilisation would be 100 %: the controller must NOT relax
        self.load(16, 40, p95=60, cache_rps=24)
        self.assertEqual(ctl.level, 1)
        # demand really drops: 8 + 4 cached -> 30 % without the cache -> relax
        self.load(8, 25, p95=55, cache_rps=4)
        self.assertEqual(ctl.level, 0)
        self.assertFalse(self.cache.fresh_enabled)

    def test_scale_only_and_static_policies(self):
        ctl = self.make(policy="scale_only")
        self.load(40, 30, p95=400)
        self.assertEqual(ctl.level, 0)
        self.assertTrue(ctl.scale_out_allowed()[0])
        ctl = self.make(policy="static")
        self.assertTrue(ctl.scale_out_allowed({"routable": 3, "pending": 0, "recovering": 0})[0])
        self.assertFalse(ctl.scale_out_allowed({"routable": 4, "pending": 0, "recovering": 0})[0])

    def test_cache_always_baseline(self):
        ctl = self.make(policy="cache_always")
        ctl.reset()
        self.assertTrue(self.cache.fresh_enabled and self.cache.stale_on_error)   # on from the start, also after reset
        self.load(40, 20, p95=400)
        self.assertEqual(ctl.level, 0)                       # no other mitigation, ever
        self.assertFalse(self.lim.enabled or self.lb.degrade)
        self.assertTrue(ctl.scale_out_allowed()[0])          # servers added reactively, like scale_only
        self.assertTrue(ctl.on("cache"))

    def test_cost_accounting(self):
        ctl = self.make()
        self.load(40, 10, p95=400)
        c = ctl.cost_summary()
        self.assertGreater(c["slo_violation_seconds"], 0)
        self.assertEqual(c["extra_server_seconds"], 0.0)
        self.assertGreaterEqual(c["weighted_cost"], c["slo_violation_seconds"] * 2.0 - 1e-9)


if __name__ == "__main__":
    unittest.main()
