"""
One-click demonstrations of the resilience controller (Resilience & Capacity page).

Each scenario replays the same three phases with the real traffic generator:
    normal  10 s at a low rate        everything is calm, no mitigation
    spike   45 s at a high rate       the controller must react
    calm    25 s at the low rate      the controller relaxes again (counterfactual check)

The traffic SHAPE decides which mitigation can help, so each scenario is built to make exactly one
rung the cheapest useful answer:

    cache        many users ask for a few popular items     -> the proxy answers repeats from its cache
    rate_limit   two clients flood the proxy                -> only those two get HTTP 429
    degrade      many users, all different requests          -> cache and rate limit cannot help: brownout
    scale_out    checkout traffic: stale, rejected or degraded responses are not acceptable
                                                            -> the only allowed answer is more servers
    failure      popular items + heavy clients, Server 2 crashes during the spike
                                                            -> the controller picks from everything

Nothing here decides FOR the controller: a scenario only sets the traffic, the allowed mitigations and
(optionally) a fault, then the controller reacts to what it measures. What actually happened is read
from its decision log and reported.
"""

import threading
import time
from typing import Any, Callable, Dict, Optional

LOW, HIGH = 8, 55          # req/s: 55 is ~92 % of three 50 ms single-worker backends (~20 req/s each)
PHASES = (("normal", 10), ("spike", 45), ("calm", 25))

SCENARIOS: Dict[str, Dict[str, Any]] = {
    "cache": {
        "title": "Many users want the same few items",
        "expect": "The proxy answers repeated requests from its cache. No new server.",
        "rung": "cache", "traffic": {"keys": 200, "heavy": 0.0},
    },
    "rate_limit": {
        "title": "Two clients flood the service",
        "expect": "Only the two flooding clients get HTTP 429 (Too Many Requests). Everyone else is served normally.",
        "rung": "rate_limit", "traffic": {"keys": 0, "heavy": 0.6},
    },
    "degrade": {
        "title": "Many users, every request different",
        "expect": "A cache or a rate limit cannot help, so the backends send lighter responses (brownout).",
        "rung": "degrade", "traffic": {"keys": 0, "heavy": 0.0},
    },
    "scale_out": {
        "title": "Checkout traffic: no shortcuts allowed",
        "expect": "Stale, rejected or lighter responses are not acceptable here, so the only answer is to add servers.",
        "rung": "scale_out", "traffic": {"keys": 0, "heavy": 0.0}, "allowed": ["scale_out"],
    },
    "failure": {
        "title": "A server crashes during a spike",
        "expect": "Server 2 fails at the start of the spike. The controller picks the cheapest fix that works.",
        "rung": None, "traffic": {"keys": 200, "heavy": 0.4}, "rate": (8, 40),
        "fault": {"server_id": "server-2", "type": "refused"},
    },
}


class ScenarioRunner:
    def __init__(self, ctx: Dict[str, Any], clock: Optional[Callable[[], float]] = None):
        self.ctx = ctx                       # traffic, faults, resilience, cache, limiter, balancer, capacity, metrics
        self._clock = clock or time.time
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.state: Dict[str, Any] = {"running": False, "name": None}
        self.last: Optional[Dict[str, Any]] = None

    # ------------------------------------------------------------------ #
    def start(self, name: str):
        if name not in SCENARIOS:
            raise ValueError(f"unknown scenario {name!r}")
        self.stop(wait=True)
        self._stop.clear()
        with self._lock:
            self.state = {"running": True, "name": name, "phase": "preparing", "phase_left_s": None,
                          "started_at": self._clock(), "total_s": sum(d for _, d in PHASES)}
        self._thread = threading.Thread(target=self._run, args=(name,), daemon=True, name="Scenario")
        self._thread.start()

    def stop(self, wait: bool = False):
        self._stop.set()
        t = self._thread
        if wait and t is not None and t.is_alive():
            t.join(timeout=5)

    # ------------------------------------------------------------------ #
    def _prepare(self, sc):
        c = self.ctx
        c["traffic"].stop()
        for sid in list(c["faults"].active_by_server()):     # every injected fault
            try:
                c["faults"].clear(sid)
            except Exception:
                pass
        c["capacity"].reset()                     # replacements from an earlier scenario go away
        c["capacity"].resume()
        if c.get("auto_recovery") is not None:    # ... and so do recovery replacements (e.g. after Data Recovery):
            c["auto_recovery"].reset()            # every scenario starts from exactly the 3 original servers
            c["auto_recovery"].resume()
        c["cache"].reset()
        c["limiter"].reset()
        c["balancer"].degrade = False
        res = c["resilience"]
        res.set_policy("ladder")
        res.reset()                               # rung 0, all mitigations allowed, costs cleared
        if sc.get("allowed"):
            res.set_allowed(sc["allowed"])

    def _run(self, name):
        sc = SCENARIOS[name]
        c = self.ctx
        low, high = sc.get("rate", (LOW, HIGH))
        try:
            self._prepare(sc)
            c["metrics"].add_log(f"Scenario started: {sc['title']}. Expected: {sc['expect']}", level="INFO",
                                 event_type="SCENARIO")
            t0 = self._clock()
            c["traffic"].start(rps=low, concurrency=32, clients=32, keys=sc["traffic"]["keys"],
                               heavy=sc["traffic"]["heavy"], heavy_clients=2, poisson=False, seed=7)
            for phase, secs in PHASES:
                rate = high if phase == "spike" else low
                c["traffic"].update_rate(rate)
                if phase == "spike" and sc.get("fault"):
                    f = sc["fault"]
                    c["faults"].inject(f["server_id"], f["type"])
                if phase == "calm" and sc.get("fault"):
                    c["faults"].clear(sc["fault"]["server_id"])
                end = self._clock() + secs
                while self._clock() < end:
                    with self._lock:
                        self.state.update(phase=phase, rate=rate, phase_left_s=round(end - self._clock(), 1),
                                          elapsed_s=round(self._clock() - t0, 1))
                    if self._stop.wait(0.5):
                        raise InterruptedError
            outcome = "finished"
        except InterruptedError:
            outcome = "stopped"
        except Exception as ex:                              # report, never crash the proxy
            outcome = f"error: {ex}"
        finally:
            c["traffic"].stop()
            if sc.get("fault"):
                try:
                    c["faults"].clear(sc["fault"]["server_id"])
                except Exception:
                    pass
        self._finish(name, sc, outcome)

    def _finish(self, name, sc, outcome):
        res = self.ctx["resilience"]
        st = res.status()
        started = self.state.get("started_at") or 0
        decisions = [d for d in st["decisions"] if d["at"] >= started][::-1]
        used = []
        for d in decisions:
            if d["action"] == "escalate" and d["rung"] not in used:
                used.append(d["rung"])
        expected = sc.get("rung")
        tr = self.ctx["traffic"].status()
        lim = self.ctx["limiter"]
        by = dict(getattr(lim, "_rejected_by", {}) or {})
        heavy_ips = {f"10.1.0.{i + 1}" for i in range(2)}            # client_ip(0), client_ip(1): the heavy hitters
        rejected_total = sum(by.values())
        rejected_heavy = sum(v for k, v in by.items() if k in heavy_ips)
        self.last = {
            "name": name, "title": sc["title"], "outcome": outcome, "expected_rung": expected,
            "rungs_used": used, "matched": (expected in used) if expected else None,
            "decisions": decisions, "costs": res.cost_summary(),
            "sent": tr["sent"], "ok": tr["ok"], "rate_limited": tr["rate_limited"], "failed": tr["failed"],
            "rejected_heavy": rejected_heavy, "rejected_total": rejected_total,
            "finished_at": time.strftime("%H:%M:%S"),
        }
        with self._lock:
            self.state = {"running": False, "name": None}
        self.ctx["metrics"].add_log(f"Scenario {outcome}: {sc['title']}. Mitigations used: "
                                    f"{', '.join(used) or 'none'}.", level="INFO", event_type="SCENARIO")

    # ------------------------------------------------------------------ #
    def status(self) -> Dict[str, Any]:
        with self._lock:
            st = dict(self.state)
        return {"state": st, "last": self.last, "phases": [{"name": n, "seconds": s} for n, s in PHASES],
                "scenarios": [{"key": k, "title": v["title"], "expect": v["expect"], "rung": v.get("rung")}
                              for k, v in SCENARIOS.items()]}
