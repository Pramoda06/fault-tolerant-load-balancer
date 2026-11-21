"""
Cost-aware resilience controller: SLO-driven escalation over a ladder of mitigations.

Problem. A reactive auto-scaler has one answer to overload and failure: add servers. That is the
most expensive answer, and it is slow (a new server must start and pass health checks). Cheaper
mitigations exist at the reverse proxy, each with a different cost to users.

Approach. The controller watches a service-level objective (SLO) measured at the proxy:
    p95 end-to-end latency <= RESILIENCE_SLO_P95_MS   and   error rate <= RESILIENCE_SLO_ERROR
together with the utilisation of the routable servers (backend demand / capacity). When the SLO is
violated - or utilisation is above RESILIENCE_UTIL_HIGH, i.e. before it is violated - it escalates
ONE rung of a ladder ordered by cost, waits RESILIENCE_DWELL_S to MEASURE the effect, and escalates
again only if the objective is still missed:

    rung 1  cache          fresh responses for popular items (short TTL) + stale-if-error
    rung 2  rate_limit     per-client token buckets at RESILIENCE_HEAVY_FACTOR x the fair share:
                           only heavy hitters receive HTTP 429
    rung 3  degrade        brownout: backends skip optional work (cheaper requests)
    rung 4  scale_out      the capacity manager may start replacement servers

Effect-aware escalation. Before escalating, the controller estimates the RELIEF of each rung from
live signals - the share of requests a fresh cache would answer (keys repeated within the TTL,
tracked even while caching is off), the share a per-client limit would reject (traffic above the
heavy-hitter limit), and the work brownout saves (1 - DEGRADED_FACTOR). It escalates directly to
the cheapest rung whose predicted relief is at least RESILIENCE_MIN_RELIEF, instead of spending a
dwell period on rungs that cannot help (e.g. a cache for non-repeating requests).

When the SLO has been met with margin for RESILIENCE_RELAX_S, it steps down one rung at a time
(most expensive first) - but only if the utilisation estimated WITHOUT that rung is low as well
(counterfactual relaxation): the load the rung currently absorbs (requests answered from the cache,
requests rejected with 429, the work saved by brownout) is added back to the measured backend
load. Relaxing on the utilisation measured WITH the mitigation would remove it during the very
overload it is absorbing, and the controller would oscillate. Every decision is logged with the measured state before it and the
measured state RESILIENCE_DWELL_S after it (the observed effect).

Allowed mitigations. Not every application tolerates every rung (checkout traffic must not be served
stale, rejected or degraded). `set_allowed()` lists the rungs the operator permits; the controller
skips the others (and logs that it did). Adding servers is always allowed.

Policies (for comparison experiments):
    ladder       the controller above (default)
    scale_only   baseline: no mitigations; the capacity manager scales out on demand
    cache_always baseline: the proxy cache (+ stale-if-error) is always on; otherwise like scale_only
    static       baseline: no mitigations, no reactive scaling; a fixed number of servers kept

Cost accounting (per policy run). Raw components are reported so that any weighting can be applied:
    extra server-seconds (replacements running), degraded responses, responses served from cache,
    rejected (429) requests, seconds in SLO violation. A weighted total in "server-second
    equivalents" uses RESILIENCE_COST_WEIGHTS.
"""

import math
import threading
import time
from collections import deque
from typing import Any, Callable, Dict, List, Optional

import config

RUNGS = ("cache", "rate_limit", "degrade", "scale_out")
RUNG_LABEL = {"cache": "Serve popular items from the proxy cache (+ stale-if-error)",
              "rate_limit": "Rate-limit heavy-hitter clients (HTTP 429)",
              "degrade": "Brownout: backends skip optional work",
              "scale_out": "Add replacement servers"}
POLICIES = ("ladder", "scale_only", "static", "cache_always")


class ResilienceController:
    def __init__(self, metrics, capacity, balancer, cache, limiter, registry, settings=None,
                 clock: Optional[Callable[[], float]] = None):
        self.metrics = metrics
        self.capacity = capacity
        self.balancer = balancer
        self.cache = cache
        self.limiter = limiter
        self.registry = registry
        self.cfg = settings or config
        self._clock = clock or time.time
        self._lock = threading.RLock()
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self.policy = getattr(self.cfg, "RESILIENCE_POLICY", "ladder")
        self.static_servers = int(getattr(self.cfg, "RESILIENCE_STATIC_SERVERS", 4))
        self._saved_min = None
        self.level = 0
        self._last_change = 0.0
        self._ok_since: Optional[float] = None
        self._decisions = deque(maxlen=40)
        self._pending_effects: List[dict] = []
        self._last_tick: Optional[float] = None
        self._last_obs: Dict[str, Any] = {}
        self.allowed = set(RUNGS)
        self.active: set = set()                   # rungs actually in effect (skipped rungs are never switched on)
        self.reset_costs()
        if capacity is not None:
            capacity.scale_out_gate = self.scale_out_allowed

    # ------------------------------------------------------------------ #
    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="ResilienceController")
        self._thread.start()

    def stop(self):
        self._running = False

    def _loop(self):
        while self._running:
            try:
                self.tick()
            except Exception as ex:                       # never kill the loop
                self.metrics.add_log(f"Resilience controller error: {ex}", level="ERROR", event_type="RESILIENCE")
            time.sleep(1.0)

    # ------------------------------------------------------------------ #
    def reset_costs(self):
        with self._lock:
            self.costs = {"seconds": 0.0, "extra_server_seconds": 0.0, "slo_violation_seconds": 0.0}
            self._last_tick = None

    def set_policy(self, policy: str, static_servers: Optional[int] = None):
        if policy not in POLICIES:
            raise ValueError(f"policy must be one of {', '.join(POLICIES)}")
        with self._lock:
            self.policy = policy
            if static_servers is not None:
                self.static_servers = int(static_servers)
            self._set_level(0, "policy changed", None)
            if policy == "static":
                if self._saved_min is None:
                    self._saved_min = self.cfg.CAPACITY_MIN_SERVERS
                self.cfg.CAPACITY_MIN_SERVERS = self.static_servers
            elif self._saved_min is not None:
                self.cfg.CAPACITY_MIN_SERVERS = self._saved_min
                self._saved_min = None
        self.metrics.add_log(f"Resilience policy set to '{policy}'"
                             + (f" ({self.static_servers} servers kept)" if policy == "static" else "") + ".",
                             level="INFO", event_type="RESILIENCE", details={"policy": policy})

    def set_allowed(self, rungs):
        bad = [r for r in rungs if r not in RUNGS]
        if bad:
            raise ValueError(f"unknown mitigation(s): {', '.join(bad)}")
        with self._lock:
            self.allowed = set(rungs) | {"scale_out"}
            self._apply()
        self.metrics.add_log("Resilience: allowed mitigations: " + ", ".join(r for r in RUNGS if r in self.allowed) + ".",
                             level="INFO", event_type="RESILIENCE")

    def on(self, rung: str) -> bool:
        """Is this rung currently in effect?"""
        if self.policy == "cache_always":
            return rung == "cache"
        return self.policy == "ladder" and rung in self.active

    def reset(self):
        with self._lock:
            self.allowed = set(RUNGS)
            self._set_level(0, "reset", None)
            self._decisions.clear()
            self._pending_effects = []
            self._ok_since = None
            self.reset_costs()

    def scale_out_allowed(self, est: Optional[Dict[str, Any]] = None):
        """Gate used by the capacity manager before it starts a NEW replacement server."""
        if self.policy == "ladder" and self.level < len(RUNGS):
            return False, (f"held by the resilience controller: cheaper mitigations first "
                           f"(rung {self.level}/{len(RUNGS)})")
        if self.policy == "static":
            have = (est or {}).get("routable", 0) + (est or {}).get("pending", 0) + (est or {}).get("recovering", 0)
            if have >= self.static_servers:
                return False, f"static policy: fixed at {self.static_servers} servers"
            return True, f"static policy: filling up to {self.static_servers} servers"
        return True, ""

    # ------------------------------------------------------------------ #
    def _observe(self, now) -> Dict[str, Any]:
        w = self.metrics.window_stats(float(self.cfg.RESILIENCE_WINDOW_S), now)
        est = self.capacity.estimate(now) if self.capacity is not None else {}
        cap = est.get("capacity_rps_per_server") or 0.0
        routable = est.get("routable") or 0
        c = w["counts"]
        # The capacity estimate is in full-work requests (degraded responses are normalised when measured).
        # A brownout response costs only BACKEND_DEGRADED_FACTOR of one, so utilisation counts work, not requests.
        f = float(getattr(self.cfg, "BACKEND_DEGRADED_FACTOR", 0.4))
        backend_n = w["backend_rps"] * float(self.cfg.RESILIENCE_WINDOW_S)
        dfrac = min(1.0, c.get("degraded_responses", 0) / backend_n) if backend_n else 0.0
        full = (w["backend_rps"] / (routable * cap)) if routable and cap else (1.0 if w["backend_rps"] else 0.0)
        util = full * ((1.0 - dfrac) + dfrac * f)
        clients = c.get("client_requests", 0)
        obs = {"p95_ms": w["p95_ms"], "error_rate": round(w["error_rate"], 4), "requests": w["n"],
               "utilization": round(util, 3), "utilization_full_work": round(full, 3), "backend_rps": round(w["backend_rps"], 2),
               "client_rps": round(clients / float(self.cfg.RESILIENCE_WINDOW_S), 2), "routable": routable,
               "replacements": est.get("replacements_routable", 0),
               "cache_share": round((c.get("cache_hits", 0) + c.get("cache_stale_served", 0)) / clients, 3) if clients else 0.0,
               "rejected_share": round(c.get("rate_limited", 0) / clients, 3) if clients else 0.0,
               "degraded_share": round(c.get("degraded_responses", 0) / clients, 3) if clients else 0.0}
        win = float(self.cfg.RESILIENCE_WINDOW_S)
        obs["cache_rps"] = round((c.get("cache_hits", 0) + c.get("cache_stale_served", 0)) / win, 2)
        obs["rejected_rps"] = round(c.get("rate_limited", 0) / win, 2)
        obs["capacity_rps_per_server"] = cap
        obs["relief"] = self.relief_estimates(obs, clients)
        slo_p95 = float(self.cfg.RESILIENCE_SLO_P95_MS)
        slo_err = float(self.cfg.RESILIENCE_SLO_ERROR)
        enough = w["n"] >= 10
        obs["slo_violated"] = bool(enough and ((w["p95_ms"] or 0) > slo_p95 or w["error_rate"] > slo_err))
        obs["at_risk"] = bool(util > float(self.cfg.RESILIENCE_UTIL_HIGH))
        obs["comfortable"] = bool(not obs["slo_violated"] and util < float(self.cfg.RESILIENCE_UTIL_LOW)
                                  and (w["p95_ms"] is None or w["p95_ms"] < 0.6 * slo_p95))
        return obs

    def _limit_for(self, obs) -> float:
        n = max(1, len(self.limiter.client_rates()))
        est = self.capacity.estimate() if self.capacity is not None else {}
        safe_total = (est.get("safe_rps_per_server") or 14.0) * max(1, obs.get("routable") or 1)
        return max(1.0, safe_total / n * float(self.cfg.RESILIENCE_HEAVY_FACTOR))

    def relief_estimates(self, obs, client_requests) -> Dict[str, float]:
        """Predicted share of the request load each rung would take off the backends."""
        win = float(self.cfg.RESILIENCE_WINDOW_S)
        return {
            "cache": round(self.cache.predicted_hit_ratio(win, client_requests), 3),
            "rate_limit": round(self.limiter.excess_share(self._limit_for(obs)), 3),
            "degrade": round(1.0 - float(getattr(self.cfg, "BACKEND_DEGRADED_FACTOR", 0.4)), 3),
            "scale_out": 1.0,
        }

    def _next_level(self, obs):
        """Cheapest rung above the current level whose predicted relief is large enough."""
        need = float(getattr(self.cfg, "RESILIENCE_MIN_RELIEF", 0.1))
        relief = obs.get("relief") or {}
        skipped = []
        for i in range(self.level, len(RUNGS)):
            r = RUNGS[i]
            if r not in self.allowed:
                skipped.append(f"{r} (not allowed)")
                continue
            if relief.get(r, 1.0) >= need or r == "scale_out":
                return i + 1, skipped
            skipped.append(f"{r} (predicted relief {relief.get(r, 0):.0%})")
        return len(RUNGS), skipped

    def utilization_without(self, rung: str, obs: Dict[str, Any]) -> float:
        """Counterfactual utilisation if `rung` were switched off now (load it absorbs added back)."""
        routable, cap = obs.get("routable") or 0, obs.get("capacity_rps_per_server") or 0.0
        if not routable or not cap:
            return 1.0
        util = obs["utilization"]
        if rung == "cache":
            return util + obs.get("cache_rps", 0.0) / (routable * cap)
        if rung == "rate_limit":
            return util + obs.get("rejected_rps", 0.0) / (routable * cap)
        if rung == "degrade":
            # without brownout every backend request costs a full request again
            return obs.get("utilization_full_work", util)
        return util          # relaxing scale_out removes no server; the capacity manager scales in safely

    def _account(self, now, obs):
        dt = 0.0 if self._last_tick is None else min(5.0, now - self._last_tick)
        self._last_tick = now
        k = self.costs
        k["seconds"] += dt
        k["extra_server_seconds"] += dt * (obs["replacements"] or 0)
        if obs["slo_violated"]:
            k["slo_violation_seconds"] += dt

    def tick(self):
        now = self._clock()
        with self._lock:
            obs = self._observe(now)
            self._last_obs = obs
            self._account(now, obs)
            # fill in the measured effect of earlier decisions
            dwell = float(self.cfg.RESILIENCE_DWELL_S)
            for d in list(self._pending_effects):
                if now - d["at"] >= dwell:
                    d["after"] = {k: obs[k] for k in ("p95_ms", "error_rate", "utilization", "backend_rps")}
                    self._pending_effects.remove(d)
            if self.policy != "ladder":
                if self.level:
                    self._set_level(0, f"policy '{self.policy}' uses no mitigations", obs)
                return
            if self.on("rate_limit"):
                self._update_rate_limit(obs)
            trouble = obs["slo_violated"] or obs["at_risk"]
            self._ok_since = (self._ok_since or now) if obs["comfortable"] else None
            since_change = now - self._last_change
            if trouble and self.level < len(RUNGS) and since_change >= dwell:
                why = ("SLO violated: p95 {:.0f} ms, errors {:.1%}".format(obs["p95_ms"] or 0, obs["error_rate"])
                       if obs["slo_violated"] else
                       f"utilisation {obs['utilization']:.0%} above {float(self.cfg.RESILIENCE_UTIL_HIGH):.0%}")
                target, skipped = self._next_level(obs)
                rung = RUNGS[target - 1]
                why += f"; predicted relief of {rung}: {obs['relief'].get(rung, 1.0):.0%}"
                if skipped:
                    why += "; skipped " + ", ".join(skipped)
                self._set_level(target, why, obs)
            elif (self.level > 0 and self._ok_since is not None and now - self._ok_since >= float(self.cfg.RESILIENCE_RELAX_S)
                  and since_change >= dwell):
                rung = RUNGS[self.level - 1]
                u_without = self.utilization_without(rung, obs)
                obs["util_without_top_rung"] = round(u_without, 3)
                if u_without < float(self.cfg.RESILIENCE_UTIL_LOW):
                    target = self.level - 1
                    while target > 0 and RUNGS[target - 1] not in self.active:      # skipped rungs: nothing to switch off
                        target -= 1
                    self._set_level(target, f"SLO met with margin for {float(self.cfg.RESILIENCE_RELAX_S):g} s; "
                                                    f"utilisation without {rung} ≈ {u_without:.0%}", obs)
                    self._ok_since = now

    # ------------------------------------------------------------------ #
    def _update_rate_limit(self, obs):
        self.limiter.configure(True, self._limit_for(obs))

    def _apply(self):
        act = self.active
        cache = "cache" in act or self.policy == "cache_always"
        self.cache.configure(fresh=cache, stale_on_error=cache, ttl=float(self.cfg.RESILIENCE_CACHE_TTL_S))
        if "rate_limit" not in act:
            self.limiter.configure(False)
        self.balancer.degrade = "degrade" in act
        if self.capacity is not None:
            self.capacity.notify()

    def _set_level(self, new: int, reason: str, obs: Optional[Dict[str, Any]]):
        new = max(0, min(len(RUNGS), new))
        old = self.level
        if new == old and reason not in ("reset", "policy changed"):
            return
        if new > old:
            rung = RUNGS[new - 1]
            self.active.add(rung)                    # only the chosen rung, not the skipped ones below it
        else:
            removed = [r for r in RUNGS if r in self.active and RUNGS.index(r) >= new]
            rung = removed[-1] if removed else (RUNGS[old - 1] if old else None)
            self.active -= set(removed)
        self.level = new
        self._last_change = self._clock()
        self._apply()
        if "rate_limit" in self.active and obs is not None:
            self._update_rate_limit(obs)
        if new == old or rung is None:
            return
        up = new > old
        before = {k: obs[k] for k in ("p95_ms", "error_rate", "utilization", "backend_rps")} if obs else None
        d = {"at": self._clock(), "time": time.strftime("%H:%M:%S"), "action": "escalate" if up else "relax",
             "rung": rung, "level": new, "label": RUNG_LABEL[rung], "reason": reason, "before": before, "after": None}
        self._decisions.append(d)
        if obs is not None:
            self._pending_effects.append(d)
        verb = "Escalated to" if up else "Relaxed"
        self.metrics.add_log(f"Resilience: {verb} rung {new}/{len(RUNGS)} — {'+' if up else '−'} {RUNG_LABEL[rung]} "
                             f"({reason}).", level="WARN" if up else "INFO",
                             event_type="RESILIENCE_ESCALATED" if up else "RESILIENCE_RELAXED",
                             details={"rung": rung, "level": new, "reason": reason, "before": before})

    # ------------------------------------------------------------------ #
    def cost_summary(self) -> Dict[str, Any]:
        k = dict(self.costs)
        w = getattr(self.cfg, "RESILIENCE_COST_WEIGHTS", {})
        weighted = (k["extra_server_seconds"] * w.get("server_second", 1.0)
                    + k["slo_violation_seconds"] * w.get("slo_violation_second", 2.0))
        k["weighted_cost"] = round(weighted, 2)
        return k

    def status(self) -> Dict[str, Any]:
        with self._lock:
            rungs = [{"key": r, "label": RUNG_LABEL[r], "active": self.on(r), "allowed": r in self.allowed}
                     for i, r in enumerate(RUNGS)]
            return {
                "policy": self.policy, "policies": list(POLICIES), "level": self.level, "max_level": len(RUNGS),
                "rungs": rungs, "observed": dict(self._last_obs),
                "slo": {"p95_ms": float(self.cfg.RESILIENCE_SLO_P95_MS), "error_rate": float(self.cfg.RESILIENCE_SLO_ERROR),
                        "window_s": float(self.cfg.RESILIENCE_WINDOW_S)},
                "thresholds": {"util_high": float(self.cfg.RESILIENCE_UTIL_HIGH), "util_low": float(self.cfg.RESILIENCE_UTIL_LOW),
                               "dwell_s": float(self.cfg.RESILIENCE_DWELL_S), "relax_s": float(self.cfg.RESILIENCE_RELAX_S)},
                "static_servers": self.static_servers, "allowed": [r for r in RUNGS if r in self.allowed],
                "decisions": [dict(d) for d in list(self._decisions)[::-1]],
                "cache": self.cache.status(), "rate_limit": self.limiter.status(), "degrade": bool(self.balancer.degrade),
                "costs": {k: (round(v, 2) if isinstance(v, float) else v) for k, v in self.costs.items()},
            }
