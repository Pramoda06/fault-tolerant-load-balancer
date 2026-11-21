"""
Load-aware capacity manager (STAGE 2 of recovery, and continuous capacity control).

Recovery and scaling are separate stages:

  stage 1  automatic recovery (auto_recovery.py) restores service with ONE minimum-viable
           replacement when no backend is usable.
  stage 2  this manager decides how many servers are needed from demand MEASURED AFTER
           service is available, and reconciles actual capacity with it continuously.

Every evaluation (every CAPACITY_EVAL_INTERVAL s, and immediately after health transitions):

  demand_rps         = client requests that arrived at the proxy in the observation window
                       / window length                                  (proxy-observed)
  service_ms         = mean processing time reported by the backends   (backend-reported,
                       measured); configured BACKEND_SERVICE_TIME_MS if too few samples
  capacity_per_server = workers / service time                           (workers configured)
  safe_rps_per_server = capacity_per_server * CAPACITY_TARGET_UTILIZATION
  desired            = clamp(ceil(demand_rps / safe_rps_per_server), MIN, MAX)
  available          = routable servers (HEALTHY, not draining, circuit not OPEN)
                       + servers RECOVERING (they rejoin after health checks)
                       + replacements being provisioned

  scale out  desired > available for CAPACITY_SCALE_OUT_SUSTAIN s and enough samples
             -> cancel a drain if one is in progress, otherwise start ONE replacement,
                health-check it, re-check the need, and only then register it
  scale in   only REPLACEMENT servers, never originals; only if the remaining routable
             servers would run below CAPACITY_SCALE_IN_UTILIZATION, for
             CAPACITY_SCALE_IN_SUSTAIN s -> drain (no new requests, in-flight requests
             finish), then stop the process and unregister it
  hold       insufficient window/samples, cooldown, sustained-condition timer running,
             another provisioning in progress, or limits reached

After service is restored from a total outage the manager only OBSERVES for a full
observation window before it decides how many replacements are needed.

Timeline events (emitted on transitions only, never once per evaluation), each with
structured details {backend_id, previous_healthy, desired, observed_rps, reason}:
  CAPACITY_EVALUATION_STARTED, CAPACITY_TARGET_UPDATED, SCALE_OUT_STARTED,
  REPLACEMENT_PROVISIONED, REPLACEMENT_HEALTHY, BACKEND_REGISTERED, SCALE_IN_STARTED,
  BACKEND_DRAINING, BACKEND_RETIRED, CAPACITY_STABILIZED
"""

import math
import time
import threading
from collections import deque
from typing import Any, Callable, Dict, List, Optional

import config
from load_balancer.health_monitor import probe_health
from load_balancer.provisioning import (InstanceAllocator, ProvisionGate, is_port_free, launch_backend_process,
                                        stop_process, control_port_for)


class _Cancelled(Exception):
    pass


class ProvisionError(Exception):
    pass


class CapacityManager:
    OWNER = "capacity"

    def __init__(self, registry, metrics, project_root: str, breakers=None, auto_recovery=None,
                 allocator: Optional[InstanceAllocator] = None, gate: Optional[ProvisionGate] = None,
                 enabled: bool = True, host: str = "127.0.0.1", port_range=(5004, 5050),
                 health_timeout: float = 1.0, health_passes: int = 2, startup_timeout: float = 15.0,
                 weight: int = 2, launcher: Optional[Callable] = None, prober: Optional[Callable] = None,
                 port_checker: Optional[Callable] = None, clock: Optional[Callable[[], float]] = None,
                 settings=None, sleep: Callable[[float], None] = time.sleep):
        self.registry = registry
        self.metrics = metrics
        self.project_root = project_root
        self.breakers = breakers
        self.auto_recovery = auto_recovery
        self.allocator = allocator or InstanceAllocator()
        self.gate = gate or ProvisionGate()
        self.enabled = enabled
        self.host = host
        self.port_range = port_range
        self.health_timeout = health_timeout
        self.health_passes = health_passes
        self.startup_timeout = startup_timeout
        self.weight = weight
        self.cfg = settings or config
        self._launch = launcher or (lambda sid, name, port: launch_backend_process(project_root, sid, name, port, host))
        self._probe = prober or (lambda url: probe_health(url, self.health_timeout)[:2])
        self._port_free = port_checker or (lambda port: is_port_free(host, port))
        self._clock = clock or time.time
        self._sleep = sleep

        self._lock = threading.RLock()
        self._wake = threading.Event()
        self._procs: Dict[str, Any] = {}
        self._generation = 0
        self._paused = False
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._prov_thread: Optional[threading.Thread] = None
        self._provisioning: Optional[Dict[str, Any]] = None
        self._observe_from = self._clock()
        self._prev_routable: Optional[int] = None
        self._out_since: Optional[float] = None
        self._in_since: Optional[float] = None
        self._cooldown_until = 0.0
        self._cooldown_kind: Optional[str] = None
        self._failures = 0
        self._backoff_until = 0.0
        self._unhealthy_since: Dict[str, float] = {}
        self._limit_logged = False
        self._history = deque(maxlen=40)
        self._last: Dict[str, Any] = {"phase": "STARTING", "decision": "hold", "reason": "Waiting for the first evaluation."}
        self._retired = deque(maxlen=10)          # recently retired replacements (topology + history)
        self._eval_announced = False
        self._target_announced: Optional[int] = None
        self._target_cand: Optional[int] = None
        self._target_cand_n = 0
        self._scaling_active = False
        self._hold: Optional[Dict[str, Any]] = None   # why a server is kept although desired < routable
        # Optional gate (set by the resilience controller): may a NEW replacement be started now?
        self.scale_out_gate: Optional[Callable[[], Any]] = None

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #
    def start(self):
        if self._running or not self.enabled:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True, name="CapacityManager")
        self._thread.start()

    def _loop(self):
        while self._running:
            try:
                self.evaluate()
            except Exception as ex:
                self.metrics.add_log(f"Capacity manager error: {ex}", level="ERROR", event_type="CAPACITY")
            self._wake.wait(self.cfg.CAPACITY_EVAL_INTERVAL)
            self._wake.clear()

    def notify(self, *_args):
        """Re-evaluate soon (called after health transitions and other state changes)."""
        self._wake.set()

    def pause(self, reason: str = ""):
        with self._lock:
            self._paused = True
            self._last = {"phase": "PAUSED", "decision": "hold", "reason": reason or "Paused."}

    def resume(self):
        with self._lock:
            self._paused = False
            self._observe_from = self._clock()
            self._out_since = self._in_since = None

    def reset(self):
        """Stop and unregister replacements this manager started; clear all decision state.
        Stays paused until resume()."""
        with self._lock:
            self._paused = True
            self._generation += 1
            procs = dict(self._procs)
            self._procs.clear()
            self._provisioning = None
            self._out_since = self._in_since = None
            self._cooldown_until = 0.0
            self._failures = 0
            self._backoff_until = 0.0
            self._unhealthy_since.clear()
            self._history.clear()
            self._limit_logged = False
            self._observe_from = self._clock()
            self._prev_routable = None
            self._retired.clear()
            self._eval_announced = False
            self._target_announced = self._target_cand = None
            self._target_cand_n = 0
            self._scaling_active = False
            self._last = {"phase": "RESET", "decision": "hold", "reason": "Simulation reset."}
            ids = [s.id for s in self.registry.get_all_servers() if s.origin == "capacity"]
        for p in procs.values():
            stop_process(p)
        for sid in set(ids) | set(procs):
            self.registry.unregister_server(sid)
            if self.breakers:
                self.breakers.forget(sid)
        return len(procs)

    def shutdown(self):
        self._running = False
        self._wake.set()
        with self._lock:
            self._generation += 1
            procs = list(self._procs.values())
            self._procs.clear()
        for p in procs:
            stop_process(p)

    # ------------------------------------------------------------------ #
    # Measurement                                                          #
    # ------------------------------------------------------------------ #
    def _routable(self, s) -> bool:
        if s.status != "HEALTHY" or s.draining or getattr(s, "standby", False):
            return False
        return self.breakers is None or self.breakers.state(s.id) != "OPEN"

    def estimate(self, now: Optional[float] = None) -> Dict[str, Any]:
        now = self._clock() if now is None else now
        c = self.cfg
        window = float(c.CAPACITY_OBSERVATION_WINDOW)
        start = max(now - window, self._observe_from)
        span = max(0.0, now - start)
        arrivals = self.metrics.arrivals_between(start, now) if span > 0 else 0
        demand = arrivals / span if span >= 1.0 else 0.0
        svc, n = self.metrics.backend_service_ms(now - window)
        if svc is not None and n >= 5:
            service_ms, service_src = svc, f"measured: mean of {n} backend-reported samples"
        else:
            service_ms, service_src = float(c.BACKEND_SERVICE_TIME_MS), "configured (too few measurements)"
        workers = int(c.BACKEND_WORKERS)
        fixed = float(getattr(c, "CAPACITY_PER_SERVER_RPS", 0) or 0)
        if fixed > 0:
            cap = fixed
            cap_kind, cap_src = "configured", f"configured: CAPACITY_PER_SERVER_RPS = {fixed:g} req/s"
        else:
            cap = workers * 1000.0 / max(service_ms, 0.001)
            if svc is not None and n >= 5:
                cap_kind = "measured"
                cap_src = (f"measured from {n} backend reports: {workers} worker / {service_ms:.1f} ms "
                           f"(the backend work is a simulated service time)")
            else:
                cap_kind = "simulated"
                cap_src = (f"simulated: configured service model {workers} worker / {service_ms:g} ms "
                           f"(too few backend measurements)")
        safe = cap * float(c.CAPACITY_TARGET_UTILIZATION)
        servers = self.registry.get_all_servers()
        routable = [s for s in servers if self._routable(s)]
        recovering = [s for s in servers if s.status == "RECOVERING"]
        pending = 1 if self._provisioning else 0
        enough_window = span >= window - 1e-9
        enough_samples = arrivals >= int(c.CAPACITY_MIN_SAMPLES)
        raw = math.ceil(demand / safe) if demand > 0 else 0
        desired = max(int(c.CAPACITY_MIN_SERVERS), min(int(c.CAPACITY_MAX_SERVERS), raw))
        # short window: a spike that has already ended must not keep driving decisions
        rspan = min(3.0, span)
        recent = (self.metrics.arrivals_between(now - rspan, now) / rspan) if rspan >= 1.0 else demand
        recent_raw = math.ceil(recent / safe) if recent > 0 else 0
        return {
            "demand_rps": round(demand, 2), "demand_recent_rps": round(recent, 2), "recent_window_s": round(rspan, 1),
            "required_recent": recent_raw, "samples": arrivals, "window_s": window,
            "observed_s": round(span, 1), "enough_window": enough_window, "enough_samples": enough_samples,
            "service_ms": round(service_ms, 2), "service_source": service_src, "workers": workers,
            "workers_source": "configured (BACKEND_WORKERS)",
            "capacity_rps_per_server": round(cap, 2), "capacity_kind": cap_kind, "capacity_source": cap_src,
            "target_utilization": float(c.CAPACITY_TARGET_UTILIZATION),
            "safe_rps_per_server": round(safe, 2), "required_raw": raw, "desired": desired,
            "routable": len(routable), "recovering": len(recovering), "pending": pending,
            "standby": sum(1 for s in servers if getattr(s, "standby", False) and s.status == "HEALTHY"),
            "standby_enabled": bool(int(getattr(c, "CAPACITY_ORIGINAL_STANDBY", 1))),
            "available": len(routable) + len(recovering) + pending,
            "originals_routable": sum(1 for s in routable if not s.is_replacement),
            "replacements_routable": sum(1 for s in routable if s.is_replacement),
            "utilization": round(demand / (len(routable) * cap), 3) if routable and cap else None,
            "min_servers": int(c.CAPACITY_MIN_SERVERS), "max_servers": int(c.CAPACITY_MAX_SERVERS),
            "formula": (f"ceil({demand:.1f} req/s / ({cap:.1f} req/s x {float(c.CAPACITY_TARGET_UTILIZATION):.0%})) = {raw}"
                        f" -> clamp [{int(c.CAPACITY_MIN_SERVERS)}, {int(c.CAPACITY_MAX_SERVERS)}] = {desired}"),
        }

    # ------------------------------------------------------------------ #
    # Decision                                                             #
    # ------------------------------------------------------------------ #
    def _decide(self, phase, decision, reason, est, log_level=None):
        self._last = {"phase": phase, "decision": decision, "reason": reason, "time": self._clock()}
        if log_level:
            entry = {"time": time.strftime("%H:%M:%S"), "phase": phase, "decision": decision, "reason": reason,
                     "demand_rps": est["demand_rps"], "desired": est["desired"], "routable": est["routable"],
                     "capacity_rps_per_server": est["capacity_rps_per_server"]}
            self._history.append(entry)
            if log_level in ("ERROR",):
                self.metrics.add_log(f"Capacity: {decision} — {reason}", level=log_level, event_type="CAPACITY_DECISION",
                                     details={"desired": est["desired"], "observed_rps": est["demand_rps"],
                                              "previous_healthy": est["routable"], "reason": reason})

    def _event(self, etype: str, message: str, est: Optional[Dict[str, Any]] = None, level: str = "INFO",
               backend=None, reason: Optional[str] = None, previous_healthy: Optional[int] = None, **extra):
        """Timeline event with structured fields. `backend` is a registry node or a dict with id/name."""
        details: Dict[str, Any] = {}
        if backend is not None:
            details["backend_id"] = backend["id"] if isinstance(backend, dict) else backend.id
            details["backend_name"] = backend["name"] if isinstance(backend, dict) else backend.name
        if est is not None:
            details["previous_healthy"] = est["routable"] if previous_healthy is None else previous_healthy
            details["desired"] = est["desired"]
            details["observed_rps"] = est["demand_rps"]
        elif previous_healthy is not None:
            details["previous_healthy"] = previous_healthy
        if reason:
            details["reason"] = reason
        details.update(extra)
        self.metrics.add_log(message, level=level, event_type=etype, details=details)

    def _remember_retired(self, node, reason: str):
        self._retired.append({"id": node.id, "name": node.name, "port": node.port, "origin": node.origin,
                              "retired_at": self._clock(), "reason": reason})

    def _track_target(self, est):
        """Announce a new desired count once it has held for 3 consecutive evaluations (no flapping events)."""
        d = est["desired"]
        if d == self._target_announced:
            self._target_cand, self._target_cand_n = None, 0
            return
        if d == self._target_cand:
            self._target_cand_n += 1
        else:
            self._target_cand, self._target_cand_n = d, 1
        if self._target_announced is None or self._target_cand_n >= 3:
            prev = self._target_announced
            self._target_announced = d
            self._target_cand, self._target_cand_n = None, 0
            self._event("CAPACITY_TARGET_UPDATED",
                        f"Capacity target {'set to' if prev is None else f'changed {prev} ->'} {d} backend(s): "
                        f"{est['formula']}; {est['routable']} routable now.", est,
                        reason=f"observed {est['demand_rps']:.1f} req/s over {est['window_s']:g} s",
                        previous_target=prev)

    def evaluate(self):
        if not self.enabled:
            return
        to_stop: List[Any] = []
        try:
            with self._lock:
                if self._paused:
                    return
                now = self._clock()
                c = self.cfg
                self._reap_exited()
                self._progress_drains(now, to_stop)
                servers = self.registry.get_all_servers()
                routable_n = sum(1 for s in servers if self._routable(s))

                # Safety net: nothing routable but an original is on standby -> activate it now.
                if routable_n == 0:
                    spare = self._standby_to_activate()
                    if spare is not None and self.registry.activate_standby(spare.id):
                        est = self.estimate(now)
                        self._event("BACKEND_ACTIVATED", f"{spare.name} returned from standby immediately: no other "
                                    f"backend was routable.", est, level="WARN", backend=spare, previous_healthy=0,
                                    reason="no routable backend left")
                        servers = self.registry.get_all_servers()
                        routable_n = sum(1 for s in servers if self._routable(s))

                # Stage 1 is automatic recovery: with no usable backend this manager only waits.
                if routable_n == 0:
                    for s in servers:
                        if s.draining and s.status == "HEALTHY" and self.registry.cancel_drain(s.id):
                            self.metrics.add_log(f"Drain of {s.name} cancelled: it is the only usable backend.",
                                                 level="WARN", event_type="CAPACITY_DECISION")
                    self._prev_routable = 0
                    self._out_since = self._in_since = None
                    self._eval_announced = False
                    # Failed replacements are retired during an outage too; otherwise they would keep
                    # stage-1 recovery at its replacement limit and service could never be restored.
                    self._retire_failed_replacements(now, servers, to_stop)
                    est = self.estimate(now)
                    self._decide("WAITING_FOR_SERVICE", "hold", "No usable backend: stage 1 (automatic recovery) "
                                 "restores service first; capacity is decided after that.", est)
                    return
                if self._prev_routable == 0:
                    self._observe_from = now
                    self._out_since = self._in_since = None
                    est = self.estimate(now)
                    msg = (f"Service restored with {routable_n} backend(s); measuring demand for "
                           f"{c.CAPACITY_OBSERVATION_WINDOW:g} s before deciding how many servers are needed.")
                    self._decide("OBSERVING", "observe", msg, est, log_level="INFO")
                    self._event("CAPACITY_EVALUATION_STARTED", msg, est, reason="service restored after an outage")
                    self._eval_announced = True
                    self._target_announced = None
                elif not self._eval_announced:
                    est = self.estimate(now)
                    self._event("CAPACITY_EVALUATION_STARTED", f"Capacity evaluation started: measuring demand at the "
                                f"proxy for {c.CAPACITY_OBSERVATION_WINDOW:g} s with {routable_n} routable backend(s).",
                                est, reason="capacity manager started" if self._prev_routable is None else "resumed")
                    self._eval_announced = True
                self._prev_routable = routable_n

                self._retire_failed_replacements(now, servers, to_stop)
                est = self.estimate(now)

                if self._provisioning is not None:
                    self._decide("PROVISIONING", "hold", f"Starting {self._provisioning['name']} "
                                 f"({self._provisioning['stage']}).", est)
                    return
                if not est["enough_window"]:
                    self._out_since = self._in_since = None
                    self._decide("OBSERVING", "observe", f"Observing demand: {est['observed_s']:.0f}/"
                                 f"{est['window_s']:.0f} s measured ({est['samples']} requests).", est)
                    return

                self._track_target(est)

                # Both the observation window and the last 3 s must agree (hysteresis against spikes).
                # (the minimum server count applies to both windows, otherwise MIN_SERVERS alone never scales out)
                recent_need = max(est["required_recent"], int(c.CAPACITY_MIN_SERVERS))
                need_more = (est["desired"] > est["available"] and recent_need > est["available"]
                             and est["enough_samples"])
                rest = est["routable"] - 1
                cap = est["capacity_rps_per_server"]
                peak = max(est["demand_rps"], est["demand_recent_rps"])
                can_shrink = (self._scale_in_victim() is not None and rest >= max(est["desired"], int(c.CAPACITY_MIN_SERVERS))
                              and (peak / (rest * cap) if rest > 0 and cap else 1.0)
                              < float(c.CAPACITY_SCALE_IN_UTILIZATION))
                self._out_since = (self._out_since or now) if need_more else None
                self._in_since = (self._in_since or now) if (can_shrink and not need_more) else None

                if now < self._cooldown_until:
                    self._decide("COOLDOWN", "hold", f"Cooldown after {self._cooldown_kind}; "
                                 f"{self._cooldown_until - now:.0f} s left. {est['formula']}.", est)
                    return

                if need_more:
                    waited = now - self._out_since
                    if waited < float(c.CAPACITY_SCALE_OUT_SUSTAIN):
                        self._decide("SCALE_OUT_PENDING", "hold", f"Demand {est['demand_rps']:.1f} req/s needs "
                                     f"{est['desired']} server(s), {est['available']} available; must persist "
                                     f"{waited:.0f}/{c.CAPACITY_SCALE_OUT_SUSTAIN:g} s.", est)
                        return
                    self._scale_out(now, est)
                    return
                if est["desired"] > est["available"] and not est["enough_samples"]:
                    self._decide("STABLE", "hold", f"Only {est['samples']} requests in the window "
                                 f"(< {c.CAPACITY_MIN_SAMPLES}): not enough samples to scale out.", est)
                    return
                if can_shrink:
                    waited = now - self._in_since
                    if waited < float(c.CAPACITY_SCALE_IN_SUSTAIN):
                        self._decide("SCALE_IN_PENDING", "hold", f"Demand {est['demand_rps']:.1f} req/s; one replacement "
                                     f"could be removed. Must persist {waited:.0f}/{c.CAPACITY_SCALE_IN_SUSTAIN:g} s.", est)
                        return
                    self._scale_in(now, est)
                    return
                if est["required_raw"] > int(c.CAPACITY_MAX_SERVERS) and est["available"] >= int(c.CAPACITY_MAX_SERVERS):
                    msg = (f"Maximum capacity reached: {est['available']} servers (max {c.CAPACITY_MAX_SERVERS}) while "
                           f"demand {est['demand_rps']:.1f} req/s needs {est['required_raw']}. Excess requests queue "
                           f"in the backends or are shed (HTTP 503).")
                    self._decide("AT_MAXIMUM", "hold", msg, est, log_level=None if self._limit_logged else "ERROR")
                    self._limit_logged = True
                    return
                self._limit_logged = False
                victim = self._scale_in_victim()
                if est["desired"] < est["routable"] and victim is not None and rest >= 1 and cap:
                    after = peak / (rest * cap)
                    self._hold = {"server": victim.name, "routable": est["routable"], "utilization_after": round(after, 3),
                                  "threshold": float(c.CAPACITY_SCALE_IN_UTILIZATION)}
                    self._decide("STABLE", "hold", f"{est['formula']}, but {est['routable']} kept: with {rest} server(s) "
                                 f"the load would be {after:.0%}, and scale-in only happens below "
                                 f"{c.CAPACITY_SCALE_IN_UTILIZATION:.0%} (headroom, avoids flapping).", est)
                else:
                    self._hold = None
                    self._decide("STABLE", "hold", f"{est['formula']}; {est['routable']} routable "
                                 f"({est['originals_routable']} original, {est['replacements_routable']} replacement).", est)
                if self._scaling_active and not any(s.draining for s in servers):
                    self._scaling_active = False
                    self._event("CAPACITY_STABILIZED", f"Capacity stabilized: {est['routable']} routable backend(s) "
                                f"({est['originals_routable']} original, {est['replacements_routable']} replacement) for "
                                f"{est['demand_rps']:.1f} req/s; desired {est['desired']}.", est, level="SUCCESS",
                                reason=est["formula"])
        finally:
            for p in to_stop:
                stop_process(p)

    def _scale_out(self, now, est):
        c = self.cfg
        # 1) A healthy replacement that is still draining is the cheapest capacity.
        for s in self.registry.get_all_servers():
            if s.draining and s.status == "HEALTHY" and self.registry.cancel_drain(s.id):
                self._out_since = None
                self._set_cooldown(now, "scale-out")
                self._decide("SCALE_OUT", "cancel_drain", f"Demand {est['demand_rps']:.1f} req/s needs {est['desired']} "
                             f"servers: kept {s.name} instead of draining it.", est, log_level="WARN")
                self._scaling_active = True
                self._event("SCALE_OUT_STARTED", f"Scale-out: demand {est['demand_rps']:.1f} req/s needs "
                            f"{est['desired']} backends; kept {s.name} instead of draining it.", est, level="WARN",
                            backend=s, reason="drain cancelled: capacity needed again")
                return
        # 2) An original server on standby is already running and health-checked: activate it.
        spare = self._standby_to_activate()
        if spare is not None and self.registry.activate_standby(spare.id):
            self._out_since = None
            self._set_cooldown(now, "scale-out")
            self._scaling_active = True
            self._decide("SCALE_OUT", "activate_standby", f"Demand {est['demand_rps']:.1f} req/s needs {est['desired']} "
                         f"servers: {spare.name} returned from standby.", est, log_level="WARN")
            self._event("SCALE_OUT_STARTED", f"Scale-out: {est['formula']}; {est['available']} available -> "
                        f"activating {spare.name} from standby (no new process needed).", est, level="WARN",
                        backend=spare, reason=f"desired {est['desired']} > available {est['available']}")
            self._event("BACKEND_ACTIVATED", f"{spare.name} returned from standby to the routing pool.", est,
                        level="SUCCESS", backend=spare, reason="scale-out", routable_after=est["routable"] + 1)
            return
        if self.scale_out_gate is not None:
            allowed, why = self.scale_out_gate(est)
            if not allowed:
                self._decide("HELD_BY_POLICY", "hold", f"Demand {est['demand_rps']:.1f} req/s needs {est['desired']} "
                             f"servers, {est['available']} available; scale-out {why}.", est)
                return
        total = est["routable"] + est["pending"] + est["recovering"]
        if total >= int(c.CAPACITY_MAX_SERVERS):
            if not self._limit_logged:
                self._limit_logged = True
                self._decide("AT_MAXIMUM", "hold", f"Maximum capacity reached: {total} servers (max "
                             f"{c.CAPACITY_MAX_SERVERS}) while demand {est['demand_rps']:.1f} req/s needs "
                             f"{est['required_raw']}. Requests may queue or be shed.", est, log_level="ERROR")
            else:
                self._decide("AT_MAXIMUM", "hold", f"Maximum of {c.CAPACITY_MAX_SERVERS} servers reached; demand still "
                             f"needs {est['required_raw']}.", est)
            return
        if now < self._backoff_until:
            self._decide("BACKOFF", "hold", f"{self._failures} provisioning failure(s); retrying in "
                         f"{self._backoff_until - now:.0f} s.", est)
            return
        if (self.auto_recovery is not None and self.auto_recovery.busy()) or not self.gate.try_acquire(self.OWNER):
            self._decide("SCALE_OUT_PENDING", "hold", "Another backend is being provisioned; waiting.", est)
            return
        sid, name = self.allocator.next_identity(self.registry)
        self._provisioning = {"id": sid, "name": name, "port": None, "pid": None, "stage": "allocating",
                              "started_at": now, "reason": est["formula"]}
        self._out_since = None
        self._decide("PROVISIONING", "scale_out", f"Demand {est['demand_rps']:.1f} req/s: {est['formula']}; "
                     f"{est['available']} available -> starting {name}.", est, log_level="WARN")
        self._scaling_active = True
        self._event("SCALE_OUT_STARTED", f"Scale-out started: {est['formula']}; {est['available']} available "
                    f"({est['routable']} routable) -> provisioning {name}.", est, level="WARN",
                    backend={"id": sid, "name": name},
                    reason=f"desired {est['desired']} > available {est['available']} for "
                           f"{c.CAPACITY_SCALE_OUT_SUSTAIN:g} s")
        gen = self._generation
        self._prov_thread = threading.Thread(target=self._provision, args=(gen, sid, name), daemon=True,
                                             name="CapacityProvisioner")
        self._prov_thread.start()

    def _scale_in_victim(self):
        """The server scale-in would take out next. Replacements first (newest capacity replacement,
        fewest connections): they are stopped. Then, if CAPACITY_ORIGINAL_STANDBY, an original
        (lowest weight first): it is put on standby, never stopped."""
        servers = self.registry.get_all_servers()
        cands = [s for s in servers if s.is_replacement and not s.draining and self._routable(s)]
        if cands:
            cands.sort(key=lambda s: (s.origin != "capacity", s.active_connections, -(s.provisioned_at or 0)))
            return cands[0]
        if not int(getattr(self.cfg, "CAPACITY_ORIGINAL_STANDBY", 1)):
            return None
        origs = [s for s in servers if not s.is_replacement and not s.draining and self._routable(s)]
        if len(origs) <= 1:
            return None
        origs.sort(key=lambda s: (s.weight, s.active_connections, -int("".join(ch for ch in s.id if ch.isdigit()) or 0)))
        return origs[0]

    def _standby_to_activate(self):
        """Healthy standby original with the highest weight (most capacity first)."""
        spare = [s for s in self.registry.get_all_servers()
                 if getattr(s, "standby", False) and s.status == "HEALTHY" and not s.draining]
        if not spare:
            return None
        spare.sort(key=lambda s: (-s.weight, s.id))
        return spare[0]

    def activate_all_standby(self) -> int:
        """Return every standby server to routing (experiments, disaster scenarios)."""
        n = 0
        for s in self.registry.get_all_servers():
            if getattr(s, "standby", False) and self.registry.activate_standby(s.id):
                n += 1
        return n

    def _scale_in(self, now, est):
        victim = self._scale_in_victim()
        if victim is None:
            return
        original = not victim.is_replacement
        if not self.registry.start_drain(victim.id, now, allow_original=original):
            return
        self._in_since = None
        self._set_cooldown(now, "scale-in")
        rest = est["routable"] - 1
        util = est["demand_rps"] / (rest * est["capacity_rps_per_server"])
        self._scaling_active = True
        self._event("SCALE_IN_STARTED", f"Scale-in started: demand {est['demand_rps']:.1f} req/s; {rest} server(s) "
                    f"would run at {util:.0%} (< {self.cfg.CAPACITY_SCALE_IN_UTILIZATION:.0%}) -> "
                    f"{'moving ' + victim.name + ' (original) to standby' if original else 'retiring ' + victim.name}.",
                    est, backend=victim,
                    reason=f"utilisation with one server fewer {util:.0%} below "
                           f"{self.cfg.CAPACITY_SCALE_IN_UTILIZATION:.0%} for {self.cfg.CAPACITY_SCALE_IN_SUSTAIN:g} s")
        self._event("BACKEND_DRAINING", f"{victim.name} ({'original server' if original else victim.origin + ' replacement'}) "
                    f"is draining: removed from routing, {victim.active_connections} request(s) in flight.", est, backend=victim,
                    reason="scale-in", in_flight=victim.active_connections)
        self._decide("DRAINING", "scale_in", f"Demand {est['demand_rps']:.1f} req/s: {rest} server(s) would run at "
                     f"{est['demand_rps'] / (rest * est['capacity_rps_per_server']):.0%} (< "
                     f"{self.cfg.CAPACITY_SCALE_IN_UTILIZATION:.0%}). Draining {victim.name} "
                     f"({'original -> standby' if original else victim.origin + ' replacement'}): no new requests, "
                     f"{victim.active_connections} in flight.", est, log_level="INFO")

    def _set_cooldown(self, now, kind):
        length = self.cfg.CAPACITY_SCALE_IN_COOLDOWN if kind == "scale-in" else self.cfg.CAPACITY_SCALE_OUT_COOLDOWN
        self._cooldown_until = now + float(length)
        self._cooldown_kind = kind

    # ------------------------------------------------------------------ #
    # Retiring replacements                                               #
    # ------------------------------------------------------------------ #
    def _stop_owner(self, sid: str, to_stop: List[Any]):
        proc = self._procs.pop(sid, None)
        if proc is not None:
            to_stop.append(proc)
        elif self.auto_recovery is not None and self.auto_recovery.owns(sid):
            threading.Thread(target=self.auto_recovery.release_instance, args=(sid,), daemon=True).start()
        if self.breakers:
            self.breakers.forget(sid)

    def _progress_drains(self, now, to_stop):
        for s in self.registry.get_all_servers():
            if not s.draining:
                continue
            if not s.is_replacement:
                started = s.drain_started_at or now
                done, node, forced = self.registry.standby_if_drained(s.id, now, float(self.cfg.CAPACITY_DRAIN_TIMEOUT))
                if done:
                    routable_after = sum(1 for x in self.registry.get_all_servers() if self._routable(x))
                    self._event("BACKEND_STANDBY", f"{node.name} drained in {(now - started) * 1000:.0f} ms and is on "
                                f"standby: process still running and health-checked, no requests until load rises.",
                                level="SUCCESS", backend=node, previous_healthy=routable_after, reason="scale-in",
                                drain_ms=round((now - started) * 1000), forced=bool(forced))
                    self._history.append({"time": time.strftime("%H:%M:%S"), "phase": "SCALE_IN", "decision": "standby",
                                          "reason": f"{node.name} (original) on standby", "demand_rps": None,
                                          "desired": None, "routable": None, "capacity_rps_per_server": None})
                continue
            removed, node, forced = self.registry.remove_if_drained(s.id, now, float(self.cfg.CAPACITY_DRAIN_TIMEOUT))
            if not removed:
                continue
            self._stop_owner(s.id, to_stop)
            took = now - (node.drain_started_at or now)
            routable_after = sum(1 for x in self.registry.get_all_servers() if self._routable(x))
            self._remember_retired(node, "scale-in (drained)")
            self._event("BACKEND_RETIRED", f"{node.name} drained in {took * 1000:.0f} ms"
                        f"{' (drain timeout: requests still in flight)' if forced else ' (0 requests in flight)'}"
                        f", process stopped and removed from the reverse proxy.", level="SUCCESS", backend=node,
                        previous_healthy=routable_after, reason="scale-in", drain_ms=round(took * 1000),
                        forced=bool(forced))
            self._history.append({"time": time.strftime("%H:%M:%S"), "phase": "SCALE_IN", "decision": "removed",
                                  "reason": f"{node.name} drained and stopped", "demand_rps": None,
                                  "desired": None, "routable": None, "capacity_rps_per_server": None})

    def _reap_exited(self):
        for sid, proc in list(self._procs.items()):
            if proc.poll() is None:
                continue
            self._procs.pop(sid, None)
            node = self.registry.get_server(sid)
            if node is not None and self.registry.unregister_server(sid):
                self._remember_retired(node, "process exited unexpectedly")
                self._event("BACKEND_RETIRED", f"{node.name} process exited unexpectedly; removed from the reverse proxy.",
                            level="ERROR", backend=node, reason="process exited")
                if self.breakers:
                    self.breakers.forget(sid)

    def _retire_failed_replacements(self, now, servers, to_stop):
        grace = float(self.cfg.CAPACITY_FAILED_REPLACEMENT_GRACE)
        for s in servers:
            if not s.is_replacement or s.draining or s.status != "UNHEALTHY":
                self._unhealthy_since.pop(s.id, None)
                continue
            since = self._unhealthy_since.setdefault(s.id, now)
            if now - since >= grace and self.registry.unregister_server(s.id):
                self._unhealthy_since.pop(s.id, None)
                self._stop_owner(s.id, to_stop)
                self._remember_retired(s, "failed health checks")
                self._event("BACKEND_RETIRED", f"{s.name} (replacement) failed its health checks for {grace:g} s and was "
                            f"retired; capacity will be re-evaluated.", level="WARN", backend=s,
                            reason=f"UNHEALTHY for {grace:g} s")

    def retire_all_replacements(self) -> int:
        """Stop and unregister every replacement immediately (used between experiment runs)."""
        to_stop = []
        n = 0
        with self._lock:
            for s in self.registry.get_all_servers():
                if s.is_replacement and self.registry.unregister_server(s.id):
                    self._stop_owner(s.id, to_stop)
                    n += 1
        for p in to_stop:
            stop_process(p)
        return n

    # ------------------------------------------------------------------ #
    # Provisioning (background thread)                                    #
    # ------------------------------------------------------------------ #
    def _stage(self, gen, stage, **fields):
        with self._lock:
            if gen != self._generation or self._provisioning is None:
                raise _Cancelled()
            self._provisioning["stage"] = stage
            self._provisioning.update(fields)

    def _provision(self, gen: int, sid: str, name: str):
        proc, port = None, None
        try:
            port = self.allocator.reserve_port(self.registry, self.port_range, self._port_free)
            if port is None:
                raise ProvisionError(f"no free port pair in range {self.port_range[0]}-{self.port_range[1]}")
            url = f"http://{self.host}:{port}"
            self._stage(gen, "starting", port=port, url=url)
            proc = self._launch(sid, name, port)
            with self._lock:
                if gen != self._generation:
                    raise _Cancelled()
                self._procs[sid] = proc
            self._stage(gen, "health_check", pid=getattr(proc, "pid", None))
            self._event("REPLACEMENT_PROVISIONED", f"{name} process started on port {port} (PID "
                        f"{getattr(proc, 'pid', '?')}) for capacity; waiting for {self.health_passes} consecutive "
                        f"passing health checks. Not in routing yet.", backend={"id": sid, "name": name},
                        reason="scale-out", port=port, pid=getattr(proc, "pid", None))
            deadline = time.time() + self.startup_timeout
            passes, latency = 0, 0.0
            while passes < self.health_passes:
                with self._lock:
                    if gen != self._generation:
                        raise _Cancelled()
                code = proc.poll()
                if code is not None:
                    raise ProvisionError(f"{name} exited with code {code} before becoming ready")
                if time.time() > deadline:
                    raise ProvisionError(f"{name} did not pass its readiness checks within {self.startup_timeout:.0f} s")
                ok, latency = self._probe(f"{url}/health")
                passes = passes + 1 if ok else 0
                if passes < self.health_passes:
                    self._sleep(0.4)

            self._event("REPLACEMENT_HEALTHY", f"{name} passed {self.health_passes}/{self.health_passes} consecutive "
                        f"health checks on port {port}.", backend={"id": sid, "name": name}, reason="readiness verified",
                        port=port)
            with self._lock:
                if gen != self._generation:
                    raise _Cancelled()
                # Reconcile again before registering: an original may have recovered meanwhile.
                self._provisioning["stage"] = "reconciling"
                est = self.estimate(self._clock())
                have = est["routable"] + est["recovering"]
                if have >= est["desired"] and est["enough_samples"]:
                    self._procs.pop(sid, None)
                    raise _NoLongerNeeded(f"capacity already satisfied ({have} available, {est['desired']} desired) "
                                          f"— e.g. an original server recovered")
                prev_routable = est["routable"]
                node = self.registry.register_server(sid, name, self.host, port, weight=self.weight, url=url)
                node.is_replacement = True
                node.origin = "capacity"
                node.pid = getattr(proc, "pid", None)
                node.provisioned_at = time.time()
                node.control_url = f"http://{self.host}:{control_port_for(port)}"
                node.last_health_check = node.last_successful_check = time.time()
                node.health_latency_ms = latency
                self._provisioning = None
                self._failures = 0
                self._set_cooldown(self._clock(), "scale-out")
                routable = len(self.registry.get_active_pool())
            self._event("BACKEND_REGISTERED", f"{name} passed {self.health_passes} health checks and was registered "
                        f"with the reverse proxy ({url}); {routable} routable backends.", est, level="SUCCESS",
                        backend={"id": sid, "name": name}, previous_healthy=prev_routable,
                        reason="scale-out", routable_after=routable, port=port)
            self._history.append({"time": time.strftime("%H:%M:%S"), "phase": "SCALE_OUT", "decision": "registered",
                                  "reason": f"{name} ready on port {port}", "demand_rps": None, "desired": None,
                                  "routable": routable, "capacity_rps_per_server": None})
        except _NoLongerNeeded as ex:
            stop_process(proc)
            with self._lock:
                self._provisioning = None
            self.metrics.add_log(f"Provisioning of {name} cancelled before registration: {ex}. Process stopped.",
                                 level="INFO", event_type="CAPACITY_DECISION")
        except _Cancelled:
            with self._lock:
                self._procs.pop(sid, None)
            stop_process(proc)
        except Exception as ex:
            with self._lock:
                self._procs.pop(sid, None)
                current = gen == self._generation
                if current:
                    self._provisioning = None
                    self._failures += 1
                    if self._failures >= int(self.cfg.CAPACITY_MAX_PROVISION_FAILURES):
                        self._backoff_until = self._clock() + float(self.cfg.CAPACITY_PROVISION_BACKOFF)
                    self._set_cooldown(self._clock(), "failed provisioning")
            stop_process(proc)
            if current:
                self.metrics.add_log(f"Provisioning of {name} failed: {ex}. Not registered.", level="ERROR",
                                     event_type="CAPACITY_DECISION")
        finally:
            self.allocator.release(sid, port)
            self.gate.release(self.OWNER)
            self.notify()

    # ------------------------------------------------------------------ #
    def status(self) -> Dict[str, Any]:
        with self._lock:
            now = self._clock()
            est = self.estimate(now)
            out = dict(est)
            out.update({
                "enabled": self.enabled and not self._paused,
                "paused": self._paused,
                "phase": self._last.get("phase"),
                "decision": self._last.get("decision"),
                "reason": self._last.get("reason"),
                "cooldown_remaining_s": round(max(0.0, self._cooldown_until - now), 1),
                "cooldown_kind": self._cooldown_kind if self._cooldown_until > now else None,
                "scale_out_pending_s": round(now - self._out_since, 1) if self._out_since else 0.0,
                "scale_in_pending_s": round(now - self._in_since, 1) if self._in_since else 0.0,
                "scale_out_sustain_s": float(self.cfg.CAPACITY_SCALE_OUT_SUSTAIN),
                "scale_in_sustain_s": float(self.cfg.CAPACITY_SCALE_IN_SUSTAIN),
                "scale_in_utilization": float(self.cfg.CAPACITY_SCALE_IN_UTILIZATION),
                "provisioning": dict(self._provisioning) if self._provisioning else None,
                "retired": [dict(r, age_s=round(now - r["retired_at"], 1)) for r in self._retired
                            if now - r["retired_at"] <= float(getattr(self.cfg, "TOPOLOGY_RETIRED_DISPLAY_S", 10.0))],
                "provision_failures": self._failures,
                "hold": dict(self._hold) if self._hold and self._last.get("phase") == "STABLE" else None,
                "scale_in_candidate": ({"id": v.id, "name": v.name} if self._in_since and (v := self._scale_in_victim())
                                       else None),
                "scale_in_eta_s": (round(max(0.0, float(self.cfg.CAPACITY_SCALE_IN_SUSTAIN) - (now - self._in_since)), 1)
                                   if self._in_since else None),
                "scale_out_eta_s": (round(max(0.0, float(self.cfg.CAPACITY_SCALE_OUT_SUSTAIN) - (now - self._out_since)), 1)
                                    if self._out_since else None),
                "history": list(self._history),
                "labels": {"demand_rps": "proxy-observed", "service_ms": est["service_source"],
                           "workers": "configured", "capacity_rps_per_server": est["capacity_source"]},
            })
            return out


class _NoLongerNeeded(Exception):
    pass
