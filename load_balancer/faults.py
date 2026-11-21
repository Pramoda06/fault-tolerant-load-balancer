"""
Fault injection and fault lifecycle tracking.

Six fault types with distinct semantics:

  type          where it happens                       what the proxy observes
  ------------  -------------------------------------  -----------------------------------------
  refused       backend (REAL): data-port listener     TCP connection refused (RST) on every
                closed, process keeps running          request and health probe
  timeout       backend (REAL): connection accepted,   read timeout (requests 3 s, probes 1 s)
                answer only after BACKEND_HANG_SECONDS
  slow          backend (REAL): every answer delayed   correct but slow responses; health still
                by delay_ms                            passes while delay < probe timeout
  http_5xx      backend (REAL): application answers    HTTP 503 from a reachable server
                HTTP 503 to /health and /process       (application failure, not a network one)
  packet_loss   proxy network layer (SIMULATED):       some exchanges get no reply -> timeouts
                probability p per exchange
  partition     proxy network layer (SIMULATED):       connect timeouts; the backend is running
                backend unreachable from the proxy     and reachable on its management port

Backend faults are injected through the backend's control (management) plane, so
they also work while the data port is closed. One backend fault per server at a
time (a new one replaces the old); network faults can be combined with it.

Every fault record carries: type, affected backend, injection time, detection time
and how it was detected (health probe, forwarded request, circuit breaker, slow
response), clear time, and the recovery outcome (when the server became routable again).
"""

import itertools
import time
import threading
from typing import Callable, Dict, List, Optional

import requests

BACKEND_FAULTS = ("refused", "timeout", "slow", "http_5xx")
NETWORK_FAULTS = ("packet_loss", "partition")
FAULT_TYPES = BACKEND_FAULTS + NETWORK_FAULTS

FAULT_INFO = {
    "refused": {"layer": "Transport (TCP)", "simulated": False,
                "summary": "Backend process running, data port closed: connection refused (TCP RST)."},
    "timeout": {"layer": "Transport / application", "simulated": False,
                "summary": "Connection accepted but no answer before the proxy's timeout."},
    "slow": {"layer": "Application", "simulated": False,
             "summary": "Every response delayed; still correct (performance degradation)."},
    "http_5xx": {"layer": "Application (HTTP)", "simulated": False,
                 "summary": "Backend reachable but answers HTTP 503 (application failure)."},
    "packet_loss": {"layer": "Network (simulated)", "simulated": True,
                    "summary": "Simulated: a fraction of request/response exchanges get no reply."},
    "partition": {"layer": "Network (simulated)", "simulated": True,
                  "summary": "Simulated: backend running but unreachable from the proxy."},
}


class FaultError(Exception):
    pass


class FaultManager:
    def __init__(self, registry, metrics, netsim, breakers=None, clock: Optional[Callable[[], float]] = None,
                 http_post=None):
        self.registry = registry
        self.metrics = metrics
        self.netsim = netsim
        self.breakers = breakers
        self._clock = clock or time.time
        self._post = http_post or (lambda url, **kw: requests.post(url, **kw))
        self._lock = threading.RLock()
        self._records: List[dict] = []
        self._ids = itertools.count(1)

    # ------------------------------------------------------------------ #
    def _backend_call(self, server, path: str, body: Optional[dict] = None):
        if server.control_url:
            r = self._post(f"{server.control_url}{path}", json=body or {}, timeout=1.0)
        else:
            # Backend without a control plane (e.g. started with app.run): only the HTTP 503 fault exists.
            legacy = {"/control/clear": "/simulate/recover"}
            if path == "/control/fault" and (body or {}).get("type") == "http_5xx":
                legacy_path = "/simulate/fail"
            elif path in legacy:
                legacy_path = legacy[path]
            else:
                raise FaultError(f"{server.name} has no control plane; only http_5xx can be injected")
            r = self._post(f"{server.url}{legacy_path}", timeout=1.0)
        if r.status_code != 200:
            raise FaultError(f"{server.name} rejected {path}: HTTP {r.status_code}")
        try:
            return r.json()
        except Exception:
            return {}

    def _active(self, server_id: Optional[str] = None, ftype: Optional[str] = None) -> List[dict]:
        return [r for r in self._records if r["cleared_at"] is None
                and (server_id is None or r["server_id"] == server_id) and (ftype is None or r["type"] == ftype)]

    # ------------------------------------------------------------------ #
    def inject(self, server_id: str, ftype: str, params: Optional[dict] = None) -> dict:
        if ftype not in FAULT_TYPES:
            raise FaultError(f"unknown fault type {ftype!r}; supported: {', '.join(FAULT_TYPES)}")
        server = self.registry.get_server(server_id)
        if server is None:
            raise FaultError(f"server {server_id} not found")
        params = dict(params or {})
        now = self._clock()
        with self._lock:
            if ftype in BACKEND_FAULTS:
                if ftype == "slow":
                    import config
                    params.setdefault("delay_ms", config.BACKEND_SLOW_DELAY_MS)
                for old in self._active(server_id):
                    if old["type"] in BACKEND_FAULTS:
                        self._close(old, now, "superseded by a new backend fault")
                reply = self._backend_call(server, "/control/fault", {"type": ftype, "delay_ms": params.get("delay_ms", 0)})
                injected_at = reply.get("failure_injected_at") or now
                server.failure_injected_at = injected_at
            else:
                if ftype == "packet_loss":
                    import config
                    p = float(params.get("loss", config.SIM_DEFAULT_LOSS))
                    if not 0.0 < p <= 1.0:
                        raise FaultError("loss must be between 0 and 1")
                    params["loss"] = p
                    self.netsim.set_loss(server_id, p)
                else:
                    self.netsim.set_partition(server_id, True)
                for old in self._active(server_id, ftype):
                    self._close(old, now, "superseded")
                injected_at = now
                server.failure_injected_at = injected_at
            rec = {"id": next(self._ids), "server_id": server_id, "server_name": server.name, "type": ftype,
                   "params": params, "layer": FAULT_INFO[ftype]["layer"], "simulated": FAULT_INFO[ftype]["simulated"],
                   "injected_at": injected_at, "detected_at": None, "detected_by": None, "cleared_at": None,
                   "recovered_at": None, "outcome": "active"}
            self._records.append(rec)
            self._records = self._records[-100:]
        detail = f" (loss {params['loss']:.0%})" if ftype == "packet_loss" else (
            f" (+{params.get('delay_ms')} ms)" if ftype == "slow" else "")
        self.metrics.add_log(f"Fault injected into {server.name}: {ftype}{detail} — {FAULT_INFO[ftype]['summary']}",
                             level="WARN", event_type="FAULT_INJECTED")
        return dict(rec)

    def _close(self, rec, now, outcome):
        rec["cleared_at"] = now
        rec["outcome"] = outcome

    def clear(self, server_id: str, ftype: Optional[str] = None) -> List[dict]:
        """Clear one fault type (or all faults) of a server. Backend faults are cleared through the
        control plane, which also re-opens a closed data port."""
        server = self.registry.get_server(server_id)
        now = self._clock()
        cleared = []
        with self._lock:
            active = self._active(server_id, ftype)
            backend = [r for r in active if r["type"] in BACKEND_FAULTS]
            if server is not None and (backend or ftype is None or ftype in BACKEND_FAULTS):
                try:
                    self._backend_call(server, "/control/clear")
                    server.recovery_injected_at = now
                except Exception:
                    # Only a backend fault needs the backend; simulated network faults live in the
                    # proxy and can be cleared even when the backend's control plane is unreachable.
                    if backend:
                        raise
            if ftype is None:
                self.netsim.clear(server_id)
            elif ftype == "partition":
                self.netsim.set_partition(server_id, False)
            elif ftype == "packet_loss":
                self.netsim.set_loss(server_id, 0.0)
            for r in active:
                self._close(r, now, "cleared — waiting for health verification")
                cleared.append(dict(r))
        if cleared:
            name = server.name if server else server_id
            self.metrics.add_log(f"Fault cleared on {name}: {', '.join(r['type'] for r in cleared)}. "
                                 f"Server must pass health checks before it rejoins routing.",
                                 level="INFO", event_type="FAULT_CLEARED")
        return cleared

    # ------------------------------------------------------------------ #
    # Detection hooks                                                      #
    # ------------------------------------------------------------------ #
    def _detect(self, server_id: str, how: str, types=None):
        now = self._clock()
        hits = []
        with self._lock:
            for r in self._active(server_id):
                if r["detected_at"] is None and (types is None or r["type"] in types):
                    r["detected_at"] = now
                    r["detected_by"] = how
                    hits.append(dict(r))
        for r in hits:
            ms = (r["detected_at"] - r["injected_at"]) * 1000.0
            self.metrics.add_log(f"Fault on {r['server_name']} ({r['type']}) detected by {how} "
                                 f"{ms:.0f} ms after injection.", level="WARN", event_type="FAULT_DETECTED")

    def on_health_failure(self, server_id: str, kind: Optional[str]):
        self._detect(server_id, f"health probe ({kind or 'failed'})", types=None)

    def on_request_failure(self, server_id: str, kind: Optional[str]):
        self._detect(server_id, f"forwarded request ({kind})", types=[t for t in FAULT_TYPES if t != "slow"])

    def on_circuit_open(self, server_id: str):
        self._detect(server_id, "circuit breaker opened")

    def on_response(self, server_id: str, latency_ms: float):
        import config
        if latency_ms >= config.SIM_SLOW_DETECT_MS:
            self._detect(server_id, f"slow response ({latency_ms:.0f} ms)", types=["slow"])

    def tick(self):
        """Mark cleared faults as recovered once their server is routable again."""
        now = self._clock()
        done = []
        with self._lock:
            for r in self._records:
                if r["cleared_at"] is None or r["recovered_at"] is not None or not r["outcome"].startswith("cleared"):
                    continue
                server = self.registry.get_server(r["server_id"])
                if server is None:
                    r["outcome"] = "server removed before recovery"
                    continue
                routable = server.status == "HEALTHY" and not server.draining and (
                    self.breakers is None or self.breakers.state(server.id) == "CLOSED")
                if routable:
                    r["recovered_at"] = now
                    r["outcome"] = "recovered — health checks passed, back in routing"
                    done.append(dict(r))
        for r in done:
            ms = (r["recovered_at"] - r["cleared_at"]) * 1000.0
            self.metrics.add_log(f"{r['server_name']} recovered from {r['type']}: back in routing "
                                 f"{ms:.0f} ms after the fault was cleared.", level="SUCCESS", event_type="FAULT_RECOVERED")

    # ------------------------------------------------------------------ #
    def active_by_server(self) -> Dict[str, List[dict]]:
        with self._lock:
            out: Dict[str, List[dict]] = {}
            for r in self._active():
                out.setdefault(r["server_id"], []).append(dict(r))
            return out

    def records(self, limit: int = 40) -> List[dict]:
        with self._lock:
            return [dict(r) for r in self._records[-limit:]]

    def reset(self):
        """Clear every fault (backend and network) and forget the history."""
        with self._lock:
            for sid in {r["server_id"] for r in self._active()}:
                server = self.registry.get_server(sid)
                if server is not None:
                    try:
                        self._backend_call(server, "/control/clear")
                    except Exception:
                        pass
            self.netsim.clear()
            self._records = []
