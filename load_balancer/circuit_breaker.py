"""
Per-backend circuit breaker (passive health checking in the reverse proxy).

The health monitor (active probes every 2 s) and the circuit breaker (real request
outcomes) are two separate signals with separate authority:

  * ServerRegistry.status  (HEALTHY / UNHEALTHY / RECOVERING) is changed ONLY by the
    health monitor's probes.
  * CircuitBreaker.state   (CLOSED / OPEN / HALF_OPEN) is changed ONLY by the outcomes
    of forwarded requests - with one exception: when the health monitor re-admits a
    server after consecutive passing checks (SERVER_READMITTED) the breaker is reset to
    CLOSED, because the server has just been verified.

A backend receives new requests only if it is HEALTHY *and* its breaker allows it.

States:
  CLOSED     requests flow; CIRCUIT_FAILURE_THRESHOLD consecutive failures -> OPEN
  OPEN       no requests for CIRCUIT_OPEN_SECONDS, then -> HALF_OPEN
  HALF_OPEN  at most CIRCUIT_HALF_OPEN_MAX_TRIALS trial requests at a time;
             CIRCUIT_HALF_OPEN_SUCCESSES successes -> CLOSED, any failure -> OPEN

Failures that count: connection refused, connect/read timeouts (real or simulated),
HTTP 5xx from the backend. A 503 marked X-Backend-Overloaded (queue full) is load
shedding, not a fault, and does not count.
"""

import time
import threading
from typing import Callable, Dict, Optional

CLOSED, OPEN, HALF_OPEN = "CLOSED", "OPEN", "HALF_OPEN"


class _Breaker:
    __slots__ = ("state", "failures", "opened_at", "trials", "successes", "last_change", "opens", "last_failure")

    def __init__(self, now):
        self.state = CLOSED
        self.failures = 0
        self.opened_at = None
        self.trials = 0
        self.successes = 0
        self.last_change = now
        self.opens = 0
        self.last_failure = None


class CircuitBreakerRegistry:
    def __init__(self, failure_threshold: int = 3, open_seconds: float = 5.0, half_open_max_trials: int = 1,
                 half_open_successes: int = 2, clock: Optional[Callable[[], float]] = None, on_event=None):
        self.failure_threshold = failure_threshold
        self.open_seconds = open_seconds
        self.half_open_max_trials = half_open_max_trials
        self.half_open_successes = half_open_successes
        self._clock = clock or time.time
        self._lock = threading.Lock()
        self._b: Dict[str, _Breaker] = {}
        self.on_event = on_event          # callback(event_type, server_id, message)

    def configure(self, **kw):
        with self._lock:
            for k, v in kw.items():
                if v is not None and hasattr(self, k):
                    setattr(self, k, v)

    def _get(self, sid) -> _Breaker:
        b = self._b.get(sid)
        if b is None:
            b = self._b[sid] = _Breaker(self._clock())
        return b

    def _emit(self, events):
        if self.on_event:
            for e in events:
                self.on_event(*e)

    def _maybe_half_open(self, sid, b, now, events):
        if b.state == OPEN and now - b.opened_at >= self.open_seconds:
            b.state = HALF_OPEN
            b.trials = 0
            b.successes = 0
            b.last_change = now
            events.append(("CIRCUIT_HALF_OPEN", sid, f"Circuit of {sid} HALF_OPEN: allowing up to "
                                                     f"{self.half_open_max_trials} trial request(s)."))

    def can_route(self, sid: str) -> bool:
        """Non-reserving check used to build the candidate pool."""
        events = []
        with self._lock:
            b = self._get(sid)
            self._maybe_half_open(sid, b, self._clock(), events)
            ok = b.state == CLOSED or (b.state == HALF_OPEN and b.trials < self.half_open_max_trials)
        self._emit(events)
        return ok

    def acquire(self, sid: str) -> bool:
        """Reserve the right to send one request (counts a HALF_OPEN trial)."""
        events = []
        with self._lock:
            b = self._get(sid)
            self._maybe_half_open(sid, b, self._clock(), events)
            if b.state == CLOSED:
                ok = True
            elif b.state == HALF_OPEN and b.trials < self.half_open_max_trials:
                b.trials += 1
                ok = True
            else:
                ok = False
        self._emit(events)
        return ok

    def record_success(self, sid: str):
        events = []
        with self._lock:
            b = self._get(sid)
            b.failures = 0
            if b.state == HALF_OPEN:
                b.trials = max(0, b.trials - 1)
                b.successes += 1
                if b.successes >= self.half_open_successes:
                    b.state = CLOSED
                    b.last_change = self._clock()
                    events.append(("CIRCUIT_CLOSED", sid, f"Circuit of {sid} CLOSED after {b.successes} "
                                                          f"successful trial request(s)."))
        self._emit(events)

    def record_failure(self, sid: str, reason: str = ""):
        events = []
        with self._lock:
            b = self._get(sid)
            now = self._clock()
            b.last_failure = reason
            if b.state == HALF_OPEN:
                b.trials = max(0, b.trials - 1)
                b.state = OPEN
                b.opened_at = now
                b.opens += 1
                b.last_change = now
                events.append(("CIRCUIT_OPENED", sid, f"Circuit of {sid} re-OPENED: trial request failed ({reason})."))
            elif b.state == CLOSED:
                b.failures += 1
                if b.failures >= self.failure_threshold:
                    b.state = OPEN
                    b.opened_at = now
                    b.opens += 1
                    b.last_change = now
                    events.append(("CIRCUIT_OPENED", sid, f"Circuit of {sid} OPENED after {b.failures} consecutive "
                                                          f"failed requests (last: {reason}). No requests for "
                                                          f"{self.open_seconds:g} s."))
        self._emit(events)

    def release(self, sid: str):
        """A reserved HALF_OPEN trial ended without an outcome (e.g. backend overloaded)."""
        with self._lock:
            b = self._get(sid)
            if b.state == HALF_OPEN:
                b.trials = max(0, b.trials - 1)

    def reset(self, sid: Optional[str] = None, reason: str = ""):
        events = []
        with self._lock:
            targets = [sid] if sid else list(self._b)
            for s in targets:
                b = self._get(s)
                if b.state != CLOSED and reason:
                    events.append(("CIRCUIT_CLOSED", s, f"Circuit of {s} CLOSED: {reason}."))
                nb = _Breaker(self._clock())
                nb.opens = b.opens if sid else 0
                self._b[s] = nb
        self._emit(events)

    def forget(self, sid: str):
        with self._lock:
            self._b.pop(sid, None)

    def state(self, sid: str) -> str:
        with self._lock:
            b = self._get(sid)
            events = []
            self._maybe_half_open(sid, b, self._clock(), events)
            st = b.state
        self._emit(events)
        return st

    def snapshot(self) -> Dict[str, dict]:
        events = []
        with self._lock:
            now = self._clock()
            out = {}
            for sid, b in self._b.items():
                self._maybe_half_open(sid, b, now, events)
                out[sid] = {"state": b.state, "consecutive_failures": b.failures, "opens": b.opens,
                            "open_remaining_s": round(max(0.0, self.open_seconds - (now - b.opened_at)), 1)
                            if b.state == OPEN else 0.0,
                            "half_open_successes": b.successes if b.state == HALF_OPEN else 0,
                            "last_failure": b.last_failure}
        self._emit(events)
        return out
