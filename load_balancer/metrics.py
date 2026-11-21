"""
Metrics and Telemetry Engine.

Tracks runtime performance, request distribution, and timing metrics:
1. Total requests, successful requests, failed requests (HTTP 503)
2. Per-server request distribution
3. Response latency (moving average and last value)
4. Health-check latency
5. Failure detection time (time from failure injection to detection by health monitor)
6. Failover time (time from failure detection to next successful rerouted request)
7. Recovery detection time (time from recovery trigger to first successful probe)
8. Re-admission time (time spent in RECOVERING state before re-admission)
9. Event routing and system log buffer

NOTE: All timing metrics are recorded from actual local execution time.time()
timestamps and millisecond deltas. No values are fabricated.
"""

import math
import time
import threading
from collections import deque
from typing import Dict, Any, List, Optional

# Counters of networking events (proxy-observed unless the name says "simulated").
COUNTERS = ("timeouts", "connection_refused", "connect_timeouts", "http_5xx", "simulated_packet_loss",
            "simulated_partition", "backend_overloaded", "shed_proxy_overload", "retries_attempted",
            "retries_succeeded", "retries_denied_budget", "not_retried_non_idempotent", "circuit_rejected",
            "blocked_data_recovery", "no_backend_503", "client_requests", "rate_limited", "cache_hits",
            "cache_stale_served", "degraded_responses")


def percentile(values, p):
    """Nearest-rank percentile of a list (None if empty)."""
    if not values:
        return None
    v = sorted(values)
    k = max(0, min(len(v) - 1, int(math.ceil(p / 100.0 * len(v))) - 1))
    return v[k]

class MetricsCollector:
    """Thread-safe collector for request counters, latency, and timing metrics."""

    # Per-request log lines; everything else is a system event shown on the timeline.
    ROUTING_EVENT_TYPES = ("ROUTING", "ROUTING_FAILED")

    def __init__(self, max_logs: int = 150):
        self._lock = threading.RLock()
        
        # Request counters
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.requests_per_server: Dict[str, int] = {}
        
        # Latency records (in milliseconds)
        self._request_latencies = deque(maxlen=50)
        self._health_latencies = deque(maxlen=50)
        self.last_request_latency_ms = 0.0
        
        # Networking & Fault Tolerance Timing Measurements (in milliseconds)
        # failure_detection_time_ms: time between failure injection and health monitor detection
        self.failure_detection_time_ms = None
        # failover_time_ms: time between failure detection and first successful request on another server
        self.failover_time_ms = None
        # recovery_detection_time_ms: time between recovery trigger and first healthy probe
        self.recovery_detection_time_ms = None
        # readmission_time_ms: time from recovery detection until threshold met and server re-admitted
        self.readmission_time_ms = None

        # Internal marker for failover timing
        self._pending_failover_start = None
        self._failed_server_id = None

        # Live Event Log (ring buffer)
        self._logs = deque(maxlen=max_logs)

        # Per-server response latency (last 20 successful requests per server)
        self._server_latencies: Dict[str, deque] = {}

        # System event timeline: state-change events only (no per-request routing lines),
        # kept in a separate buffer so heavy traffic never pushes failures out of view.
        self._events = deque(maxlen=200)

        # Networking telemetry (timestamped, for windowed rates and percentiles)
        self._arrivals = deque(maxlen=20000)                 # proxy-observed: client request arrivals
        self._e2e = deque(maxlen=5000)                       # proxy-observed: (t, ms, ok) end-to-end
        self._backend_reports: Dict[str, deque] = {}         # backend-reported: (t, service_ms, queue_ms)
        self._health = deque(maxlen=2000)                    # proxy-observed: (t, ms, ok) health probes
        self._counter_events = deque(maxlen=20000)           # (t, name)
        self.counters = {c: 0 for c in COUNTERS}

    def record_request_result(self, server_id: str, success: bool, latency_ms: float, client_ip: str = "127.0.0.1"):
        """
        Record the completion of a client request routed through the load balancer.
        
        Calculation:
        - Latency = (time_after_response - time_before_dispatch) in milliseconds.
        - If success: increment total & success count, add latency to moving average.
        - If failover was pending: calculate failover_time_ms as the duration between
          failure detection and this first successful rerouted request.
        """
        with self._lock:
            self.total_requests += 1
            now_str = time.strftime("%H:%M:%S")

            if success:
                self.successful_requests += 1
                self.requests_per_server[server_id] = self.requests_per_server.get(server_id, 0) + 1
                self._request_latencies.append(latency_ms)
                self.last_request_latency_ms = latency_ms
                self._server_latencies.setdefault(server_id, deque(maxlen=20)).append(latency_ms)

                # Check if this request represents a failover resolution
                if self._pending_failover_start is not None and server_id != self._failed_server_id:
                    self.failover_time_ms = round((time.time() - self._pending_failover_start) * 1000, 2)
                    self._pending_failover_start = None
                    self._failed_server_id = None
                    self.add_log(
                        f"Automatic failover completed: Request #{self.total_requests} redirected to {server_id} in {self.failover_time_ms} ms",
                        level="SUCCESS",
                        event_type="FAILOVER"
                    )

                self.add_log(
                    f"Request #{self.total_requests} → {server_id} → SUCCESS ({round(latency_ms, 2)} ms)",
                    level="INFO",
                    event_type="ROUTING"
                )
            else:
                self.failed_requests += 1
                self.add_log(
                    f"Request #{self.total_requests} → FAILED (No healthy servers available / HTTP 503)",
                    level="ERROR",
                    event_type="ROUTING_FAILED"
                )

    def record_health_latency(self, latency_ms: float, ok: bool = True):
        """Record health check probe round-trip latency."""
        with self._lock:
            self._health_latencies.append(latency_ms)
            self._health.append((time.time(), latency_ms, ok))

    # ------------------------------------------------------------------ #
    # Networking telemetry                                                 #
    # ------------------------------------------------------------------ #
    def record_arrival(self, t: Optional[float] = None):
        with self._lock:
            self._arrivals.append(time.time() if t is None else t)

    def response_series(self, seconds: int = 60, now: Optional[float] = None) -> Dict[str, Any]:
        """What clients received, per second (proxy-observed): HTTP 200 from a backend (full or brownout),
        200 answered by the proxy cache (fresh or stale-if-error), 429 Too Many Requests, and errors (5xx)."""
        now = time.time() if now is None else now
        end = int(now)
        start = end - int(seconds) + 1
        keys = ("backend_full", "backend_light", "cache", "rejected_429", "errors")
        b = {k: [0] * int(seconds) for k in keys}
        with self._lock:
            for t, _ms, ok in self._e2e:
                i = int(t) - start
                if 0 <= i < seconds:
                    b["backend_full" if ok else "errors"][i] += 1
            for t, name in self._counter_events:
                i = int(t) - start
                if not 0 <= i < seconds:
                    continue
                if name in ("cache_hits", "cache_stale_served"):
                    b["cache"][i] += 1
                    b["backend_full"][i] -= 1           # counted as ok in _e2e, but no backend was used
                elif name == "degraded_responses":
                    b["backend_light"][i] += 1
                    b["backend_full"][i] -= 1
                elif name == "rate_limited":
                    b["rejected_429"][i] += 1
        for k in keys:
            b[k] = [max(0, v) for v in b[k]]
        b["t"] = [time.strftime("%H:%M:%S", time.localtime(start + i)) for i in range(int(seconds))]
        return b

    def record_e2e(self, latency_ms: float, ok: bool, t: Optional[float] = None):
        """End-to-end time of a client request inside the proxy (including retries)."""
        with self._lock:
            self._e2e.append((time.time() if t is None else t, latency_ms, ok))

    def record_backend_report(self, server_id: str, service_ms, queue_ms):
        """Processing / queueing time reported by the backend in its response (backend-reported)."""
        if service_ms is None:
            return
        with self._lock:
            self._backend_reports.setdefault(server_id, deque(maxlen=500)).append(
                (time.time(), float(service_ms), float(queue_ms or 0.0)))

    def count(self, name: str, n: int = 1):
        with self._lock:
            self.counters[name] = self.counters.get(name, 0) + n
            now = time.time()
            for _ in range(n):
                self._counter_events.append((now, name))

    def window_stats(self, seconds: float, now: Optional[float] = None) -> Dict[str, Any]:
        """Client-side quality over the last `seconds`: p95 latency and error rate of requests the proxy
        answered (rate-limited requests are counted separately), plus counter increments."""
        now = time.time() if now is None else now
        t0 = now - seconds
        with self._lock:
            e2e = [(ms, ok) for (t, ms, ok) in self._e2e if t >= t0]
            counts: Dict[str, int] = {}
            for t, name in self._counter_events:
                if t >= t0:
                    counts[name] = counts.get(name, 0) + 1
            backend = sum(1 for t in self._arrivals if t >= t0)
        ok_lat = [ms for ms, ok in e2e if ok]
        n = len(e2e)
        return {"n": n, "p95_ms": percentile(ok_lat, 95), "p50_ms": percentile(ok_lat, 50),
                "error_rate": (sum(1 for _, ok in e2e if not ok) / n) if n else 0.0,
                "backend_rps": backend / seconds, "counts": counts}

    def arrivals_between(self, t0: float, t1: Optional[float] = None) -> int:
        t1 = time.time() if t1 is None else t1
        with self._lock:
            return sum(1 for t in self._arrivals if t0 <= t <= t1)

    def backend_service_ms(self, since: float, server_id: Optional[str] = None):
        """Mean backend-reported service time since `since` (None if no samples) and the sample count."""
        with self._lock:
            src = [self._backend_reports.get(server_id, ())] if server_id else list(self._backend_reports.values())
            vals = [s for d in src for (t, s, q) in d if t >= since]
        return (sum(vals) / len(vals) if vals else None), len(vals)

    def telemetry(self, window: float = 10.0) -> Dict[str, Any]:
        """Windowed networking telemetry with the source of every value."""
        now = time.time()
        t0 = now - window
        with self._lock:
            arrivals = sum(1 for t in self._arrivals if t >= t0)
            e2e = [(ms, ok) for (t, ms, ok) in self._e2e if t >= t0]
            ok_lat = [ms for ms, ok in e2e if ok]
            ok_n = sum(1 for _, ok in e2e if ok)
            fail_n = len(e2e) - ok_n
            health = [(ms, ok) for (t, ms, ok) in self._health if t >= t0]
            backend = {}
            for sid, d in self._backend_reports.items():
                rows = [(s, q) for (t, s, q) in d if t >= t0]
                if rows:
                    backend[sid] = {"service_ms_avg": round(sum(r[0] for r in rows) / len(rows), 2),
                                    "queue_ms_avg": round(sum(r[1] for r in rows) / len(rows), 2),
                                    "queue_ms_p95": round(percentile([r[1] for r in rows], 95), 2),
                                    "samples": len(rows)}
            window_counts = {c: 0 for c in self.counters}
            for t, name in self._counter_events:
                if t >= t0:
                    window_counts[name] = window_counts.get(name, 0) + 1
            return {
                "window_s": window,
                "proxy_observed": {
                    "incoming_rps": round(arrivals / window, 2),
                    "throughput_rps": round(ok_n / window, 2),
                    "requests_in_window": len(e2e),
                    "success_rate": round(ok_n / len(e2e), 4) if e2e else None,
                    "error_rate": round(fail_n / len(e2e), 4) if e2e else None,
                    "e2e_latency_ms": {
                        "p50": _r(percentile(ok_lat, 50)), "p95": _r(percentile(ok_lat, 95)),
                        "p99": _r(percentile(ok_lat, 99)),
                        "samples": len(ok_lat),
                        "enough_samples": len(ok_lat) >= 20,
                    },
                    "health_probe_ms": {"p50": _r(percentile([h[0] for h in health], 50)),
                                        "p95": _r(percentile([h[0] for h in health], 95)),
                                        "failed": sum(1 for h in health if not h[1]), "probes": len(health)},
                    "timeout_rate": round(window_counts.get("timeouts", 0) / len(e2e), 4) if e2e else None,
                },
                "backend_reported": backend,
                "counters_window": window_counts,
                "counters_total": dict(self.counters),
                "labels": {
                    "proxy_observed": "measured by the reverse proxy",
                    "backend_reported": "measured inside the backend and returned in its response",
                    "simulated": "simulated_packet_loss / simulated_partition come from the proxy's network simulator",
                },
            }

    def record_failure_detection(self, server_id: str, server_name: str, failure_injected_at: float, detected_at: float):
        """
        Record failure detection metric.
        
        Calculation:
        Failure Detection Time = (detected_at - failure_injected_at) * 1000 ms.
        This represents the elapsed duration between when a server fault occurred
        and when the health monitoring subsystem identified it and updated the state.
        """
        with self._lock:
            if failure_injected_at:
                detection_duration = round((detected_at - failure_injected_at) * 1000, 2)
                self.failure_detection_time_ms = max(detection_duration, 0.0)
            else:
                self.failure_detection_time_ms = None

            # Mark start of failover window to measure time until next request is served by a healthy peer
            self._pending_failover_start = detected_at
            self._failed_server_id = server_id

            msg = f"{server_name} ({server_id}) failure DETECTED and REMOVED from active pool."
            if self.failure_detection_time_ms is not None:
                msg += f" Detection time: {self.failure_detection_time_ms} ms."

            self.add_log(msg, level="WARN", event_type="FAILURE_DETECTED")

    def record_recovery_detection(self, server_id: str, server_name: str, recovery_triggered_at: float, detected_at: float):
        """
        Record recovery detection metric.
        
        Calculation:
        Recovery Detection Time = (detected_at - recovery_triggered_at) * 1000 ms.
        Time from when server resumed operational capacity to when the probe verified it.
        """
        with self._lock:
            if recovery_triggered_at:
                rec_duration = round((detected_at - recovery_triggered_at) * 1000, 2)
                self.recovery_detection_time_ms = max(rec_duration, 0.0)
            else:
                self.recovery_detection_time_ms = None

            msg = f"{server_name} recovery DETECTED. Server entered RECOVERING state for verification."
            if self.recovery_detection_time_ms is not None:
                msg += f" Detection time: {self.recovery_detection_time_ms} ms."

            self.add_log(msg, level="INFO", event_type="RECOVERY_DETECTED")

    def record_readmission(self, server_id: str, server_name: str, recovery_detected_at: float, readmitted_at: float):
        """
        Record server re-admission metric.
        
        Calculation:
        Re-admission Time = (readmitted_at - recovery_detected_at) * 1000 ms.
        Time the server spent undergoing consecutive health check confirmations before
        being re-admitted to the active pool.
        """
        with self._lock:
            if recovery_detected_at:
                readmit_duration = round((readmitted_at - recovery_detected_at) * 1000, 2)
                self.readmission_time_ms = max(readmit_duration, 0.0)
            else:
                self.readmission_time_ms = None

            msg = f"{server_name} successfully RE-ADMITTED to active pool."
            if self.readmission_time_ms is not None:
                msg += f" Verification time: {self.readmission_time_ms} ms."

            self.add_log(msg, level="SUCCESS", event_type="READMISSION")

    def add_log(self, message: str, level: str = "INFO", event_type: str = "GENERAL",
                details: Optional[Dict[str, Any]] = None):
        """Append an entry to the live ring-buffer log. `details` holds structured fields
        (e.g. backend_id, previous_healthy, desired, observed_rps, reason) for the dashboard."""
        with self._lock:
            entry = {
                "timestamp": time.strftime("%H:%M:%S"),
                "time_raw": time.time(),
                "level": level,
                "event_type": event_type,
                "message": message
            }
            if details:
                entry["details"] = dict(details)
            self._logs.append(entry)
            if event_type not in self.ROUTING_EVENT_TYPES:
                self._events.append(entry)

    def get_logs(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Retrieve recent logs in reverse chronological or chronological order."""
        with self._lock:
            return list(self._logs)[-limit:]

    def get_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Retrieve recent system events (state changes, failover, recovery, config)."""
        with self._lock:
            return list(self._events)[-limit:]

    def clear_logs(self):
        """Clear the routing log and event timeline (used by simulation reset)."""
        with self._lock:
            self._logs.clear()
            self._events.clear()

    def get_metrics_snapshot(self) -> Dict[str, Any]:
        """Return a snapshot of all telemetry for dashboard rendering."""
        with self._lock:
            avg_req_latency = (
                round(sum(self._request_latencies) / len(self._request_latencies), 2)
                if self._request_latencies else 0.0
            )
            avg_health_latency = (
                round(sum(self._health_latencies) / len(self._health_latencies), 2)
                if self._health_latencies else 0.0
            )

            return {
                "total_requests": self.total_requests,
                "successful_requests": self.successful_requests,
                "failed_requests": self.failed_requests,
                "requests_per_server": dict(self.requests_per_server),
                "avg_response_latency_ms": avg_req_latency,
                "last_response_latency_ms": round(self.last_request_latency_ms, 2),
                "avg_health_latency_ms": avg_health_latency,
                "failure_detection_time_ms": self.failure_detection_time_ms,
                "failover_time_ms": self.failover_time_ms,
                "recovery_detection_time_ms": self.recovery_detection_time_ms,
                "readmission_time_ms": self.readmission_time_ms,
                "avg_latency_per_server": {
                    sid: round(sum(d) / len(d), 2)
                    for sid, d in self._server_latencies.items() if d
                },
            }

    def reset_metrics(self):
        """Reset counters and metrics for clean demonstration runs."""
        with self._lock:
            self.total_requests = 0
            self.successful_requests = 0
            self.failed_requests = 0
            self.requests_per_server.clear()
            self._request_latencies.clear()
            self._health_latencies.clear()
            self._server_latencies.clear()
            self.last_request_latency_ms = 0.0
            self.failure_detection_time_ms = None
            self.failover_time_ms = None
            self.recovery_detection_time_ms = None
            self.readmission_time_ms = None
            self._pending_failover_start = None
            self._failed_server_id = None
            self._arrivals.clear()
            self._e2e.clear()
            self._backend_reports.clear()
            self._health.clear()
            self._counter_events.clear()
            self.counters = {c: 0 for c in COUNTERS}
            self.add_log("Telemetry metrics and request counters reset.", level="INFO", event_type="SYSTEM")


def _r(v):
    return None if v is None else round(v, 2)
