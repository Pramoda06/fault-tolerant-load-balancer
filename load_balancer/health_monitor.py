"""
Active Health Monitor for Load Balancer.

Runs as a background thread, periodically probing every backend server via GET /health.
Measures latency, updates server registry state machine, and triggers telemetry events:
- Failure detection & pool exclusion
- Recovery detection & confirmation
- Server re-admission
"""

import time
import threading
import requests
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from load_balancer.server_registry import ServerRegistry
from load_balancer.metrics import MetricsCollector

def probe_health(url: str, timeout: float, server_id: str = None):
    """
    The health check used by the reverse proxy: GET <backend>/health.
    A backend is healthy only if it answers HTTP 200 with {"status": "healthy"}
    within `timeout` seconds.

    Returns (is_healthy, latency_ms, response_json_or_None).
    """
    ok, latency_ms, data, _kind = probe_health_detailed(url, timeout, server_id)
    return ok, latency_ms, data


def probe_health_detailed(url: str, timeout: float, server_id: str = None):
    """Like probe_health, plus the kind of failure (None, http_5xx, timeout, connection_refused,
    partition, packet_loss, ...). Probes pass through the proxy's network simulator."""
    from load_balancer.netsim import NETSIM
    from load_balancer.retry_policy import failure_kind
    data = None
    kind = None
    t0 = time.perf_counter()
    try:
        NETSIM.before_send(server_id, timeout=timeout, kind="health probe")
        resp = requests.get(url, timeout=timeout)
        latency_ms = (time.perf_counter() - t0) * 1000.0
        try:
            data = resp.json()
        except Exception:
            data = None
        is_healthy = resp.status_code == 200 and isinstance(data, dict) and data.get("status") == "healthy"
        if not is_healthy:
            kind = failure_kind(status_code=resp.status_code) if resp.status_code != 200 else "bad_response"
    except (requests.RequestException, Exception) as ex:
        latency_ms = (time.perf_counter() - t0) * 1000.0
        is_healthy = False
        kind = failure_kind(exc=ex)
    return is_healthy, latency_ms, data, kind


def backend_status(server, timeout: float = 0.5):
    """Backend-reported status: via the control (management) plane when the backend has one,
    otherwise via the data plane (which is subject to simulated network faults)."""
    from load_balancer.netsim import NETSIM
    try:
        if getattr(server, "control_url", None):
            return requests.get(f"{server.control_url}/control/status", timeout=timeout).json()
        NETSIM.before_send(server.id, timeout=timeout, kind="status")
        return requests.get(f"{server.url}/status", timeout=timeout).json()
    except Exception:
        return None


class HealthMonitor:
    """Active background health monitor polling backend server /health endpoints."""

    def __init__(self, registry: ServerRegistry, metrics: MetricsCollector,
                 interval: float = 2.0, timeout: float = 1.0):
        self.registry = registry
        self.metrics = metrics
        self.interval = interval
        self.timeout = timeout
        self._running = False
        self._thread: Optional[threading.Thread] = None
        # Servers whose first health check result has been reported on the event timeline
        self._reported_initial = set()
        # Optional callback run after every full check cycle (used by automatic backend recovery)
        self.on_cycle = None
        # Optional callback on every state transition: on_transition(server, event_type, failure_kind)
        self.on_transition = None
        self.last_probe_kind = {}

    def start(self):
        """Start the background monitoring thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True, name="HealthMonitorThread")
        self._thread.start()
        self.metrics.add_log(
            f"Active Health Monitor started. Interval: {self.interval}s, Timeout: {self.timeout}s, Confirmation threshold: {self.registry.recovery_threshold}",
            level="INFO",
            event_type="HEALTH_MONITOR"
        )

    def report_initial_health_again(self):
        """After a simulation reset, report each server's next passing check on the timeline."""
        self._reported_initial.clear()

    def stop(self):
        """Signal the background thread to stop."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        self.metrics.add_log("Active Health Monitor stopped.", level="INFO", event_type="HEALTH_MONITOR")

    def _monitor_loop(self):
        """Main periodic polling loop."""
        while self._running:
            try:
                self.check_all_servers()
                if self.on_cycle:
                    self.on_cycle()
            except Exception as e:
                self.metrics.add_log(f"Error in health monitor loop: {str(e)}", level="ERROR", event_type="HEALTH_MONITOR")
            
            # Sleep in small increments for responsive shutdown
            slept = 0.0
            while self._running and slept < self.interval:
                time.sleep(0.1)
                slept += 0.1

    def check_all_servers(self):
        """Poll /health on all registered backend servers (in parallel, so one hung backend
        cannot delay the detection of failures on the others)."""
        servers = self.registry.get_all_servers()
        if len(servers) <= 1:
            for server in servers:
                self._check_single_server(server)
            return
        with ThreadPoolExecutor(max_workers=len(servers), thread_name_prefix="HealthProbe") as ex:
            list(ex.map(self._check_single_server, servers))

    def _check_single_server(self, server):
        """Send a single health check probe and handle state transitions."""
        health_url = f"{server.url}/health"
        is_healthy = False
        latency_ms = 0.0
        check_time = time.time()
        backend_injected_time = None
        backend_recovery_time = None
        epoch = self.registry.epoch

        is_healthy, latency_ms, data, kind = probe_health_detailed(health_url, self.timeout, server.id)
        self.last_probe_kind[server.id] = kind
        if not is_healthy and isinstance(data, dict):
            # 503 response from simulated failure carries the injection timestamp
            backend_injected_time = data.get("failure_injected_at")

        self.metrics.record_health_latency(latency_ms, is_healthy)

        # Update registry state machine
        event_type, details = self.registry.update_health_result(
            server_id=server.id,
            is_healthy=is_healthy,
            latency_ms=latency_ms,
            check_time=check_time,
            epoch=epoch
        )

        # Report each server's first successful health check on the event timeline
        if getattr(server, "is_replacement", False):
            self._reported_initial.add(server.id)   # its health checks are reported by auto-recovery
        if is_healthy and event_type is None and server.id not in self._reported_initial:
            self._reported_initial.add(server.id)
            self.metrics.add_log(
                f"{server.name} healthy — first health check passed ({round(latency_ms, 1)} ms).",
                level="SUCCESS",
                event_type="HEALTH_OK"
            )
        elif event_type == "FAILURE_DETECTED":
            self._reported_initial.add(server.id)

        if event_type and self.on_transition:
            try:
                self.on_transition(server, event_type, kind)
            except Exception as e:
                self.metrics.add_log(f"Health transition hook error: {e}", level="ERROR", event_type="HEALTH_MONITOR")

        # Process state transitions for telemetry & logs
        if event_type == "FAILURE_DETECTED":
            # If backend didn't supply injected time in /health body, fetch from /status
            if not backend_injected_time:
                backend_injected_time = (backend_status(server) or {}).get("failure_injected_at")

            self.metrics.record_failure_detection(
                server_id=server.id,
                server_name=server.name,
                failure_injected_at=backend_injected_time,
                detected_at=check_time
            )

        elif event_type == "RECOVERY_DETECTED":
            # Fetch recovery trigger timestamp from /status
            backend_recovery_time = (backend_status(server) or {}).get("recovery_triggered_at")

            self.metrics.record_recovery_detection(
                server_id=server.id,
                server_name=server.name,
                recovery_triggered_at=backend_recovery_time,
                detected_at=check_time
            )

        elif event_type == "SERVER_READMITTED":
            self.metrics.record_readmission(
                server_id=server.id,
                server_name=server.name,
                recovery_detected_at=server.recovery_detected_at,
                readmitted_at=check_time
            )
