"""
Server Registry and State Machine for Load Balancer.

Maintains the authoritative state of all registered backend servers:
States:
- HEALTHY: Available for request routing.
- UNHEALTHY: Failed health checks or unreachable; excluded from active routing pool.
- RECOVERING: Returned to responsive state, undergoing consecutive health check verification before re-admission.
"""

import time
import threading
from collections import deque
from typing import Dict, List, Optional, Tuple, Any

# Least Response Time uses only recent measurements: successful requests of the
# last RESPONSE_WINDOW_S seconds (at most RESPONSE_SAMPLES per server).
RESPONSE_WINDOW_S = 10.0
RESPONSE_SAMPLES = 20

class ServerNode:
    """Represents a single backend server and its operational state."""

    def __init__(self, server_id: str, name: str, host: str, port: int, weight: int = 1, url: str = None):
        self.id = server_id
        self.name = name
        self.host = host
        self.port = port
        self.weight = weight
        self.url = url or f"http://{host}:{port}"
        
        # State tracking: HEALTHY, UNHEALTHY, RECOVERING
        self.status = "HEALTHY"
        self.consecutive_successes = 0
        self.consecutive_failures = 0
        
        # Performance & telemetry
        self.last_health_check = None
        self.last_successful_check = None
        self.last_failed_check = None
        self.health_latency_ms = 0.0
        self.requests_served = 0

        # Routing inputs measured by the proxy (used by connection / response-time strategies)
        self.active_connections = 0          # requests currently being forwarded to this server
        self._response_samples = deque(maxlen=RESPONSE_SAMPLES)   # (time, ms) of successful requests
        
        # Dynamic provisioning: True for replacement backends started by auto-recovery
        # or by the capacity manager. Original (configured) servers are never removed.
        self.is_replacement = False
        self.origin = "original"             # original | recovery | capacity
        self.pid = None
        self.provisioned_at = None
        self.control_url = None              # management (control-plane) URL, if the backend has one

        # Graceful draining before removal (capacity scale-in)
        self.draining = False
        self.drain_started_at = None
        # Standby (capacity scale-in of an ORIGINAL server): process keeps running and is still
        # health-checked, but receives no requests until it is activated again.
        self.standby = False
        self.standby_since = None

        # Lifecycle transition timestamps for exact metrics
        self.failure_injected_at = None
        self.failure_detected_at = None
        self.recovery_injected_at = None
        self.recovery_detected_at = None
        self.readmitted_at = None

    def reset_state(self, is_healthy: bool = True):
        """Return the node to a clean initial state (used by simulation reset)."""
        self.status = "HEALTHY" if is_healthy else "UNHEALTHY"
        self.consecutive_successes = 0
        self.consecutive_failures = 0
        self.requests_served = 0
        self._response_samples.clear()
        self.draining = False
        self.drain_started_at = None
        self.standby = False
        self.standby_since = None
        self.failure_injected_at = None
        self.failure_detected_at = None
        self.recovery_injected_at = None
        self.recovery_detected_at = None
        self.readmitted_at = None

    def recent_response_ms(self, now: Optional[float] = None) -> Optional[float]:
        """Mean response time of this server's successful requests in the last RESPONSE_WINDOW_S
        seconds, or None if there is no recent measurement."""
        now = time.time() if now is None else now
        recent = [ms for t, ms in list(self._response_samples) if now - t <= RESPONSE_WINDOW_S]
        return sum(recent) / len(recent) if recent else None

    def to_dict(self) -> Dict[str, Any]:
        """Convert node properties to dictionary for API responses."""
        recent_ms = self.recent_response_ms()
        return {
            "id": self.id,
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "weight": self.weight,
            "url": self.url,
            "status": self.status,
            "is_active": self.status == "HEALTHY" and not self.draining and not self.standby,
            "consecutive_successes": self.consecutive_successes,
            "consecutive_failures": self.consecutive_failures,
            "last_health_check": self.last_health_check,
            "last_successful_check": self.last_successful_check,
            "last_failed_check": self.last_failed_check,
            "health_latency_ms": round(self.health_latency_ms, 2),
            "requests_served": self.requests_served,
            "active_connections": self.active_connections,
            "recent_response_ms": None if recent_ms is None else round(recent_ms, 2),
            "failure_injected_at": self.failure_injected_at,
            "recovery_injected_at": self.recovery_injected_at,
            "failure_detected_at": self.failure_detected_at,
            "recovery_detected_at": self.recovery_detected_at,
            "readmitted_at": self.readmitted_at,
            "is_replacement": self.is_replacement,
            "origin": self.origin,
            "draining": self.draining,
            "drain_started_at": self.drain_started_at,
            "standby": self.standby,
            "standby_since": self.standby_since,
            "pid": self.pid,
            "provisioned_at": self.provisioned_at,
        }


class ServerRegistry:
    """Thread-safe registry managing backend nodes and their lifecycle state machine."""

    def __init__(self, recovery_threshold: int = 2):
        self._servers: Dict[str, ServerNode] = {}
        self._lock = threading.RLock()
        self.recovery_threshold = recovery_threshold
        # Incremented on simulation reset so health results probed before the
        # reset cannot overwrite the freshly reset state.
        self.epoch = 0

    def register_server(self, server_id: str, name: str, host: str, port: int, weight: int = 1, url: str = None) -> ServerNode:
        """Register a new backend server."""
        with self._lock:
            node = ServerNode(server_id, name, host, port, weight, url)
            self._servers[server_id] = node
            return node

    def unregister_server(self, server_id: str) -> bool:
        """Remove a REPLACEMENT backend from the pool entirely. Original servers are never removed."""
        with self._lock:
            node = self._servers.get(server_id)
            if node is None or not node.is_replacement:
                return False
            del self._servers[server_id]
            return True

    def get_server(self, server_id: str) -> Optional[ServerNode]:
        """Retrieve a server by its ID."""
        with self._lock:
            return self._servers.get(server_id)

    def get_all_servers(self) -> List[ServerNode]:
        """Return a copy of all registered servers."""
        with self._lock:
            return list(self._servers.values())

    def get_active_pool(self) -> List[ServerNode]:
        """Return the servers eligible for new requests: HEALTHY, not being drained, not on standby.
        Safety net: if no such server exists but a HEALTHY standby server does, the standby servers
        are used at once (the capacity manager then activates them formally), so standby never
        causes an outage."""
        with self._lock:
            active = [s for s in self._servers.values() if s.status == "HEALTHY" and not s.draining and not s.standby]
            if active:
                return active
            return [s for s in self._servers.values() if s.status == "HEALTHY" and not s.draining and s.standby]

    def try_connection_started(self, server_id: str) -> bool:
        """Atomically check that the server may still take a new request and count it as active."""
        with self._lock:
            node = self._servers.get(server_id)
            if node is None or node.draining or node.status != "HEALTHY":
                return False
            node.active_connections += 1
            return True

    # ---------------- graceful draining (capacity scale-in) ----------------
    def start_drain(self, server_id: str, now: float, allow_original: bool = False) -> bool:
        """Stop routing NEW requests to a server. Originals are refused unless allow_original
        (they are drained into standby, never removed)."""
        with self._lock:
            node = self._servers.get(server_id)
            if node is None or node.draining or node.standby or (not node.is_replacement and not allow_original):
                return False
            node.draining = True
            node.drain_started_at = now
            return True

    def cancel_drain(self, server_id: str) -> bool:
        with self._lock:
            node = self._servers.get(server_id)
            if node is None or not node.draining:
                return False
            node.draining = False
            node.drain_started_at = None
            return True

    def remove_if_drained(self, server_id: str, now: float, timeout: float):
        """
        Unregister a draining replacement once it has no active connections (or the drain
        timeout has passed). Done under the registry lock together with the eligibility check
        in try_connection_started(), so no new request can start on it afterwards.
        Returns (removed, node, forced).
        """
        with self._lock:
            node = self._servers.get(server_id)
            if node is None or not node.draining:
                return False, node, False
            forced = (now - (node.drain_started_at or now)) >= timeout
            if node.active_connections > 0 and not forced:
                return False, node, False
            del self._servers[server_id]
            return True, node, forced and node.active_connections > 0

    def standby_if_drained(self, server_id: str, now: float, timeout: float):
        """Put a draining ORIGINAL server on standby once its in-flight requests have finished (or the
        drain timeout passed). It stays registered and health-checked. Returns (done, node, forced)."""
        with self._lock:
            node = self._servers.get(server_id)
            if node is None or not node.draining or node.is_replacement:
                return False, node, False
            forced = (now - (node.drain_started_at or now)) >= timeout
            if node.active_connections > 0 and not forced:
                return False, node, False
            node.draining = False
            node.drain_started_at = None
            node.standby = True
            node.standby_since = now
            return True, node, forced and node.active_connections > 0

    def activate_standby(self, server_id: str) -> bool:
        """Return a standby server to the routing pool."""
        with self._lock:
            node = self._servers.get(server_id)
            if node is None or not node.standby:
                return False
            node.standby = False
            node.standby_since = None
            return True

    def increment_request_count(self, server_id: str):
        """Thread-safely increment requests processed by this server."""
        with self._lock:
            server = self._servers.get(server_id)
            if server:
                server.requests_served += 1

    def connection_started(self, server_id: str):
        """The proxy is about to forward a request to this server."""
        with self._lock:
            server = self._servers.get(server_id)
            if server:
                server.active_connections += 1

    def connection_finished(self, server_id: str, latency_ms: Optional[float] = None):
        """The forwarded request finished. latency_ms is given only for successful requests."""
        with self._lock:
            server = self._servers.get(server_id)
            if server:
                if server.active_connections > 0:
                    server.active_connections -= 1
                if latency_ms is not None:
                    server._response_samples.append((time.time(), latency_ms))

    def reset_all(self, health_by_id: Dict[str, bool]):
        """Reset every node (simulation reset). health_by_id: live probe result per server."""
        with self._lock:
            self.epoch += 1
            for sid, node in self._servers.items():
                node.reset_state(health_by_id.get(sid, True))

    def update_health_result(self, server_id: str, is_healthy: bool, latency_ms: float, check_time: float, epoch: Optional[int] = None) -> Tuple[Optional[str], Optional[dict]]:
        """
        Update health check result for a server and advance its state machine.
        
        Returns:
            (event_type, details_dict) if a state transition occurred, or (None, None).
        """
        with self._lock:
            server = self._servers.get(server_id)
            if not server:
                return None, None
            if epoch is not None and epoch != self.epoch:
                # Probe started before a simulation reset; discard the stale result.
                return None, None

            server.last_health_check = check_time
            server.health_latency_ms = latency_ms

            event_type = None
            details = None

            if is_healthy:
                server.last_successful_check = check_time
                server.consecutive_failures = 0
                server.consecutive_successes += 1

                if server.status == "UNHEALTHY":
                    # Transition: UNHEALTHY -> RECOVERING
                    server.status = "RECOVERING"
                    server.recovery_detected_at = check_time
                    event_type = "RECOVERY_DETECTED"
                    details = {
                        "server_id": server.id,
                        "server_name": server.name,
                        "recovery_detected_at": check_time,
                        "consecutive_successes": server.consecutive_successes,
                        "threshold": self.recovery_threshold
                    }

                elif server.status == "RECOVERING":
                    # Check if recovery confirmation threshold is met for re-admission
                    if server.consecutive_successes >= self.recovery_threshold:
                        # Transition: RECOVERING -> HEALTHY (Re-admitted)
                        server.status = "HEALTHY"
                        server.readmitted_at = check_time
                        event_type = "SERVER_READMITTED"
                        details = {
                            "server_id": server.id,
                            "server_name": server.name,
                            "readmitted_at": check_time,
                            "consecutive_successes": server.consecutive_successes
                        }

            else:
                # Health probe failed
                server.last_failed_check = check_time
                server.consecutive_failures += 1
                server.consecutive_successes = 0

                if server.status in ("HEALTHY", "RECOVERING"):
                    # Transition: HEALTHY / RECOVERING -> UNHEALTHY
                    old_status = server.status
                    server.status = "UNHEALTHY"
                    server.failure_detected_at = check_time
                    event_type = "FAILURE_DETECTED"
                    details = {
                        "server_id": server.id,
                        "server_name": server.name,
                        "previous_status": old_status,
                        "failure_detected_at": check_time,
                        "consecutive_failures": server.consecutive_failures
                    }

            return event_type, details
