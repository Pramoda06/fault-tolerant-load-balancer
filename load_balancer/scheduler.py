"""
Load Balancing Schedulers.

Implements:
1. Baseline Round Robin (cyclic distribution)
2. Smooth Weighted Round Robin (proportional distribution based on capacity)
3. Least Connections (fewest requests currently in flight)
4. Weighted Least Connections (lowest active_connections / weight)
5. Least Response Time (lowest measured recent response time)
6. IP Hash / sticky routing (rendezvous hashing of the client IP)
7. SchedulerManager allowing runtime algorithm switching

Every scheduler receives only the eligible (HEALTHY) servers, so an unhealthy
server can never be selected, whatever the strategy.
"""

import hashlib
import threading
from typing import List, Optional
from load_balancer.server_registry import ServerNode

class BaseScheduler:
    """Abstract base class for load balancing scheduling strategies."""

    def select(self, servers: List[ServerNode], client_ip: Optional[str] = None) -> Optional[ServerNode]:
        raise NotImplementedError


class RoundRobinScheduler(BaseScheduler):
    """
    Standard Round Robin load balancing algorithm.
    Sequentially distributes requests across healthy backend servers.
    Example: Server 1 -> Server 2 -> Server 3 -> Server 1 ...
    """

    def __init__(self):
        self._index = 0
        self._lock = threading.Lock()

    def select(self, servers: List[ServerNode], client_ip: Optional[str] = None) -> Optional[ServerNode]:
        """Select the next available server in cyclic order."""
        if not servers:
            return None

        with self._lock:
            server = servers[self._index % len(servers)]
            self._index = (self._index + 1) % len(servers)
            return server

    def reset(self):
        """Reset index pointer."""
        with self._lock:
            self._index = 0


class WeightedRoundRobinScheduler(BaseScheduler):
    """
    Smooth Weighted Round Robin scheduling (Nginx algorithm).
    
    Rather than bunching requests (e.g., S1, S1, S1, S2, S2, S3),
    smooth weighted round robin interleaves requests evenly:
    
    Algorithm:
    1. For each healthy server: current_weight += effective_weight
    2. Select server with maximum current_weight
    3. For selected server: current_weight -= total_weight_of_all_healthy_servers
    """

    def __init__(self):
        self._current_weights = {}
        self._lock = threading.Lock()

    def select(self, servers: List[ServerNode], client_ip: Optional[str] = None) -> Optional[ServerNode]:
        if not servers:
            return None

        with self._lock:
            total_weight = sum(s.weight for s in servers)
            if total_weight <= 0:
                # Fallback to first server if all weights are zero
                return servers[0]

            # Update current weights
            for s in servers:
                self._current_weights[s.id] = self._current_weights.get(s.id, 0) + s.weight

            # Find server with greatest current weight
            best_server = max(servers, key=lambda s: self._current_weights.get(s.id, 0))

            # Reduce selected server's current weight by total weight
            self._current_weights[best_server.id] -= total_weight

            return best_server

    def reset(self):
        with self._lock:
            self._current_weights.clear()


class _Rotation:
    """Tie-breaker: cycles through tied servers, continuing after the one chosen last."""

    def __init__(self):
        self._last_id = None
        self._order: List[str] = []

    def pick(self, tied: List[ServerNode]) -> ServerNode:
        for s in tied:
            if s.id not in self._order:
                self._order.append(s.id)
        ids = {s.id: s for s in tied}
        chosen = tied[0]
        if self._last_id in self._order:
            start = self._order.index(self._last_id)
            n = len(self._order)
            for step in range(1, n + 1):
                sid = self._order[(start + step) % n]
                if sid in ids:
                    chosen = ids[sid]
                    break
        self._last_id = chosen.id
        return chosen

    def reset(self):
        self._last_id = None
        self._order = []


class LeastConnectionsScheduler(BaseScheduler):
    """
    Least Connections: choose the server with the fewest active connections, i.e.
    requests the proxy is currently forwarding to it (ServerNode.active_connections,
    incremented before forwarding and decremented when the forward finishes).
    Weights are not used. Ties (e.g. all servers idle) are broken by rotating
    through the tied servers, so idle servers share the traffic evenly.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._rotation = _Rotation()

    def select(self, servers: List[ServerNode], client_ip: Optional[str] = None) -> Optional[ServerNode]:
        if not servers:
            return None
        with self._lock:
            least = min(s.active_connections for s in servers)
            return self._rotation.pick([s for s in servers if s.active_connections == least])

    def reset(self):
        with self._lock:
            self._rotation.reset()


class WeightedLeastConnectionsScheduler(BaseScheduler):
    """
    Weighted Least Connections: choose the server with the lowest

        score = active_connections / weight

    (compared exactly as a1 * w2 < a2 * w1). A server with weight 3 is allowed three
    times the concurrent requests of a server with weight 1 before it looks equally
    loaded. Ties - including the common case of all servers idle - are broken with
    Smooth Weighted Round Robin over the tied servers (as nginx least_conn does), so
    with no concurrency the split follows the weights.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._wrr = WeightedRoundRobinScheduler()

    def select(self, servers: List[ServerNode], client_ip: Optional[str] = None) -> Optional[ServerNode]:
        if not servers:
            return None
        with self._lock:
            usable = [s for s in servers if s.weight > 0] or servers
            best = usable[0]
            for s in usable[1:]:
                # s.active / s.weight < best.active / best.weight
                if s.active_connections * max(best.weight, 1) < best.active_connections * max(s.weight, 1):
                    best = s
            tied = [s for s in usable
                    if s.active_connections * max(best.weight, 1) == best.active_connections * max(s.weight, 1)]
        return self._wrr.select(tied)

    def reset(self):
        with self._lock:
            self._wrr.reset()


class LeastResponseTimeScheduler(BaseScheduler):
    """
    Least Response Time: choose the server with the lowest measured response time.

    Response time = mean round-trip time of the server's successful requests through
    the proxy in the last 10 s (at most 20 samples; ServerNode.recent_response_ms()).
    Missing measurements: a server with no recent measurement (new, just recovered, or
    not used for 10 s) is chosen first so that it gets measured; this also keeps the
    values of slower servers from going stale. Ties are broken by fewer active
    connections, then by rotation.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._rotation = _Rotation()

    def select(self, servers: List[ServerNode], client_ip: Optional[str] = None) -> Optional[ServerNode]:
        if not servers:
            return None
        with self._lock:
            times = {s.id: s.recent_response_ms() for s in servers}
            unmeasured = [s for s in servers if times[s.id] is None]
            if unmeasured:
                return self._rotation.pick(unmeasured)
            fastest = min(times.values())
            tied = [s for s in servers if times[s.id] == fastest]
            least = min(s.active_connections for s in tied)
            return self._rotation.pick([s for s in tied if s.active_connections == least])

    def reset(self):
        with self._lock:
            self._rotation.reset()


class IPHashScheduler(BaseScheduler):
    """
    IP Hash (sticky routing) using rendezvous (highest-random-weight) hashing:
    for each eligible server compute md5(client_ip + "|" + server_id) and pick the
    server with the highest value. The same client IP therefore always reaches the
    same server while the pool is unchanged.

    When the pool changes the mapping changes: if a client's server fails, that
    client moves to its next-highest server (other clients keep their server); when
    the server returns, its clients move back to it. Requests without a client IP
    are treated as client "0.0.0.0".
    """

    @staticmethod
    def score(client_ip: str, server_id: str) -> int:
        return int(hashlib.md5(f"{client_ip}|{server_id}".encode()).hexdigest(), 16)

    def select(self, servers: List[ServerNode], client_ip: Optional[str] = None) -> Optional[ServerNode]:
        if not servers:
            return None
        ip = (client_ip or "0.0.0.0").split(",")[0].strip()
        return max(servers, key=lambda s: self.score(ip, s.id))

    def reset(self):
        pass


class SchedulerManager:
    """Manages active scheduling strategy and allows runtime toggling."""

    ALGORITHMS = {
        "ROUND_ROBIN": RoundRobinScheduler,
        "WEIGHTED_ROUND_ROBIN": WeightedRoundRobinScheduler,
        "LEAST_CONNECTIONS": LeastConnectionsScheduler,
        "WEIGHTED_LEAST_CONNECTIONS": WeightedLeastConnectionsScheduler,
        "LEAST_RESPONSE_TIME": LeastResponseTimeScheduler,
        "IP_HASH": IPHashScheduler,
    }

    def __init__(self, initial_algorithm: str = "ROUND_ROBIN"):
        self._current_name = initial_algorithm if initial_algorithm in self.ALGORITHMS else "ROUND_ROBIN"
        self._schedulers = {name: cls() for name, cls in self.ALGORITHMS.items()}
        self._lock = threading.Lock()

    @property
    def current_algorithm(self) -> str:
        with self._lock:
            return self._current_name

    def set_algorithm(self, algorithm_name: str) -> bool:
        """Switch current scheduling algorithm."""
        algorithm_name = algorithm_name.upper()
        with self._lock:
            if algorithm_name in self.ALGORITHMS:
                self._current_name = algorithm_name
                return True
            return False

    def select_server(self, servers: List[ServerNode], client_ip: Optional[str] = None) -> Optional[ServerNode]:
        """Delegate server selection to currently active scheduler."""
        with self._lock:
            scheduler = self._schedulers[self._current_name]
        return scheduler.select(servers, client_ip=client_ip)

    def reset(self):
        with self._lock:
            for s in self._schedulers.values():
                s.reset()
