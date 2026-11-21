"""
Simulated network faults between the reverse proxy and a backend (SIMULATED).

On localhost the operating system's network cannot be made lossy or partitioned
without administrator rights (firewall rules / traffic control). These two faults are
therefore simulated in the proxy's own outbound network layer: every request the
proxy sends to a backend (forwarded client requests AND health probes) first passes
through NetworkSimulator.before_send(). The backend process itself keeps running and
stays reachable through its control (management) port.

  partition  every exchange with the backend fails as if TCP could not connect:
             after SIM_PARTITION_WAIT seconds a ConnectTimeout is raised. The request
             never reaches the backend, so a retry elsewhere is always safe.
  loss (p)   each exchange is lost with probability p: no reply arrives, and after
             SIM_LOSS_WAIT seconds a ReadTimeout is raised. Whether the request reached
             the backend is unknown, so non-idempotent requests are not retried.

These are not measurements of real packet loss; counters are labelled "simulated".
"""

import time
import random
import threading
from typing import Dict, Optional

import requests


class SimulatedPartition(requests.exceptions.ConnectTimeout):
    """Simulated network partition: the backend is unreachable from the proxy."""


class SimulatedPacketLoss(requests.exceptions.ReadTimeout):
    """Simulated loss of a request/response exchange."""


class NetworkSimulator:
    def __init__(self, seed: Optional[int] = None, partition_wait: float = 0.5, loss_wait: float = 1.0,
                 sleep=time.sleep):
        self._lock = threading.Lock()
        self._faults: Dict[str, dict] = {}          # server_id -> {"partition": bool, "loss": float}
        self._rng = random.Random(seed)
        self.partition_wait = partition_wait
        self.loss_wait = loss_wait
        self._sleep = sleep
        self.stats = {"lost": 0, "partition_drops": 0, "exchanges": 0}
        self.per_server: Dict[str, dict] = {}

    # -- configuration -------------------------------------------------------
    def set_partition(self, server_id: str, on: bool):
        with self._lock:
            self._faults.setdefault(server_id, {"partition": False, "loss": 0.0})["partition"] = on
            self._cleanup(server_id)

    def set_loss(self, server_id: str, probability: float):
        p = max(0.0, min(1.0, float(probability)))
        with self._lock:
            self._faults.setdefault(server_id, {"partition": False, "loss": 0.0})["loss"] = p
            self._cleanup(server_id)

    def clear(self, server_id: Optional[str] = None):
        with self._lock:
            if server_id is None:
                self._faults.clear()
            else:
                self._faults.pop(server_id, None)

    def reset(self, seed: Optional[int] = None):
        with self._lock:
            self._faults.clear()
            self.stats = {"lost": 0, "partition_drops": 0, "exchanges": 0}
            self.per_server = {}
            if seed is not None:
                self._rng = random.Random(seed)

    def reseed(self, seed: int):
        with self._lock:
            self._rng = random.Random(seed)

    def _cleanup(self, server_id):
        f = self._faults.get(server_id)
        if f and not f["partition"] and f["loss"] <= 0:
            del self._faults[server_id]

    def faults(self) -> Dict[str, dict]:
        with self._lock:
            return {k: dict(v) for k, v in self._faults.items()}

    # -- data path -----------------------------------------------------------
    def before_send(self, server_id: Optional[str], timeout: Optional[float] = None, kind: str = "request"):
        """Raise a simulated network error for this exchange, or return to let it proceed."""
        if not server_id:
            return
        with self._lock:
            self.stats["exchanges"] += 1
            f = self._faults.get(server_id)
            if not f:
                return
            ps = self.per_server.setdefault(server_id, {"lost": 0, "partition_drops": 0})
            if f["partition"]:
                self.stats["partition_drops"] += 1
                ps["partition_drops"] += 1
                wait = self.partition_wait if timeout is None else min(timeout, self.partition_wait)
                err = SimulatedPartition(f"simulated network partition: {server_id} unreachable ({kind})")
            elif f["loss"] > 0 and self._rng.random() < f["loss"]:
                self.stats["lost"] += 1
                ps["lost"] += 1
                wait = self.loss_wait if timeout is None else min(timeout, self.loss_wait)
                err = SimulatedPacketLoss(f"simulated packet loss: no reply from {server_id} ({kind})")
            else:
                return
        self._sleep(wait)
        raise err

    def snapshot(self) -> dict:
        with self._lock:
            return {"faults": {k: dict(v) for k, v in self._faults.items()}, "stats": dict(self.stats),
                    "per_server": {k: dict(v) for k, v in self.per_server.items()},
                    "label": "simulated in the proxy's outbound network layer"}


# Process-wide instance used by the balancer and the health monitor.
NETSIM = NetworkSimulator()
