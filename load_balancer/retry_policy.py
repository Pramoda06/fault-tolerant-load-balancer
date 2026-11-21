"""
Retry control for the reverse proxy (prevents retry storms).

1. Bounded attempts: at most MAX_FAILOVER_RETRIES retries per client request, each on a
   different backend (enforced by LoadBalancer).
2. Idempotency: a request is retried on another backend only if
     - its HTTP method is idempotent (GET, HEAD, PUT, DELETE, OPTIONS), or
     - the failure proves the request never reached the backend
       (connection refused, connect timeout, simulated partition).
   A POST that timed out or got a 5xx may already have been executed, so it is not
   retried blindly.
3. Backoff: exponential with jitter before each retry (base * 2^(n-1), capped).
4. Retry budget: a token bucket shared by all requests. Every client request adds
   RETRY_BUDGET_RATIO tokens (max RETRY_BUDGET_MAX_TOKENS); each retry spends one. When
   the bucket is empty, failures are returned to the client instead of being retried,
   so a widespread failure cannot multiply the load on the remaining backends.
"""

import random
import threading
import requests


NOT_DELIVERED = (requests.exceptions.ConnectionError,)   # includes ConnectTimeout and refused connections


def failure_kind(exc: Exception = None, status_code: int = None, overloaded: bool = False) -> str:
    """Classify a failed exchange for metrics, circuit breaker and retry decisions."""
    from load_balancer.netsim import SimulatedPacketLoss, SimulatedPartition
    if exc is not None:
        if isinstance(exc, SimulatedPartition):
            return "partition"
        if isinstance(exc, SimulatedPacketLoss):
            return "packet_loss"
        if isinstance(exc, requests.exceptions.ConnectTimeout):
            return "connect_timeout"
        if isinstance(exc, requests.exceptions.Timeout):
            return "timeout"
        if isinstance(exc, requests.exceptions.ConnectionError):
            return "connection_refused"
        return "error"
    if overloaded:
        return "backend_overloaded"
    if status_code is not None and status_code >= 500:
        return "http_5xx"
    return "http_error"


def safe_to_retry(method: str, kind: str, idempotent_methods) -> bool:
    if method.upper() in idempotent_methods:
        return True
    # the request provably did not reach the backend (or was rejected before processing)
    return kind in ("connection_refused", "connect_timeout", "partition", "backend_overloaded")


class RetryBudget:
    def __init__(self, ratio: float = 0.2, min_tokens: float = 5, max_tokens: float = 20):
        self.ratio = ratio
        self.max_tokens = max_tokens
        self._tokens = float(min_tokens)
        self._min = float(min_tokens)
        self._lock = threading.Lock()
        self.denied = 0
        self.spent = 0

    def on_request(self):
        with self._lock:
            self._tokens = min(self.max_tokens, self._tokens + self.ratio)

    def try_spend(self) -> bool:
        with self._lock:
            if self._tokens >= 1.0 - 1e-9:        # tolerate float accumulation (10 x 0.1 = 0.999...)
                self._tokens = max(0.0, self._tokens - 1.0)
                self.spent += 1
                return True
            self.denied += 1
            return False

    def reset(self):
        with self._lock:
            self._tokens = self._min
            self.denied = 0
            self.spent = 0

    def snapshot(self) -> dict:
        with self._lock:
            return {"tokens": round(self._tokens, 2), "max_tokens": self.max_tokens, "ratio": self.ratio,
                    "retries_spent": self.spent, "retries_denied": self.denied}


def backoff_seconds(retry_number: int, base_ms: float, cap_ms: float, rng=random) -> float:
    """Exponential backoff with up to 50 % jitter, for retry_number = 1, 2, ..."""
    delay = min(cap_ms, base_ms * (2 ** max(0, retry_number - 1)))
    return (delay * (1.0 + 0.5 * rng.random())) / 1000.0
