"""
Per-client rate limiting of heavy hitters (an alternative to adding servers).

Each client (X-Forwarded-For address) has a token bucket. When the limiter is enabled, a client
may send `limit_rps` requests per second on average (bursts up to `burst` requests); beyond that
the proxy answers HTTP 429 Too Many Requests with a Retry-After header, without using a backend.

The resilience controller sets `limit_rps` to a multiple of the fair share
(safe capacity of the routable servers / number of active clients), so only clients sending
much more than their share are limited; ordinary clients are not affected.
"""

import threading
import time
from collections import deque
from typing import Callable, Dict, Optional


class HeavyHitterLimiter:
    WINDOW_S = 5.0

    def __init__(self, clock: Optional[Callable[[], float]] = None):
        self._clock = clock or time.time
        self._lock = threading.Lock()
        self.enabled = False
        self.limit_rps = 0.0
        self.burst = 0.0
        self._buckets: Dict[str, list] = {}            # client -> [tokens, last_refill]
        self._recent = deque(maxlen=50000)             # (t, client) of every request, for shares
        self.rejected = 0
        self._rejected_by: Dict[str, int] = {}

    def configure(self, enabled: bool, limit_rps: float = 0.0):
        with self._lock:
            self.enabled = bool(enabled)
            self.limit_rps = max(0.1, float(limit_rps)) if enabled else 0.0
            self.burst = max(2.0, 2.0 * self.limit_rps)
            if not enabled:
                self._buckets.clear()

    def note(self, client: str):
        """Record every request (also when the limiter is off) to know the per-client rates."""
        with self._lock:
            self._recent.append((self._clock(), client))

    def allow(self, client: str) -> bool:
        with self._lock:
            if not self.enabled:
                return True
            now = self._clock()
            b = self._buckets.get(client)
            if b is None:
                b = self._buckets[client] = [self.burst, now]
            b[0] = min(self.burst, b[0] + (now - b[1]) * self.limit_rps)
            b[1] = now
            if b[0] >= 1.0:
                b[0] -= 1.0
                return True
            self.rejected += 1
            self._rejected_by[client] = self._rejected_by.get(client, 0) + 1
            return False

    def client_rates(self) -> Dict[str, float]:
        """Requests per second of every client in the last WINDOW_S seconds."""
        with self._lock:
            t0 = self._clock() - self.WINDOW_S
            counts: Dict[str, int] = {}
            for t, c in self._recent:
                if t >= t0:
                    counts[c] = counts.get(c, 0) + 1
        return {c: n / self.WINDOW_S for c, n in counts.items()}

    def excess_share(self, limit_rps: float) -> float:
        """Share of all requests that a per-client limit of `limit_rps` would reject (relief estimate)."""
        rates = self.client_rates()
        total = sum(rates.values())
        if not total:
            return 0.0
        return sum(max(0.0, r - limit_rps) for r in rates.values()) / total

    def reset(self):
        with self._lock:
            self.enabled = False
            self.limit_rps = self.burst = 0.0
            self._buckets.clear()
            self._recent.clear()
            self.rejected = 0
            self._rejected_by.clear()

    def status(self) -> Dict[str, object]:
        rates = self.client_rates()
        top = sorted(rates.items(), key=lambda kv: -kv[1])[:5]
        with self._lock:
            return {"enabled": self.enabled, "limit_rps": round(self.limit_rps, 2), "burst": self.burst,
                    "rejected": self.rejected, "active_clients": len(rates),
                    "top_clients": [{"client": c, "rps": round(r, 2), "rejected": self._rejected_by.get(c, 0)}
                                    for c, r in top]}
