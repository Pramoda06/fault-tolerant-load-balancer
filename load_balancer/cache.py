"""
Proxy response cache (an alternative to adding servers).

Two independent behaviours, switched on by the resilience controller (or manually):

  fresh      GET responses for a key (e.g. /api/request?item=42) are kept for `ttl` seconds and
             answered by the proxy itself (X-Cache: HIT). Popular keys stop reaching the backends,
             which lowers backend load without adding a server. Cost: data up to `ttl` s old.
  stale-if-error
             when every attempt to reach a backend failed, the last good response for the key is
             returned (X-Cache: STALE, with its age) if it is at most `stale_max` seconds old
             (cf. RFC 5861 "stale-if-error"). Cost: clearly marked stale data instead of an error.

Only requests that carry a key are cached; requests without one always go to a backend.

Relief estimate (also while caching is OFF): every keyed request is noted with whether the same
key was seen within the last `ttl` seconds - i.e. whether it WOULD have been a cache hit. The
resilience controller uses this predicted hit ratio to decide whether turning the cache on would
actually relieve the backends before it does so.
"""

import threading
import time
from collections import OrderedDict, deque
from typing import Any, Callable, Dict, Optional, Tuple


class ResponseCache:
    def __init__(self, ttl: float = 2.0, stale_max: float = 60.0, max_entries: int = 2000,
                 clock: Optional[Callable[[], float]] = None):
        self.ttl = float(ttl)
        self.stale_max = float(stale_max)
        self.max_entries = int(max_entries)
        self.fresh_enabled = False
        self.stale_on_error = False
        self._clock = clock or time.time
        self._lock = threading.Lock()
        self._data: "OrderedDict[str, Tuple[float, Dict[str, Any]]]" = OrderedDict()
        self.hits = self.misses = self.stale_served = 0
        self._seen: "OrderedDict[str, float]" = OrderedDict()
        self._probe = deque(maxlen=20000)          # (t, would_hit)

    def note(self, key: str):
        """Record a keyed request for the predicted hit ratio (independent of whether caching is on)."""
        now = self._clock()
        with self._lock:
            last = self._seen.get(key)
            self._probe.append((now, last is not None and now - last <= self.ttl))
            self._seen[key] = now
            self._seen.move_to_end(key)
            while len(self._seen) > self.max_entries:
                self._seen.popitem(last=False)

    def predicted_hit_ratio(self, window: float, total_requests: int) -> float:
        """Share of ALL requests in the window that a fresh cache would have answered."""
        t0 = self._clock() - window
        with self._lock:
            hits = sum(1 for t, h in self._probe if t >= t0 and h)
        return hits / total_requests if total_requests else 0.0

    def configure(self, fresh: Optional[bool] = None, stale_on_error: Optional[bool] = None,
                  ttl: Optional[float] = None):
        with self._lock:
            if fresh is not None:
                self.fresh_enabled = bool(fresh)
            if stale_on_error is not None:
                self.stale_on_error = bool(stale_on_error)
            if ttl is not None:
                self.ttl = float(ttl)

    def put(self, key: str, body: Dict[str, Any]):
        with self._lock:
            self._data[key] = (self._clock(), dict(body))
            self._data.move_to_end(key)
            while len(self._data) > self.max_entries:
                self._data.popitem(last=False)

    def _get(self, key: str, max_age: float):
        e = self._data.get(key)
        if e is None:
            return None
        age = self._clock() - e[0]
        if age > max_age:
            return None
        return dict(e[1]), age

    def fresh(self, key: str):
        """(body, age) if fresh caching is on and the key is younger than ttl, else None."""
        with self._lock:
            if not self.fresh_enabled:
                return None
            hit = self._get(key, self.ttl)
            if hit is None:
                self.misses += 1
                return None
            self.hits += 1
            return hit

    def stale(self, key: str):
        """(body, age) for stale-if-error, else None."""
        with self._lock:
            if not self.stale_on_error:
                return None
            hit = self._get(key, self.stale_max)
            if hit is not None:
                self.stale_served += 1
            return hit

    def reset(self):
        with self._lock:
            self._data.clear()
            self.fresh_enabled = self.stale_on_error = False
            self.hits = self.misses = self.stale_served = 0
            self._seen.clear()
            self._probe.clear()

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {"fresh_enabled": self.fresh_enabled, "stale_on_error": self.stale_on_error, "ttl_s": self.ttl,
                    "stale_max_s": self.stale_max, "entries": len(self._data), "hits": self.hits,
                    "misses": self.misses, "stale_served": self.stale_served}
