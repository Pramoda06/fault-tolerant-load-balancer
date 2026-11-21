"""
Server-side traffic generator (a real HTTP client).

It runs in its own threads inside the proxy process and sends real HTTP requests to the
proxy's public endpoint (http://127.0.0.1:5000/api/request and /api/records), exactly as
an external client would. It is "open loop": requests are scheduled at a fixed rate; if
all `concurrency` client workers are busy when a request is due, that request is NOT sent
and is counted as `not_sent_client_busy` (the client itself is saturated).

Client addresses: request i is sent with X-Forwarded-For = one of `clients` simulated
addresses (10.1.x.y), which the proxy uses as the client IP (IP Hash routing).

Optional workload realism (used by the resilience experiments):
  keys > 0      each GET asks for one of `keys` items (/api/request?item=K), chosen with a Zipf(1.1)
                popularity: a few items are very popular, as in real web traffic (cacheable).
  poisson       random (exponential) gaps between requests, like independent users, instead of
                evenly spaced requests; bursts make queueing visible.
  heavy > 0     a fraction `heavy` of all requests comes from `heavy_clients` addresses ("heavy
                hitters"), the rest is spread over the other clients.

Limitations: the generator shares the CPU (and Python's GIL) with the proxy, so it is
suitable for tens of requests per second on a laptop, not for benchmarking at scale.
"""

import time
import random
import bisect
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

import requests


def client_ip(i: int) -> str:
    return f"10.1.{(i // 250) % 250}.{i % 250 + 1}"


class TrafficGenerator:
    def __init__(self, base_url: str, max_rps: float = 100, max_concurrency: int = 32):
        self.base_url = base_url.rstrip("/")
        self.max_rps = max_rps
        self.max_concurrency = max_concurrency
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._pool: Optional[ThreadPoolExecutor] = None
        self._local = threading.local()
        self._route_seq = 0                          # monotonic across restarts (dashboard merges by it)
        self._routes = deque(maxlen=80)
        self._reset_stats()
        self.params: Dict[str, Any] = {}

    def _reset_stats(self):
        self.sent = 0
        self.ok = 0
        self.failed = 0
        self.not_sent = 0
        self.limited = 0
        self.inflight = 0
        self.started_at = None
        self.stopped_at = None
        self._results = deque(maxlen=20000)      # (t, status, latency_ms)

    def _session(self):
        s = getattr(self._local, "s", None)
        if s is None:
            s = self._local.s = requests.Session()
        return s

    # ------------------------------------------------------------------ #
    def start(self, rps: float, concurrency: int = 8, clients: int = 16, write_ratio: float = 0.0,
              duration: Optional[float] = None, seed: int = 1, keys: int = 0, heavy: float = 0.0,
              heavy_clients: int = 2, poisson: bool = False) -> Dict[str, Any]:
        rps = float(rps)
        concurrency = int(concurrency)
        clients = int(clients)
        write_ratio = float(write_ratio)
        if not 0 < rps <= self.max_rps:
            raise ValueError(f"rps must be in (0, {self.max_rps}]")
        if not 1 <= concurrency <= self.max_concurrency:
            raise ValueError(f"concurrency must be 1..{self.max_concurrency}")
        if not 1 <= clients <= 10000:
            raise ValueError("clients must be 1..10000")
        if not 0.0 <= write_ratio <= 1.0:
            raise ValueError("write_ratio must be 0..1")
        keys, heavy, heavy_clients = int(keys), float(heavy), int(heavy_clients)
        if not 0 <= keys <= 100000 or not 0.0 <= heavy < 1.0 or not 1 <= heavy_clients < max(2, clients):
            raise ValueError("keys 0..100000, heavy 0..<1, heavy_clients 1..clients-1")
        self.stop()
        with self._lock:
            self._reset_stats()
            self.params = {"rps": rps, "concurrency": concurrency, "clients": clients, "write_ratio": write_ratio,
                           "duration": duration, "seed": seed, "keys": keys, "heavy": heavy,
                           "heavy_clients": heavy_clients, "poisson": bool(poisson)}
            self._stop.clear()
            self._pool = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="TrafficClient")
            self.started_at = time.time()
            self._thread = threading.Thread(target=self._schedule, args=(rps, concurrency, clients, write_ratio,
                                                                          duration, seed), daemon=True,
                                            name="TrafficScheduler")
            self._thread.start()
        return self.status()

    def update_rate(self, rps: float):
        """Change the request rate of a running generator without resetting its counters."""
        rps = float(rps)
        if not 0 < rps <= self.max_rps:
            raise ValueError(f"rps must be in (0, {self.max_rps}]")
        with self._lock:
            self.params["rps"] = rps

    def _schedule(self, rps, concurrency, clients, write_ratio, duration, seed):
        rng = random.Random(seed)
        p = self.params
        keys, heavy, hc = p.get("keys", 0), p.get("heavy", 0.0), p.get("heavy_clients", 2)
        cdf = None
        if keys:
            w = [1.0 / (k ** 1.1) for k in range(1, keys + 1)]
            tot, acc, cdf = sum(w), 0.0, []
            for x in w:
                acc += x / tot
                cdf.append(acc)
        i = 0
        next_t = time.perf_counter()
        t_end = None if duration is None else next_t + duration
        while not self._stop.is_set():
            now = time.perf_counter()
            if t_end is not None and now >= t_end:
                break
            if now < next_t:
                self._stop.wait(min(next_t - now, 0.05))
                continue
            with self._lock:
                rate = self.params.get("rps", rps)
                busy = self.inflight >= concurrency
                if busy:
                    self.not_sent += 1
                else:
                    self.inflight += 1
            if not busy:
                write = rng.random() < write_ratio
                if heavy and rng.random() < heavy:
                    ci = rng.randrange(hc)                         # heavy hitter
                elif heavy:
                    ci = hc + rng.randrange(max(1, clients - hc))  # ordinary client
                else:
                    ci = i % clients
                key = (bisect.bisect_left(cdf, rng.random()) + 1) if cdf else None
                self._pool.submit(self._one, i, clients, write, ci, key)
            i += 1
            next_t += rng.expovariate(rate) if self.params.get("poisson") else 1.0 / rate
            if next_t < now - 1.0:          # fell far behind (e.g. machine paused): do not burst
                next_t = now
        with self._lock:
            self.stopped_at = time.time()

    def _one(self, i, clients, write, ci=None, key=None):
        ip = client_ip(i % clients if ci is None else ci)
        t0 = time.perf_counter()
        status = 0
        try:
            if write:
                r = self._session().post(f"{self.base_url}/api/records",
                                         json={"name": f"load-{i}", "value": f"generated #{i}"},
                                         headers={"X-Forwarded-For": ip}, timeout=8)
            else:
                url = f"{self.base_url}/api/request" + (f"?item={key}" if key else "")
                r = self._session().get(url, headers={"X-Forwarded-For": ip}, timeout=8)
            status = r.status_code
            try:
                body = r.json() if not write else {}
            except ValueError:
                body = {}
        except Exception:
            status = 0
            body = {}
        ms = (time.perf_counter() - t0) * 1000.0
        with self._lock:
            self.inflight -= 1
            self.sent += 1
            if status in (200, 201):
                self.ok += 1
            elif status == 429:
                self.limited += 1                     # rate-limited on purpose (not a failure)
            else:
                self.failed += 1
            self._results.append((time.time(), status, ms))
            if not write:
                self._route_seq += 1
                self._routes.append({"n": self._route_seq, "sid": body.get("server_id") if status == 200 else None,
                                     "ok": status == 200, "status": status, "retries": body.get("failover_retries", 0) or 0,
                                     "cache": body.get("cache")})

    def results_between(self, t0: float, t1: float):
        """Client-observed results (t, status, latency_ms) of requests completed in [t0, t1]."""
        with self._lock:
            return [r for r in self._results if t0 <= r[0] <= t1]

    def stop(self):
        self._stop.set()
        t = self._thread
        if t is not None and t.is_alive():
            t.join(timeout=2)
        pool = self._pool
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)
        with self._lock:
            self._thread = None
            self._pool = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def status(self) -> Dict[str, Any]:
        with self._lock:
            now = time.time()
            recent = [r for r in self._results if r[0] >= now - 5]
            lat = sorted(r[2] for r in recent if r[1] in (200, 201))

            def pct(p):
                if not lat:
                    return None
                return round(lat[min(len(lat) - 1, max(0, int(round(p / 100 * len(lat))) - 1))], 1)
            return {
                "running": self.running, "params": dict(self.params), "sent": self.sent, "ok": self.ok,
                "failed": self.failed, "rate_limited": self.limited, "not_sent_client_busy": self.not_sent,
                "inflight": self.inflight,
                "achieved_rps_5s": round(len(recent) / 5.0, 2),
                "success_rate_5s": round(sum(1 for r in recent if r[1] in (200, 201)) / len(recent), 3) if recent else None,
                "client_latency_ms_5s": {"p50": pct(50), "p95": pct(95)},
                "started_at": self.started_at, "stopped_at": self.stopped_at,
                "routes": list(self._routes)[-40:],
                "label": "client-observed (generator threads inside the proxy process)",
            }
