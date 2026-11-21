"""
Core Load Balancer Engine.

Handles:
1. Routing incoming client requests to healthy backends.
2. In-flight failure detection and automatic failover retries.
3. Returning HTTP 503 when no healthy servers are in the pool.
4. Measuring real request latencies and recording metrics.

Network-level protections added on top of the original design:
- Admission control: at most PROXY_MAX_INFLIGHT client requests are processed at once;
  beyond that the proxy answers 503 immediately (load shedding, no unbounded queue).
- Data-recovery gate: while a disaster recovery has not verified the restored data,
  requests are not routed (503).
- Circuit breaker per backend (passive health): backends with an OPEN circuit are skipped.
- Failure classification: connection refused, connect timeout, read timeout,
  HTTP 5xx, backend overloaded, simulated partition / packet loss.
- Retry control: idempotency-aware retries, exponential backoff with jitter, and a
  shared retry budget (see retry_policy.py).
"""

import time
import threading
import requests
from typing import Tuple, Dict, Any, Callable, Optional
from load_balancer.server_registry import ServerRegistry
from load_balancer.scheduler import SchedulerManager
from load_balancer.metrics import MetricsCollector
from load_balancer.circuit_breaker import CircuitBreakerRegistry
from load_balancer.retry_policy import RetryBudget, failure_kind, safe_to_retry, backoff_seconds
from load_balancer import netsim as _netsim
import config

COUNTER_FOR_KIND = {
    "timeout": "timeouts", "connect_timeout": "connect_timeouts", "connection_refused": "connection_refused",
    "http_5xx": "http_5xx", "packet_loss": "simulated_packet_loss", "partition": "simulated_partition",
    "backend_overloaded": "backend_overloaded",
}


class LoadBalancer:
    """Core load balancer coordinating scheduling, proxying, and failover."""

    def __init__(self, registry: ServerRegistry, scheduler: SchedulerManager, metrics: MetricsCollector,
                 breakers: Optional[CircuitBreakerRegistry] = None, retry_budget: Optional[RetryBudget] = None,
                 netsim=None, sleep: Callable[[float], None] = time.sleep, cache=None, limiter=None):
        self.registry = registry
        self.scheduler = scheduler
        self.metrics = metrics
        self.request_timeout = config.REQUEST_TIMEOUT
        self.max_retries = config.MAX_FAILOVER_RETRIES
        self.breakers = breakers or CircuitBreakerRegistry(
            failure_threshold=config.CIRCUIT_FAILURE_THRESHOLD, open_seconds=config.CIRCUIT_OPEN_SECONDS,
            half_open_max_trials=config.CIRCUIT_HALF_OPEN_MAX_TRIALS,
            half_open_successes=config.CIRCUIT_HALF_OPEN_SUCCESSES)
        self.retry_budget = retry_budget or RetryBudget(config.RETRY_BUDGET_RATIO, config.RETRY_BUDGET_MIN_TOKENS,
                                                        config.RETRY_BUDGET_MAX_TOKENS)
        self.netsim = netsim
        self._sleep = sleep
        self._inflight = 0
        self._inflight_lock = threading.Lock()
        self.routing_gate: Optional[Callable[[], Optional[str]]] = None   # returns a reason to block, or None
        self.on_request_failure: Optional[Callable[[str, str], None]] = None
        self.on_request_success: Optional[Callable[[str, float], None]] = None
        # Cheaper alternatives to adding servers (switched by the resilience controller)
        self.cache = cache                  # ResponseCache: fresh hits + stale-if-error
        self.limiter = limiter              # HeavyHitterLimiter: per-client 429
        self.degrade = False                # brownout: ask backends to skip optional work (X-Degrade: 1)

    # ------------------------------------------------------------------ #
    def _net(self):
        return self.netsim if self.netsim is not None else _netsim.NETSIM

    def _admit(self) -> bool:
        with self._inflight_lock:
            if self._inflight >= config.PROXY_MAX_INFLIGHT:
                return False
            self._inflight += 1
            return True

    def _leave(self):
        with self._inflight_lock:
            self._inflight = max(0, self._inflight - 1)

    @property
    def inflight(self) -> int:
        return self._inflight

    def _error(self, client_ip, code, message, **extra):
        self.metrics.record_request_result(server_id="NONE", success=False, latency_ms=0.0, client_ip=client_ip)
        body = {"status": "error", "code": code, "error": "Service Unavailable", "message": message,
                "algorithm": self.scheduler.current_algorithm}
        body.update(extra)
        return code, body

    # ------------------------------------------------------------------ #
    def handle_request(self, client_ip: str = "127.0.0.1", method: str = "GET",
                       payload: Dict = None, headers: Dict = None, cache_key: Optional[str] = None,
                       rate_limit: bool = True, trace: Optional[list] = None) -> Tuple[int, Dict[str, Any]]:
        """
        Process an incoming client request:
        1. Select a healthy backend using the active scheduling algorithm.
        2. Dispatch request to backend /process endpoint.
        3. If selected backend fails mid-request, automatically failover to another healthy backend.
        4. If no healthy backends exist, return HTTP 503.
        """
        t_start = time.perf_counter()
        T = self._tracer(trace, t_start)
        self.metrics.count("client_requests")
        # 1) per-client rate limiting (heavy hitters get HTTP 429 without using a backend)
        if self.limiter is not None and rate_limit:
            self.limiter.note(client_ip)
            allowed = self.limiter.allow(client_ip)
            T("rate_limit", "429 Too Many Requests" if not allowed else "pass",
              (f"limit {self.limiter.limit_rps:g} req/s per client" if self.limiter.enabled else "rate limiting is off"))
            if not allowed:
                self.metrics.count("rate_limited")
                return 429, {"status": "error", "code": 429, "error": "Too Many Requests",
                             "message": "Rate limit for this client exceeded (heavy hitter); retry later.",
                             "retry_after_s": 1, "client_ip": client_ip}
        # 2) fresh cache hit: answered by the proxy
        cacheable = self.cache is not None and cache_key is not None and method.upper() == "GET"
        if cacheable:
            self.cache.note(cache_key)                 # predicted hit ratio, also while caching is off
            hit = self.cache.fresh(cache_key)
            T("cache", ("HIT" if hit is not None else "MISS") if self.cache.fresh_enabled else "off",
              f"item {cache_key}" + (f", cached {hit[1]:.2f} s ago — answered by the proxy, no backend used" if hit is not None else ""))
            if hit is not None:
                body, age = hit
                body.update(cache="HIT", cache_age_s=round(age, 3))
                self.metrics.count("cache_hits")
                e2e = (time.perf_counter() - t_start) * 1000.0
                self.metrics.record_e2e(e2e, True)
                body["proxy_latency_ms"] = round(e2e, 2)
                body.pop("trace", None)
                return 200, body
        elif trace is not None:
            T("cache", "not cacheable", "only GET requests for an item (?item=K) can be cached")
        # 3) everything else goes to a backend: this is the demand the servers must carry
        self.metrics.record_arrival()
        self.retry_budget.on_request()
        if not self._admit():
            self.metrics.count("shed_proxy_overload")
            code, body = self._error(client_ip, 503, "PROXY OVERLOADED: too many concurrent requests (load shed)",
                                     max_inflight=config.PROXY_MAX_INFLIGHT)
            self.metrics.record_e2e((time.perf_counter() - t_start) * 1000.0, False)
            return code, body
        try:
            code, body = self._route(client_ip, method, payload, headers, T)
        finally:
            self._leave()
        if cacheable:
            if code == 200:
                self.cache.put(cache_key, body)
            elif not body.get("data_recovery"):
                stale = self.cache.stale(cache_key)            # stale-if-error (never during data recovery)
                if stale is not None:
                    code, body = 200, dict(stale[0], cache="STALE", cache_age_s=round(stale[1], 3),
                                          stale_reason=body.get("message"))
                    self.metrics.count("cache_stale_served")
                    T("stale-if-error", "200 from cache", f"every backend attempt failed; answered with a copy {stale[1]:.1f} s old")
        if code == 200 and body.get("cache") is None and isinstance(body.get("backend_response"), dict) \
                and body["backend_response"].get("degraded"):
            self.metrics.count("degraded_responses")
        e2e = (time.perf_counter() - t_start) * 1000.0
        self.metrics.record_e2e(e2e, code == 200)
        body["proxy_latency_ms"] = round(e2e, 2)
        return code, body

    @staticmethod
    def _tracer(trace, t_start):
        """Return a function that appends one step to `trace` (no-op when not tracing)."""
        if trace is None:
            return lambda *a, **k: None

        def add(step, result, detail="", **extra):
            trace.append(dict(step=step, result=result, detail=detail,
                              t_ms=round((time.perf_counter() - t_start) * 1000.0, 2), **extra))
        return add

    def _route(self, client_ip, method, payload, headers, T=None):
        T = T or (lambda *a, **k: None)
        block = self.routing_gate() if self.routing_gate else None
        if block:
            T("routing", "blocked", block)
            self.metrics.count("blocked_data_recovery")
            return self._error(client_ip, 503, block, data_recovery=True)

        active_pool = self.registry.get_active_pool()

        # Fault tolerance check: No servers available
        T("pool", f"{len(active_pool)} healthy", ", ".join(s.name for s in active_pool) or "none",
          servers=[{"name": s.name, "port": s.port, "active_connections": s.active_connections, "weight": s.weight,
                    "circuit_ok": self.breakers.can_route(s.id)} for s in active_pool])
        if not active_pool:
            self.metrics.count("no_backend_503")
            return self._error(client_ip, 503, "NO HEALTHY BACKEND SERVERS AVAILABLE IN ACTIVE POOL", active_servers=0)

        eligible = [s for s in active_pool if self.breakers.can_route(s.id)]
        if not eligible:
            self.metrics.count("circuit_rejected")
            return self._error(client_ip, 503, "All healthy backends have an OPEN circuit breaker; failing fast.",
                               active_servers=len(active_pool))

        tried_servers = set()
        attempt = 0
        max_attempts = min(len(eligible), self.max_retries + 1)
        last_kind = None
        stop_reason = None

        while attempt < max_attempts:
            # Candidate pool excluding servers already attempted in this request
            candidate_pool = [s for s in eligible if s.id not in tried_servers and self.breakers.can_route(s.id)]
            if not candidate_pool:
                break

            # Select backend server via active scheduling policy
            selected_server = self.scheduler.select_server(candidate_pool, client_ip=client_ip)
            if not selected_server:
                break
            tried_servers.add(selected_server.id)
            T("select", selected_server.name, f"{self.scheduler.current_algorithm} chose it from "
              f"{', '.join(s.name for s in candidate_pool)}", port=selected_server.port)

            if not self.breakers.acquire(selected_server.id):
                continue                                   # circuit opened meanwhile: not an attempt
            # Active connection = request being forwarded to this backend (Least Connections);
            # refused atomically if the server was drained or failed since the pool snapshot.
            if not self.registry.try_connection_started(selected_server.id):
                self.breakers.release(selected_server.id)
                continue
            attempt += 1

            target_url = f"{selected_server.url}/process"
            req_headers = {"X-Forwarded-For": client_ip}
            if headers:
                req_headers.update({k: v for k, v in headers.items() if k.lower() not in ("host", "content-length")})
            if self.degrade:
                req_headers["X-Degrade"] = "1"

            success_latency = None
            kind = None
            t0 = time.perf_counter()
            try:
                self._net().before_send(selected_server.id, timeout=self.request_timeout, kind="request")
                timeout = (config.CONNECT_TIMEOUT, self.request_timeout)
                if method.upper() == "POST":
                    resp = requests.post(target_url, json=payload, headers=req_headers, timeout=timeout)
                else:
                    resp = requests.get(target_url, headers=req_headers, timeout=timeout)

                latency_ms = (time.perf_counter() - t0) * 1000.0
                T("forward", f"HTTP {resp.status_code}", f"{method.upper()} {target_url} — attempt {attempt}",
                  ms=round(latency_ms, 2), port=selected_server.port)

                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict):
                        T("backend", selected_server.name,
                          f"waited {data.get('queue_ms', 0):.1f} ms in its queue, worked {data.get('service_ms', 0):.1f} ms"
                          + (" (brownout: lighter response)" if data.get("degraded") else ""),
                          queue_ms=data.get("queue_ms"), service_ms=data.get("service_ms"))
                    success_latency = latency_ms
                    self.breakers.record_success(selected_server.id)
                    # Increment server statistics
                    self.registry.increment_request_count(selected_server.id)
                    # Record telemetry
                    self.metrics.record_request_result(
                        server_id=selected_server.id,
                        success=True,
                        latency_ms=latency_ms,
                        client_ip=client_ip
                    )
                    if isinstance(data, dict):
                        svc = data.get("service_ms")
                        if data.get("degraded") and svc is not None:
                            # brownout response: report the full-work equivalent, so the capacity estimate
                            # is not inflated by the lighter responses the controller itself switched on
                            svc = float(svc) / max(0.05, float(getattr(config, "BACKEND_DEGRADED_FACTOR", 0.4)))
                        self.metrics.record_backend_report(selected_server.id, svc, data.get("queue_ms"))
                    if attempt > 1:
                        self.metrics.count("retries_succeeded")
                    if self.on_request_success:
                        self.on_request_success(selected_server.id, latency_ms)

                    return 200, {
                        "status": "success",
                        "server": selected_server.name,
                        "server_id": selected_server.id,
                        "port": selected_server.port,
                        "backend_response": data,
                        "roundtrip_latency_ms": round(latency_ms, 2),
                        "algorithm": self.scheduler.current_algorithm,
                        "failover_retries": attempt - 1,
                        "attempts": attempt,
                    }
                else:
                    overloaded = str(resp.headers.get("X-Backend-Overloaded", "")) == "1" if hasattr(resp, "headers") else False
                    kind = failure_kind(status_code=resp.status_code, overloaded=overloaded)
                    # Backend returned non-200 (e.g. 503 from injected failure)
                    self.metrics.add_log(
                        f"In-flight request to {selected_server.name} returned HTTP {resp.status_code}. Attempting automatic failover...",
                        level="WARN",
                        event_type="FAILOVER_TRIGGERED"
                    )

            except (requests.RequestException, Exception) as ex:
                kind = failure_kind(exc=ex)
                T("forward", kind, f"{method.upper()} {target_url} — attempt {attempt}: {type(ex).__name__}",
                  ms=round((time.perf_counter() - t0) * 1000.0, 2), port=selected_server.port)
                self.metrics.add_log(
                    f"In-flight request to {selected_server.name} failed ({kind}: {type(ex).__name__}). Attempting automatic failover...",
                    level="WARN",
                    event_type="FAILOVER_TRIGGERED"
                )
            finally:
                self.registry.connection_finished(selected_server.id, success_latency)

            # ---- the attempt failed ----
            last_kind = kind
            if kind in COUNTER_FOR_KIND:
                self.metrics.count(COUNTER_FOR_KIND[kind])
            if kind == "backend_overloaded":
                self.breakers.release(selected_server.id)          # load shedding is not a fault
            else:
                self.breakers.record_failure(selected_server.id, kind)
            if self.on_request_failure:
                self.on_request_failure(selected_server.id, kind)

            if attempt >= max_attempts:
                break
            if not safe_to_retry(method, kind, config.IDEMPOTENT_METHODS):
                self.metrics.count("not_retried_non_idempotent")
                stop_reason = f"{method} request not retried after {kind}: it may already have been executed"
                break
            if not self.retry_budget.try_spend():
                self.metrics.count("retries_denied_budget")
                stop_reason = "retry budget exhausted (protection against retry storms)"
                break
            self.metrics.count("retries_attempted")
            back = backoff_seconds(attempt, config.RETRY_BACKOFF_BASE_MS, config.RETRY_BACKOFF_CAP_MS)
            T("retry", "try another server", f"{last_kind} is safe to retry for {method.upper()}; backoff {back * 1000:.0f} ms")
            self._sleep(back)

        # All eligible attempts failed
        if stop_reason:
            T("retry", "stopped", stop_reason)
        return self._error(client_ip, 503, "All attempted backend servers failed to respond.",
                           attempts=attempt, last_failure=last_kind, stop_reason=stop_reason)
