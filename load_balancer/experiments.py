"""
Repeatable experiments (run in a background thread, results saved as JSON in reports/).

1. Failover comparison - the same workload and the same failure scenario for every routing
   strategy: a fixed number of requests at a fixed rate and concurrency, from a fixed sequence
   of client addresses, with a fault injected into one backend at a fixed point of the run and
   cleared at another. Measured from the client side of each request: throughput, latency
   percentiles, success rate, retries, per-backend distribution.

2. Disaster-recovery comparison - for each recovery strategy (backup, replica, wal, none):
   reset the data tier, write the same records (some before and some after a backup), run the
   total-failure-with-data-loss scenario, and record RTO, RPO, restored records and integrity.

3. Utilisation vs latency - the three original servers receive random (Poisson) traffic at
   increasing fractions of their measured capacity (20 % ... 110 %). For each level: achieved
   throughput, latency percentiles, error rate and the backend-reported queue wait. Shows why
   servers are planned below 100 % utilisation (queueing delay grows sharply near saturation).

4. Resilience policy comparison - the same trace (low load, a spike, a backend failure during
   the spike, low load again; Poisson arrivals, popular items, heavy-hitter clients) under each
   policy: static over-provisioning, reactive scale-out only, and the cost-aware ladder. Reports
   SLO-violation time, client latency, errors, rejected / cached / degraded shares and extra
   server-seconds, plus a per-second timeline.

Limitations (also stated in every report): everything runs on one machine over loopback; the
backends are simulated with a configured service time; the load generator shares the CPU with
the proxy; numbers vary between runs and machines and do not predict production behaviour.
No strategy is "best" in general - the results only describe this workload and this fault.
"""

import os
import json
import math
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional

import requests

from load_balancer.metrics import percentile
from load_balancer.traffic import client_ip

LIMITATIONS = ("Single machine over loopback (127.0.0.1); simulated backend service time; the load generator "
               "shares the CPU with the proxy; results vary between runs and machines and are not production "
               "measurements. They describe this workload and this fault only - no strategy is best in general.")


class ExperimentRunner:
    def __init__(self, ctx: Dict[str, Any], report_dir: str):
        """ctx: base_url, registry, scheduler, faults, breakers, netsim, retry_budget, capacity, traffic,
        datastore, disaster, write_record(name, value) -> (status, body), metrics"""
        self.ctx = ctx
        self.report_dir = report_dir
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._cancel = threading.Event()
        self.state = {"running": False, "kind": None, "progress": None, "error": None}
        self.last_reports: Dict[str, Any] = {}

    # ------------------------------------------------------------------ #
    def busy(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def cancel(self):
        self._cancel.set()

    def _start(self, kind: str, fn, params):
        with self._lock:
            if self.busy():
                raise RuntimeError("an experiment is already running")
            self._cancel.clear()
            self.state = {"running": True, "kind": kind, "progress": "starting", "error": None,
                          "started_at": time.time(), "params": params}
            self._thread = threading.Thread(target=self._run, args=(kind, fn, params), daemon=True,
                                            name=f"Experiment-{kind}")
            self._thread.start()

    def _run(self, kind, fn, params):
        m = self.ctx["metrics"]
        try:
            report = fn(params)
            report["limitations"] = LIMITATIONS
            report["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            os.makedirs(self.report_dir, exist_ok=True)
            path = os.path.join(self.report_dir, f"{kind}_{time.strftime('%Y%m%d_%H%M%S')}.json")
            with open(path, "w") as f:
                json.dump(report, f, indent=2)
            report["file"] = os.path.relpath(path, os.path.dirname(self.report_dir))
            self.last_reports[kind] = report
            self.state.update(running=False, progress="finished")
            m.add_log(f"Experiment '{kind}' finished; report saved to {report['file']}.", level="SUCCESS",
                      event_type="EXPERIMENT")
        except Exception as ex:
            self.state.update(running=False, progress="failed", error=str(ex))
            m.add_log(f"Experiment '{kind}' stopped: {ex}", level="ERROR", event_type="EXPERIMENT")
        finally:
            cap = self.ctx.get("capacity")
            if cap is not None:
                cap.resume()

    def status(self) -> Dict[str, Any]:
        return {"state": dict(self.state, running=self.busy()), "reports": self.last_reports}

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #
    def _progress(self, text):
        self.state["progress"] = text

    def _check_cancel(self):
        if self._cancel.is_set():
            raise RuntimeError("cancelled")

    def _wait(self, cond: Callable[[], bool], timeout: float, what: str):
        end = time.time() + timeout
        while time.time() < end:
            self._check_cancel()
            if cond():
                return
            time.sleep(0.2)
        raise RuntimeError(f"timed out waiting for {what}")

    def _originals_routable(self):
        reg, br = self.ctx["registry"], self.ctx["breakers"]
        orig = [s for s in reg.get_all_servers() if not s.is_replacement]
        return all(s.status == "HEALTHY" and br.state(s.id) == "CLOSED" for s in orig)

    def _clean_baseline(self):
        c = self.ctx
        c["traffic"].stop()
        c["capacity"].pause("experiment running")
        c["faults"].reset()
        c["netsim"].reset()
        c["capacity"].retire_all_replacements()
        c["capacity"].activate_all_standby()
        self._wait(self._originals_routable, 20, "all original servers to be healthy")
        c["breakers"].reset()
        c["retry_budget"].reset()

    # ------------------------------------------------------------------ #
    # 1. Failover comparison                                              #
    # ------------------------------------------------------------------ #
    def start_failover(self, params: Dict[str, Any]):
        p = {"strategies": ["ROUND_ROBIN", "WEIGHTED_ROUND_ROBIN", "LEAST_CONNECTIONS", "WEIGHTED_LEAST_CONNECTIONS",
                            "LEAST_RESPONSE_TIME", "IP_HASH"],
             "requests": 120, "rps": 15.0, "concurrency": 6, "clients": 16, "fault": "refused",
             "target": "server-2", "fault_at": 0.3, "clear_at": 0.7, "seed": 42, "loss": 0.3, "delay_ms": 400}
        p.update({k: v for k, v in (params or {}).items() if v is not None})
        p["requests"] = int(p["requests"])
        p["rps"] = float(p["rps"])
        p["concurrency"] = int(p["concurrency"])
        p["clients"] = int(p["clients"])
        if not 10 <= p["requests"] <= 2000 or not 1 <= p["rps"] <= 100 or not 1 <= p["concurrency"] <= 32:
            raise ValueError("requests 10..2000, rps 1..100, concurrency 1..32")
        self._start("failover_comparison", self._failover, p)

    def _one(self, session, i, p):
        ip = client_ip(i % p["clients"])
        sent = time.time()
        t0 = time.perf_counter()
        try:
            r = session.get(f"{self.ctx['base_url']}/api/request", headers={"X-Forwarded-For": ip}, timeout=10)
            body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
            return {"i": i, "status": r.status_code, "ms": (time.perf_counter() - t0) * 1000.0,
                    "server": body.get("server_id"), "retries": body.get("failover_retries", 0) or 0,
                    "t": time.time(), "sent": sent}
        except Exception as ex:
            return {"i": i, "status": 0, "ms": (time.perf_counter() - t0) * 1000.0, "server": None, "retries": 0,
                    "error": type(ex).__name__, "t": time.time(), "sent": sent}

    def _failover(self, p):
        c = self.ctx
        prev_algo = c["scheduler"].current_algorithm
        results = []
        try:
            for n, algo in enumerate(p["strategies"], 1):
                self._progress(f"{n}/{len(p['strategies'])}: preparing {algo}")
                self._clean_baseline()
                c["scheduler"].set_algorithm(algo)
                c["scheduler"].reset()
                c["netsim"].reseed(p["seed"])
                self._progress(f"{n}/{len(p['strategies'])}: running {algo}")
                results.append(self._failover_run(algo, p))
                c["faults"].reset()
        finally:
            c["scheduler"].set_algorithm(prev_algo)
            c["faults"].reset()
        return {"kind": "failover_comparison", "params": p, "results": results,
                "note": "All values measured by the experiment client (requests through the proxy on port 5000)."}

    def _failover_run(self, algo, p):
        c = self.ctx
        N = p["requests"]
        fault_i, clear_i = int(N * p["fault_at"]), int(N * p["clear_at"])
        out: List[dict] = []
        local = threading.local()

        def session():
            s = getattr(local, "s", None)
            if s is None:
                s = local.s = requests.Session()
            return s

        injected_at = cleared_at = None
        t_start = time.time()
        with ThreadPoolExecutor(max_workers=p["concurrency"]) as pool:
            futures = []
            next_t = time.perf_counter()
            for i in range(N):
                self._check_cancel()
                if i == fault_i:
                    params = {"loss": p["loss"]} if p["fault"] == "packet_loss" else {"delay_ms": p["delay_ms"]}
                    c["faults"].inject(p["target"], p["fault"], params)
                    injected_at = time.time()
                if i == clear_i:
                    c["faults"].clear(p["target"])
                    cleared_at = time.time()
                delay = next_t - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)
                # bounded concurrency: wait for a free worker (closed loop once saturated)
                while sum(1 for f in futures if not f.done()) >= p["concurrency"]:
                    time.sleep(0.002)
                futures.append(pool.submit(lambda i=i: self._one(session(), i, p)))
                next_t += 1.0 / p["rps"]
            for f in futures:
                out.append(f.result())
        duration = max(o["t"] for o in out) - t_start
        ok = [o for o in out if o["status"] == 200]
        lat = [o["ms"] for o in ok]
        dist: Dict[str, int] = {}
        for o in ok:
            dist[o["server"]] = dist.get(o["server"], 0) + 1
        # requests SENT while the fault was active (a request already in flight when the fault was
        # injected may legitimately complete on the target and is not counted)
        during = [o for o in out if injected_at and o["sent"] >= injected_at and (cleared_at is None or o["sent"] <= cleared_at)]
        return {
            "strategy": algo, "requests": N, "duration_s": round(duration, 2),
            "throughput_rps": round(len(ok) / duration, 2) if duration > 0 else None,
            "success_rate": round(len(ok) / N, 4), "failed": N - len(ok),
            "failed_by_status": {str(k): sum(1 for o in out if o["status"] == k) for k in sorted({o["status"] for o in out if o["status"] != 200})},
            "latency_ms": {"p50": _r(percentile(lat, 50)), "p95": _r(percentile(lat, 95)), "p99": _r(percentile(lat, 99)),
                           "max": _r(max(lat) if lat else None)},
            "retries": sum(o["retries"] for o in out),
            "distribution": dict(sorted(dist.items())),
            "during_fault": {"requests": len(during), "succeeded": sum(1 for o in during if o["status"] == 200),
                             "served_by_target": sum(1 for o in during if o["server"] == p["target"])},
            "fault": {"type": p["fault"], "target": p["target"], "injected_at_request": fault_i,
                      "cleared_at_request": clear_i},
        }

    # ------------------------------------------------------------------ #
    # 3. Utilisation vs latency                                           #
    # ------------------------------------------------------------------ #
    def start_utilisation(self, params: Dict[str, Any]):
        p = {"levels": [0.2, 0.4, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1], "seconds": 12.0, "concurrency": 64, "seed": 7,
             "algorithm": "ROUND_ROBIN"}
        p.update({k: v for k, v in (params or {}).items() if v is not None})
        p["levels"] = [float(x) for x in p["levels"]]
        p["seconds"] = float(p["seconds"])
        if not p["levels"] or any(not 0.05 <= x <= 1.5 for x in p["levels"]) or not 3 <= p["seconds"] <= 30:
            raise ValueError("levels 0.05..1.5, seconds 3..30")
        self._start("utilisation", self._utilisation, p)

    def _utilisation(self, p):
        c = self.ctx
        prev_algo = c["scheduler"].current_algorithm
        res = c.get("resilience")
        prev_policy = res.policy if res is not None else None
        rng = random.Random(p["seed"])
        results = []
        try:
            self._clean_baseline()
            if res is not None:
                res.set_policy("scale_only")      # no mitigations while measuring raw queueing behaviour
            c["scheduler"].set_algorithm(p["algorithm"])
            c["scheduler"].reset()
            servers = [s for s in c["registry"].get_all_servers() if not s.is_replacement]
            n = len(servers)
            # capacity per server: measured from backend reports when available (as the capacity manager)
            est = c["capacity"].estimate()
            cap = est["capacity_rps_per_server"]
            for k, level in enumerate(p["levels"], 1):
                self._progress(f"{k}/{len(p['levels'])}: {level:.0%} of capacity")
                rps = level * cap * n
                total = max(10, int(round(rps * p["seconds"])))
                out = self._poisson_run(rps, total, p["concurrency"], rng)
                ok = [o for o in out if o["status"] == 200]
                lat = [o["ms"] for o in ok]
                dur = max(o["t"] for o in out) - min(o["sent"] for o in out)
                svc = [o.get("queue_ms") for o in ok if o.get("queue_ms") is not None]
                results.append({
                    "target_utilization": level, "offered_rps": round(rps, 2), "requests": len(out),
                    "throughput_rps": round(len(ok) / dur, 2) if dur > 0 else None,
                    "success_rate": round(len(ok) / len(out), 4), "errors": len(out) - len(ok),
                    "latency_ms": {"p50": _r(percentile(lat, 50)), "p95": _r(percentile(lat, 95)),
                                   "p99": _r(percentile(lat, 99))},
                    "queue_wait_ms_mean": _r(sum(svc) / len(svc)) if svc else None,
                })
                time.sleep(1.5)                      # let queues drain between levels
        finally:
            c["scheduler"].set_algorithm(prev_algo)
            if res is not None:
                res.set_policy(prev_policy)
        base = results[0]["latency_ms"]["p95"] if results and results[0]["latency_ms"]["p95"] else None
        return {"kind": "utilisation", "params": p, "servers": n, "capacity_rps_per_server": cap,
                "capacity_source": est.get("capacity_source"), "results": results, "baseline_p95_ms": base,
                "note": ("Poisson arrivals (random gaps, like independent users) through the proxy to the original "
                         "servers, Round Robin. Utilisation = offered load / (servers x measured capacity).")}

    def _poisson_run(self, rps, total, concurrency, rng):
        """Open-loop Poisson arrivals at `rps` (exponential gaps), bounded by `concurrency` workers."""
        out: List[dict] = []
        local = threading.local()

        def session():
            s = getattr(local, "s", None)
            if s is None:
                s = local.s = requests.Session()
            return s

        def one(i):
            sent = time.time()
            t0 = time.perf_counter()
            try:
                r = session().get(f"{self.ctx['base_url']}/api/request", headers={"X-Forwarded-For": client_ip(i % 64)},
                                  timeout=10)
                body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
                br = body.get("backend_response") or {}
                return {"status": r.status_code, "ms": (time.perf_counter() - t0) * 1000.0, "t": time.time(),
                        "sent": sent, "queue_ms": br.get("queue_ms")}
            except Exception:
                return {"status": 0, "ms": (time.perf_counter() - t0) * 1000.0, "t": time.time(), "sent": sent}

        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = []
            next_t = time.perf_counter()
            for i in range(total):
                self._check_cancel()
                delay = next_t - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)
                futures.append(pool.submit(one, i))
                next_t += rng.expovariate(rps)
            for f in futures:
                out.append(f.result())
        return out

    # ------------------------------------------------------------------ #
    # 4. Resilience policy comparison                                     #
    # ------------------------------------------------------------------ #
    def start_policies(self, params: Dict[str, Any]):
        p = {"policies": ["static", "scale_only", "ladder"],
             "phases": [[15, 12], [45, 40], [20, 12]],           # [seconds, req/s]
             "fault": "refused", "target": "server-2", "fault_at": 20, "clear_at": 60,
             "keys": 200, "heavy": 0.4, "heavy_clients": 2, "clients": 32, "concurrency": 32, "seed": 11,
             "static_servers": 4}
        p.update({k: v for k, v in (params or {}).items() if v is not None})
        if any(x not in ("static", "scale_only", "ladder", "cache_always") for x in p["policies"]):
            raise ValueError("policies must be static, scale_only, cache_always or ladder")
        total = sum(int(a) for a, _ in p["phases"])
        if not 10 <= total <= 600 or any(not 0 < float(r) <= 100 for _, r in p["phases"]):
            raise ValueError("phases: total 10..600 s, rates 0..100 req/s")
        self._start("policy_comparison", self._policies, p)

    def _policies(self, p):
        c = self.ctx
        res, prev_policy = c["resilience"], c["resilience"].policy
        results = []
        try:
            for n, policy in enumerate(p["policies"], 1):
                tag = f"{n}/{len(p['policies'])} {policy}"
                self._progress(f"{tag}: preparing")
                self._clean_baseline()
                c["cache"].reset()
                c["limiter"].reset()
                c["balancer"].degrade = False
                res.reset()
                res.set_policy(policy, static_servers=p["static_servers"])
                c["capacity"].resume()
                tr = c["traffic"]
                common = dict(concurrency=p["concurrency"], clients=p["clients"], keys=p["keys"], heavy=p["heavy"],
                              heavy_clients=p["heavy_clients"], poisson=True, seed=p["seed"])
                if policy == "static":
                    self._progress(f"{tag}: provisioning {p['static_servers']} servers before the trace")
                    tr.start(rps=float(p["phases"][0][1]), **common)
                    self._wait(lambda: len(c["registry"].get_active_pool()) >= p["static_servers"], 90,
                               f"{p['static_servers']} servers for the static policy")
                    tr.stop()
                    time.sleep(1.0)
                res.reset_costs()
                counters0 = dict(c["metrics"].counters)
                t0 = time.time()
                tr.start(rps=float(p["phases"][0][1]), **common)
                timeline = []
                bounds, acc = [], 0
                for secs, rate in p["phases"]:
                    bounds.append((acc, acc + int(secs), float(rate)))
                    acc += int(secs)
                injected = cleared = False
                current_rate = float(p["phases"][0][1])
                for sec in range(acc):
                    self._check_cancel()
                    rate = next(r for a, b, r in bounds if a <= sec < b)
                    if rate != current_rate:
                        tr.update_rate(rate)
                        current_rate = rate
                    if p["fault"] and not injected and sec >= p["fault_at"]:
                        c["faults"].inject(p["target"], p["fault"], {})
                        injected = True
                    if p["fault"] and injected and not cleared and sec >= p["clear_at"]:
                        c["faults"].clear(p["target"])
                        cleared = True
                    self._progress(f"{tag}: {sec}/{acc} s")
                    time.sleep(max(0.0, t0 + sec + 1 - time.time()))
                    w = c["metrics"].window_stats(2.0)
                    st = res.status()
                    timeline.append({"t": sec + 1, "offered_rps": rate, "p95_ms": _r(w["p95_ms"]),
                                     "error_rate": round(w["error_rate"], 4), "backend_rps": round(w["backend_rps"], 2),
                                     "routable": len(c["registry"].get_active_pool()), "level": st["level"],
                                     "replacements": sum(1 for s in c["registry"].get_all_servers() if s.is_replacement)})
                t1 = time.time()
                tr.stop()
                if p["fault"] and not cleared:
                    c["faults"].clear(p["target"])
                time.sleep(0.5)
                rows = c["traffic"].results_between(t0, t1 + 0.5)
                sent = len(rows)
                ok = [r for r in rows if r[1] in (200, 201)]
                limited = sum(1 for r in rows if r[1] == 429)
                failed = sent - len(ok) - limited
                lat = [r[2] for r in ok]
                d = {k: c["metrics"].counters.get(k, 0) - counters0.get(k, 0)
                     for k in ("cache_hits", "cache_stale_served", "rate_limited", "degraded_responses")}
                costs = res.cost_summary()
                lim = c["limiter"].status()
                rejected_by = {x["client"]: x["rejected"] for x in lim["top_clients"] if x["rejected"]}
                heavy_ips = {client_ip(i) for i in range(int(p["heavy_clients"]))} if p["heavy"] else set()
                results.append({
                    "policy": policy, "duration_s": round(t1 - t0, 1), "requests": sent,
                    "client_latency_ms": {"p50": _r(percentile(lat, 50)), "p95": _r(percentile(lat, 95)),
                                          "p99": _r(percentile(lat, 99))},
                    "error_rate": round(failed / sent, 4) if sent else None,
                    "rejected_share": round(limited / sent, 4) if sent else None,
                    "cache_share": round((d["cache_hits"] + d["cache_stale_served"]) / sent, 4) if sent else None,
                    "degraded_share": round(d["degraded_responses"] / sent, 4) if sent else None,
                    "rejected_heavy_clients": sum(v for k, v in rejected_by.items() if k in heavy_ips),
                    "rejected_other_clients": c["limiter"].rejected - sum(v for k, v in rejected_by.items() if k in heavy_ips),
                    "slo_violation_s": round(costs["slo_violation_seconds"], 1),
                    "slo_violation_pct": round(100 * costs["slo_violation_seconds"] / max(1e-9, costs["seconds"]), 1),
                    "extra_server_seconds": round(costs["extra_server_seconds"], 1),
                    "weighted_cost": costs["weighted_cost"],
                    "decisions": [{"time": x["time"], "action": x["action"], "rung": x["rung"], "reason": x["reason"]}
                                  for x in res.status()["decisions"]][::-1],
                    "timeline": timeline,
                })
        finally:
            c["traffic"].stop()
            c["faults"].reset()
            c["cache"].reset()
            c["limiter"].reset()
            c["balancer"].degrade = False
            res.set_policy(prev_policy)
        return {"kind": "policy_comparison", "params": p, "results": results,
                "slo": {"p95_ms": res.cfg.RESILIENCE_SLO_P95_MS, "error_rate": res.cfg.RESILIENCE_SLO_ERROR},
                "cost_weights": res.cfg.RESILIENCE_COST_WEIGHTS,
                "note": ("Same trace for every policy: Poisson arrivals, Zipf-popular items, heavy-hitter clients, "
                         "and a backend failure during the spike. Client-side numbers come from the traffic generator; "
                         "SLO-violation time and extra server-seconds from the controller's per-second accounting.")}

    # ------------------------------------------------------------------ #
    # 2. Disaster-recovery comparison                                    #
    # ------------------------------------------------------------------ #
    def start_dr(self, params: Dict[str, Any]):
        p = {"strategies": ["backup", "replica", "wal", "none"], "records_before_backup": 20,
             "records_after_backup": 10, "updates_after_backup": 3, "accidental_delete": False,
             "corrupt": None, "fault": "refused", "timeout": 45}
        p.update({k: v for k, v in (params or {}).items() if v is not None})
        for k in ("records_before_backup", "records_after_backup", "updates_after_backup"):
            p[k] = int(p[k])
            if not 0 <= p[k] <= 500:
                raise ValueError(f"{k} must be 0..500")
        if p["corrupt"] not in (None, "", "snapshot", "wal", "replica"):
            raise ValueError("corrupt must be snapshot, wal or replica")
        self._start("dr_comparison", self._dr, p)

    def _dr(self, p):
        c = self.ctx
        results = []
        try:
            for n, strategy in enumerate(p["strategies"], 1):
                self._progress(f"{n}/{len(p['strategies'])}: preparing data for '{strategy}'")
                c["disaster"].reset()
                self._clean_baseline()
                c["datastore"].reset()
                written = 0
                for i in range(p["records_before_backup"]):
                    self._write(f"order-{i}", f"amount={100 + i}")
                    written += 1
                c["datastore"].snapshot("experiment backup")
                for i in range(p["records_after_backup"]):
                    self._write(f"order-late-{i}", f"amount={500 + i}")
                    written += 1
                for i in range(min(p["updates_after_backup"], p["records_before_backup"])):
                    c["datastore"].update(i + 1, f"amount={100 + i} (updated)")
                if p["accidental_delete"] and p["records_before_backup"]:
                    c["datastore"].delete(1)
                if p["corrupt"]:
                    c["datastore"].corrupt(p["corrupt"])
                acked = len(c["datastore"].ledger)
                self._progress(f"{n}/{len(p['strategies'])}: disaster + recovery with '{strategy}'")
                c["disaster"].start(strategy, p["fault"])
                self._wait(lambda: c["disaster"].state in ("RESTORED", "DATA_LOSS_ACK_REQUIRED", "FAILED"),
                           p["timeout"], "the recovery to finish")
                needed_ack = c["disaster"].state != "RESTORED"
                if needed_ack:
                    c["disaster"].acknowledge()
                # first client request after recovery -> RTO
                end = time.time() + 15
                while time.time() < end and c["disaster"].status()["timings_ms"]["rto"] is None:
                    try:
                        requests.get(f"{c['base_url']}/api/request", timeout=5)
                    except Exception:
                        pass
                    time.sleep(0.1)
                st = c["disaster"].status()
                rep = st["data_report"] or {}
                results.append({
                    "strategy": strategy, "acknowledged_records": acked,
                    "restored_records": rep.get("restored_records"),
                    "lost_records": rep.get("rpo_records"), "lost_writes": rep.get("rpo_writes"),
                    "rpo_seconds": rep.get("rpo_seconds"), "missing_ids": rep.get("missing_records"),
                    "stale_ids": rep.get("stale_records"), "resurrected_ids": rep.get("unexpected_records"),
                    "integrity": rep.get("integrity"), "source": rep.get("source"), "notes": rep.get("notes"),
                    "zero_data_loss": rep.get("zero_data_loss"), "operator_ack_required": needed_ack,
                    "timings_ms": st["timings_ms"],
                })
                c["faults"].reset()
        finally:
            c["disaster"].reset()
            c["faults"].reset()
        return {"kind": "dr_comparison", "params": p, "results": results,
                "note": ("RPO is measured against the in-memory ledger of acknowledged writes (measurement only). "
                         "RTO = disaster start -> first client request served after recovery, on this machine.")}

    def _write(self, name, value):
        status, body = self.ctx["write_record"](name, value)
        if status not in (200, 201):
            raise RuntimeError(f"write failed during experiment setup: HTTP {status} {body.get('message')}")


def _r(v):
    return None if v is None else round(v, 2)
