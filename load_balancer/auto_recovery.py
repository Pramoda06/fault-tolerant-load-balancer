"""
Automatic Backend Recovery with Dynamic Replacement Server Provisioning.

STAGE 1 of recovery: restore service with a MINIMUM VIABLE replacement. How many
servers are needed afterwards is decided by the load-aware capacity manager
(capacity.py, stage 2) from demand measured after service is restored.

When the reverse proxy has ZERO healthy backends, this manager:

  1. records the outage                         (event NO_HEALTHY_BACKENDS)
  2. starts automatic recovery                  (AUTO_RECOVERY_STARTED)
  3. picks the next server identity and a free local port
  4. launches a new backend process             (REPLACEMENT_STARTED, real PID)
     python backend_servers/replacement_server.py --id server-N --port P
  5. waits until it answers and passes the same GET /health check the
     health monitor uses, RECOVERY_THRESHOLD times in a row (REPLACEMENT_HEALTHY)
  6. registers it in the ServerRegistry         (REPLACEMENT_REGISTERED)
     -> it is HEALTHY, so it is immediately part of the active routing pool
                                                (REPLACEMENT_ADDED)
  7. reports the first client request it serves (TRAFFIC_RESTORED)

Safeguards against runaway provisioning:
  - only one provisioning at a time (in-progress flag under a lock)
  - nothing is provisioned while any backend is HEALTHY or RECOVERING
  - at most `max_replacements` replacement processes alive at once
  - at most `max_attempts` failed provisioning attempts per outage, with a
    cooldown between attempts; counters reset once any backend is healthy again
  - a replacement that fails to start or fails its health checks is killed and
    never registered
"""

import os
import sys
import time
import socket
import threading
import subprocess
from typing import Callable, Dict, Optional, Any

from load_balancer.server_registry import ServerRegistry
from load_balancer.metrics import MetricsCollector
from load_balancer.health_monitor import probe_health
from load_balancer.provisioning import (InstanceAllocator, ProvisionGate, is_port_free, launch_backend_process,
                                        stop_process, control_port_for)


class ProvisioningError(Exception):
    pass


class _Cancelled(Exception):
    pass


class AutoRecoveryManager:
    STAGES = ("detected", "provisioning", "started", "health_check", "healthy", "registered", "routing", "restored")

    def __init__(self, registry: ServerRegistry, metrics: MetricsCollector, project_root: str,
                 enabled: bool = True, host: str = "127.0.0.1",
                 port_range=(5004, 5050), max_replacements: int = 2, max_attempts: int = 2,
                 retry_cooldown: float = 5.0, startup_timeout: float = 15.0,
                 health_timeout: float = 1.0, health_passes: int = 2, weight: int = 2,
                 launcher: Optional[Callable] = None, prober: Optional[Callable] = None,
                 port_checker: Optional[Callable] = None, allocator: Optional[InstanceAllocator] = None,
                 gate: Optional[ProvisionGate] = None):
        self.registry = registry
        self.metrics = metrics
        self.project_root = project_root
        self.enabled = enabled
        self.host = host
        self.port_range = port_range
        self.max_replacements = max_replacements
        self.max_attempts = max_attempts
        self.retry_cooldown = retry_cooldown
        self.startup_timeout = startup_timeout
        self.health_timeout = health_timeout
        self.health_passes = health_passes
        self.weight = weight

        self._launch = launcher or self._launch_process
        self._probe = prober or (lambda url: probe_health(url, self.health_timeout)[:2])
        self._port_free = port_checker or self._is_port_free
        self.allocator = allocator or InstanceAllocator()
        self.gate = gate or ProvisionGate()

        self._lock = threading.RLock()
        self._procs: Dict[str, Any] = {}          # server_id -> Popen of replacement processes
        self._episode: Optional[Dict[str, Any]] = None
        self._generation = 0                      # bumped by reset() to cancel work in flight
        self._in_progress = False
        self._failed_attempts = 0
        self._last_failure_at = 0.0
        self._outage_logged = False
        self._limit_logged = False
        self._awaiting_traffic: Optional[str] = None
        self._thread: Optional[threading.Thread] = None
        self._paused = False                      # set while a simulation reset is in progress
        self._blocked_reason: Optional[str] = None  # why recovery is not provisioning during this outage

    # ------------------------------------------------------------------ #
    # Trigger (called by the health monitor after every check cycle)       #
    # ------------------------------------------------------------------ #
    def evaluate(self):
        if not self.enabled:
            return
        with self._lock:
            if self._paused:
                return
            servers = self.registry.get_all_servers()
            if any(s.status == "HEALTHY" for s in servers):
                # Outage over (or never happened): re-arm the per-outage safeguards.
                self._failed_attempts = 0
                self._outage_logged = False
                self._limit_logged = False
                self._blocked_reason = None
                return
            if self._in_progress:
                return
            if any(s.status == "RECOVERING" for s in servers):
                return  # a backend is already coming back; give it the chance
            if self.gate.busy_by_other("auto-recovery"):
                return  # the capacity manager is already starting a backend; it will restore service

            if not self._outage_logged:
                self._outage_logged = True
                self.metrics.add_log(
                    f"No healthy backend servers available (0/{len(servers)}).",
                    level="ERROR", event_type="NO_HEALTHY_BACKENDS")

            live = [sid for sid, p in self._procs.items() if p.poll() is None]
            if len(live) >= self.max_replacements:
                self._log_limit(f"Automatic recovery limit reached: {len(live)} replacement backends already "
                                f"running (maximum {self.max_replacements}). No further replacement will be provisioned.")
                return
            if self._failed_attempts >= self.max_attempts:
                self._log_limit(f"Automatic recovery stopped after {self._failed_attempts} failed provisioning "
                                f"attempts. Recover a server manually or reset the simulation.")
                return
            if self._failed_attempts and time.time() - self._last_failure_at < self.retry_cooldown:
                return

            if not self.gate.try_acquire("auto-recovery"):
                return
            sid, name = self.allocator.next_identity(self.registry)
            now = time.time()
            self._in_progress = True
            gen = self._generation
            self._episode = {
                "started_at": now,
                "initial_healthy": 0,
                "initial_total": len(servers),
                "attempt": self._failed_attempts + 1,
                "candidate": {"id": sid, "name": name, "port": None, "pid": None, "url": None},
                "stage": "detected",
                "stages": {"detected": now},
                "error": None,
                "outcome": "in_progress",
            }
            self.metrics.add_log(
                f"Automatic backend recovery initiated: provisioning a replacement for {len(servers)} failed backends.",
                level="WARN", event_type="AUTO_RECOVERY_STARTED")

        self._thread = threading.Thread(target=self._provision, args=(gen, sid, name),
                                        daemon=True, name="AutoRecoveryProvisioner")
        self._thread.start()

    def _log_limit(self, msg: str):
        self._blocked_reason = msg
        if not self._limit_logged:
            self._limit_logged = True
            self.metrics.add_log(msg, level="ERROR", event_type="AUTO_RECOVERY_LIMIT")

    # ------------------------------------------------------------------ #
    # Provisioning (background thread)                                    #
    # ------------------------------------------------------------------ #
    def _stage(self, gen: int, stage: str):
        with self._lock:
            if gen != self._generation:
                raise _Cancelled()
            if self._episode is not None:
                self._episode["stage"] = stage
                self._episode["stages"][stage] = time.time()

    def _provision(self, gen: int, sid: str, name: str):
        proc = None
        port = None
        try:
            self._stage(gen, "provisioning")
            port = self._find_free_port()
            if port is None:
                raise ProvisioningError(f"no free port in range {self.port_range[0]}-{self.port_range[1]}")
            url = f"http://{self.host}:{port}"
            with self._lock:
                self._episode["candidate"].update({"port": port, "url": url})
            self.metrics.add_log(f"Provisioning replacement backend {name} on port {port}...",
                                 level="INFO", event_type="PROVISIONING")

            proc = self._launch(sid, name, port)
            with self._lock:
                if gen != self._generation:
                    raise _Cancelled()
                self._procs[sid] = proc
                self._episode["candidate"]["pid"] = proc.pid
            self._stage(gen, "started")
            self.metrics.add_log(f"{name} process started on port {port} (PID {proc.pid}).",
                                 level="INFO", event_type="REPLACEMENT_STARTED")

            # Health check: the same GET /health probe used by the health monitor,
            # required to pass `health_passes` times in a row.
            self._stage(gen, "health_check")
            deadline = time.time() + self.startup_timeout
            passes, latency = 0, 0.0
            while passes < self.health_passes:
                with self._lock:
                    if gen != self._generation:
                        raise _Cancelled()
                code = proc.poll()
                if code is not None:
                    raise ProvisioningError(f"{name} process exited with code {code} before becoming healthy")
                if time.time() > deadline:
                    raise ProvisioningError(f"{name} did not pass its health check within {self.startup_timeout:.0f} s")
                ok, latency = self._probe(f"{url}/health")
                passes = passes + 1 if ok else 0
                if passes < self.health_passes:
                    time.sleep(0.4)

            self._stage(gen, "healthy")
            self.metrics.add_log(
                f"{name} health check passed ({passes}/{self.health_passes} consecutive, {round(latency, 1)} ms).",
                level="SUCCESS", event_type="REPLACEMENT_HEALTHY")

            # Dynamic registration with the reverse proxy.
            with self._lock:
                if gen != self._generation:
                    raise _Cancelled()
                node = self.registry.register_server(sid, name, self.host, port, weight=self.weight, url=url)
                node.is_replacement = True
                node.origin = "recovery"
                node.control_url = f"http://{self.host}:{control_port_for(port)}"
                node.pid = proc.pid
                node.provisioned_at = time.time()
                node.last_health_check = time.time()
                node.last_successful_check = node.last_health_check
                node.health_latency_ms = latency
            self._stage(gen, "registered")
            self.metrics.add_log(f"{name} registered with reverse proxy as a replacement backend ({url}).",
                                 level="SUCCESS", event_type="REPLACEMENT_REGISTERED")

            servers = self.registry.get_all_servers()
            healthy = sum(1 for s in servers if s.status == "HEALTHY")
            self._stage(gen, "routing")
            self.metrics.add_log(f"{name} added to routing pool — {healthy}/{len(servers)} healthy backends.",
                                 level="SUCCESS", event_type="REPLACEMENT_ADDED")
            with self._lock:
                self._episode["outcome"] = "active"
                self._awaiting_traffic = sid

        except _Cancelled:
            self._stop_process(proc)
            with self._lock:
                self._procs.pop(sid, None)
        except Exception as ex:
            self._stop_process(proc)
            with self._lock:
                self._procs.pop(sid, None)
                if gen == self._generation:
                    self._failed_attempts += 1
                    self._last_failure_at = time.time()
                    if self._episode is not None:
                        self._episode["stage"] = "failed"
                        self._episode["error"] = str(ex)
                        self._episode["outcome"] = "failed"
                        self._episode["stages"]["failed"] = time.time()
            if gen == self._generation:
                self.metrics.add_log(f"Replacement provisioning failed: {ex}. {name} was NOT added to the routing pool.",
                                     level="ERROR", event_type="AUTO_RECOVERY_FAILED")
        finally:
            self.allocator.release(sid, port)
            self.gate.release("auto-recovery")
            with self._lock:
                if gen == self._generation:
                    self._in_progress = False

    # ------------------------------------------------------------------ #
    # Traffic confirmation                                                 #
    # ------------------------------------------------------------------ #
    def note_request_served(self, server_id: Optional[str]):
        """Called after every successful client request; confirms restored traffic once."""
        if not server_id or self._awaiting_traffic != server_id:
            return
        with self._lock:
            if self._awaiting_traffic != server_id:
                return
            self._awaiting_traffic = None
            if self._episode is not None:
                self._episode["stage"] = "restored"
                self._episode["stages"]["restored"] = time.time()
                self._episode["outcome"] = "restored"
            node = self.registry.get_server(server_id)
            name = node.name if node else server_id
        self.metrics.add_log(f"Traffic successfully restored — client request served by {name} through the reverse proxy.",
                             level="SUCCESS", event_type="TRAFFIC_RESTORED")

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #
    def _next_identity(self, servers):
        nums = []
        for s in servers:
            try:
                nums.append(int(s.id.rsplit("-", 1)[1]))
            except (IndexError, ValueError):
                pass
        n = max(nums + [0]) + 1
        return f"server-{n}", f"Server {n}"

    def _is_port_free(self, port: int) -> bool:
        return is_port_free(self.host, port)

    def _find_free_port(self) -> Optional[int]:
        return self.allocator.reserve_port(self.registry, self.port_range, self._port_free)

    def _launch_process(self, sid: str, name: str, port: int):
        return launch_backend_process(self.project_root, sid, name, port, self.host)

    @staticmethod
    def _stop_process(proc):
        stop_process(proc)

    # ------------------------------------------------------------------ #
    # Hand-over to the capacity manager                                    #
    # ------------------------------------------------------------------ #
    def owns(self, server_id: str) -> bool:
        with self._lock:
            return server_id in self._procs

    def process_alive(self, server_id: str) -> Optional[bool]:
        with self._lock:
            p = self._procs.get(server_id)
        return None if p is None else p.poll() is None

    def release_instance(self, server_id: str) -> bool:
        """Stop a replacement this manager started (after the capacity manager drained and unregistered it)."""
        with self._lock:
            proc = self._procs.pop(server_id, None)
            if self._awaiting_traffic == server_id:
                self._awaiting_traffic = None
        if proc is None:
            return False
        self._stop_process(proc)
        return True

    def busy(self) -> bool:
        with self._lock:
            return self._in_progress

    # ------------------------------------------------------------------ #
    # Reset / shutdown / status                                            #
    # ------------------------------------------------------------------ #
    def reset(self):
        """
        Stop every replacement process, unregister replacements and clear state.
        Auto-recovery stays paused until resume() is called, so it cannot react to
        the temporary state while the rest of the simulation is being reset.
        """
        with self._lock:
            self._paused = True
            self._generation += 1
            procs = dict(self._procs)
            self._procs.clear()
            self._episode = None
            self._in_progress = False
            self._failed_attempts = 0
            self._last_failure_at = 0.0
            self._outage_logged = False
            self._limit_logged = False
            self._blocked_reason = None
            self._awaiting_traffic = None
            replacement_ids = [s.id for s in self.registry.get_all_servers() if getattr(s, "is_replacement", False)]
        for proc in procs.values():
            self._stop_process(proc)
        for sid in set(replacement_ids) | set(procs.keys()):
            self.registry.unregister_server(sid)
        return len(procs)

    def resume(self):
        with self._lock:
            self._paused = False

    def shutdown(self):
        with self._lock:
            self._generation += 1
            procs = list(self._procs.values())
            self._procs.clear()
        for proc in procs:
            self._stop_process(proc)

    def status(self) -> Dict[str, Any]:
        with self._lock:
            episode = None
            if self._episode is not None:
                episode = dict(self._episode)
                episode["candidate"] = dict(self._episode["candidate"])
                episode["stages"] = dict(self._episode["stages"])
                episode["stage_times"] = {k: time.strftime("%H:%M:%S", time.localtime(v))
                                          for k, v in self._episode["stages"].items()}
            replacements = [{"id": sid, "pid": p.pid, "alive": p.poll() is None} for sid, p in self._procs.items()]
            return {
                "enabled": self.enabled and not self._paused,
                "in_progress": self._in_progress,
                "max_replacements": self.max_replacements,
                "max_attempts": self.max_attempts,
                "failed_attempts": self._failed_attempts,
                "port_range": list(self.port_range),
                "replacements": replacements,
                "episode": episode,
                "blocked_reason": self._blocked_reason,
            }
