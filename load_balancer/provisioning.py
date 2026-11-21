"""
Shared helpers for starting and stopping replacement backend processes.

Automatic recovery (stage 1: restore service) and the capacity manager (stage 2:
match capacity to demand) both start real backend processes
(backend_servers/replacement_server.py). They share:

  InstanceAllocator  unique server ids ("server-N") and free port PAIRS
                     (data port + management port = data port + CONTROL_PORT_OFFSET),
                     reserved until released, so concurrent provisioning can never
                     collide on an id or a port.
  ProvisionGate      at most one provisioning operation at a time across both
                     managers, so they cannot make contradictory decisions or start
                     duplicate replacements.
"""

import os
import sys
import socket
import subprocess
import threading
from typing import Callable, Optional, Set, Tuple


def control_port_for(port: int) -> int:
    import config
    return port + config.CONTROL_PORT_OFFSET


class InstanceAllocator:
    def __init__(self):
        self._lock = threading.Lock()
        self._ids: Set[str] = set()
        self._ports: Set[int] = set()

    @staticmethod
    def _num(sid: str) -> int:
        try:
            return int(sid.rsplit("-", 1)[1])
        except (IndexError, ValueError):
            return 0

    def next_identity(self, registry) -> Tuple[str, str]:
        with self._lock:
            nums = [self._num(s.id) for s in registry.get_all_servers()] + [self._num(i) for i in self._ids]
            n = max(nums + [0]) + 1
            sid = f"server-{n}"
            self._ids.add(sid)
            return sid, f"Server {n}"

    def reserve_port(self, registry, port_range, port_free: Callable[[int], bool]) -> Optional[int]:
        """First port in range whose data AND control ports are free and unreserved."""
        with self._lock:
            used = {s.port for s in registry.get_all_servers()} | self._ports
            for port in range(port_range[0], port_range[1] + 1):
                if port in used:
                    continue
                if port_free(port):
                    self._ports.add(port)
                    return port
            return None

    def release(self, sid: Optional[str] = None, port: Optional[int] = None):
        with self._lock:
            if sid:
                self._ids.discard(sid)
            if port is not None:
                self._ports.discard(port)


class ProvisionGate:
    """Mutual exclusion for provisioning between automatic recovery and the capacity manager."""

    def __init__(self):
        self._lock = threading.Lock()
        self.owner: Optional[str] = None

    def try_acquire(self, owner: str) -> bool:
        with self._lock:
            if self.owner is None:
                self.owner = owner
                return True
            return self.owner == owner

    def release(self, owner: str):
        with self._lock:
            if self.owner == owner:
                self.owner = None

    def busy_by_other(self, owner: str) -> bool:
        with self._lock:
            return self.owner is not None and self.owner != owner


def _single_port_free(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.3)
        if s.connect_ex((host, port)) == 0:
            return False
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((host, port))
            return True
        except OSError:
            return False


def is_port_free(host: str, port: int) -> bool:
    """True if both the data port and its management port are free."""
    return _single_port_free(host, port) and _single_port_free(host, control_port_for(port))


def launch_backend_process(project_root: str, sid: str, name: str, port: int, host: str):
    """Start a real backend process (same Flask backend as Server 1/2/3) on the given port."""
    script = os.path.join(project_root, "backend_servers", "replacement_server.py")
    cmd = [sys.executable, script, "--id", sid, "--name", name, "--port", str(port),
           "--host", host, "--parent-pid", str(os.getpid()), "--control-port", str(control_port_for(port))]
    return subprocess.Popen(cmd, cwd=project_root, stdin=subprocess.DEVNULL,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def stop_process(proc, timeout: float = 3.0):
    """Terminate a backend process; kill it if it does not exit in time."""
    if proc is None:
        return
    try:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=timeout)
            except Exception:
                proc.kill()
                try:
                    proc.wait(timeout=timeout)
                except Exception:
                    pass
    except Exception:
        pass
