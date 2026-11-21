"""
Tests for Automatic Backend Recovery (load_balancer/auto_recovery.py).

Covers:
1. No replacement while at least one backend is healthy (1 or 2 servers failed).
2. Replacement provisioned, health-checked and registered when 0 backends are healthy.
3. Replacement that fails its health check is never registered; attempts are capped.
4. Duplicate provisioning is prevented.
5. Real process that never becomes healthy is killed and not registered.
6. Provisioning fails cleanly when no port is free.
7. Reset stops and unregisters replacements.
"""

import os
import sys
import time
import socket
import subprocess
import unittest
import threading

from load_balancer.server_registry import ServerRegistry
from load_balancer.metrics import MetricsCollector
from load_balancer.auto_recovery import AutoRecoveryManager

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


class FakeProc:
    _next_pid = 9000

    def __init__(self):
        FakeProc._next_pid += 1
        self.pid = FakeProc._next_pid
        self.returncode = None
        self.terminated = False

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated = True
        self.returncode = -15

    def wait(self, timeout=None):
        return self.returncode

    def kill(self):
        self.terminate()


def make_registry(statuses):
    reg = ServerRegistry(recovery_threshold=2)
    for i, st in enumerate(statuses, start=1):
        node = reg.register_server(f"server-{i}", f"Server {i}", "127.0.0.1", 5000 + i, weight=1)
        node.status = st
    return reg


class TestAutoRecovery(unittest.TestCase):

    def make_manager(self, reg, healthy=True, **kw):
        self.launched = []

        def launcher(sid, name, port):
            p = FakeProc()
            self.launched.append((sid, name, port, p))
            return p

        defaults = dict(enabled=True, launcher=launcher,
                        prober=lambda url: (healthy, 2.0),
                        port_checker=lambda port: True,
                        startup_timeout=1.0, retry_cooldown=0.0)
        defaults.update(kw)
        self.metrics = MetricsCollector()
        return AutoRecoveryManager(reg, self.metrics, ROOT, **defaults)

    def run_once(self, mgr):
        mgr.evaluate()
        if mgr._thread:
            mgr._thread.join(timeout=5)

    def event_types(self):
        return [e["event_type"] for e in self.metrics.get_events()]

    def test_no_replacement_when_one_or_two_servers_failed(self):
        for statuses in (["HEALTHY", "UNHEALTHY", "HEALTHY"], ["HEALTHY", "UNHEALTHY", "UNHEALTHY"]):
            reg = make_registry(statuses)
            mgr = self.make_manager(reg)
            self.run_once(mgr)
            self.assertEqual(self.launched, [])
            self.assertEqual(len(reg.get_all_servers()), 3)

    def test_replacement_provisioned_and_registered_when_all_failed(self):
        reg = make_registry(["UNHEALTHY"] * 3)
        mgr = self.make_manager(reg)
        self.run_once(mgr)

        self.assertEqual(len(self.launched), 1)
        sid, name, port, _ = self.launched[0]
        self.assertEqual((sid, name, port), ("server-4", "Server 4", 5004))
        node = reg.get_server("server-4")
        self.assertIsNotNone(node)
        self.assertTrue(node.is_replacement)
        self.assertEqual([s.id for s in reg.get_active_pool()], ["server-4"])
        self.assertEqual(self.event_types(), [
            "NO_HEALTHY_BACKENDS", "AUTO_RECOVERY_STARTED", "PROVISIONING", "REPLACEMENT_STARTED",
            "REPLACEMENT_HEALTHY", "REPLACEMENT_REGISTERED", "REPLACEMENT_ADDED"])

        mgr.note_request_served("server-4")
        mgr.note_request_served("server-4")
        self.assertEqual(self.event_types().count("TRAFFIC_RESTORED"), 1)
        self.assertEqual(mgr.status()["episode"]["stage"], "restored")

        # a healthy replacement exists -> evaluating again must not provision another
        self.run_once(mgr)
        self.assertEqual(len(self.launched), 1)

    def test_unhealthy_replacement_is_not_registered_and_attempts_are_capped(self):
        reg = make_registry(["UNHEALTHY"] * 3)
        mgr = self.make_manager(reg, healthy=False, max_attempts=2, startup_timeout=0.3)
        for _ in range(5):
            self.run_once(mgr)
        self.assertEqual(len(self.launched), 2)                 # capped at max_attempts
        self.assertTrue(all(p.terminated for *_, p in self.launched))
        self.assertIsNone(reg.get_server("server-4"))
        self.assertEqual(reg.get_active_pool(), [])
        self.assertEqual(self.event_types().count("AUTO_RECOVERY_FAILED"), 2)
        self.assertIn("AUTO_RECOVERY_LIMIT", self.event_types())
        self.assertIsNotNone(mgr.status()["blocked_reason"])

    def test_concurrent_evaluations_start_only_one_provisioning(self):
        reg = make_registry(["UNHEALTHY"] * 3)
        gate = threading.Event()

        def slow_prober(url):
            gate.wait(2)
            return True, 1.0

        mgr = self.make_manager(reg, prober=slow_prober)
        threads = [threading.Thread(target=mgr.evaluate) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        gate.set()
        mgr._thread.join(timeout=5)
        self.assertEqual(len(self.launched), 1)

    def test_max_live_replacements(self):
        reg = make_registry(["UNHEALTHY"] * 3)
        mgr = self.make_manager(reg, max_replacements=1)
        self.run_once(mgr)
        reg.get_server("server-4").status = "UNHEALTHY"        # replacement fails too
        self.run_once(mgr)
        self.assertEqual(len(self.launched), 1)
        self.assertIn("AUTO_RECOVERY_LIMIT", self.event_types())

    def test_waits_while_a_server_is_recovering(self):
        reg = make_registry(["UNHEALTHY", "RECOVERING", "UNHEALTHY"])
        mgr = self.make_manager(reg)
        self.run_once(mgr)
        self.assertEqual(self.launched, [])

    def test_reset_stops_and_unregisters_replacements(self):
        reg = make_registry(["UNHEALTHY"] * 3)
        mgr = self.make_manager(reg)
        self.run_once(mgr)
        proc = self.launched[0][3]
        mgr.reset()
        self.assertTrue(proc.terminated)
        self.assertIsNone(reg.get_server("server-4"))
        mgr.evaluate()                                          # paused until resume()
        self.assertEqual(len(self.launched), 1)
        mgr.resume()
        self.assertEqual(mgr.status()["replacements"], [])


class TestAutoRecoveryRealProcesses(unittest.TestCase):

    def test_real_process_that_never_becomes_healthy_is_killed(self):
        reg = make_registry(["UNHEALTHY"] * 3)
        procs = []

        def launcher(sid, name, port):
            # a real process that never listens on the port
            p = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
            procs.append(p)
            return p

        metrics = MetricsCollector()
        mgr = AutoRecoveryManager(reg, metrics, ROOT, launcher=launcher, startup_timeout=1.5,
                                  port_range=(5590, 5599), health_timeout=0.3)
        mgr.evaluate()
        mgr._thread.join(timeout=10)
        self.assertEqual(len(procs), 1)
        self.assertIsNotNone(procs[0].poll(), "unhealthy replacement process must be stopped")
        self.assertIsNone(reg.get_server("server-4"))
        failed = [e for e in metrics.get_events() if e["event_type"] == "AUTO_RECOVERY_FAILED"]
        self.assertEqual(len(failed), 1)
        self.assertIn("did not pass its health check", failed[0]["message"])

    def test_no_free_port(self):
        reg = make_registry(["UNHEALTHY"] * 3)
        blocker = socket.socket()
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        busy_port = blocker.getsockname()[1]
        try:
            launched = []
            metrics = MetricsCollector()
            mgr = AutoRecoveryManager(reg, metrics, ROOT, port_range=(busy_port, busy_port),
                                      launcher=lambda *a: launched.append(a))
            mgr.evaluate()
            mgr._thread.join(timeout=5)
            self.assertEqual(launched, [])
            self.assertIn("no free port", mgr.status()["episode"]["error"])
            self.assertEqual(mgr.status()["episode"]["outcome"], "failed")
        finally:
            blocker.close()


if __name__ == "__main__":
    unittest.main()
