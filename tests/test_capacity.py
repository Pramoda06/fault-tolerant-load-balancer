"""
Unit tests for the load-aware capacity manager (load_balancer/capacity.py).

Deterministic: a fake clock drives the manager, client arrivals are recorded with explicit
timestamps, backend processes and health probes are fakes, and provisioning waits are skipped.
With no backend-reported samples the manager uses the configured service model:
1 worker / 50 ms = 20 req/s per server, safe = 20 * 0.7 = 14 req/s.

Covers: scale-out on sustained demand (6), safe drained scale-in (7), stability around a
threshold (8), a recovering original reducing the need (9), maximum capacity (10), failed
startup / no free port (11), a backend failing while a replacement starts (5), observation
after a total outage before any decision (3), reset during provisioning (18) and shutdown (20).
"""

import os
import sys
import time
import types
import socket
import subprocess
import unittest

import config
from load_balancer.server_registry import ServerRegistry
from load_balancer.metrics import MetricsCollector
from load_balancer.capacity import CapacityManager
from load_balancer.provisioning import ProvisionGate

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def settings(**over):
    s = {k: getattr(config, k) for k in dir(config) if k.isupper()}
    s.update(dict(BACKEND_WORKERS=1, BACKEND_SERVICE_TIME_MS=50.0, CAPACITY_TARGET_UTILIZATION=0.7,
                  CAPACITY_SCALE_IN_UTILIZATION=0.5, CAPACITY_MIN_SERVERS=1, CAPACITY_MAX_SERVERS=6,
                  CAPACITY_OBSERVATION_WINDOW=10.0, CAPACITY_MIN_SAMPLES=20, CAPACITY_SCALE_OUT_SUSTAIN=5.0,
                  CAPACITY_SCALE_IN_SUSTAIN=20.0, CAPACITY_SCALE_OUT_COOLDOWN=8.0, CAPACITY_SCALE_IN_COOLDOWN=15.0,
                  CAPACITY_DRAIN_TIMEOUT=10.0, CAPACITY_FAILED_REPLACEMENT_GRACE=15.0,
                  CAPACITY_MAX_PROVISION_FAILURES=3, CAPACITY_PROVISION_BACKOFF=10.0))
    s.update(over)
    return types.SimpleNamespace(**s)


class Clock:
    def __init__(self):
        self.t = 1_000_000.0

    def __call__(self):
        return self.t


class FakeProc:
    _pid = 30000

    def __init__(self):
        FakeProc._pid += 1
        self.pid = FakeProc._pid
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


def make_registry(n=3, status="HEALTHY"):
    reg = ServerRegistry(recovery_threshold=2)
    for i in range(1, n + 1):
        reg.register_server(f"server-{i}", f"Server {i}", "127.0.0.1", 5000 + i, weight=1).status = status
    return reg


class CapacityTestCase(unittest.TestCase):
    def make(self, reg=None, healthy=True, settings_over=None, **kw):
        self.reg = reg or make_registry()
        self.clock = Clock()
        self.metrics = MetricsCollector()
        self.launched = []
        self.probe_ok = healthy

        def launcher(sid, name, port):
            p = FakeProc()
            self.launched.append((sid, name, port, p))
            return p

        defaults = dict(launcher=launcher, prober=lambda url: (self.probe_ok, 1.0), port_checker=lambda port: True,
                        clock=self.clock, settings=settings(**(settings_over or {})), sleep=lambda s: None,
                        startup_timeout=2.0)
        defaults.update(kw)
        self.cap = CapacityManager(self.reg, self.metrics, ROOT, **defaults)
        return self.cap

    def run_load(self, rps, seconds, step=1.0):
        """Advance the fake clock, recording `rps` arrivals per second, evaluating once per step."""
        n = int(round(seconds / step))
        for _ in range(n):
            for k in range(int(round(rps * step))):
                self.metrics.record_arrival(self.clock.t + k * step / max(1, rps * step))
            self.clock.t += step
            self.cap.evaluate()
            self.join()

    def join(self):
        t = self.cap._prov_thread
        if t is not None:
            t.join(timeout=5)

    def replacements(self):
        return [s for s in self.reg.get_all_servers() if s.is_replacement]

    CAPACITY_EVENTS = ("CAPACITY_DECISION", "CAPACITY_EVALUATION_STARTED", "CAPACITY_TARGET_UPDATED",
                       "SCALE_OUT_STARTED", "REPLACEMENT_PROVISIONED", "REPLACEMENT_HEALTHY", "BACKEND_REGISTERED",
                       "SCALE_IN_STARTED", "BACKEND_DRAINING", "BACKEND_RETIRED", "CAPACITY_STABILIZED")

    def decisions(self):
        return [e["message"] for e in self.metrics.get_events() if e["event_type"] in self.CAPACITY_EVENTS]

    def events(self, etype=None):
        return [e for e in self.metrics.get_events() if etype is None or e["event_type"] == etype]


class TestCapacityFormula(CapacityTestCase):
    def test_required_servers_formula(self):
        self.make()
        self.run_load(42, 12)
        est = self.cap.estimate()
        # 42 req/s / (20 req/s * 0.7) = 3.0 -> 3
        self.assertEqual(est["capacity_rps_per_server"], 20.0)
        self.assertEqual(est["required_raw"], 3)
        self.assertEqual(est["desired"], 3)
        self.assertIn("configured", est["service_source"])

    def test_measured_service_time_changes_capacity(self):
        self.make()
        for _ in range(10):
            self.metrics.record_backend_report("server-1", 100.0, 0.0)   # backend reports 100 ms
        self.clock.t = time.time()
        est = self.cap.estimate()
        self.assertEqual(est["capacity_rps_per_server"], 10.0)
        self.assertIn("measured", est["service_source"])


class TestScaleOut(CapacityTestCase):
    def test_sustained_demand_scales_out_with_readiness_checks(self):           # case 6
        self.make()
        self.run_load(55, 25)        # needs ceil(55 / 14) = 4 > 3 originals
        reps = self.replacements()
        self.assertEqual(len(reps), 1)
        self.assertEqual(reps[0].origin, "capacity")
        self.assertEqual(reps[0].status, "HEALTHY")
        self.assertTrue(reps[0].control_url.endswith(":6004"))
        self.assertTrue(any("passed 2 health checks and was registered" in d for d in self.decisions()))

    def test_insufficient_window_or_samples_do_not_scale(self):
        self.make()
        self.run_load(55, 8)         # window of 10 s not yet observed
        self.assertEqual(self.launched, [])
        self.make(settings_over={"CAPACITY_MIN_SAMPLES": 1000})
        self.run_load(55, 30)        # never 1000 samples in the window
        self.assertEqual(self.launched, [])

    def test_transient_spike_does_not_scale(self):                              # case 8 (spike)
        self.make()
        self.run_load(30, 12)
        self.run_load(80, 3)
        self.run_load(30, 20)
        self.assertEqual(self.launched, [])

    def test_maximum_capacity_reached(self):                                    # case 10
        self.make(settings_over={"CAPACITY_MAX_SERVERS": 4, "CAPACITY_SCALE_OUT_COOLDOWN": 0.0})
        self.run_load(200, 60)
        self.assertEqual(len(self.reg.get_all_servers()), 4)
        st = self.cap.status()
        self.assertEqual(st["phase"], "AT_MAXIMUM")
        self.assertTrue(any("Maximum capacity reached" in d for d in self.decisions()))

    def test_startup_failure_not_registered_and_backoff(self):                  # case 11
        self.make(healthy=False, settings_over={"CAPACITY_SCALE_OUT_COOLDOWN": 0.0,
                                                "CAPACITY_MAX_PROVISION_FAILURES": 2})
        self.run_load(55, 40)
        self.assertEqual(self.replacements(), [])
        self.assertTrue(all(p.terminated for *_, p in self.launched))
        self.assertTrue(any("failed" in d and "Not registered" in d for d in self.decisions()))
        self.assertGreaterEqual(self.cap.status()["provision_failures"], 2)

    def test_no_free_port(self):                                                # case 11 (port)
        self.make(port_checker=lambda port: False)
        self.run_load(55, 25)
        self.assertEqual(self.launched, [])
        self.assertTrue(any("no free port" in d for d in self.decisions()))

    def test_replacement_process_dies_during_startup(self):                     # case 5
        self.make()
        self.probe_ok = False

        def launcher(sid, name, port):
            p = FakeProc()
            p.returncode = 1                       # exits immediately
            self.launched.append((sid, name, port, p))
            return p
        self.cap._launch = launcher
        self.run_load(55, 25)
        self.assertEqual(self.replacements(), [])
        self.assertTrue(any("exited with code 1" in d for d in self.decisions()))

    def test_backend_fails_while_replacement_starting(self):                    # case 5
        self.make()
        gate_open = {"ok": False}
        calls = []

        def prober(url):
            calls.append(url)
            if len(calls) == 1:
                # while the replacement is being health-checked, Server 2 fails
                self.reg.get_server("server-2").status = "UNHEALTHY"
            return True, 1.0
        self.cap._probe = prober
        self.run_load(55, 25)
        names = [s.id for s in self.replacements()]
        # one replacement registered for the original demand; Server 2's loss is handled by a later decision
        self.assertGreaterEqual(len(names), 1)
        self.assertEqual(len(names), len(set(names)))              # no duplicate registrations


class TestScaleIn(CapacityTestCase):
    def scale_out(self):
        self.run_load(55, 25)
        self.assertEqual(len(self.replacements()), 1)
        return self.replacements()[0]

    def test_low_demand_drains_then_removes_replacement(self):                  # case 7
        self.make()
        rep = self.scale_out()
        proc = self.launched[0][3]
        rep.active_connections = 1                 # a request is in flight
        self.run_load(5, 31)                       # window drains (~6 s) + 20 s sustained low demand
        self.assertTrue(self.reg.get_server(rep.id).draining)
        self.assertNotIn(self.reg.get_server(rep.id), self.reg.get_active_pool())
        self.assertFalse(proc.terminated)          # still finishing its request
        rep.active_connections = 0
        self.run_load(5, 2)
        self.assertIsNone(self.reg.get_server(rep.id))
        self.assertTrue(proc.terminated)
        self.assertEqual(len(self.reg.get_all_servers()), 3)     # originals kept

    def test_drain_timeout_forces_removal(self):
        self.make()
        rep = self.scale_out()
        rep.active_connections = 1                 # never finishes
        self.run_load(5, 45)
        self.assertIsNone(self.reg.get_server(rep.id))
        self.assertTrue(any("drain timeout" in d for d in self.decisions()))

    def test_originals_never_removed(self):
        self.make()
        self.run_load(1, 120)
        self.assertEqual([s.id for s in self.reg.get_all_servers()], ["server-1", "server-2", "server-3"])
        self.assertFalse(self.reg.start_drain("server-1", 0))
        self.assertFalse(self.reg.unregister_server("server-1"))

    def test_stable_around_threshold(self):                                     # case 8
        self.make()
        self.scale_out()                            # 4 servers
        # demand alternating 34 / 44 req/s: 3 servers would run at 57-73 % (> 50 %): never scale in,
        # and 44 < 4 * 14 = 56: never scale out
        for _ in range(8):
            self.run_load(44, 5)
            self.run_load(34, 5)
        self.assertEqual(len(self.replacements()), 1)
        self.assertEqual(len(self.launched), 1)


class TestRecoveryStages(CapacityTestCase):
    def test_waits_for_stage1_then_observes_before_deciding(self):              # case 3
        reg = make_registry(status="UNHEALTHY")
        self.make(reg=reg)
        self.run_load(45, 6)
        self.assertEqual(self.launched, [])
        self.assertEqual(self.cap.status()["phase"], "WAITING_FOR_SERVICE")
        # stage 1 (automatic recovery) restores service with one replacement
        node = reg.register_server("server-4", "Server 4", "127.0.0.1", 5004)
        node.is_replacement, node.origin = True, "recovery"
        self.run_load(45, 6)
        self.assertEqual(self.cap.status()["phase"], "OBSERVING")
        self.assertEqual(self.launched, [])        # no decision during the observation window
        self.run_load(45, 30)
        # desired = ceil(45 / 14) = 4 -> three more replacements, one at a time
        self.assertEqual(len([s for s in reg.get_all_servers() if s.origin == "capacity"]), 3)
        self.assertTrue(any("Service restored" in d for d in self.decisions()))

    def test_recovering_original_reduces_need(self):                            # case 9
        reg = make_registry()
        reg.get_server("server-2").status = "UNHEALTHY"
        reg.get_server("server-3").status = "RECOVERING"       # will rejoin after health checks
        self.make(reg=reg)
        self.run_load(25, 25)    # needs 2: 1 routable + 1 recovering = 2 available -> nothing to start
        self.assertEqual(self.launched, [])

    def test_provisioning_cancelled_when_original_recovers(self):               # case 9
        reg = make_registry()
        self.make(reg=reg)

        def prober(url):
            # during readiness checks two original servers come back and demand is covered
            for sid in ("server-1", "server-2", "server-3"):
                reg.get_server(sid).status = "HEALTHY"
            return True, 1.0
        reg.get_server("server-2").status = "UNHEALTHY"
        reg.get_server("server-3").status = "UNHEALTHY"
        self.cap._probe = prober
        self.run_load(25, 25)          # needs 2, only 1 routable -> starts one ...
        self.assertTrue(self.launched)
        self.assertEqual(self.replacements(), [])                # ... but cancels it before registering
        self.assertTrue(self.launched[0][3].terminated)
        self.assertTrue(any("cancelled before registration" in d for d in self.decisions()))

    def test_shared_gate_prevents_parallel_provisioning(self):
        gate = ProvisionGate()
        self.assertTrue(gate.try_acquire("auto-recovery"))
        self.make(gate=gate)
        self.run_load(55, 25)
        self.assertEqual(self.launched, [])        # auto-recovery holds the gate
        gate.release("auto-recovery")
        self.run_load(55, 15)
        self.assertEqual(len(self.launched), 1)


class TestResetAndShutdown(CapacityTestCase):
    def test_reset_during_provisioning_cancels(self):                           # case 18
        self.make()
        started = {}

        def prober(url):
            if "reset" not in started:
                started["reset"] = True
                self.cap.reset()                    # simulation reset while the replacement is starting
            return True, 1.0
        self.cap._probe = prober
        self.run_load(55, 25)
        self.assertEqual(self.replacements(), [])
        self.assertTrue(self.launched and self.launched[0][3].terminated)
        self.assertEqual(self.cap.status()["phase"], "RESET")

    def test_shutdown_stops_thread_and_processes(self):                         # case 20
        self.make()
        self.run_load(55, 25)
        proc = self.launched[0][3]
        self.cap.start()
        self.assertTrue(self.cap._thread.is_alive())
        self.cap.shutdown()
        self.cap._thread.join(timeout=3)
        self.assertFalse(self.cap._thread.is_alive())
        self.assertTrue(proc.terminated)


class TestRealProcessCleanup(unittest.TestCase):
    def test_real_replacement_process_stopped_on_shutdown(self):                # case 20 (real process)
        reg = make_registry()
        metrics = MetricsCollector()
        procs = []

        def launcher(sid, name, port):
            p = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
            procs.append(p)
            return p
        clock = Clock()
        cap = CapacityManager(reg, metrics, ROOT, launcher=launcher, prober=lambda url: (True, 1.0),
                              port_checker=lambda p: True, clock=clock, settings=settings(), sleep=lambda s: None)
        for _ in range(25):
            for k in range(55):
                metrics.record_arrival(clock.t + k / 55)
            clock.t += 1
            cap.evaluate()
            if cap._prov_thread:
                cap._prov_thread.join(5)
        self.assertEqual(len(procs), 1)
        self.assertIsNone(procs[0].poll())
        cap.shutdown()
        procs[0].wait(timeout=5)
        self.assertIsNotNone(procs[0].poll())


if __name__ == "__main__":
    unittest.main()
