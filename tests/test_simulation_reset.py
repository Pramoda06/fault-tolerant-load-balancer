"""
Tests for the demonstration support added to the load balancer:
1. Event timeline keeps state-change events separately from per-request routing logs.
2. Per-server response latency is tracked.
3. Simulation reset returns every node to a clean state.
4. Health results probed before a reset are discarded (no stale state after reset).
"""

import time
import unittest
from unittest.mock import patch, MagicMock
from load_balancer.health_monitor import HealthMonitor
from load_balancer.server_registry import ServerRegistry
from load_balancer.metrics import MetricsCollector


class TestEventTimelineAndLatency(unittest.TestCase):

    def test_routing_lines_do_not_enter_event_timeline(self):
        m = MetricsCollector(max_logs=10)
        m.add_log("Server 2 failure DETECTED", level="WARN", event_type="FAILURE_DETECTED")
        for _ in range(50):
            m.record_request_result("server-1", success=True, latency_ms=12.0)
        events = m.get_events()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_type"], "FAILURE_DETECTED")
        # the routing log is a ring buffer and has rolled over, the event has not
        self.assertTrue(all(l["event_type"] == "ROUTING" for l in m.get_logs()))

    def test_per_server_latency_average(self):
        m = MetricsCollector()
        m.record_request_result("server-1", success=True, latency_ms=10.0)
        m.record_request_result("server-1", success=True, latency_ms=20.0)
        m.record_request_result("server-2", success=True, latency_ms=5.0)
        snap = m.get_metrics_snapshot()
        self.assertEqual(snap["avg_latency_per_server"], {"server-1": 15.0, "server-2": 5.0})

    def test_clear_logs(self):
        m = MetricsCollector()
        m.add_log("x", event_type="CONFIG")
        m.clear_logs()
        self.assertEqual(m.get_logs(), [])
        self.assertEqual(m.get_events(), [])


class TestRegistryReset(unittest.TestCase):

    def setUp(self):
        self.registry = ServerRegistry(recovery_threshold=2)
        self.s1 = self.registry.register_server("server-1", "Server 1", "127.0.0.1", 5001, weight=3)
        self.s2 = self.registry.register_server("server-2", "Server 2", "127.0.0.1", 5002, weight=2)

    def test_reset_restores_clean_state(self):
        self.registry.update_health_result("server-2", False, 1.0, time.time())
        self.s2.requests_served = 7
        self.assertEqual(self.s2.status, "UNHEALTHY")

        self.registry.reset_all({"server-1": True, "server-2": True})
        self.assertEqual(self.s2.status, "HEALTHY")
        self.assertEqual(self.s2.requests_served, 0)
        self.assertIsNone(self.s2.failure_detected_at)
        self.assertEqual(len(self.registry.get_active_pool()), 2)

    def test_reset_uses_live_probe_result(self):
        self.registry.reset_all({"server-1": True, "server-2": False})
        self.assertEqual(self.s2.status, "UNHEALTHY")
        self.assertEqual([s.id for s in self.registry.get_active_pool()], ["server-1"])

    def test_stale_probe_from_before_reset_is_ignored(self):
        epoch_before = self.registry.epoch
        self.registry.reset_all({"server-1": True, "server-2": True})
        event, _ = self.registry.update_health_result("server-2", False, 1.0, time.time(), epoch=epoch_before)
        self.assertIsNone(event)
        self.assertEqual(self.s2.status, "HEALTHY")


class TestInitialHealthEvent(unittest.TestCase):

    @patch("load_balancer.health_monitor.requests.get")
    def test_first_passing_check_is_reported_once(self, mock_get):
        ok = MagicMock(status_code=200)
        ok.json.return_value = {"status": "healthy"}
        mock_get.return_value = ok
        registry = ServerRegistry(recovery_threshold=2)
        registry.register_server("server-1", "Server 1", "127.0.0.1", 5001)
        metrics = MetricsCollector()
        monitor = HealthMonitor(registry, metrics, interval=2.0, timeout=1.0)
        monitor.check_all_servers()
        monitor.check_all_servers()
        healthy = [e for e in metrics.get_events() if e["event_type"] == "HEALTH_OK"]
        self.assertEqual(len(healthy), 1)
        self.assertIn("Server 1 healthy", healthy[0]["message"])

        monitor.report_initial_health_again()     # as done by a simulation reset
        monitor.check_all_servers()
        self.assertEqual(len([e for e in metrics.get_events() if e["event_type"] == "HEALTH_OK"]), 2)


if __name__ == "__main__":
    unittest.main()
