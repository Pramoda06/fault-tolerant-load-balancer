"""
Unit tests specifically for HealthMonitor probing and timing calculations.

Verifies:
1. Active health polling calls /health on registered servers.
2. Latency calculation and telemetry recording.
3. Accurate state transition triggering during live probe cycle.
"""

import unittest
from unittest.mock import patch, MagicMock
import time

from load_balancer.server_registry import ServerRegistry
from load_balancer.metrics import MetricsCollector
from load_balancer.health_monitor import HealthMonitor

class TestHealthMonitorProbing(unittest.TestCase):

    def setUp(self):
        self.registry = ServerRegistry(recovery_threshold=2)
        self.metrics = MetricsCollector()
        self.s1 = self.registry.register_server("server-1", "Server 1", "127.0.0.1", 5001)
        self.monitor = HealthMonitor(
            registry=self.registry,
            metrics=self.metrics,
            interval=1.0,
            timeout=0.5
        )

    @patch("requests.get")
    def test_health_check_success_updates_status(self, mock_get):
        """Probe returning 200 updates server status and latencies."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"status": "healthy", "server": "Server 1"}
        mock_get.return_value = mock_resp

        self.monitor.check_all_servers()

        self.assertEqual(self.s1.status, "HEALTHY")
        self.assertIsNotNone(self.s1.last_successful_check)
        self.assertEqual(self.s1.consecutive_successes, 1)

    @patch("requests.get")
    def test_health_check_failure_triggers_exclusion(self, mock_get):
        """Probe returning 503 triggers UNHEALTHY transition and records failure detection."""
        t_injected = time.time() - 0.5
        mock_resp = MagicMock()
        mock_resp.status_code = 503
        mock_resp.json.return_value = {
            "status": "unhealthy",
            "error": "Simulated failure",
            "failure_injected_at": t_injected
        }
        mock_get.return_value = mock_resp

        self.monitor.check_all_servers()

        self.assertEqual(self.s1.status, "UNHEALTHY")
        self.assertNotIn("server-1", [s.id for s in self.registry.get_active_pool()])
        
        # Verify failure detection timing was computed
        snapshot = self.metrics.get_metrics_snapshot()
        self.assertIsNotNone(snapshot["failure_detection_time_ms"])
        self.assertGreater(snapshot["failure_detection_time_ms"], 0.0)

    @patch("requests.get")
    def test_health_check_recovery_and_readmission_flow(self, mock_get):
        """Simulate recovery probe sequence: UNHEALTHY -> RECOVERING -> HEALTHY."""
        # 1. Server starts UNHEALTHY
        self.s1.status = "UNHEALTHY"

        # 2. First probe passes -> RECOVERING
        mock_pass = MagicMock()
        mock_pass.status_code = 200
        mock_pass.json.return_value = {"status": "healthy", "server": "Server 1"}
        mock_get.return_value = mock_pass

        self.monitor.check_all_servers()
        self.assertEqual(self.s1.status, "RECOVERING")
        self.assertEqual(self.s1.consecutive_successes, 1)

        # 3. Second probe passes -> RE-ADMITTED (threshold=2)
        self.monitor.check_all_servers()
        self.assertEqual(self.s1.status, "HEALTHY")
        self.assertEqual(self.s1.consecutive_successes, 2)
        self.assertIn("server-1", [s.id for s in self.registry.get_active_pool()])

if __name__ == "__main__":
    unittest.main()
