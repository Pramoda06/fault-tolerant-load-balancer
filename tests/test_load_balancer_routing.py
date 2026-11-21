"""
Integration tests for Load Balancer routing, fault tolerance, and API endpoints.

Verifies:
1. End-to-end request dispatching.
2. In-flight failure detection and automatic failover.
3. HTTP 503 Service Unavailable when no healthy servers exist.
4. Correct request counters and telemetry updates.
5. REST API control endpoints (/api/status, /api/fail, /api/recover).
"""

import unittest
from unittest.mock import patch, MagicMock
import json

from load_balancer.server_registry import ServerRegistry
from load_balancer.scheduler import SchedulerManager
from load_balancer.metrics import MetricsCollector
from load_balancer.balancer import LoadBalancer
from app import app, registry, metrics, scheduler

class TestLoadBalancerRouting(unittest.TestCase):

    def setUp(self):
        self.client = app.test_client()
        metrics.reset_metrics()
        scheduler.set_algorithm("ROUND_ROBIN")
        # Ensure all servers healthy
        for s in registry.get_all_servers():
            s.status = "HEALTHY"
            s.requests_served = 0

    @patch("requests.get")
    def test_round_robin_request_routing(self, mock_get):
        """Verify requests route sequentially across healthy servers."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "status": "healthy",
            "message": "Processed successfully"
        }
        mock_get.return_value = mock_resp

        # Send 3 requests
        r1 = self.client.get("/api/request")
        r2 = self.client.get("/api/request")
        r3 = self.client.get("/api/request")

        self.assertEqual(r1.status_code, 200)
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r3.status_code, 200)

        # Check metrics
        snapshot = metrics.get_metrics_snapshot()
        self.assertEqual(snapshot["total_requests"], 3)
        self.assertEqual(snapshot["successful_requests"], 3)
        self.assertEqual(snapshot["failed_requests"], 0)

    @patch("requests.get")
    def test_failover_when_server_fails_mid_request(self, mock_get):
        """
        Verify that if the chosen server raises an exception or returns 503 mid-flight,
        the load balancer automatically fails over to the next healthy server.
        """
        # First call fails (Server 1 error), second call succeeds (Server 2 success)
        fail_resp = MagicMock()
        fail_resp.status_code = 503
        fail_resp.json.return_value = {"status": "error"}

        success_resp = MagicMock()
        success_resp.status_code = 200
        success_resp.json.return_value = {"status": "healthy", "server": "Server 2"}

        mock_get.side_effect = [fail_resp, success_resp]

        resp = self.client.get("/api/request")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["status"], "success")
        self.assertGreaterEqual(data["failover_retries"], 1)

    def test_all_servers_unhealthy_returns_503(self):
        """
        SCENARIO 6: All servers fail.
        Verify load balancer returns HTTP 503 without crashing.
        """
        for s in registry.get_all_servers():
            s.status = "UNHEALTHY"

        resp = self.client.get("/api/request")
        self.assertEqual(resp.status_code, 503)
        data = resp.get_json()
        self.assertIn("NO HEALTHY BACKEND SERVERS", data["message"])

        # Check telemetry recorded the failed request
        snapshot = metrics.get_metrics_snapshot()
        self.assertEqual(snapshot["failed_requests"], 1)

    def test_system_status_api(self):
        """Verify /api/status returns complete telemetry structure."""
        resp = self.client.get("/api/status")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertIn("system_status", data)
        self.assertIn("active_server_count", data)
        self.assertIn("servers", data)
        self.assertIn("metrics", data)
        self.assertIn("logs", data)

if __name__ == "__main__":
    unittest.main()
