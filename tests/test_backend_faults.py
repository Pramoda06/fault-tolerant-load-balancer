"""
Tests for the backend's service model and fault endpoints (backend_servers/base_server.py).

Uses Flask's test client for the data and control planes, and one real loopback listener to
show that the 'refused' fault produces a real TCP connection refusal.
"""

import socket
import time
import unittest

import requests

from backend_servers.base_server import create_backend_app, create_control_app, DataPlane


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class TestBackendModel(unittest.TestCase):
    def setUp(self):
        self.app = create_backend_app("server-9", "Server 9", 5999, workers=1, service_time_ms=20,
                                      queue_limit=0, hang_seconds=0.2)
        self.c = self.app.test_client()
        self.plane = DataPlane(self.app, "127.0.0.1", free_port())
        self.ctrl = create_control_app(self.app, self.plane).test_client()

    def test_process_reports_measured_times(self):
        body = self.c.get("/process").get_json()
        self.assertGreaterEqual(body["service_ms"], 19)
        self.assertEqual(body["workers"], 1)
        self.assertIn("queue_ms", body)

    def test_http_5xx_fault_and_clear(self):
        self.ctrl.post("/control/fault", json={"type": "http_5xx"})
        self.assertEqual(self.c.get("/health").status_code, 503)
        self.assertEqual(self.c.get("/process").status_code, 503)
        self.ctrl.post("/control/clear")
        self.assertEqual(self.c.get("/health").status_code, 200)

    def test_slow_fault_delays_but_succeeds(self):
        self.ctrl.post("/control/fault", json={"type": "slow", "delay_ms": 150})
        t = time.perf_counter()
        r = self.c.get("/process")
        self.assertEqual(r.status_code, 200)
        self.assertGreaterEqual((time.perf_counter() - t) * 1000, 160)
        self.assertEqual(self.ctrl.get("/control/status").get_json()["fault"]["type"], "slow")

    def test_queue_full_is_explicit_overload(self):
        st = self.app.config["BACKEND_STATE"]
        st["busy"] = st["workers"]                 # every worker busy ...
        st["waiting"] = st["queue_limit"]          # ... and the queue already full
        r = self.c.get("/process")
        self.assertEqual(r.status_code, 503)
        self.assertEqual(r.headers.get("X-Backend-Overloaded"), "1")

    def test_unknown_fault_rejected(self):
        self.assertEqual(self.ctrl.post("/control/fault", json={"type": "fire"}).status_code, 400)

    def test_legacy_endpoints_still_work(self):
        self.c.post("/simulate/fail")
        self.assertEqual(self.c.get("/health").status_code, 503)
        self.c.post("/simulate/recover")
        self.assertEqual(self.c.get("/health").status_code, 200)


class TestRealRefusedConnection(unittest.TestCase):
    def test_refused_closes_listener_and_clear_reopens(self):
        port = free_port()
        app = create_backend_app("server-8", "Server 8", port, service_time_ms=1)
        plane = DataPlane(app, "127.0.0.1", port)
        plane.start()
        ctrl = create_control_app(app, plane).test_client()
        try:
            self.assertEqual(requests.get(f"http://127.0.0.1:{port}/health", timeout=2).status_code, 200)
            ctrl.post("/control/fault", json={"type": "refused"})
            with self.assertRaises(requests.exceptions.ConnectionError):
                requests.get(f"http://127.0.0.1:{port}/health", timeout=2)
            self.assertFalse(ctrl.get("/control/status").get_json()["listener_up"])
            ctrl.post("/control/clear")
            self.assertEqual(requests.get(f"http://127.0.0.1:{port}/health", timeout=2).status_code, 200)
        finally:
            plane.stop()

    def test_timeout_fault_causes_client_read_timeout(self):
        port = free_port()
        app = create_backend_app("server-7", "Server 7", port, service_time_ms=1, hang_seconds=1.5)
        plane = DataPlane(app, "127.0.0.1", port)
        plane.start()
        ctrl = create_control_app(app, plane).test_client()
        try:
            ctrl.post("/control/fault", json={"type": "timeout"})
            with self.assertRaises(requests.exceptions.ReadTimeout):
                requests.get(f"http://127.0.0.1:{port}/process", timeout=0.5)
        finally:
            ctrl.post("/control/clear")
            plane.stop()


if __name__ == "__main__":
    unittest.main()
