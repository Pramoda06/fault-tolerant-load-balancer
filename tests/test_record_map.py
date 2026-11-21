"""
Tests for the Data Recovery record map (/api/data/map) and the brownout accounting.

The record map's preview must be exactly what recover() then restores, for every strategy and also
when a copy is corrupted, so the dashboard never promises something the recovery does not deliver.
"""

import shutil
import tempfile
import unittest

from load_balancer.datastore import DataStore


class TestRecordMap(unittest.TestCase):
    def setUp(self):
        self.dirs = []

    def tearDown(self):
        for d in self.dirs:
            shutil.rmtree(d, ignore_errors=True)

    def store(self, corrupt=None):
        d = tempfile.mkdtemp(prefix="map_test_")
        self.dirs.append(d)
        ds = DataStore(d, snapshot_interval=0)
        for i in range(8):
            ds.create(f"r{i}", "v")
        ds.snapshot("test")
        for i in range(4):
            ds.create(f"late{i}", "w")
        ds.update(2, "changed")
        ds.delete(1)
        if corrupt:
            ds.corrupt(corrupt)
        return ds

    def test_cells_reflect_each_copy(self):
        m = self.store().record_map()
        self.assertEqual(m["acknowledged"], 11)                     # 12 written, 1 deleted
        self.assertEqual(m["copies"]["backup"]["2"], 1)             # older version in the backup
        self.assertEqual(m["truth"]["2"], 2)
        self.assertIn("1", m["copies"]["backup"])                   # deleted record still in the backup
        self.assertNotIn("1", m["copies"]["replica"])               # the replica copied the delete
        self.assertNotIn("9", m["copies"]["backup"])                # written after the backup
        p = m["preview"]["backup"]
        self.assertEqual((p["lost"], p["stale"], p["resurrected"]), ([9, 10, 11, 12], [2], [1]))
        self.assertEqual(m["preview"]["wal"]["restored"], 11)
        self.assertEqual(m["preview"]["none"]["restored"], 0)

    def test_preview_equals_real_recovery_for_every_strategy_and_corruption(self):
        for corrupt in (None, "snapshot", "wal", "replica"):
            for strategy in ("backup", "replica", "wal", "none"):
                with self.subTest(corrupt=corrupt, strategy=strategy):
                    ds = self.store(corrupt)
                    p = ds.record_map()["preview"][strategy]
                    ds.wipe_backend_local()
                    r = ds.recover(strategy)
                    self.assertEqual(p["lost"], r["missing_records"])
                    self.assertEqual(p["stale"], r["stale_records"])
                    self.assertEqual(p["resurrected"], r["unexpected_records"])
                    self.assertEqual(p["restored"], r["expected_records"] - len(r["missing_records"]) - len(r["stale_records"]))

    def test_timeline_reach_equals_real_restore(self):
        """The write timeline's markers are exactly how far each strategy's restore reaches."""
        for corrupt in (None, "snapshot", "wal", "replica"):
            for strategy, key in (("backup", "backup_lsn"), ("wal", "log_valid_lsn")):
                with self.subTest(corrupt=corrupt, strategy=strategy):
                    ds = self.store(corrupt)
                    t = ds.timeline()
                    self.assertEqual([e["op"] for e in t["events"][-3:]], ["create", "update", "delete"])
                    ds.wipe_backend_local()
                    r = ds.recover(strategy)
                    self.assertEqual(t[key] or 0, r["restored_lsn"])
                    self.assertEqual(r["timeline"]["events"], t["events"])      # the story up to the failure is kept

    def test_write_path_is_timed(self):
        w = self.store().write_path_ms()
        self.assertEqual(w["samples"], 14)
        self.assertTrue(all(w[k] >= 0 for k in ("log_ms", "primary_ms", "replica_ms")))

    def test_map_after_wipe_shows_primary_gone(self):
        ds = self.store()
        ds.wipe_backend_local()
        m = ds.record_map()
        self.assertIsNone(m["copies"]["primary"])
        self.assertEqual(len(m["copies"]["replica"]), 11)

    def test_limit_keeps_newest_records(self):
        m = self.store().record_map(limit=5)
        self.assertEqual(m["ids"], [8, 9, 10, 11, 12])
        self.assertEqual(m["hidden"], 7)


class TestBrownoutAccounting(unittest.TestCase):
    def test_degraded_service_time_reported_as_full_work(self):
        """A brownout response (20 ms of work at factor 0.4) is recorded as 50 ms of full work."""
        import json, threading
        from http.server import BaseHTTPRequestHandler, HTTPServer
        import config
        from load_balancer.metrics import MetricsCollector
        from load_balancer.server_registry import ServerRegistry
        from load_balancer.scheduler import SchedulerManager
        from load_balancer.balancer import LoadBalancer

        class H(BaseHTTPRequestHandler):
            def do_GET(self):
                deg = self.headers.get("X-Degrade") == "1"
                body = json.dumps({"status": "success", "server_id": "server-1", "service_ms": 20.0 if deg else 50.0,
                                   "queue_ms": 0.0, "degraded": deg}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass
        srv = HTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            reg, m = ServerRegistry(), MetricsCollector()
            reg.register_server("server-1", "Server 1", "127.0.0.1", srv.server_address[1])
            lb = LoadBalancer(reg, SchedulerManager(reg), m)
            got = []
            m.record_backend_report = lambda sid, svc, q: got.append(svc)
            self.assertEqual(lb.handle_request()[0], 200)
            lb.degrade = True
            code, body = lb.handle_request()
            self.assertEqual(code, 200)
            self.assertAlmostEqual(got[0], 50.0)
            self.assertAlmostEqual(got[1], 20.0 / config.BACKEND_DEGRADED_FACTOR)     # 50 ms, not 20 ms
        finally:
            srv.shutdown()
            srv.server_close()

    def test_utilisation_counts_work_and_counterfactual_is_full_work(self):
        from tests.test_resilience_controller import ControllerCase
        case = ControllerCase()
        ctl = case.make()
        case.lb.degrade = True
        for k in range(30):                                   # 30 req/s, all degraded, on 2 x 20 req/s
            t = case.clk.t - 4 + k / 30 * 4
            case.metrics.record_arrival(t)
            case.metrics.record_e2e(60.0, True, t)
            with case.metrics._lock:
                case.metrics._counter_events.append((t, "client_requests"))
                case.metrics._counter_events.append((t, "degraded_responses"))
        obs = ctl._observe(case.clk.t)
        full = obs["utilization_full_work"]
        self.assertAlmostEqual(obs["utilization"], full * 0.4, places=2)   # brownout work only
        self.assertAlmostEqual(ctl.utilization_without("degrade", obs), full, places=3)


if __name__ == "__main__":
    unittest.main()
