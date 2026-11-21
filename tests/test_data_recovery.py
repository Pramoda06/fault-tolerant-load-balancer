"""
Tests for the durable data tier and the total-outage disaster recovery.

All backends fail while durable data remains (14), snapshot + valid WAL replay (15),
no-backup data loss (16), corrupted backup / invalid log / corrupted replica detection (17),
reset during a recovery (18), replication of an accidental delete, routing gate behaviour.
"""

import os
import json
import shutil
import tempfile
import unittest

from load_balancer.datastore import DataStore, DataUnavailable, STATE_OK, STATE_LOST, STATE_DATA_LOST
from load_balancer.disaster import DisasterRecovery, IDLE, OUTAGE, RESTORED, DATA_LOSS
from load_balancer.server_registry import ServerRegistry
from load_balancer.metrics import MetricsCollector


class DataTestCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="dr_test_")
        self.ds = DataStore(self.dir, snapshot_interval=0)

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def seed(self, before=20, after=10, updates=3):
        for i in range(before):
            self.ds.create(f"r{i}", f"v{i}")
        self.ds.snapshot("test")
        for i in range(after):
            self.ds.create(f"late{i}", f"w{i}")
        for i in range(updates):
            self.ds.update(i + 1, f"v{i} updated")


class TestDataStore(DataTestCase):
    def test_write_path_is_durable_and_replicated(self):
        rec = self.ds.create("a", "1")
        self.assertEqual(rec["lsn"], 1)
        with open(self.ds.wal_path) as f:
            entry = json.loads(f.readline())
        self.assertEqual((entry["lsn"], entry["op"], entry["name"]), (1, "upsert", "a"))
        st = self.ds.status()
        self.assertEqual((st["primary"]["records"], st["replica"]["records"], st["wal"]["entries"]), (1, 1, 1))

    def test_snapshot_manifest(self):
        self.seed(5, 0, 0)
        st = self.ds.status()
        snap = st["snapshots"][-1]
        self.assertEqual((snap["records"], snap["lsn"], snap["valid"]), (5, 5, True))
        self.assertEqual(st["manifest"]["last_valid_snapshot_lsn"], 5)

    def test_backup_restore_loses_later_writes(self):
        self.seed()
        self.ds.wipe_backend_local()
        self.assertEqual(self.ds.state, STATE_LOST)
        with self.assertRaises(DataUnavailable):
            self.ds.create("x", "y")
        r = self.ds.recover("backup")
        self.assertEqual(r["restored_records"], 20)
        self.assertEqual(r["expected_records"], 30)
        self.assertEqual(len(r["missing_records"]), 10)        # written after the backup
        self.assertEqual(len(r["stale_records"]), 3)           # updated after the backup
        self.assertEqual(r["rpo_records"], 13)
        self.assertGreater(r["rpo_seconds"], 0)
        self.assertTrue(r["integrity_ok"])
        self.assertFalse(r["zero_data_loss"])
        self.assertEqual(self.ds.state, STATE_OK)

    def test_snapshot_plus_wal_replay_restores_everything(self):                # case 15
        self.seed()
        self.ds.wipe_backend_local()
        r = self.ds.recover("wal")
        self.assertIn("WAL replay", r["source"])
        self.assertEqual((r["restored_records"], r["rpo_records"], r["restored_lsn"]), (30, 0, 33))
        self.assertTrue(r["zero_data_loss"])
        records = {x["id"]: x for x in self.ds.list_records()}
        self.assertEqual(records[1]["value"], "v0 updated")

    def test_replica_restores_everything(self):
        self.seed()
        self.ds.wipe_backend_local()
        r = self.ds.recover("replica")
        self.assertTrue(r["zero_data_loss"])
        self.assertEqual(r["restored_records"], 30)

    def test_no_backup_means_permanent_loss(self):                              # case 16
        self.seed()
        self.ds.wipe_backend_local()
        r = self.ds.recover("none")
        self.assertEqual((r["restored_records"], r["rpo_records"]), (0, 30))
        self.assertFalse(r["source_available"])
        self.assertEqual(self.ds.state, STATE_DATA_LOST)
        with self.assertRaises(DataUnavailable):
            self.ds.create("x", "y")                            # not until the loss is acknowledged
        self.assertTrue(self.ds.accept_loss())
        self.assertEqual(self.ds.state, STATE_OK)
        self.ds.create("fresh", "start")
        self.assertEqual(len(self.ds.list_records()), 1)

    def test_corrupted_backup_detected_and_older_used(self):                    # case 17
        for i in range(5):
            self.ds.create(f"r{i}", "v")
        self.ds.snapshot("first")
        for i in range(5):
            self.ds.create(f"s{i}", "v")
        self.ds.snapshot("second")
        self.ds.corrupt("snapshot")                             # newest backup corrupted
        self.ds.wipe_backend_local()
        r = self.ds.recover("backup")
        self.assertTrue(any("SHA-256 mismatch" in n for n in r["notes"]))
        self.assertEqual(r["restored_records"], 5)              # fell back to the first backup
        self.assertIn("snap_0001", r["source"])

    def test_invalid_wal_entry_stops_replay(self):                              # case 17
        self.seed(10, 10, 0)
        self.ds.corrupt("wal")                                  # middle entry (LSN 11) tampered
        self.ds.wipe_backend_local()
        r = self.ds.recover("wal")
        self.assertTrue(any("checksum mismatch" in n for n in r["notes"]))
        self.assertEqual(r["restored_lsn"], 10)                 # replay stopped before the bad entry
        self.assertEqual(r["restored_records"], 10)
        self.assertEqual(r["rpo_records"], 10)
        self.assertTrue(r["integrity_ok"])                      # what WAS restored is consistent

    def test_corrupted_replica_record_excluded(self):                           # case 17
        self.seed(5, 0, 0)
        self.ds.corrupt("replica")
        self.ds.wipe_backend_local()
        r = self.ds.recover("replica")
        self.assertTrue(any("failed their checksum" in n for n in r["notes"]))
        self.assertEqual(r["restored_records"], 4)
        self.assertEqual(r["missing_records"], [1])

    def test_replica_also_copies_accidental_delete(self):
        self.seed(5, 0, 0)
        self.ds.delete(2)                                       # mistake replicated synchronously
        self.ds.wipe_backend_local()
        r = self.ds.recover("replica")
        self.assertNotIn(2, [x["id"] for x in self.ds.list_records()])
        self.assertTrue(r["zero_data_loss"])                    # relative to acknowledged state
        # the older backup still has it - but returns it as a "resurrected" record
        ds2_dir = tempfile.mkdtemp()
        try:
            ds2 = DataStore(ds2_dir, snapshot_interval=0)
            for i in range(5):
                ds2.create(f"r{i}", f"v{i}")
            ds2.snapshot()
            ds2.delete(2)
            ds2.wipe_backend_local()
            r2 = ds2.recover("backup")
            self.assertEqual(r2["unexpected_records"], [2])
        finally:
            shutil.rmtree(ds2_dir, ignore_errors=True)

    def test_survives_restart(self):
        self.seed(3, 0, 0)
        reopened = DataStore(self.dir, snapshot_interval=0)
        self.assertEqual(len(reopened.list_records()), 3)
        self.assertEqual(reopened.create("x", "y")["lsn"], 4)


class FakeFaults:
    def __init__(self, registry):
        self.registry = registry
        self.injected = []

    def inject(self, sid, ftype, params=None):
        self.injected.append((sid, ftype))
        return {}


class TestDisasterRecovery(DataTestCase):
    def setUp(self):
        super().setUp()
        self.reg = ServerRegistry()
        for i in (1, 2, 3):
            self.reg.register_server(f"server-{i}", f"Server {i}", "127.0.0.1", 5000 + i)
        self.metrics = MetricsCollector()
        self.faults = FakeFaults(self.reg)
        self.dr = DisasterRecovery(self.reg, self.faults, self.ds, self.metrics)

    def fail_all(self):
        for s in self.reg.get_all_servers():
            s.status = "UNHEALTHY"

    def replacement_ready(self):
        n = self.reg.register_server("server-4", "Server 4", "127.0.0.1", 5004)
        n.is_replacement = True

    def test_total_outage_with_durable_data(self):                              # case 14
        self.seed()
        self.dr.start("wal")
        self.assertEqual(len(self.faults.injected), 3)          # every backend failed
        self.assertEqual(self.dr.state, OUTAGE)
        self.assertIsNotNone(self.dr.routing_block_reason())    # no routing during the outage
        self.fail_all()
        self.dr.tick()                                          # outage detected
        self.assertIsNotNone(self.dr.status()["timings_ms"]["detection"])
        self.assertEqual(self.dr.state, OUTAGE)                 # no usable backend yet
        self.replacement_ready()
        self.dr.tick()                                          # backend ready -> restore -> verify
        self.assertEqual(self.dr.state, RESTORED)
        self.assertIsNone(self.dr.routing_block_reason())       # routing resumes only now
        rep = self.dr.status()["data_report"]
        self.assertEqual((rep["restored_records"], rep["rpo_records"], rep["integrity"]), (30, 0, "PASSED"))
        self.dr.on_request_served("server-4")
        self.assertIsNotNone(self.dr.status()["timings_ms"]["rto"])

    def test_no_durable_copy_requires_acknowledgement(self):                     # case 16
        self.seed()
        self.dr.start("none")
        self.fail_all()
        self.dr.tick()
        self.replacement_ready()
        self.dr.tick()
        self.assertEqual(self.dr.state, DATA_LOSS)
        self.assertIn("DATA LOST", self.dr.routing_block_reason())
        self.assertNotIn("restored and verified", self.dr.message)   # no false success message
        self.assertTrue(self.dr.acknowledge())
        self.assertIsNone(self.dr.routing_block_reason())

    def test_restore_waits_for_detection_and_ready_backend(self):
        self.seed(3, 0, 0)
        self.dr.start("replica")
        self.dr.tick()                    # servers still marked HEALTHY: outage not detected yet
        self.assertEqual(self.dr.state, OUTAGE)
        self.assertIsNone(self.ds.last_recovery)

    def test_reset_during_recovery(self):                                       # case 18
        self.seed(3, 0, 0)
        self.dr.start("wal")
        self.fail_all()
        self.dr.tick()
        self.dr.reset()
        self.assertEqual(self.dr.state, IDLE)
        self.assertIsNone(self.dr.routing_block_reason())
        self.ds.reset()
        self.assertEqual(self.ds.state, STATE_OK)
        self.assertEqual(self.ds.status()["primary"]["records"], 0)

    def test_invalid_strategy(self):
        with self.assertRaises(ValueError):
            self.dr.start("magic")


if __name__ == "__main__":
    unittest.main()
