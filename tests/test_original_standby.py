"""
Unit tests: scale-in of ORIGINAL servers into standby (never terminated), and their return.

Deterministic (fake clock, fake processes, fake probes; see tests/test_capacity.py).
Capacity model: 20 req/s per server, safe 14 req/s, scale-in only if the rest stays < 50 %.
"""

import unittest

from load_balancer.server_registry import ServerRegistry
from load_balancer.topology import build_topology
from tests.test_capacity import CapacityTestCase, make_registry


def weighted_registry():
    reg = ServerRegistry(recovery_threshold=2)
    for i, w in ((1, 3), (2, 2), (3, 1)):
        reg.register_server(f"server-{i}", f"Server {i}", "127.0.0.1", 5000 + i, weight=w).status = "HEALTHY"
    return reg


class TestOriginalStandby(CapacityTestCase):
    """Standby is optional (CAPACITY_ORIGINAL_STANDBY, default 0); these tests switch it on."""

    def make(self, settings_over=None, **kw):
        over = {"CAPACITY_ORIGINAL_STANDBY": 1}
        over.update(settings_over or {})
        return super().make(settings_over=over, **kw)

    def test_default_keeps_all_three_originals(self):
        CapacityTestCase.make(self, reg=weighted_registry())
        self.settle(5, 90)
        self.assertEqual(len(self.routable()), 3)
        self.assertFalse(self.events("BACKEND_STANDBY"))

    def settle(self, rps, seconds):
        for _ in range(seconds):
            self.run_load(rps, 1)

    def routable(self):
        return [s.id for s in self.reg.get_all_servers() if self.cap._routable(s)]

    def test_low_demand_moves_originals_to_standby_lowest_weight_first(self):
        self.make(reg=weighted_registry())
        self.settle(5, 90)
        self.assertEqual(self.routable(), ["server-1"])                       # highest weight stays
        standby_order = [e["details"]["backend_id"] for e in self.events("BACKEND_STANDBY")]
        self.assertEqual(standby_order, ["server-3", "server-2"])
        self.assertEqual([s.id for s in self.reg.get_all_servers()], ["server-1", "server-2", "server-3"])
        self.assertTrue(all(s.status == "HEALTHY" for s in self.reg.get_all_servers()))   # still health-checked
        self.assertEqual(self.launched, [])                                    # nothing started or stopped
        self.assertEqual(len(self.events("BACKEND_DRAINING")), 2)
        self.assertFalse(self.events("BACKEND_RETIRED"))                       # originals are never retired

    def test_hysteresis_keeps_a_server_and_explains_why(self):
        # 10 req/s: desired 1, but with one server the load would be 50 % (scale-in only below 50 %)
        self.make(reg=weighted_registry())
        self.settle(10, 90)
        self.assertEqual(self.routable(), ["server-1", "server-2"])
        st = self.cap.status()
        self.assertEqual(st["desired"], 1)
        self.assertIsNotNone(st["hold"])
        self.assertAlmostEqual(st["hold"]["utilization_after"], 0.5, places=2)
        self.assertIn("scale-in only happens below 50%", st["reason"])

    def test_min_servers_respected(self):
        self.make(reg=weighted_registry(), settings_over={"CAPACITY_MIN_SERVERS": 2})
        self.settle(5, 90)
        self.assertEqual(len(self.routable()), 2)

    def test_demand_rise_activates_standby_before_starting_processes(self):
        self.make(reg=weighted_registry())
        self.settle(5, 90)
        self.assertEqual(len(self.routable()), 1)
        launched_at_activation = []
        for _ in range(60):
            self.run_load(36, 1)                 # desired 3
            if len(self.routable()) == 3:
                launched_at_activation = list(self.launched)
                break
        self.assertEqual(sorted(self.routable()), ["server-1", "server-2", "server-3"])
        self.assertEqual(launched_at_activation, [], "a process was started while standby servers were available")
        act = [e["details"]["backend_id"] for e in self.events("BACKEND_ACTIVATED")]
        self.assertEqual(act, ["server-2", "server-3"])                       # highest weight first

    def test_replacements_scaled_in_before_originals(self):
        reg = make_registry()
        for sid in ("server-2", "server-3"):
            reg.get_server(sid).status = "UNHEALTHY"
        self.make(reg=reg)
        self.settle(36, 60)                              # two capacity replacements
        reps = [s.id for s in self.replacements()]
        self.assertEqual(len(reps), 2)
        for sid in ("server-2", "server-3"):
            reg.get_server(sid).status = "HEALTHY"
        self.settle(4, 150)
        order = [(e["event_type"], e["details"]["backend_id"]) for e in self.events()
                 if e["event_type"] in ("BACKEND_RETIRED", "BACKEND_STANDBY")]
        kinds = [k for k, _ in order]
        self.assertEqual(kinds[:2], ["BACKEND_RETIRED", "BACKEND_RETIRED"])
        self.assertTrue(set(kinds[2:]) <= {"BACKEND_STANDBY"})
        self.assertEqual(self.replacements(), [])
        self.assertEqual(len(self.routable()), 1)

    def test_routable_failure_activates_standby_immediately(self):
        self.make(reg=weighted_registry())
        self.settle(5, 90)
        self.assertEqual(self.routable(), ["server-1"])
        self.reg.get_server("server-1").status = "UNHEALTHY"
        # the registry already serves from standby before the manager reacts (no outage)
        self.assertEqual(sorted(s.id for s in self.reg.get_active_pool()), ["server-2", "server-3"])
        self.run_load(5, 1)
        self.assertEqual(self.routable(), ["server-2"])
        self.assertTrue(self.events("BACKEND_ACTIVATED"))
        self.assertEqual(self.launched, [])

    def test_disabled_keeps_originals_routable(self):
        self.make(reg=weighted_registry(), settings_over={"CAPACITY_ORIGINAL_STANDBY": 0})
        self.settle(5, 90)
        self.assertEqual(len(self.routable()), 3)
        self.assertFalse(self.events("BACKEND_STANDBY"))

    def test_topology_and_reset(self):
        self.make(reg=weighted_registry())
        self.settle(5, 90)
        servers = []
        for s in self.reg.get_all_servers():
            d = s.to_dict()
            d["routable"] = self.cap._routable(s)
            servers.append(d)
        t = build_topology(servers, None, self.cap.status())
        st = {n["id"]: n for n in t["nodes"]}
        self.assertEqual(st["server-3"]["state"], "STANDBY")
        self.assertFalse(st["server-3"]["routable"])
        self.assertIn("standby", st["server-3"]["eligibility"])
        self.assertEqual(t["standby"], 2)
        self.assertEqual(self.cap.status()["standby"], 2)
        self.reg.reset_all({})
        self.assertFalse(any(s.standby for s in self.reg.get_all_servers()))


class TestRegistryStandby(unittest.TestCase):
    def test_original_needs_explicit_permission_to_drain(self):
        reg = weighted_registry()
        self.assertFalse(reg.start_drain("server-3", 0.0))
        self.assertTrue(reg.start_drain("server-3", 0.0, allow_original=True))
        done, node, _ = reg.standby_if_drained("server-3", 1.0, 10.0)
        self.assertTrue(done and node.standby)
        self.assertNotIn("server-3", [s.id for s in reg.get_active_pool()])
        self.assertIsNotNone(reg.get_server("server-3"))          # still registered
        self.assertTrue(reg.activate_standby("server-3"))
        self.assertIn("server-3", [s.id for s in reg.get_active_pool()])


if __name__ == "__main__":
    unittest.main()
