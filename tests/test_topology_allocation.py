"""
Unit tests for load-based server allocation as shown in the topology diagram.

Deterministic: the capacity manager runs on a fake clock with fake processes and probes (see
tests/test_capacity.py). The topology view (load_balancer/topology.py) is built from the same
registry and manager state that /api/status serves to the dashboard.

Covers: capacity formula with measured / configured / simulated capacity, the scale-out sequence
and its events with structured details, a candidate never routable before registration,
failed provisioning never registered, scale-in with a retired node that leaves the topology,
no duplicate events in steady state or around a threshold, reset clearing the topology,
the topology builder itself, and the /api/status + /api/faults/clear endpoints.
"""

import unittest

from load_balancer.topology import build_topology
from tests.test_capacity import CapacityTestCase, make_registry


def server_dicts(cap):
    """What app._server_dicts provides: registry node + routable flag."""
    out = []
    for s in cap.registry.get_all_servers():
        d = s.to_dict()
        d["routable"] = cap._routable(s)
        out.append(d)
    return out


def topo(cap):
    st = cap.status()
    return build_topology(server_dicts(cap), None, st)


def one_healthy_registry():
    reg = make_registry()
    for sid in ("server-2", "server-3"):
        reg.get_server(sid).status = "UNHEALTHY"
    return reg


class TestCapacityFormula(CapacityTestCase):
    def test_example_from_brief_with_configured_capacity(self):
        # 120 req/s, 50 req/s per backend, target 70 % -> ceil(120 / 35) = 4
        cap = self.make(settings_over={"CAPACITY_PER_SERVER_RPS": 50.0})
        self.run_load(120, 10)
        est = cap.estimate()
        self.assertEqual(est["capacity_kind"], "configured")
        self.assertEqual(est["capacity_rps_per_server"], 50.0)
        self.assertEqual(est["safe_rps_per_server"], 35.0)
        self.assertEqual(est["desired"], 4)

    def test_simulated_capacity_is_labelled_when_not_measured(self):
        cap = self.make()
        est = cap.estimate()
        self.assertEqual(est["capacity_kind"], "simulated")
        self.assertIn("simulated", est["capacity_source"])
        self.assertEqual(est["capacity_rps_per_server"], 20.0)

    def test_measured_capacity_from_backend_reports(self):
        cap = self.make()
        for _ in range(10):
            self.metrics.record_backend_report("server-1", 100.0, 0.0)
        est = cap.estimate()
        self.assertEqual(est["capacity_kind"], "measured")
        self.assertAlmostEqual(est["capacity_rps_per_server"], 10.0, places=1)

    def test_limits_applied(self):
        cap = self.make(settings_over={"CAPACITY_MAX_SERVERS": 4})
        self.run_load(200, 10)
        self.assertEqual(cap.estimate()["desired"], 4)


class TestScaleOutInTopology(CapacityTestCase):
    def test_scale_out_sequence_events_and_topology(self):
        seen, in_registry = [], []

        def prober(url):
            t = topo(self.cap)                # topology while the candidate is being health-checked
            seen.append(t)
            in_registry.extend(self.reg.get_server(n["id"]) is not None for n in t["nodes"] if n["kind"] == "candidate")
            return True, 1.0

        cap = self.make(reg=one_healthy_registry(), prober=prober)
        # 10 s window + 5 s sustained overload; desired 3, one routable -> first replacement
        for _ in range(20):
            self.run_load(36, 1)
            if self.replacements():
                break
        self.assertTrue(seen, "no candidate was health-checked")
        t = seen[0]
        cand = [n for n in t["nodes"] if n["kind"] == "candidate"]
        self.assertEqual(len(cand), 1)
        self.assertFalse(cand[0]["routable"])
        self.assertEqual(cand[0]["role"], "capacity")
        self.assertEqual(cand[0]["state"], "HEALTH CHECK")
        self.assertEqual(set(in_registry), {False})          # never in the registry while a candidate
        self.assertEqual(t["pending_provisions"], 1)
        self.assertEqual(t["routable"], 1)

        # after registration the node is a routable server, not a candidate
        after = topo(cap)
        rep = [n for n in after["nodes"] if n["role"] == "capacity"]
        self.assertEqual(len(rep), 1)
        self.assertEqual(rep[0]["kind"], "server")
        self.assertTrue(rep[0]["routable"])
        self.assertEqual(after["pending_provisions"], 0)

        order = [e["event_type"] for e in self.events() if e["event_type"] in
                 ("SCALE_OUT_STARTED", "REPLACEMENT_PROVISIONED", "REPLACEMENT_HEALTHY", "BACKEND_REGISTERED")]
        self.assertEqual(order[:4], ["SCALE_OUT_STARTED", "REPLACEMENT_PROVISIONED", "REPLACEMENT_HEALTHY",
                                     "BACKEND_REGISTERED"])
        reg_ev = self.events("BACKEND_REGISTERED")[0]["details"]
        self.assertEqual(reg_ev["backend_id"], rep[0]["id"])
        self.assertEqual(reg_ev["previous_healthy"], 1)
        self.assertEqual(reg_ev["desired"], 3)
        self.assertGreater(reg_ev["observed_rps"], 30)
        self.assertIn("reason", reg_ev)
        self.assertTrue(self.events("CAPACITY_EVALUATION_STARTED"))
        tgt = self.events("CAPACITY_TARGET_UPDATED")
        self.assertEqual(tgt[-1]["details"]["desired"], 3)

    def test_reaches_desired_one_at_a_time_and_stabilizes(self):
        counts = []
        cap = self.make(reg=one_healthy_registry())
        for _ in range(60):
            self.run_load(36, 1)
            counts.append(topo(cap)["pending_provisions"])
        t = topo(cap)
        self.assertEqual(t["routable"], 3)
        self.assertEqual(t["replacements_active"], 2)
        self.assertLessEqual(max(counts), 1)
        self.assertEqual(len(self.events("BACKEND_REGISTERED")), 2)
        self.assertTrue(self.events("CAPACITY_STABILIZED"))

    def test_failed_health_checks_never_register(self):
        cap = self.make(reg=one_healthy_registry(), healthy=False)
        self.run_load(36, 12)
        t = topo(cap)
        self.assertFalse(any(n["role"] == "capacity" for n in t["nodes"]))
        self.assertEqual(t["routable"], 1)
        self.assertFalse(self.events("BACKEND_REGISTERED"))
        self.assertTrue(all(p.terminated for _, _, _, p in self.launched))

    def test_scale_in_drains_retires_and_leaves_topology(self):
        cap = self.make(reg=one_healthy_registry())
        for _ in range(60):
            self.run_load(36, 1)
        reps = {s.id for s in self.replacements()}
        self.assertEqual(len(reps), 2)
        draining_seen, retired_seen = set(), set()
        for _ in range(120):
            self.run_load(4, 1)
            t = topo(cap)
            draining_seen |= {n["id"] for n in t["nodes"] if n["state"] == "DRAINING"}
            for n in t["nodes"]:
                if n["kind"] == "retired":
                    retired_seen.add(n["id"])
                    self.assertFalse(n["routable"])
                    self.assertIsNone(self.reg.get_server(n["id"]))
            if not self.replacements() and not any(n["kind"] == "retired" for n in t["nodes"]):
                break
        self.assertEqual(draining_seen, reps)
        self.assertEqual(retired_seen, reps)
        t = topo(cap)
        self.assertEqual([n["id"] for n in t["nodes"]], ["server-1", "server-2", "server-3"])
        self.assertEqual({e["details"]["backend_id"] for e in self.events("BACKEND_RETIRED")}, reps)
        self.assertEqual(len(self.events("SCALE_IN_STARTED")), 2)
        self.assertEqual(len(self.events("BACKEND_DRAINING")), 2)
        self.assertEqual(self.reg.get_server("server-2").status, "UNHEALTHY")   # originals untouched

    def test_no_duplicate_events_in_steady_state(self):
        cap = self.make()
        self.run_load(10, 90)          # let scaling settle first (originals may move to standby)
        n = len(self.events())
        self.run_load(10, 30)
        new = self.events()[n:]
        self.assertEqual(new, [], [e["event_type"] for e in new])

    def test_target_not_announced_for_short_fluctuations(self):
        cap = self.make()
        self.run_load(13, 12)                       # desired 1 announced
        base = len(self.events("CAPACITY_TARGET_UPDATED"))
        for _ in range(10):                          # around the 14 req/s threshold
            self.run_load(15, 1)
            self.run_load(13, 1)
        self.assertLessEqual(len(self.events("CAPACITY_TARGET_UPDATED")) - base, 1)

    def test_reset_clears_candidates_and_retired_nodes(self):
        cap = self.make(reg=one_healthy_registry())
        for _ in range(60):
            self.run_load(36, 1)
        for _ in range(25):
            self.run_load(4, 1)
            if any(n["kind"] == "retired" for n in topo(cap)["nodes"]):
                break
        cap.reset()
        t = topo(cap)
        self.assertEqual([n["id"] for n in t["nodes"]], ["server-1", "server-2", "server-3"])
        self.assertEqual(t["pending_provisions"], 0)
        self.assertTrue(all(p.terminated for _, _, _, p in self.launched))


class TestOutageAndScaleInFixes(CapacityTestCase):
    def test_failed_replacements_retired_during_total_outage(self):
        """A total outage must not leave dead replacements registered: they would keep stage-1
        recovery at its replacement limit and service could never come back."""
        cap = self.make(reg=one_healthy_registry())
        for _ in range(60):
            self.run_load(36, 1)
        reps = [s.id for s in self.replacements()]
        self.assertEqual(len(reps), 2)
        for s in self.reg.get_all_servers():
            s.status = "UNHEALTHY"                      # everything down, replacements included
        for _ in range(20):
            self.run_load(0, 1)
        self.assertEqual(self.replacements(), [])
        self.assertEqual({e["details"]["backend_id"] for e in self.events("BACKEND_RETIRED")}, set(reps))
        self.assertTrue(all(p.terminated for _, _, _, p in self.launched))

    def test_scale_in_candidate_and_countdown_reported(self):
        cap = self.make(reg=one_healthy_registry())
        for _ in range(60):
            self.run_load(36, 1)
        for sid in ("server-2", "server-3"):
            self.reg.get_server(sid).status = "HEALTHY"
        seen = None
        for _ in range(20):
            self.run_load(4, 1)
            st = cap.status()
            if st["scale_in_candidate"]:
                seen = st
                break
        self.assertIsNotNone(seen, "scale-in never became pending")
        self.assertIn(seen["scale_in_candidate"]["id"], [s.id for s in self.replacements()])
        self.assertGreaterEqual(seen["scale_in_eta_s"], 0)
        self.assertLessEqual(seen["scale_in_eta_s"], seen["scale_in_sustain_s"])


class TestDisasterRemovesReplacements(unittest.TestCase):
    def test_disaster_start_retires_replacements_first(self):
        from load_balancer.disaster import DisasterRecovery
        from load_balancer.metrics import MetricsCollector

        class Cap:
            calls = 0

            def retire_all_replacements(self):
                Cap.calls += 1
                return 2

            def activate_all_standby(self):
                return 0

            def notify(self):
                pass

        class Faults:
            def __init__(self):
                self.injected = []

            def inject(self, sid, ftype):
                self.injected.append(sid)

        class Store:
            ledger = {}

            def wipe_backend_local(self):
                return 0

        reg = make_registry()
        f = Faults()
        dr = DisasterRecovery(reg, f, Store(), MetricsCollector(), capacity=Cap())
        dr.start("wal")
        self.assertEqual(Cap.calls, 1)
        self.assertEqual(f.injected, ["server-1", "server-2", "server-3"])


class TestTopologyBuilder(unittest.TestCase):
    def srv(self, i, status="HEALTHY", **kw):
        d = {"id": f"server-{i}", "name": f"Server {i}", "port": 5000 + i, "status": status, "origin": "original",
             "is_replacement": False, "draining": False, "routable": status == "HEALTHY", "pid": None,
             "circuit": {"state": "CLOSED"}}
        d.update(kw)
        return d

    def test_recovery_candidate_and_dedupe(self):
        servers = [self.srv(1, "UNHEALTHY"), self.srv(2, "UNHEALTHY")]
        rec = {"episode": {"stage": "health_check", "candidate": {"id": "server-4", "name": "Server 4", "port": 5004}}}
        t = build_topology(servers, rec, None)
        c = [n for n in t["nodes"] if n["kind"] == "candidate"]
        self.assertEqual((c[0]["id"], c[0]["role"], c[0]["routable"]), ("server-4", "recovery", False))
        servers.append(self.srv(4, origin="recovery", is_replacement=True))
        t = build_topology(servers, rec, None)
        self.assertEqual([n["id"] for n in t["nodes"]], ["server-1", "server-2", "server-4"])
        self.assertEqual(t["nodes"][-1]["kind"], "server")

    def test_failed_candidate_hidden_when_service_exists(self):
        rec = {"episode": {"stage": "failed", "candidate": {"id": "server-4", "name": "Server 4"}}}
        self.assertEqual(len(build_topology([self.srv(1)], rec, None)["nodes"]), 1)
        self.assertEqual(build_topology([self.srv(1, "UNHEALTHY")], rec, None)["nodes"][-1]["state"], "FAILED")

    def test_draining_and_circuit_open_not_routable(self):
        servers = [self.srv(1), self.srv(5, origin="capacity", is_replacement=True, draining=True, routable=False),
                   self.srv(6, routable=False, circuit={"state": "OPEN"})]
        t = build_topology(servers, None, {"desired": 2})
        st = {n["id"]: n for n in t["nodes"]}
        self.assertEqual(st["server-5"]["state"], "DRAINING")
        self.assertIn("draining", st["server-5"]["eligibility"])
        self.assertIn("circuit open", st["server-6"]["eligibility"])
        self.assertEqual((t["routable"], t["draining"], t["desired"]), (1, 1, 2))

    def test_desired_is_a_number_not_nodes(self):
        t = build_topology([self.srv(1)], None, {"desired": 5})
        self.assertEqual(len(t["nodes"]), 1)
        self.assertEqual(t["desired"], 5)


class TestStatusApi(unittest.TestCase):
    def test_status_exposes_topology_matching_registry(self):
        from app import app
        data = app.test_client().get("/api/status").get_json()
        self.assertIn("topology", data)
        self.assertEqual([n["id"] for n in data["topology"]["nodes"] if n["kind"] == "server"],
                         [s["id"] for s in data["servers"]])
        for k in ("demand_rps", "capacity_rps_per_server", "capacity_kind", "target_utilization", "desired",
                  "pending", "phase", "reason"):
            self.assertIn(k, data["capacity"])

    def test_clear_all_faults_without_server_id(self):
        from app import app, faults
        faults.reset()                       # independent of faults left by other tests
        c = app.test_client()
        r = c.post("/api/faults/inject", json={"server_id": "server-2", "type": "partition"})
        self.assertEqual(r.status_code, 200)
        self.assertIn("server-2", faults.active_by_server())
        r = c.post("/api/faults/clear", json={})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(faults.active_by_server(), {})


if __name__ == "__main__":
    unittest.main()
