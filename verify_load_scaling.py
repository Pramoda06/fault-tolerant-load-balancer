"""
Live demonstration / integration test: load-based server allocation after recovery.

Real processes, real HTTP traffic from the proxy's traffic generator, real replacement backends.
Every check reads the proxy's API (/api/status: registry, capacity manager, topology view, events).
The dashboard diagram draws exactly the `topology.nodes` checked here.

  1. start Server 1-3 and the reverse proxy (capacity manager on)
  2. controlled client traffic: 10 req/s  -> desired 1, no replacement (the count follows demand);
     the 3 original servers keep serving (originals are never scaled in by default)
  3. Server 2 and Server 3 fail (connection refused) -> one healthy backend is left
  4. service continues on Server 1; demand is still measured; no scale-out at 10 req/s
  5. demand rises to 36 req/s -> desired = ceil(36 / (20 x 0.7)) = 3 -> scale-out
  6. replacements appear one at a time: candidate (not routable) -> health checks -> registered
     (routable); topology, registry and capacity numbers agree at every sample
  7. demand falls to 6 req/s -> replacements drain, are retired (process stopped) and vanish
     from the topology; 0 client errors while draining
  8. the original servers are never terminated (Server 2/3 stay registered while failed, their
     processes keep running, and they rejoin after recovery)
  8b. demand rises again -> the recovered originals cover it; no new process is started
  9. failed servers receive no requests while they are not routable
 10. reset during a scale-out leaves no replacement process and no stale topology node

Usage:
    python verify_load_scaling.py
"""

import os
import sys
import time
import tempfile
import subprocess

import requests

BASE = "http://127.0.0.1:5000"
HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable
ORIGINALS = ["server-1", "server-2", "server-3"]
CAP_EVENTS = ("CAPACITY_EVALUATION_STARTED", "CAPACITY_TARGET_UPDATED", "SCALE_OUT_STARTED", "REPLACEMENT_PROVISIONED",
              "REPLACEMENT_HEALTHY", "BACKEND_REGISTERED", "SCALE_IN_STARTED", "BACKEND_DRAINING", "BACKEND_RETIRED",
              "CAPACITY_STABILIZED", "BACKEND_STANDBY", "BACKEND_ACTIVATED")


def start(script, env=None):
    return subprocess.Popen([PY, script], cwd=HERE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)


def wait_http(url, tries=60):
    for _ in range(tries):
        try:
            requests.get(url, timeout=1.0)
            return True
        except Exception:
            time.sleep(0.25)
    return False


class Watcher:
    """Polls /api/status, keeps every capacity event once, and checks consistency on each sample."""

    def __init__(self):
        self.events = {}
        self.max_candidates = 0
        self.seen_candidate_ids = set()
        self.samples = 0

    def status(self):
        st = requests.get(f"{BASE}/api/status", timeout=3).json()
        self.samples += 1
        for e in st["events"]:
            if e["event_type"] in CAP_EVENTS:
                self.events[(e["time_raw"], e["event_type"], e["message"])] = e
        self.check(st)
        return st

    def check(self, st):
        topo, servers = st["topology"], st["servers"]
        ids = [s["id"] for s in servers]
        nodes = topo["nodes"]
        reg_nodes = [n["id"] for n in nodes if n["kind"] == "server"]
        assert reg_nodes == ids, f"topology servers {reg_nodes} != registry {ids}"
        cands = [n for n in nodes if n["kind"] == "candidate"]
        retired = [n for n in nodes if n["kind"] == "retired"]
        assert all(not n["routable"] for n in cands + retired), "a candidate/retired node is marked routable"
        assert not any(n["id"] in ids for n in cands + retired), "candidate/retired node is also registered"
        for n in nodes:
            if n["kind"] == "server":
                s = next(x for x in servers if x["id"] == n["id"])
                assert n["routable"] == s["routable"], f"routable mismatch for {n['id']}"
                if n["routable"]:
                    assert s["status"] == "HEALTHY" and not s["draining"], f"{n['id']} routable but {s['status']}"
        assert topo["routable"] == sum(1 for s in servers if s["routable"])
        self.max_candidates = max(self.max_candidates, len(cands))
        self.seen_candidate_ids |= {n["id"] for n in cands}

    def of(self, etype):
        return sorted((e for e in self.events.values() if e["event_type"] == etype), key=lambda e: e["time_raw"])

    def wait(self, cond, timeout, step=0.25):
        end = time.time() + timeout
        while time.time() < end:
            st = self.status()
            if cond(st):
                return st
            time.sleep(step)
        return None


def srv(st, sid):
    return next((s for s in st["servers"] if s["id"] == sid), None)


def pid_alive(pid):
    if not pid:
        return False
    if os.name == "nt":
        import ctypes
        h = ctypes.windll.kernel32.OpenProcess(0x00100000 | 0x1000, False, pid)
        if not h:
            return False
        try:
            return ctypes.windll.kernel32.WaitForSingleObject(h, 0) == 0x102
        finally:
            ctypes.windll.kernel32.CloseHandle(h)
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().split(")")[-1].split()[0] != "Z"
    except OSError:
        return True


def ok(msg):
    print(f"  [OK] {msg}")


def main():
    print("=" * 72)
    print(" LOAD-BASED SERVER ALLOCATION AFTER RECOVERY — LIVE VERIFICATION")
    print("=" * 72)
    procs = []
    env = dict(os.environ, LB_DATA_DIR=tempfile.mkdtemp(prefix="lb_scale_"), LB_CAPACITY="1",
               LB_RESILIENCE_POLICY="scale_only")   # tests the reactive capacity mechanism itself
    w = Watcher()
    try:
        print("\n[1] Start Server 1-3 and the reverse proxy")
        for i in (1, 2, 3):
            procs.append(start(os.path.join("backend_servers", f"server{i}.py")))
        for port in (5001, 5002, 5003):
            assert wait_http(f"http://127.0.0.1:{port}/health"), f"backend {port} did not start"
        procs.append(start("app.py", env))
        assert wait_http(f"{BASE}/api/status"), "proxy did not start"
        st = w.wait(lambda s: all(srv(s, x)["status"] == "HEALTHY" for x in ORIGINALS), 15)
        assert st and [n["id"] for n in st["topology"]["nodes"]] == ORIGINALS
        ok("3 original servers registered; topology shows exactly these 3 nodes")

        print("\n[2] Controlled traffic: 10 req/s")
        requests.post(f"{BASE}/api/traffic/start", json={"rps": 10, "concurrency": 24, "clients": 16})
        st = w.wait(lambda s: s["capacity"]["enough_window"] and s["capacity"]["phase"] == "STABLE"
                    and s["capacity"]["demand_rps"] >= 9, 40)
        assert st, "capacity manager never became STABLE"
        c = st["capacity"]
        assert c["desired"] == 1 and not any(s["is_replacement"] for s in st["servers"]), c
        ok(f"{c['formula']} -> no replacement (capacity per server {c['capacity_rps_per_server']} req/s, "
           f"{c['capacity_kind']})")
        time.sleep(6)
        st = w.status()
        assert [n["state"] for n in st["topology"]["nodes"]] == ["HEALTHY", "HEALTHY", "HEALTHY"], \
            "an original server left routing at low demand (standby is off by default)"
        ok("the 3 original servers keep serving at low demand (CAPACITY_ORIGINAL_STANDBY = 0)")

        print("\n[3] Server 2 and Server 3 fail (connection refused): one healthy backend left")
        for sid in ("server-2", "server-3"):
            r = requests.post(f"{BASE}/api/faults/inject", json={"server_id": sid, "type": "refused"}).json()
            assert r["status"] == "success", r
        st = w.wait(lambda s: all(srv(s, x)["status"] == "UNHEALTHY" for x in ("server-2", "server-3")), 15)
        assert st, "failures not detected"
        assert st["topology"]["routable"] == 1 and [n["id"] for n in st["topology"]["nodes"] if n["routable"]] == ["server-1"]
        ok("failures detected and isolated: Server 2/3 UNHEALTHY and not routable; Server 1 is the only routable node")

        print("\n[4] Service continues; demand measured; no scale-out at 10 req/s")
        served_before = {x: srv(st, x)["requests_served"] for x in ("server-2", "server-3")}
        tr0 = st["traffic"]
        end = time.time() + 12
        while time.time() < end:
            s = w.status()
            assert not s["topology"]["pending_provisions"] and not any(x["is_replacement"] for x in s["servers"]), \
                "provisioned although demand fits one server"
            time.sleep(0.5)
        st = w.status()
        tr1 = st["traffic"]
        c = st["capacity"]
        ok(f"12 s on one backend: {tr1['ok'] - tr0['ok']}/{tr1['sent'] - tr0['sent']} client requests succeeded; "
           f"{c['formula']}; nothing provisioned (the count follows demand, not the failures)")

        print("\n[5] Demand rises to 36 req/s")
        requests.post(f"{BASE}/api/traffic/rate", json={"rps": 36})
        t_up = time.time()
        st = w.wait(lambda s: s["topology"]["pending_provisions"] >= 1 or any(x["is_replacement"] for x in s["servers"]), 40, 0.2)
        assert st, "no scale-out"
        ok(f"scale-out started {time.time() - t_up:.1f} s after the increase "
           f"(observation window + {st['capacity']['scale_out_sustain_s']:g} s sustained overload)")

        print("\n[6] Replacements provisioned, checked and registered one at a time")
        st = w.wait(lambda s: s["topology"]["routable"] >= 3 and s["capacity"]["desired"] == 3
                    and not s["topology"]["pending_provisions"], 90, 0.2)
        assert st, "did not reach 3 routable backends"
        reps = [s for s in st["servers"] if s["is_replacement"]]
        assert len(reps) == 2 and all(s["origin"] == "capacity" for s in reps), [s["name"] for s in reps]
        assert w.max_candidates == 1, f"{w.max_candidates} replacements provisioned at once"
        assert {s["id"] for s in reps} <= w.seen_candidate_ids, "a replacement was never shown as a candidate first"
        for s in reps:
            regs = [e for e in w.of("BACKEND_REGISTERED") if e["details"]["backend_id"] == s["id"]]
            heal = [e for e in w.of("REPLACEMENT_HEALTHY") if e["details"]["backend_id"] == s["id"]]
            prov = [e for e in w.of("REPLACEMENT_PROVISIONED") if e["details"]["backend_id"] == s["id"]]
            assert prov and heal and regs, f"missing events for {s['id']}"
            assert prov[0]["time_raw"] <= heal[0]["time_raw"] <= regs[0]["time_raw"]
        for e in w.of("BACKEND_REGISTERED"):
            d = e["details"]
            assert {"backend_id", "previous_healthy", "desired", "observed_rps", "reason"} <= set(d), d
        topo = st["topology"]
        assert [n["id"] for n in topo["nodes"]] == [s["id"] for s in st["servers"]]
        served_after = {x: srv(st, x)["requests_served"] for x in ("server-2", "server-3")}
        assert served_after == served_before, f"failed servers received requests: {served_before} -> {served_after}"
        ok("each replacement: SCALE_OUT_STARTED -> REPLACEMENT_PROVISIONED (candidate, not routable) -> "
           "REPLACEMENT_HEALTHY -> BACKEND_REGISTERED (routable); never two at once")
        ok(f"{st['capacity']['formula']}; routable: "
           + ", ".join(n["name"] for n in topo["nodes"] if n["routable"]) + f"; topology nodes = registry ({len(topo['nodes'])})")
        ok("Server 2/3 served 0 requests while failed")

        print("\n[7] Demand falls to 6 req/s: safe scale-in")
        requests.post(f"{BASE}/api/traffic/rate", json={"rps": 6})
        fail0 = st["traffic"]["failed"]
        rep_pids = {s["id"]: s["pid"] for s in reps}
        seen_retired = set()

        def done(s):
            seen_retired.update(n["id"] for n in s["topology"]["nodes"] if n["kind"] == "retired")
            return not any(x["is_replacement"] for x in s["servers"])
        st = w.wait(done, 120, 0.3)
        assert st, "replacements not retired"
        assert [s["id"] for s in st["servers"]] == ORIGINALS
        assert seen_retired == set(rep_pids), f"retired nodes shown: {seen_retired}"
        assert st["traffic"]["failed"] - fail0 == 0, f"{st['traffic']['failed'] - fail0} client errors while draining"
        for sid, pid in rep_pids.items():
            assert not pid_alive(pid), f"{sid} process still running"
        retired_ev = w.of("BACKEND_RETIRED")
        assert {e["details"]["backend_id"] for e in retired_ev} == set(rep_pids)
        assert len([e for e in w.of("BACKEND_DRAINING") if e["details"]["backend_id"] in rep_pids]) == 2
        assert len([e for e in w.of("SCALE_IN_STARTED") if e["details"]["backend_id"] in rep_pids]) == 2
        ok("SCALE_IN_STARTED -> BACKEND_DRAINING -> BACKEND_RETIRED for both replacements; processes exited; "
           "0 client errors while draining")
        st = w.wait(lambda s: not any(n["kind"] == "retired" for n in s["topology"]["nodes"]), 20)
        assert st and [n["id"] for n in st["topology"]["nodes"]] == ORIGINALS
        ok("retired nodes were shown greyed without a link, then left the topology; 3 original nodes remain")
        st = w.wait(lambda s: w.of("CAPACITY_STABILIZED") and w.of("CAPACITY_STABILIZED")[-1]["time_raw"]
                    >= retired_ev[-1]["time_raw"], 20)
        assert st, "no CAPACITY_STABILIZED after scale-in"
        ok(f"CAPACITY_STABILIZED: {w.of('CAPACITY_STABILIZED')[-1]['message']}")

        print("\n[8] Original servers were never terminated")
        for port in (6002, 6003):
            cs = requests.get(f"http://127.0.0.1:{port}/control/status", timeout=2).json()
            assert cs["listener_up"] is False, f"port {port}: {cs}"
        requests.post(f"{BASE}/api/faults/clear", json={})
        st = w.wait(lambda s: all(srv(s, x)["status"] == "HEALTHY" for x in ORIGINALS), 20)
        assert st and [s["id"] for s in st["servers"]] == ORIGINALS
        ok("Server 2/3 stayed registered while failed, their processes kept running, and they rejoined after recovery")

        print("\n[8b] Demand rises again: the recovered originals cover it")
        cands_before = set(w.seen_candidate_ids)
        requests.post(f"{BASE}/api/traffic/rate", json={"rps": 36})
        st = w.wait(lambda s: s["topology"]["routable"] == 3 and s["capacity"]["phase"] == "STABLE", 60, 0.3)
        assert st, "originals did not cover the demand"
        assert [n["id"] for n in st["topology"]["nodes"] if n["routable"]] == ORIGINALS
        assert not any(s["is_replacement"] for s in st["servers"]) and w.seen_candidate_ids == cands_before, \
            "a new process was started although the original servers were available"
        ok(f"{st['capacity']['formula']}; the 3 originals are routable; no new process started")

        print("\n[9] No duplicate events")
        n0 = len(w.events)
        end = time.time() + 8
        while time.time() < end:
            w.status()
            time.sleep(0.5)
        st = w.status()
        new = [e for e in w.events.values() if e["time_raw"] > end - 8]
        assert all(e["event_type"] in ("CAPACITY_TARGET_UPDATED", "CAPACITY_STABILIZED") for e in new), \
            [e["event_type"] for e in new]
        assert len(new) <= 2, f"{len(new)} capacity events in 8 s of steady state"
        counts = {t: len(w.of(t)) for t in CAP_EVENTS}
        ok(f"{len(new)} capacity event(s) in 8 s of steady state; totals {counts}")

        print("\n[10] Reset during a scale-out leaves nothing behind")
        for sid in ("server-2", "server-3"):
            requests.post(f"{BASE}/api/faults/inject", json={"server_id": sid, "type": "refused"})
        requests.post(f"{BASE}/api/traffic/rate", json={"rps": 36})
        st = w.wait(lambda s: s["topology"]["pending_provisions"] >= 1 or any(x["is_replacement"] for x in s["servers"]), 60, 0.2)
        assert st, "no scale-out to interrupt"
        pids = [n["pid"] for n in st["topology"]["nodes"] if n["kind"] in ("candidate", "server") and n["role"] != "original"]
        pids = [p for p in pids if p]
        requests.post(f"{BASE}/api/reset-simulation", timeout=15)
        st = w.wait(lambda s: [n["id"] for n in s["topology"]["nodes"]] == ORIGINALS
                    and all(srv(s, x)["status"] == "HEALTHY" for x in ORIGINALS), 20)
        assert st, "topology not back to the 3 originals after reset"
        time.sleep(1.5)
        assert not any(pid_alive(p) for p in pids), f"orphaned replacement process(es): {pids}"
        assert not st["traffic"]["running"]
        ok(f"reset: only the 3 originals remain (registry and topology); replacement process(es) {pids or '(none started yet)'} stopped")

        print(f"\n  {w.samples} API samples checked: topology == registry, no candidate or retired node ever routable.")
        print("\n" + "=" * 72)
        print(" LOAD-BASED ALLOCATION VERIFIED — ALL STEPS PASSED")
        print("=" * 72)
        return 0
    except AssertionError as e:
        print(f"\n  [FAIL] {e}")
        return 1
    finally:
        try:
            requests.post(f"{BASE}/api/traffic/stop", timeout=2)
        except Exception:
            pass
        for p in procs:
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    p.kill()


if __name__ == "__main__":
    sys.exit(main())
