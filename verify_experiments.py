"""
Live verification of the two repeatable experiments (integration test).

1. Disaster-recovery comparison (backup / replica / wal / none) on identical data:
   20 records before a backup, 10 after it, 3 updates after it, then total backend failure
   with loss of the backend-local data. Expected from the design (checked, not assumed):
     backup   restores 20 records, loses the 10 later records and the 3 later updates
     replica  restores all 30, no acknowledged write lost
     wal      backup + log replay restores all 30
     none     restores nothing, needs operator acknowledgement
   A second, short run corrupts the newest backup and checks that it is detected.
2. Failover comparison of the six routing strategies with the same workload and fault
   (connection refused on Server 2 between 30 % and 70 % of the run).

Usage:
    python verify_experiments.py
"""

import os
import sys
import json
import time
import tempfile
import subprocess

import requests

BASE = "http://127.0.0.1:5000"
HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable


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


def run_experiment(kind, params, timeout):
    r = requests.post(f"{BASE}/api/experiments/{kind}", json=params, timeout=5)
    assert r.status_code == 200, r.text
    name = "dr_comparison" if kind == "dr" else "failover_comparison"
    end = time.time() + timeout
    last = None
    while time.time() < end:
        st = requests.get(f"{BASE}/api/experiments", timeout=5).json()
        prog = st["state"]["progress"]
        if prog != last:
            print(f"     ... {prog}")
            last = prog
        if not st["state"]["running"]:
            assert st["state"]["progress"] == "finished", st["state"]
            return st["reports"][name]
        time.sleep(1)
    raise AssertionError(f"{kind} experiment did not finish in {timeout} s")


def ok(msg):
    print(f"  [OK] {msg}")


def main():
    print("=" * 70)
    print(" EXPERIMENTS — LIVE VERIFICATION")
    print("=" * 70)
    procs = []
    env = dict(os.environ, LB_DATA_DIR=tempfile.mkdtemp(prefix="lb_exp_"), LB_CAPACITY="1",
               LB_RESILIENCE_POLICY="scale_only")
    try:
        for i in (1, 2, 3):
            procs.append(start(os.path.join("backend_servers", f"server{i}.py")))
        for port in (5001, 5002, 5003):
            assert wait_http(f"http://127.0.0.1:{port}/health"), f"backend {port} did not start"
        procs.append(start("app.py", env))
        assert wait_http(f"{BASE}/api/status"), "proxy did not start"
        time.sleep(2.5)

        print("\n[1] Disaster-recovery comparison")
        rep = run_experiment("dr", {}, 300)
        res = {r["strategy"]: r for r in rep["results"]}
        for s in ("backup", "replica", "wal", "none"):
            r = res[s]
            print(f"     {s:8s} restored {r['restored_records']}/{r['acknowledged_records']}  lost {r['lost_records']}  "
                  f"RPO {r['rpo_seconds']} s  integrity {r['integrity']}  RTO {r['timings_ms']['rto']} ms  "
                  f"ack needed {r['operator_ack_required']}")
        assert res["backup"]["restored_records"] == 20 and res["backup"]["lost_records"] == 13, res["backup"]
        assert res["replica"]["lost_records"] == 0 and res["replica"]["restored_records"] == 30
        assert res["wal"]["lost_records"] == 0 and res["wal"]["restored_records"] == 30
        assert res["none"]["restored_records"] == 0 and res["none"]["operator_ack_required"]
        assert all(res[s]["timings_ms"]["rto"] for s in res), "RTO missing"
        assert all(res[s]["integrity"] == "PASSED" for s in ("backup", "replica", "wal"))
        ok("backup lost 13 (10 later writes + 3 later updates); replica and WAL lost 0; none lost all and "
           "needed acknowledgement; integrity verified; RTO measured for every run")
        ok(f"report saved: {rep.get('file')}")

        print("\n[1b] Corrupted newest backup is detected")
        rep2 = run_experiment("dr", {"strategies": ["backup"], "corrupt": "snapshot", "records_before_backup": 5,
                                     "records_after_backup": 0, "updates_after_backup": 0}, 120)
        r = rep2["results"][0]
        assert any("SHA-256 mismatch" in n for n in r["notes"]), r["notes"]
        print(f"     notes: {r['notes']}")
        ok(f"corruption detected; restored {r['restored_records']} records, lost {r['lost_records']} "
           f"(only one backup existed, so nothing valid was left)")

        print("\n[2] Failover comparison (6 strategies, same workload and fault)")
        rep3 = run_experiment("failover", {"requests": 90, "rps": 15, "concurrency": 6}, 400)
        assert len(rep3["results"]) == 6
        for r in rep3["results"]:
            print(f"     {r['strategy']:27s} {r['throughput_rps']:5} req/s  success {r['success_rate']:.0%}  "
                  f"p50 {r['latency_ms']['p50']} ms  p95 {r['latency_ms']['p95']} ms  retries {r['retries']}  "
                  f"{r['distribution']}  served by target during fault: {r['during_fault']['served_by_target']}")
            assert r["requests"] == 90
            assert r["during_fault"]["served_by_target"] == 0, "failed server served requests during the fault"
        ok("all six strategies measured with the same workload; the failed server served no request while "
           "its data port was closed")
        ok(f"report saved: {rep3.get('file')}")

        print("\n" + "=" * 70)
        print(" EXPERIMENTS VERIFIED — ALL STEPS PASSED")
        print("=" * 70)
        return 0
    except AssertionError as e:
        print(f"\n  [FAIL] {e}")
        return 1
    finally:
        for p in procs:
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    p.kill()


if __name__ == "__main__":
    sys.exit(main())
