"""
Live verification of the cost-aware resilience controller (integration test).

Real processes and real HTTP traffic. Runs the resilience policy comparison (same trace for every
policy: low load, a spike to 40 req/s, Server 2 failing during the spike, low load again; Poisson
arrivals, Zipf-popular items, two heavy-hitter clients) under:
    static      fixed 4 servers, no mitigations
    scale_only  reactive scale-out (the previous behaviour)
    ladder      cost-aware mitigation ladder (cache -> rate limit -> brownout -> scale-out)

Checks (structural, not "who wins"):
  - every policy completed the same trace and the report was saved
  - the ladder escalated at least once and every decision is logged with its reason
  - the ladder used no more extra server-seconds than scale_only, and fewer than static
  - client error rates stay low for every policy
The measured numbers are printed; they vary between runs and machines.

Usage:
    python verify_resilience.py
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


def ok(msg):
    print(f"  [OK] {msg}")


def main():
    print("=" * 72)
    print(" COST-AWARE RESILIENCE CONTROLLER — LIVE VERIFICATION")
    print("=" * 72)
    procs = []
    env = dict(os.environ, LB_DATA_DIR=tempfile.mkdtemp(prefix="lb_res_"), LB_CAPACITY="1", LB_RESILIENCE_POLICY="ladder")
    try:
        for i in (1, 2, 3):
            procs.append(start(os.path.join("backend_servers", f"server{i}.py")))
        for port in (5001, 5002, 5003):
            assert wait_http(f"http://127.0.0.1:{port}/health"), f"backend {port} did not start"
        procs.append(start("app.py", env))
        assert wait_http(f"{BASE}/api/status"), "proxy did not start"
        time.sleep(2.5)
        st = requests.get(f"{BASE}/api/resilience", timeout=3).json()
        assert st["policy"] == "ladder" and st["level"] == 0
        ok("controller running with the cost-aware ladder, rung 0 (no mitigation at idle)")

        print("\n[1] Same trace under static, scale_only and ladder (about 4 minutes)")
        body = {"phases": [[12, 12], [36, 40], [15, 12]], "fault_at": 16, "clear_at": 46}
        r = requests.post(f"{BASE}/api/experiments/policies", json=body, timeout=10)
        assert r.status_code == 200, r.text
        last = None
        while True:
            time.sleep(3)
            s = requests.get(f"{BASE}/api/experiments", timeout=5).json()
            prog = s["state"]["progress"]
            if prog != last and prog and not prog[-3:].strip().endswith("s"):
                print(f"     ... {prog}")
            last = prog
            if not s["state"]["running"]:
                assert prog == "finished", s["state"]
                rep = s["reports"]["policy_comparison"]
                break
        res = {x["policy"]: x for x in rep["results"]}
        assert set(res) == {"static", "scale_only", "ladder"}
        for name in ("static", "scale_only", "ladder"):
            x = res[name]
            print(f"     {name:10s} SLO violation {x['slo_violation_s']:5.1f} s  p95 {x['client_latency_ms']['p95']} ms  "
                  f"errors {x['error_rate']:.2%}  429 {x['rejected_share']:.1%}  cache {x['cache_share']:.1%}  "
                  f"degraded {x['degraded_share']:.1%}  extra server-s {x['extra_server_seconds']}")
            assert x["requests"] > 500 and x["error_rate"] < 0.05, x
        lad = res["ladder"]
        assert lad["decisions"] and all(d["reason"] for d in lad["decisions"]), "ladder made no logged decision"
        assert lad["extra_server_seconds"] <= res["scale_only"]["extra_server_seconds"] + 1e-6
        assert lad["extra_server_seconds"] < res["static"]["extra_server_seconds"]
        ok(f"ladder decisions: {[(d['action'], d['rung']) for d in lad['decisions']]}")
        ok(f"extra server-seconds: ladder {lad['extra_server_seconds']} <= scale_only "
           f"{res['scale_only']['extra_server_seconds']} < static {res['static']['extra_server_seconds']}")
        ok(f"report saved: {rep.get('file')}")
        st = requests.get(f"{BASE}/api/resilience", timeout=3).json()
        assert st["policy"] == "ladder", "policy not restored after the experiment"
        ok("policy restored to 'ladder' after the experiment")

        print("\n" + "=" * 72)
        print(" RESILIENCE CONTROLLER VERIFIED — ALL STEPS PASSED")
        print("=" * 72)
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
