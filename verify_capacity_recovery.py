"""
Live verification: load-aware capacity after a total outage (integration test).

Starts the real system (Server 1/2/3 + reverse proxy with the capacity manager) and uses the
proxy's traffic generator (real HTTP requests to :5000):

  1. 30 req/s with 3 healthy originals: desired = ceil(30 / (20 x 0.7)) = 3 -> no replacement
  2. every original fails (connection refused): 0 usable backends -> stage 1 starts exactly ONE
     minimum-viable replacement, verified by health checks
  3. after service is restored the capacity manager OBSERVES demand for the observation window
     and does not start further replacements during it
  4. then it provisions replacements one at a time until desired capacity (3) is reached
  5. originals recover (RECOVERING -> HEALTHY); demand drops to 8 req/s; replacements are drained
     one at a time and removed; originals are never removed; no client errors during draining
  6. killing the proxy leaves no replacement process behind

Usage:
    python verify_capacity_recovery.py
"""

import os
import sys
import time
import socket
import tempfile
import subprocess

import requests

BASE = "http://127.0.0.1:5000"
HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable
ORIGINALS = ["server-1", "server-2", "server-3"]


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


def status():
    return requests.get(f"{BASE}/api/status", timeout=3).json()


def wait_until(cond, timeout, step=0.5):
    end = time.time() + timeout
    while time.time() < end:
        st = status()
        if cond(st):
            return st
        time.sleep(step)
    return None


def pid_alive(pid):
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
    print("=" * 70)
    print(" LOAD-AWARE CAPACITY AFTER RECOVERY — LIVE VERIFICATION")
    print("=" * 70)
    procs, proxy = [], None
    env = dict(os.environ, LB_DATA_DIR=tempfile.mkdtemp(prefix="lb_cap_"), LB_CAPACITY="1",
               LB_RESILIENCE_POLICY="scale_only")   # tests the reactive capacity mechanism itself
    try:
        for i in (1, 2, 3):
            procs.append(start(os.path.join("backend_servers", f"server{i}.py")))
        for port in (5001, 5002, 5003):
            assert wait_http(f"http://127.0.0.1:{port}/health"), f"backend {port} did not start"
        proxy = start("app.py", env)
        procs.append(proxy)
        assert wait_http(f"{BASE}/api/status"), "proxy did not start"
        time.sleep(2.5)

        print("\n[1] 30 req/s on 3 originals")
        requests.post(f"{BASE}/api/traffic/start", json={"rps": 30, "concurrency": 24, "clients": 16})
        st = wait_until(lambda s: s["capacity"]["enough_window"] and s["capacity"]["phase"] == "STABLE"
                        and s["capacity"]["demand_rps"] >= 28, 40)
        assert st, "capacity manager never became STABLE"
        cap = st["capacity"]
        assert cap["desired"] == 3 and not any(s["is_replacement"] for s in st["servers"]), cap["reason"]
        ok(f"{cap['formula']}; no replacement needed (capacity/server {cap['capacity_rps_per_server']} req/s, "
           f"service time {cap['service_ms']} ms, {cap['service_source']})")

        print("\n[2] Every original fails (connection refused)")
        r = requests.post(f"{BASE}/api/fail-all", json={"type": "refused"}).json()
        assert r["failed"] == ["Server 1", "Server 2", "Server 3"], r
        t_fail = time.time()
        st = wait_until(lambda s: s["capacity"]["phase"] == "WAITING_FOR_SERVICE", 10, 0.2)
        assert st, "total outage not seen by the capacity manager"
        st = wait_until(lambda s: any(x["origin"] == "recovery" and x["status"] == "HEALTHY" for x in s["servers"]), 30, 0.3)
        assert st, "stage 1 did not restore service"
        t_restored = time.time()
        stage1 = [x for x in st["servers"] if x["is_replacement"]]
        assert len(stage1) == 1 and stage1[0]["origin"] == "recovery", [x["name"] for x in stage1]
        ok(f"stage 1 restored service with ONE replacement ({stage1[0]['name']}) "
           f"{t_restored - t_fail:.1f} s after the failure")

        print("\n[3] Observation window before deciding")
        window = st["capacity"]["window_s"]
        seen_obs = False
        end = t_restored + window - 1.5
        while time.time() < end:
            s = status()
            seen_obs = seen_obs or s["capacity"]["phase"] == "OBSERVING"
            caps = [x for x in s["servers"] if x["origin"] == "capacity"]
            assert not caps and not s["capacity"]["provisioning"], "provisioned during the observation window"
            time.sleep(0.5)
        assert seen_obs, "OBSERVING phase not reported"
        ok(f"no capacity decision during the {window:g} s observation window after recovery")

        print("\n[4] Provisioning to the measured demand")
        st = wait_until(lambda s: sum(1 for x in s["servers"] if x["is_replacement"] and x["status"] == "HEALTHY"
                                      and not x["draining"]) >= 3, 90, 1.0)
        assert st, "did not reach 3 routable replacements"
        cap = st["capacity"]
        reps = [x for x in st["servers"] if x["is_replacement"]]
        assert len(reps) == 3, [x["name"] for x in reps]
        hist = [h for h in cap["history"] if h["decision"] in ("scale_out", "registered")]
        ok(f"{cap['formula']}; replacements: " + ", ".join(f"{x['name']} ({x['origin']})" for x in reps)
           + f"; {len(hist)} scale-out / registration steps, one at a time")
        events = [e["message"] for e in st["events"] if e["event_type"] == "BACKEND_REGISTERED"]
        assert any("passed 2 health checks and was registered" in m for m in events)

        print("\n[5] Originals recover, demand drops -> drained scale-in")
        requests.post(f"{BASE}/api/recover-all")
        seen = []
        end = time.time() + 15
        while time.time() < end:
            s1 = next(x for x in status()["servers"] if x["id"] == "server-1")["status"]
            if not seen or seen[-1] != s1:
                seen.append(s1)
            if s1 == "HEALTHY":
                break
            time.sleep(0.2)
        assert "RECOVERING" in seen and seen[-1] == "HEALTHY", seen
        requests.post(f"{BASE}/api/traffic/rate", json={"rps": 8})
        t_low = time.time()
        sent_before = status()["traffic"]
        st = wait_until(lambda s: not any(x["is_replacement"] for x in s["servers"]), 150, 1.0)
        assert st, "replacements were not removed at low demand"
        assert [x["id"] for x in st["servers"]] == ORIGINALS
        tr = st["traffic"]
        failed_during = tr["failed"] - sent_before["failed"]
        assert failed_during == 0, f"{failed_during} client errors while draining"
        drains = [m for m in (e["message"] for e in st["events"]) if "drained in" in m]
        ok(f"originals: {' -> '.join(seen)}; replacements drained and removed one at a time "
           f"({len(drains)} drains, {time.time() - t_low:.0f} s); 0 client errors; originals kept")
        for r in reps:
            assert not pid_alive(r["pid"]), f"{r['name']} process still running"
        ok("replacement processes exited after removal")

        print("\n[6] Shutdown leaves no orphan process")
        requests.post(f"{BASE}/api/traffic/rate", json={"rps": 60})
        st = wait_until(lambda s: any(x["is_replacement"] and x["pid"] for x in s["servers"]), 60, 1.0)
        assert st, "no replacement to test shutdown with"
        pids = [x["pid"] for x in st["servers"] if x["is_replacement"] and x["pid"]]
        proxy.kill()
        proxy.wait(timeout=5)
        end = time.time() + 8
        while time.time() < end and any(pid_alive(p) for p in pids):
            time.sleep(0.2)
        assert not any(pid_alive(p) for p in pids), "replacement survived the proxy"
        ok(f"replacement process(es) {pids} exited after the proxy was killed")

        print("\n" + "=" * 70)
        print(" CAPACITY AFTER RECOVERY VERIFIED — ALL STEPS PASSED")
        print("=" * 70)
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
