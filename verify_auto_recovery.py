"""
Live verification of Automatic Backend Recovery (dynamic replacement provisioning).

Starts the real system (Server 1/2/3 + reverse proxy) and checks, step by step:

  1. S1/S2/S3 healthy and serving requests
  2. Fail all servers  ->  0 healthy backends detected
  3. Automatic recovery triggers
  4. A NEW backend process is created on a previously free port
  5. Its health check succeeds
  6. It is registered with the proxy and is in the routing pool
  7. A client request is routed to the replacement backend
  8. The event log records the complete lifecycle
  9. Recover all -> original servers rejoin: pool becomes [S1, S2, S3, S4]
 10. Reset -> replacement process stopped, port released, pool back to 3
 11. Stopping the proxy also stops a running replacement (no orphan process)

Usage:
    python verify_auto_recovery.py
"""

import os
import sys
import time
import socket
import subprocess
import requests

BASE = "http://127.0.0.1:5000"
HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable


def start(script, env=None):
    return subprocess.Popen([PY, script], cwd=HERE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            env=env or os.environ.copy())


def wait_http(url, tries=40, delay=0.25):
    for _ in range(tries):
        try:
            requests.get(url, timeout=1.0)
            return True
        except Exception:
            time.sleep(delay)
    return False


def port_open(port):
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0


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
        return True
    except OSError:
        return False


def status():
    return requests.get(f"{BASE}/api/status", timeout=2).json()


def wait_until(cond, timeout, step=0.2):
    end = time.time() + timeout
    while time.time() < end:
        st = status()
        if cond(st):
            return st
        time.sleep(step)
    return None


def ok(msg):
    print(f"  [OK] {msg}")


def main():
    print("=" * 66)
    print(" AUTOMATIC BACKEND RECOVERY — LIVE VERIFICATION")
    print("=" * 66)
    procs = []
    try:
        for i in (1, 2, 3):
            procs.append(start(os.path.join("backend_servers", f"server{i}.py")))
        for port in (5001, 5002, 5003):
            assert wait_http(f"http://127.0.0.1:{port}/health"), f"backend on {port} did not start"
        assert not port_open(5004), "port 5004 is already in use before the test — stop other instances first"
        # Stage 1 (automatic recovery) is verified on its own; the capacity manager (stage 2) is
        # verified by verify_capacity_recovery.py.
        proxy = start("app.py", env=dict(os.environ, LB_CAPACITY="0", LB_RESILIENCE_POLICY="scale_only"))
        procs.append(proxy)
        assert wait_http(f"{BASE}/api/status"), "reverse proxy did not start"
        time.sleep(2.5)

        print("\n[1] Initial state")
        st = status()
        assert st["active_server_count"] == 3 and st["total_server_count"] == 3, st["active_server_count"]
        assert st["auto_recovery"]["enabled"]
        routed = [requests.get(f"{BASE}/api/request").json()["server_id"] for _ in range(6)]
        assert set(routed) == {"server-1", "server-2", "server-3"}, routed
        ok(f"S1/S2/S3 healthy; requests distributed: {routed}")

        print("\n[2] Fail all servers")
        r = requests.post(f"{BASE}/api/fail-all").json()
        assert r["failed"] == ["Server 1", "Server 2", "Server 3"], r
        st = wait_until(lambda s: any(e["event_type"] == "NO_HEALTHY_BACKENDS" for e in s["events"]), 5)
        assert st, "proxy never reported 0 healthy backends"
        ok("proxy detected 0 healthy backends")

        print("\n[3] Automatic recovery")
        st = wait_until(lambda s: (s["auto_recovery"]["episode"] or {}).get("stage") in ("routing", "restored", "failed"), 20)
        assert st, "auto-recovery did not finish"
        ep = st["auto_recovery"]["episode"]
        assert ep["stage"] != "failed", f"provisioning failed: {ep['error']}"
        cand = ep["candidate"]
        ok(f"recovery triggered and completed: {cand['name']} on port {cand['port']}")

        print("\n[4] New backend process")
        original_pids = {p.pid for p in procs}
        assert cand["pid"] not in original_pids, "replacement PID is one of the original processes"
        assert pid_alive(cand["pid"]), "replacement process is not running"
        direct = requests.get(f"http://127.0.0.1:{cand['port']}/status", timeout=2).json()
        assert direct["server_id"] == cand["id"] and direct["is_healthy"], direct
        ok(f"new OS process PID {cand['pid']} listening on port {cand['port']} (was free before)")

        print("\n[5]-[6] Health check, registration, routing pool")
        node = next(s for s in st["servers"] if s["id"] == cand["id"])
        assert node["status"] == "HEALTHY" and node["is_replacement"], node
        assert st["active_server_count"] == 1 and st["total_server_count"] == 4
        ok(f"{cand['name']} HEALTHY, registered as replacement; pool = 1/4 healthy")

        print("\n[7] Client request through the proxy")
        resp = requests.get(f"{BASE}/api/request")
        assert resp.status_code == 200, resp.text
        assert resp.json()["server_id"] == cand["id"], resp.json()
        ok(f"request served by {resp.json()['server']} (port {resp.json()['port']})")

        print("\n[8] Event log lifecycle")
        time.sleep(0.3)
        types = [e["event_type"] for e in status()["events"]]
        expected = ["FAILURE_DETECTED", "NO_HEALTHY_BACKENDS", "AUTO_RECOVERY_STARTED", "PROVISIONING",
                    "REPLACEMENT_STARTED", "REPLACEMENT_HEALTHY", "REPLACEMENT_REGISTERED", "REPLACEMENT_ADDED",
                    "TRAFFIC_RESTORED"]
        pos = -1
        for t in expected:
            assert t in types[pos + 1:], f"missing or out-of-order event {t}: {types}"
            pos = types.index(t, pos + 1)
        ok("events recorded in order: " + " -> ".join(expected))

        print("\n[9] Recover original servers")
        requests.post(f"{BASE}/api/recover-all")
        st = wait_until(lambda s: s["active_server_count"] == 4, 10)
        assert st, "original servers did not rejoin"
        routed = {requests.get(f"{BASE}/api/request").json()["server_id"] for _ in range(8)}
        assert routed == {"server-1", "server-2", "server-3", cand["id"]}, routed
        ok(f"pool = [S1, S2, S3, {cand['name']}], traffic reaches all four")

        print("\n[10] Reset simulation")
        requests.post(f"{BASE}/api/reset-simulation")
        time.sleep(1.0)
        st = status()
        assert [s["id"] for s in st["servers"]] == ["server-1", "server-2", "server-3"], st["servers"]
        assert not pid_alive(cand["pid"]) and not port_open(cand["port"])
        ok("replacement stopped, port released, pool back to 3 servers")

        print("\n[11] No orphan process when the proxy stops")
        requests.post(f"{BASE}/api/fail-all")
        st = wait_until(lambda s: (s["auto_recovery"]["episode"] or {}).get("stage") in ("routing", "restored"), 20)
        assert st, "second recovery did not complete"
        pid2 = st["auto_recovery"]["episode"]["candidate"]["pid"]
        port2 = st["auto_recovery"]["episode"]["candidate"]["port"]
        proxy.kill()                       # hard kill: no atexit cleanup runs
        proxy.wait(timeout=5)
        end = time.time() + 6
        while time.time() < end and pid_alive(pid2):
            time.sleep(0.2)
        assert not pid_alive(pid2) and not port_open(port2), "replacement survived the proxy"
        ok(f"replacement PID {pid2} exited on its own after the proxy was killed")

        print("\n" + "=" * 66)
        print(" AUTOMATIC BACKEND RECOVERY VERIFIED — ALL STEPS PASSED")
        print("=" * 66)
        return 0
    except AssertionError as e:
        print(f"\n  [FAIL] {e}")
        return 1
    finally:
        for p in procs:
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    p.kill()


if __name__ == "__main__":
    sys.exit(main())
