"""
Live verification of the six fault types, the circuit breaker and retries (integration test).

Real system, Round Robin, 15 req/s from the proxy's traffic generator. For each fault type on
Server 2: inject, wait until the proxy detects it, observe 6 s of traffic, clear it, and wait
until Server 2 is back in routing. Reports the measured detection time and what the proxy saw.
The capacity manager is disabled so the pool stays Server 1/2/3 and results are comparable.

Usage:
    python verify_network_faults.py
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
TARGET = "server-2"


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


def server(st, sid):
    return next(x for x in st["servers"] if x["id"] == sid)


def wait_until(cond, timeout, step=0.3):
    end = time.time() + timeout
    while time.time() < end:
        st = status()
        if cond(st):
            return st
        time.sleep(step)
    return None


def ok(msg):
    print(f"  [OK] {msg}")


CASES = [
    ("refused", {}, "connection refused (real TCP)"),
    ("timeout", {}, "read timeout (real)"),
    ("http_5xx", {}, "HTTP 503 (application)"),
    ("slow", {"delay_ms": 400}, "slow responses (real)"),
    ("partition", {}, "simulated partition"),
    ("packet_loss", {"loss": 0.5}, "simulated 50 % loss"),
]


def main():
    print("=" * 70)
    print(" NETWORK FAULTS, CIRCUIT BREAKER AND RETRIES — LIVE VERIFICATION")
    print("=" * 70)
    procs = []
    env = dict(os.environ, LB_DATA_DIR=tempfile.mkdtemp(prefix="lb_net_"), LB_CAPACITY="0", LB_AUTO_RECOVERY="0",
               LB_RESILIENCE_POLICY="scale_only")
    try:
        for i in (1, 2, 3):
            procs.append(start(os.path.join("backend_servers", f"server{i}.py")))
        for port in (5001, 5002, 5003):
            assert wait_http(f"http://127.0.0.1:{port}/health"), f"backend {port} did not start"
        procs.append(start("app.py", env))
        assert wait_http(f"{BASE}/api/status"), "proxy did not start"
        time.sleep(2.5)
        requests.post(f"{BASE}/api/traffic/start", json={"rps": 15, "concurrency": 16, "clients": 16})
        time.sleep(3)
        rows = []
        for ftype, params, label in CASES:
            print(f"\n[{ftype}] {label}")
            before = status()["telemetry"]["counters_total"]
            r = requests.post(f"{BASE}/api/faults/inject", json=dict(server_id=TARGET, type=ftype, **params)).json()
            assert r["status"] == "success", r
            if ftype == "refused":
                with socket.socket() as sck:
                    sck.settimeout(1)
                    assert sck.connect_ex(("127.0.0.1", 5002)) != 0, "data port still accepting"
                ctrl = requests.get("http://127.0.0.1:6002/control/status", timeout=2).json()
                assert ctrl["listener_up"] is False
                ok("data port 5002 refuses TCP connections; management port 6002 still answers")
            if ftype == "partition":
                d = requests.get("http://127.0.0.1:5002/health", timeout=2)
                assert d.status_code == 200
                ok("Server 2 is running and answers directly; only the proxy's path to it is (simulated) cut")
            st = wait_until(lambda s: any(f["detected_at"] for f in server(s, TARGET)["faults"]), 15)
            assert st, f"{ftype}: not detected"
            f = next(f for f in server(st, TARGET)["faults"] if f["detected_at"])
            det_ms = (f["detected_at"] - f["injected_at"]) * 1000
            ok(f"detected by {f['detected_by']} after {det_ms:.0f} ms")
            tr0 = status()["traffic"]
            time.sleep(6)
            st = status()
            tr1 = st["traffic"]
            sent = tr1["sent"] - tr0["sent"]
            failed = tr1["failed"] - tr0["failed"]
            s2 = server(st, TARGET)
            after = st["telemetry"]["counters_total"]
            delta = {k: after[k] - before.get(k, 0) for k in after if after[k] - before.get(k, 0)}
            ok(f"6 s under fault: {sent - failed}/{sent} client requests succeeded; Server 2 status "
               f"{s2['status']}, circuit {s2['circuit']['state']}; proxy counters {delta}")
            if ftype in ("refused", "http_5xx", "partition"):
                assert failed == 0, f"{failed} client errors (retries should hide this fault)"
            requests.post(f"{BASE}/api/faults/clear", json={"server_id": TARGET})
            t_clear = time.time()
            st = wait_until(lambda s: server(s, TARGET)["routable"] and not server(s, TARGET)["faults"]
                            and any(r["server_id"] == TARGET and r["type"] == ftype and r["recovered_at"]
                                    for r in s["faults"]["records"]), 30)
            assert st, f"{ftype}: Server 2 did not return to routing"
            ok(f"cleared -> back in routing after {time.time() - t_clear:.1f} s (health checks / circuit trial passed)")
            rows.append((ftype, det_ms, f["detected_by"], sent, failed))
            time.sleep(2)
        st = status()
        events = [e["event_type"] for e in st["events"]]
        for ev in ("FAULT_INJECTED", "FAULT_DETECTED", "FAULT_CLEARED", "FAULT_RECOVERED", "CIRCUIT_OPENED"):
            assert ev in events, f"event {ev} missing"
        ok("event timeline contains FAULT_INJECTED / DETECTED / CLEARED / RECOVERED and CIRCUIT_OPENED")
        print("\n  Summary (this run):")
        for ftype, det, by, sent, failed in rows:
            print(f"    {ftype:12s} detected after {det:6.0f} ms by {by:38s} client success {sent - failed}/{sent}")
        print("\n" + "=" * 70)
        print(" NETWORK FAULTS VERIFIED — ALL STEPS PASSED")
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
