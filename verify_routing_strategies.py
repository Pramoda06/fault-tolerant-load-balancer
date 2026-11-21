"""
Live verification of the six routing strategies.

Starts the real system (Server 1/2/3 + reverse proxy) and sends real HTTP traffic
through http://127.0.0.1:5000/api/request with a fixed workload per strategy:

  1. Every strategy: distribution of 60 requests (sequential, then 6 in parallel,
     then 8 simulated client IPs via X-Forwarded-For); active connections return to 0.
  2. Round Robin equal split; Weighted Round Robin 3:2:1; Weighted Least Connections
     follows weights; IP Hash sticky per client.
  3. Server 2 failed: no strategy sends it requests, no client errors; IP Hash only
     moves Server 2's clients.
  4. Server 2 recovered (RECOVERING -> HEALTHY) and receives traffic again; IP Hash
     mapping restored.
  5. Switching strategy keeps weights, counters and fault-tolerance metrics.

Usage:
    python verify_routing_strategies.py
"""

import os
import sys
import time
import subprocess
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import requests

BASE = "http://127.0.0.1:5000"
HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable
STRATEGIES = ["ROUND_ROBIN", "WEIGHTED_ROUND_ROBIN", "LEAST_CONNECTIONS",
              "WEIGHTED_LEAST_CONNECTIONS", "LEAST_RESPONSE_TIME", "IP_HASH"]
CLIENTS = [f"10.0.0.{i}" for i in range(1, 9)]


def start(script, env=None):
    return subprocess.Popen([PY, script], cwd=HERE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)


def wait_http(url, tries=40):
    for _ in range(tries):
        try:
            requests.get(url, timeout=1.0)
            return True
        except Exception:
            time.sleep(0.25)
    return False


def status():
    return requests.get(f"{BASE}/api/status", timeout=2).json()


def set_algo(name):
    r = requests.post(f"{BASE}/api/set-algorithm", json={"algorithm": name}, timeout=2)
    assert r.status_code == 200 and status()["active_algorithm"] == name, r.text


def one(ip=None):
    headers = {"X-Forwarded-For": ip} if ip else {}
    r = requests.get(f"{BASE}/api/request", headers=headers, timeout=5)
    return r.status_code, (r.json().get("server_id") if r.status_code == 200 else None), ip


def workload(n=60, parallel=1, clients=False):
    ips = [CLIENTS[i % len(CLIENTS)] if clients else None for i in range(n)]
    with ThreadPoolExecutor(max_workers=parallel) as ex:
        return list(ex.map(one, ips))


def dist(results):
    return dict(sorted(Counter(sid for code, sid, _ in results if code == 200).items()))


def ok(msg):
    print(f"  [OK] {msg}")


def main():
    print("=" * 70)
    print(" ROUTING STRATEGIES — LIVE VERIFICATION")
    print("=" * 70)
    procs = []
    try:
        for i in (1, 2, 3):
            procs.append(start(os.path.join("backend_servers", f"server{i}.py")))
        for port in (5001, 5002, 5003):
            assert wait_http(f"http://127.0.0.1:{port}/health"), f"backend {port} did not start"
        # Routing is verified in isolation: the load-aware capacity manager is disabled so that it
        # cannot add servers during the test (adding a server legitimately changes IP Hash mappings).
        procs.append(start("app.py", env=dict(os.environ, LB_CAPACITY="0", LB_RESILIENCE_POLICY="scale_only")))
        assert wait_http(f"{BASE}/api/status"), "proxy did not start"
        time.sleep(2.5)

        print("\n[1] Distribution of the same workload under every strategy")
        table = {}
        for algo in STRATEGIES:
            set_algo(algo)
            seq = workload(60, 1)
            par = workload(60, 6)
            cli = workload(64, 4, clients=True)
            fails = sum(1 for r in seq + par + cli if r[0] != 200)
            assert fails == 0, f"{algo}: {fails} failed requests"
            active = [s["active_connections"] for s in status()["servers"]]
            assert active == [0, 0, 0], f"{algo}: active connections not released {active}"
            table[algo] = (dist(seq), dist(par), dist(cli), cli)
            print(f"  {algo:27s} sequential {dist(seq)}")
            print(f"  {'':27s} 6 parallel {dist(par)}")
            print(f"  {'':27s} 8 clients  {dist(cli)}")
        rr, wrr, wlc, ih = table["ROUND_ROBIN"], table["WEIGHTED_ROUND_ROBIN"], table["WEIGHTED_LEAST_CONNECTIONS"], table["IP_HASH"]
        assert rr[0] == {"server-1": 20, "server-2": 20, "server-3": 20}, rr[0]
        assert wrr[0] == {"server-1": 30, "server-2": 20, "server-3": 10}, wrr[0]
        assert wlc[0] == {"server-1": 30, "server-2": 20, "server-3": 10}, wlc[0]     # idle ties -> weights
        assert len(ih[0]) == 1, ih[0]                                                  # one client -> one server
        sticky = {}
        for code, sid, ip in ih[3]:
            sticky.setdefault(ip, set()).add(sid)
        assert all(len(v) == 1 for v in sticky.values()), sticky
        ip_map = {ip: next(iter(v)) for ip, v in sticky.items()}
        ok("0 failed requests; active connections back to 0 after every run")
        ok("RR 20/20/20, WRR 30/20/10, WLC (idle) 30/20/10, IP Hash: each of 8 clients always on one server")

        print("\n[2] Switching keeps weights, counters and fault-tolerance state")
        before = status()
        set_algo("LEAST_CONNECTIONS")
        after = status()
        assert [s["weight"] for s in after["servers"]] == [3, 2, 1]
        assert after["metrics"]["total_requests"] == before["metrics"]["total_requests"]
        assert [s["requests_served"] for s in after["servers"]] == [s["requests_served"] for s in before["servers"]]
        assert [s["status"] for s in after["servers"]] == [s["status"] for s in before["servers"]]
        ok(f"weights {[s['weight'] for s in after['servers']]}, {after['metrics']['total_requests']} total requests kept")

        print("\n[3] Server 2 fails -> bypassed by every strategy")
        requests.post(f"{BASE}/api/fail/server-2")
        end = time.time() + 6
        while time.time() < end and next(s for s in status()["servers"] if s["id"] == "server-2")["status"] != "UNHEALTHY":
            time.sleep(0.2)
        st = status()
        assert next(s for s in st["servers"] if s["id"] == "server-2")["status"] == "UNHEALTHY"
        detect_ms = st["metrics"]["failure_detection_time_ms"]
        for algo in STRATEGIES:
            set_algo(algo)
            res = workload(60, 3) + workload(48, 3, clients=True)
            d = dist(res)
            assert "server-2" not in d, f"{algo} sent requests to failed server-2: {d}"
            assert all(r[0] == 200 for r in res), f"{algo}: errors while server-2 down"
            if algo == "IP_HASH":
                for code, sid, ip in res:
                    if ip and ip_map[ip] != "server-2":
                        assert sid == ip_map[ip], f"client {ip} moved although its server is healthy"
            print(f"  {algo:27s} {d}")
        assert status()["metrics"]["failure_detection_time_ms"] == detect_ms
        ok("no strategy routed to server-2, 0 errors; IP Hash moved only server-2's clients; "
           f"failure detection time ({detect_ms} ms) unchanged by switching")

        print("\n[4] Server 2 recovers -> rejoins after health checks")
        requests.post(f"{BASE}/api/recover/server-2")
        seen = []
        end = time.time() + 10
        while time.time() < end:
            s2 = next(s for s in status()["servers"] if s["id"] == "server-2")["status"]
            if not seen or seen[-1] != s2:
                seen.append(s2)
            if s2 == "HEALTHY":
                break
            time.sleep(0.2)
        assert seen[-1] == "HEALTHY" and "RECOVERING" in seen, seen
        set_algo("IP_HASH")
        res = workload(48, 3, clients=True)
        now_map = {ip: sid for _, sid, ip in res}
        assert now_map == ip_map, "IP Hash mapping not restored after recovery"
        set_algo("ROUND_ROBIN")
        assert "server-2" in dist(workload(30, 1))
        ok(f"server-2 {' -> '.join(seen)}; IP Hash mapping restored; Round Robin sends to server-2 again")

        print("\n" + "=" * 70)
        print(" ROUTING STRATEGIES VERIFIED — ALL STEPS PASSED")
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
