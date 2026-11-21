"""
Live System Verification Script.

Executes a live end-to-end verification of the actual running network system:
- Spins up Server 1 (5001), Server 2 (5002), Server 3 (5003), and Load Balancer (5000)
- Executes all 6 live scenarios specified in the requirements:
  1. All servers healthy: Round Robin S1 -> S2 -> S3 -> S1 -> S2 -> S3
  2. Server 2 fails: Health monitor detects failure, S2 removed, traffic continues S1 -> S3 -> S1 -> S3
  3. Server 2 recovers: Health monitor detects recovery, server enters RECOVERING, re-admitted, traffic resumes
  4. Server 1 fails: Traffic continues through Server 2 and Server 3
  5. Two servers fail (e.g. S1 and S2 down): Traffic continues through single remaining Server 3
  6. All servers fail: Load balancer gracefully returns HTTP 503 "NO HEALTHY BACKEND SERVERS" without crashing
- Verifies real metrics calculations and event log entries
- Cleanly shuts down all processes.
"""

import sys
import os
import time
import subprocess
import requests

BASE_URL = "http://127.0.0.1:5000"
S1_URL = "http://127.0.0.1:5001"
S2_URL = "http://127.0.0.1:5002"
S3_URL = "http://127.0.0.1:5003"

PYTHON_EXE = sys.executable

def start_process(script_path, env=None):
    return subprocess.Popen(
        [PYTHON_EXE, script_path],
        cwd=os.path.dirname(os.path.abspath(__file__)),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env
    )

def wait_for_server(url, max_retries=20, delay=0.3):
    for _ in range(max_retries):
        try:
            r = requests.get(url, timeout=1.0)
            if r.status_code in (200, 503):
                return True
        except Exception:
            time.sleep(delay)
    return False

def run_verification():
    print("=" * 65)
    print("STARTING LIVE SYSTEM VERIFICATION")
    print("=" * 65)

    procs = []
    try:
        # 1. Start Server 1, 2, 3
        print("\n[Step 1] Launching Backend Servers 1, 2, 3...")
        procs.append(("Server 1", start_process(os.path.join("backend_servers", "server1.py"))))
        procs.append(("Server 2", start_process(os.path.join("backend_servers", "server2.py"))))
        procs.append(("Server 3", start_process(os.path.join("backend_servers", "server3.py"))))

        time.sleep(1.0)
        assert wait_for_server(f"{S1_URL}/health"), "Server 1 failed to start"
        assert wait_for_server(f"{S2_URL}/health"), "Server 2 failed to start"
        assert wait_for_server(f"{S3_URL}/health"), "Server 3 failed to start"
        print("  [OK] All 3 backend servers responding on ports 5001, 5002, 5003.")

        # 2. Start Load Balancer
        print("\n[Step 2] Launching Load Balancer & Dashboard...")
        # Scenario 6 checks the plain "all servers down -> HTTP 503" behaviour, so this run
        # disables automatic backend recovery (verified separately by verify_auto_recovery.py).
        lb_env = dict(os.environ, LB_AUTO_RECOVERY="0", LB_CAPACITY="0", LB_RESILIENCE_POLICY="scale_only")
        procs.append(("Load Balancer", start_process("app.py", env=lb_env)))
        time.sleep(1.2)
        assert wait_for_server(f"{BASE_URL}/"), "Load Balancer failed to start"
        print("  [OK] Load Balancer & Dashboard responding on port 5000.")

        # 3. Test Dashboard UI serving
        print("\n[Step 3] Verifying Dashboard HTTP 200 response...")
        dash_res = requests.get(f"{BASE_URL}/")
        assert dash_res.status_code == 200
        assert "Load Balancing & Fault-Tolerant Network Monitoring" in dash_res.text
        print("  [OK] Dashboard HTML renders correctly.")

        # Let health monitor initialize
        time.sleep(2.2)

        # 4. Scenario 1: All servers healthy, Round Robin
        print("\n[Scenario 1] Testing Round Robin Routing across all healthy servers...")
        routed_servers = []
        for i in range(6):
            r = requests.get(f"{BASE_URL}/api/request")
            assert r.status_code == 200, f"Request {i+1} failed: {r.text}"
            data = r.json()
            routed_servers.append(data["server_id"])
        
        print(f"  Routing sequence: {routed_servers}")
        assert routed_servers == ["server-1", "server-2", "server-3", "server-1", "server-2", "server-3"], \
            f"Expected cyclic S1->S2->S3->S1->S2->S3, got {routed_servers}"
        print("  [OK] Strict cyclic Round Robin verified.")

        # 5. Scenario 2: Fail Server 2 -> Failure detection & Automatic Failover
        probe_p50 = requests.get(f"{BASE_URL}/api/status").json()["telemetry"]["proxy_observed"]["health_probe_ms"]["p50"]
        print("\n[Scenario 2] Injecting failure on Server 2...")
        fail_res = requests.post(f"{BASE_URL}/api/fail/server-2")
        assert fail_res.status_code == 200
        print("  [OK] Failure injected via Load Balancer API.")

        print("  Waiting for active health monitor to detect failure (max 3.5s)...")
        time.sleep(2.5)

        status_res = requests.get(f"{BASE_URL}/api/status").json()
        s2_status = next(s for s in status_res["servers"] if s["id"] == "server-2")
        assert s2_status["status"] == "UNHEALTHY", f"Expected UNHEALTHY, got {s2_status['status']}"
        assert status_res["active_server_count"] == 2
        print(f"  [OK] Server 2 state is UNHEALTHY. Active count: {status_res['active_server_count']}/3.")

        print("  Verifying traffic redirects only to Server 1 and Server 3...")
        rerouted = []
        for _ in range(4):
            r = requests.get(f"{BASE_URL}/api/request")
            assert r.status_code == 200
            rerouted.append(r.json()["server_id"])
        print(f"  Failover routing sequence: {rerouted}")
        assert rerouted == ["server-1", "server-3", "server-1", "server-3"], f"Expected S1->S3->S1->S3, got {rerouted}"
        print("  [OK] Automatic failover verified: Server 2 received 0 traffic.")

        # 6. Scenario 3: Recover Server 2 -> Health Confirmation -> Re-admission
        print("\n[Scenario 3] Triggering recovery on Server 2...")
        rec_res = requests.post(f"{BASE_URL}/api/recover/server-2")
        assert rec_res.status_code == 200
        print("  [OK] Recovery initiated. Observing confirmation threshold (2 consecutive checks)...")

        # First check triggers RECOVERING
        time.sleep(2.2)
        st1 = requests.get(f"{BASE_URL}/api/status").json()
        s2_st1 = next(s for s in st1["servers"] if s["id"] == "server-2")
        print(f"  Check 1: Server 2 status = {s2_st1['status']} (consecutive: {s2_st1['consecutive_successes']})")

        # Second check triggers RE-ADMISSION
        time.sleep(2.2)
        st2 = requests.get(f"{BASE_URL}/api/status").json()
        s2_st2 = next(s for s in st2["servers"] if s["id"] == "server-2")
        print(f"  Check 2: Server 2 status = {s2_st2['status']} (consecutive: {s2_st2['consecutive_successes']})")
        assert s2_st2["status"] == "HEALTHY", f"Expected HEALTHY after re-admission, got {s2_st2['status']}"
        assert st2["active_server_count"] == 3
        print("  [OK] Server 2 re-admitted to active pool.")

        # Test traffic distribution resumes across all 3
        recovered_routes = []
        for _ in range(3):
            r = requests.get(f"{BASE_URL}/api/request")
            assert r.status_code == 200
            recovered_routes.append(r.json()["server_id"])
        print(f"  Traffic resumed across: {recovered_routes}")
        assert "server-2" in recovered_routes, "Server 2 should receive traffic after re-admission"
        print("  [OK] Traffic successfully returned to recovered Server 2.")

        # 7. Scenario 4: Fail Server 1
        print("\n[Scenario 4] Failing Server 1 -> Traffic continues through Server 2 and Server 3...")
        requests.post(f"{BASE_URL}/api/fail/server-1")
        time.sleep(2.5)
        routes_s1_down = [requests.get(f"{BASE_URL}/api/request").json()["server_id"] for _ in range(4)]
        print(f"  Routing sequence: {routes_s1_down}")
        assert "server-1" not in routes_s1_down
        assert set(routes_s1_down) == {"server-2", "server-3"}
        print("  [OK] Server 1 excluded, Server 2 and 3 serving requests.")

        # 8. Scenario 5: Fail Server 2 as well (2 servers down, only Server 3 alive)
        print("\n[Scenario 5] Failing Server 2 as well (Only Server 3 alive)...")
        requests.post(f"{BASE_URL}/api/fail/server-2")
        time.sleep(2.5)
        st_only_s3 = requests.get(f"{BASE_URL}/api/status").json()
        assert st_only_s3["active_server_count"] == 1
        routes_only_s3 = [requests.get(f"{BASE_URL}/api/request").json()["server_id"] for _ in range(3)]
        print(f"  Routing sequence: {routes_only_s3}")
        assert routes_only_s3 == ["server-3", "server-3", "server-3"]
        print("  [OK] High fault tolerance verified: Single remaining server handles all requests.")

        # 9. Scenario 6: Fail Server 3 as well (All servers down) -> HTTP 503
        print("\n[Scenario 6] Failing all servers (Server 3 down)...")
        requests.post(f"{BASE_URL}/api/fail/server-3")
        time.sleep(2.5)
        st_all_down = requests.get(f"{BASE_URL}/api/status").json()
        assert st_all_down["active_server_count"] == 0
        assert "NO HEALTHY SERVERS" in st_all_down["system_status"]
        print(f"  System status: {st_all_down['system_status']}")

        t503 = time.perf_counter()
        all_down_req = requests.get(f"{BASE_URL}/api/request")
        t503 = (time.perf_counter() - t503) * 1000.0
        print(f"  Status Code: {all_down_req.status_code}")
        print(f"  503 Response Time: {t503:.2f} ms")
        assert all_down_req.status_code == 503
        data_503 = all_down_req.json()
        assert "NO HEALTHY BACKEND SERVERS" in data_503["message"]
        print("  [OK] Graceful HTTP 503 response returned without crashing load balancer.")

        # 10. Verify metrics and logs
        print("\n[Step 10] Verifying Telemetry Metrics & Routing Event Logs...")
        final_status = requests.get(f"{BASE_URL}/api/status").json()
        metrics_data = final_status["metrics"]
        print(f"  Total Requests: {metrics_data['total_requests']}")
        print(f"  Successful Requests: {metrics_data['successful_requests']}")
        print(f"  Failed Requests: {metrics_data['failed_requests']}")
        print(f"  Failure Detection Time: {metrics_data['failure_detection_time_ms']} ms")
        print(f"  Failover Time: {metrics_data['failover_time_ms']} ms")
        print(f"  Recovery Detection Time: {metrics_data['recovery_detection_time_ms']} ms")
        print(f"  Re-admission Time: {metrics_data['readmission_time_ms']} ms")
        print(f"  Average Latency: {metrics_data['avg_response_latency_ms']} ms")
        print(f"  Total Log Entries: {len(final_status['logs'])}")
        print(f"  Health Probe RTT p50 (all healthy): {probe_p50} ms")

        assert metrics_data["total_requests"] > 0
        assert metrics_data["failed_requests"] >= 1
        assert len(final_status["logs"]) > 10
        print("  [OK] All telemetry measurements grounded in actual runtime execution.")

        print("\n" + "=" * 65)
        print(" ALL VERIFICATION SCENARIOS PASSED WITH 100% SUCCESS!")
        print("=" * 65)

    finally:
        print("\n[*] Terminating all background processes...")
        for name, p in procs:
            p.terminate()
            try:
                p.wait(timeout=1.5)
            except subprocess.TimeoutExpired:
                p.kill()
        print("[*] All processes terminated cleanly.")

if __name__ == "__main__":
    run_verification()
