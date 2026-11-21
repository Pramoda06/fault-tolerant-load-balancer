"""
Master Launcher for Load Balancing & Fault-Tolerant Network Monitoring.

Starts all components in managed subprocesses:
1. Server 1 on http://127.0.0.1:5001
2. Server 2 on http://127.0.0.1:5002
3. Server 3 on http://127.0.0.1:5003
4. Load Balancer & Dashboard on http://127.0.0.1:5000

Provides a clean unified console with graceful Ctrl+C shutdown.
"""

import sys
import os
import time
import subprocess
import signal

PYTHON_EXE = sys.executable

PROCESSES = [
    {
        "name": "Backend Server 1",
        "cmd": [PYTHON_EXE, os.path.join("backend_servers", "server1.py")],
        "port": 5001
    },
    {
        "name": "Backend Server 2",
        "cmd": [PYTHON_EXE, os.path.join("backend_servers", "server2.py")],
        "port": 5002
    },
    {
        "name": "Backend Server 3",
        "cmd": [PYTHON_EXE, os.path.join("backend_servers", "server3.py")],
        "port": 5003
    },
    {
        "name": "Load Balancer & Dashboard",
        "cmd": [PYTHON_EXE, "app.py"],
        "port": 5000
    }
]

def main():
    running_procs = []
    print("=" * 65)
    print(" LOAD BALANCING & FAULT-TOLERANT NETWORK MONITORING")
    print(" Computer Networks Project System Launcher")
    print("=" * 65)

    base_dir = os.path.dirname(os.path.abspath(__file__))

    try:
        # Launch each backend server with a slight stagger
        for item in PROCESSES:
            print(f"[+] Starting {item['name']} on port {item['port']}...")
            proc = subprocess.Popen(
                item["cmd"],
                cwd=base_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            running_procs.append((item["name"], proc))
            time.sleep(0.4)

        print("\n" + "=" * 65)
        print(" ALL SERVICES ONLINE AND ACTIVE!")
        print(" Open the Dashboard in your browser at:")
        print("   >>> http://127.0.0.1:5000 <<<")
        print(" Press Ctrl+C at any time to shut down all processes cleanly.")
        print("=" * 65 + "\n")

        # Keep parent process alive monitoring children
        while True:
            for name, proc in running_procs:
                ret = proc.poll()
                if ret is not None:
                    print(f"[!] Warning: {name} exited with code {ret}")
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\n[*] Shutting down all services gracefully...")
    finally:
        for name, proc in running_procs:
            if proc.poll() is None:
                print(f"[-] Terminating {name}...")
                proc.terminate()
                try:
                    proc.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    proc.kill()
        print("[*] All services stopped. Goodbye!")

if __name__ == "__main__":
    main()
