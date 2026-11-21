"""
Replacement Backend Server — started on demand by the reverse proxy's
Automatic Backend Recovery (load_balancer/auto_recovery.py) and by the load-aware
Capacity Manager (load_balancer/capacity.py).

It is the same backend as Server 1/2/3 (built by create_backend_app), but its
identity and port are given on the command line, so the proxy can start a new
instance on any free port:

    python backend_servers/replacement_server.py --id server-4 --name "Server 4" --port 5004

When --parent-pid is given, the server watches that process (the reverse proxy)
and exits as soon as it disappears, so no orphan backend keeps a port busy after
the proxy is stopped.
"""

import os
import sys
import time
import argparse
import threading

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from backend_servers.base_server import run_backend


def parent_alive(pid: int) -> bool:
    """True while the process with this PID is still running (Windows and POSIX)."""
    if os.name == "nt":
        import ctypes
        SYNCHRONIZE = 0x00100000
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        WAIT_TIMEOUT = 0x102
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return False
        try:
            return kernel32.WaitForSingleObject(handle, 0) == WAIT_TIMEOUT
        finally:
            kernel32.CloseHandle(handle)
    # POSIX: when the parent exits we are re-parented, so our parent PID changes.
    return os.getppid() == pid


def watch_parent(pid: int, interval: float = 1.0):
    while True:
        time.sleep(interval)
        if not parent_alive(pid):
            os._exit(0)


def main():
    parser = argparse.ArgumentParser(description="Replacement backend server")
    parser.add_argument("--id", required=True, help="server id, e.g. server-4")
    parser.add_argument("--name", required=True, help='display name, e.g. "Server 4"')
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--parent-pid", type=int, default=None)
    parser.add_argument("--control-port", type=int, default=None, help="management port (default: port + offset)")
    args = parser.parse_args()

    if args.parent_pid:
        threading.Thread(target=watch_parent, args=(args.parent_pid,), daemon=True).start()

    parser_ctrl = args.control_port
    run_backend(args.id, args.name, args.host, args.port, control_port=parser_ctrl)


if __name__ == "__main__":
    main()
