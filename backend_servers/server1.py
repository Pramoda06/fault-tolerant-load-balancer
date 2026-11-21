"""
Backend Server 1 - data plane on port 5001, control (management) plane on port 6001.
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from backend_servers.base_server import create_backend_app, run_backend
import config

server_info = next(s for s in config.BACKEND_SERVERS if s["id"] == "server-1")
app = create_backend_app(
    server_id=server_info["id"],
    server_name=server_info["name"],
    port=server_info["port"]
)

if __name__ == "__main__":
    run_backend(server_info["id"], server_info["name"], server_info["host"], server_info["port"])
