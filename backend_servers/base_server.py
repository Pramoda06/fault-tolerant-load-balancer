"""
Base Backend Server implementation for simulated network nodes.

Each backend server simulates an independent network service.

DATA PLANE (port 5001, 5002, ...), used by the reverse proxy:
1. Health checks              GET  /health
2. Request processing         GET|POST /process
3. Failure injection (legacy) POST /simulate/fail, /simulate/recover   (HTTP 503 fault)
4. Status inspection          GET  /status

Service-capacity model (SIMULATED, configurable in config.py):
  /process uses `workers` worker slots and a fixed `service_time_ms`. A request that
  finds every worker busy waits in a bounded queue (`queue_limit`); when the queue is
  full the backend answers HTTP 503 with header X-Backend-Overloaded: 1. Every
  response reports the measured queue wait (queue_ms) and processing time (service_ms).

Backend-side faults (real behaviour of this process):
  http_5xx  /health and /process answer HTTP 503             (application failure)
  slow      /health and /process are delayed by delay_ms      (still correct, just slow)
  timeout   the connection is accepted but the answer comes only after hang_seconds,
            longer than the proxy waits -> the proxy sees a read timeout
  refused   (control plane only) the data-plane listener is closed; the process keeps
            running, but connections to the data port are refused (TCP RST)

CONTROL PLANE (management port = data port + CONTROL_PORT_OFFSET, e.g. 6001), started by
run_backend(). It models an out-of-band management network: the proxy uses it to inject
and clear faults, and it stays reachable when the data port is closed.
"""

import time
import logging
import threading
from flask import Flask, jsonify, request

FAULT_TYPES = ("http_5xx", "slow", "timeout", "refused")


def create_backend_app(server_id: str, server_name: str, port: int, workers: int = None,
                       service_time_ms: float = None, queue_limit: int = None,
                       hang_seconds: float = None) -> Flask:
    """Factory creating an independent Flask backend server application (data plane)."""
    try:
        import config as _cfg
    except Exception:          # pragma: no cover - config always importable in the project
        _cfg = None
    workers = workers if workers is not None else getattr(_cfg, "BACKEND_WORKERS", 1)
    service_time_ms = service_time_ms if service_time_ms is not None else getattr(_cfg, "BACKEND_SERVICE_TIME_MS", 10.0)
    queue_limit = queue_limit if queue_limit is not None else getattr(_cfg, "BACKEND_QUEUE_LIMIT", 20)
    hang_seconds = hang_seconds if hang_seconds is not None else getattr(_cfg, "BACKEND_HANG_SECONDS", 5.0)

    app = Flask(server_name)
    log = logging.getLogger(f"Backend_{server_id}")
    log.setLevel(logging.INFO)

    # Server internal state
    state = {
        "server_id": server_id,
        "server_name": server_name,
        "port": port,
        "is_healthy": True,              # False while the http_5xx fault is active
        "requests_served": 0,
        "requests_rejected_busy": 0,
        "failure_injected_at": None,
        "recovery_triggered_at": None,
        "fault": None,                   # {"type", "delay_ms", "injected_at"} for slow / timeout
        "listener_up": True,
        "workers": workers,
        "service_time_ms": service_time_ms,
        "degraded_factor": float(getattr(_cfg, "BACKEND_DEGRADED_FACTOR", 0.4)),
        "queue_limit": queue_limit,
        "hang_seconds": hang_seconds,
        "waiting": 0,
        "busy": 0,
        "lock": threading.Lock()
    }
    slots = threading.Semaphore(workers)
    app.config["BACKEND_STATE"] = state

    def _fault_delay():
        """Extra delay (seconds) caused by an active slow / timeout fault."""
        f = state["fault"]
        if not f:
            return 0.0
        if f["type"] == "slow":
            return f["delay_ms"] / 1000.0
        if f["type"] == "timeout":
            return state["hang_seconds"]
        return 0.0

    @app.route("/health", methods=["GET"])
    def health_check():
        """
        Health probe endpoint used by the Load Balancer.
        Returns HTTP 200 when healthy, or HTTP 503 when the http_5xx fault is active.
        Slow / timeout faults delay the answer (out-of-band of the worker pool).
        """
        delay = _fault_delay()
        if delay:
            time.sleep(delay)
        with state["lock"]:
            if not state["is_healthy"]:
                return jsonify({
                    "status": "unhealthy",
                    "server_id": state["server_id"],
                    "server": state["server_name"],
                    "port": state["port"],
                    "error": "Simulated hardware/network failure",
                    "failure_injected_at": state["failure_injected_at"]
                }), 503

            return jsonify({
                "status": "healthy",
                "server_id": state["server_id"],
                "server": state["server_name"],
                "port": state["port"],
                "timestamp": time.time()
            }), 200

    @app.route("/process", methods=["GET", "POST"])
    def process_request():
        """
        Simulates servicing a client payload/request with the bounded worker model.
        Rejects requests with HTTP 503 if the server is unhealthy or its queue is full.
        """
        with state["lock"]:
            if not state["is_healthy"]:
                return jsonify({
                    "status": "error",
                    "server_id": state["server_id"],
                    "server": state["server_name"],
                    "message": f"Server {state['server_name']} is currently down/unhealthy"
                }), 503
            fault = state["fault"]
        if fault and fault["type"] == "timeout":
            time.sleep(state["hang_seconds"])           # accepted, but no timely answer
        with state["lock"]:
            if state["busy"] + state["waiting"] >= state["workers"] + state["queue_limit"]:
                state["requests_rejected_busy"] += 1
                resp = jsonify({"status": "busy", "server_id": state["server_id"], "server": state["server_name"],
                                "message": "queue full", "queue_limit": state["queue_limit"]})
                return resp, 503, {"X-Backend-Overloaded": "1", "Retry-After": "1"}
            state["waiting"] += 1
        t_queue = time.perf_counter()
        slots.acquire()
        queue_ms = (time.perf_counter() - t_queue) * 1000.0
        with state["lock"]:
            state["waiting"] -= 1
            state["busy"] += 1
            state["requests_served"] += 1
            current_count = state["requests_served"]
        # Brownout: the proxy may ask for a degraded response (X-Degrade: 1). The optional part of the
        # work (e.g. recommendations) is skipped, so the request costs DEGRADED_FACTOR of the time.
        degraded = request.headers.get("X-Degrade") == "1"
        work_ms = state["service_time_ms"] * (state["degraded_factor"] if degraded else 1.0)
        t_service = time.perf_counter()
        try:
            time.sleep(work_ms / 1000.0 + (fault["delay_ms"] / 1000.0 if fault and fault["type"] == "slow" else 0.0))
        finally:
            service_ms = (time.perf_counter() - t_service) * 1000.0
            with state["lock"]:
                state["busy"] -= 1
            slots.release()

        client_ip = request.headers.get("X-Forwarded-For", request.remote_addr)

        return jsonify({
            "status": "healthy",
            "server_id": state["server_id"],
            "server": state["server_name"],
            "port": state["port"],
            "message": f"Request processed successfully by {state['server_name']}",
            "requests_served": current_count,
            "client_ip": client_ip,
            "timestamp": time.time(),
            "queue_ms": round(queue_ms, 2),          # backend-reported (measured)
            "service_ms": round(service_ms, 2),      # backend-reported (measured)
            "workers": state["workers"],             # backend-reported (configured)
            "degraded": degraded,                    # optional content skipped (brownout)
            "optional_content": None if degraded else ["recommendation-1", "recommendation-2"],
        }), 200

    @app.route("/simulate/fail", methods=["POST"])
    def simulate_failure():
        """
        Simulate a controlled server/hardware outage without killing the process (HTTP 503 fault).
        """
        with state["lock"]:
            state["is_healthy"] = False
            state["failure_injected_at"] = time.time()
            injected_time = state["failure_injected_at"]

        app.logger.warning(f"[{server_name}] Failure injected at {injected_time}")
        return jsonify({
            "status": "failure_injected",
            "server_id": state["server_id"],
            "server": state["server_name"],
            "failure_injected_at": injected_time
        }), 200

    @app.route("/simulate/recover", methods=["POST"])
    def simulate_recovery():
        """
        Simulate server recovery.
        The health probe will start responding 200, allowing the load balancer
        to initiate health confirmation and re-admission.
        """
        with state["lock"]:
            state["is_healthy"] = True
            state["fault"] = None
            state["recovery_triggered_at"] = time.time()
            recovered_time = state["recovery_triggered_at"]

        app.logger.info(f"[{server_name}] Recovery triggered at {recovered_time}")
        return jsonify({
            "status": "recovery_triggered",
            "server_id": state["server_id"],
            "server": state["server_name"],
            "recovery_triggered_at": recovered_time
        }), 200

    @app.route("/status", methods=["GET"])
    def get_status():
        """Returns internal state and metrics for debugging or dashboard inspection."""
        return jsonify(status_dict(state)), 200

    return app


def status_dict(state) -> dict:
    with state["lock"]:
        f = state["fault"]
        return {
            "server_id": state["server_id"],
            "server_name": state["server_name"],
            "port": state["port"],
            "is_healthy": state["is_healthy"],
            "requests_served": state["requests_served"],
            "requests_rejected_busy": state["requests_rejected_busy"],
            "failure_injected_at": state["failure_injected_at"],
            "recovery_triggered_at": state["recovery_triggered_at"],
            "fault": dict(f) if f else ({"type": "http_5xx", "injected_at": state["failure_injected_at"]}
                                        if not state["is_healthy"] else None),
            "listener_up": state["listener_up"],
            "workers": state["workers"],
            "service_time_ms": state["service_time_ms"],
            "queue_limit": state["queue_limit"],
            "busy_workers": state["busy"],
            "queue_depth": state["waiting"],
        }


# ---------------------------------------------------------------------------
# Running a backend with a separate control plane
# ---------------------------------------------------------------------------
class DataPlane:
    """The data-plane HTTP listener; can be closed and re-opened (refused fault)."""

    def __init__(self, app: Flask, host: str, port: int):
        self.app, self.host, self.port = app, host, port
        self._server = None
        self._thread = None
        self._lock = threading.Lock()

    def start(self):
        from werkzeug.serving import make_server
        with self._lock:
            if self._server is not None:
                return
            last = None
            for _ in range(20):                       # the port may need a moment after close()
                try:
                    self._server = make_server(self.host, self.port, self.app, threaded=True)
                    break
                except OSError as ex:
                    last = ex
                    time.sleep(0.1)
            if self._server is None:
                raise last
            self._thread = threading.Thread(target=self._server.serve_forever, daemon=True, name="DataPlane")
            self._thread.start()

    def stop(self):
        with self._lock:
            srv, self._server = self._server, None
        if srv is not None:
            srv.shutdown()
            srv.server_close()

    @property
    def up(self) -> bool:
        return self._server is not None


def create_control_app(app: Flask, plane: DataPlane) -> Flask:
    """Management API on the control port (out-of-band of the data plane)."""
    state = app.config["BACKEND_STATE"]
    ctrl = Flask(f"{state['server_name']}-control")

    @ctrl.route("/control/status", methods=["GET"])
    def control_status():
        d = status_dict(state)
        d["listener_up"] = plane.up
        d["role"] = "control-plane"
        return jsonify(d), 200

    @ctrl.route("/control/fault", methods=["POST"])
    def control_fault():
        data = request.get_json(silent=True) or {}
        ftype = data.get("type")
        now = time.time()
        if ftype not in FAULT_TYPES:
            return jsonify({"status": "error", "message": f"unknown fault type {ftype!r}; use {FAULT_TYPES}"}), 400
        with state["lock"]:
            if ftype == "http_5xx":
                state["is_healthy"] = False
                state["failure_injected_at"] = now
            elif ftype in ("slow", "timeout"):
                delay = float(data.get("delay_ms", 0) or 0)
                state["fault"] = {"type": ftype, "delay_ms": delay, "injected_at": now}
                state["failure_injected_at"] = now
            elif ftype == "refused":
                state["failure_injected_at"] = now
                state["listener_up"] = False
        if ftype == "refused":
            plane.stop()
        return jsonify({"status": "fault_injected", "type": ftype, "server_id": state["server_id"],
                        "failure_injected_at": now}), 200

    @ctrl.route("/control/clear", methods=["POST"])
    def control_clear():
        now = time.time()
        with state["lock"]:
            state["is_healthy"] = True
            state["fault"] = None
            state["recovery_triggered_at"] = now
            state["listener_up"] = True
        plane.start()
        return jsonify({"status": "recovery_triggered", "server_id": state["server_id"],
                        "recovery_triggered_at": now}), 200

    return ctrl


def run_backend(server_id: str, server_name: str, host: str, port: int, control_port: int = None):
    """Start the data plane and the control plane; blocks serving the control plane."""
    try:
        import config as _cfg
        offset = _cfg.CONTROL_PORT_OFFSET
    except Exception:
        offset = 1000
    control_port = control_port or port + offset
    app = create_backend_app(server_id=server_id, server_name=server_name, port=port)
    plane = DataPlane(app, host, port)
    plane.start()
    ctrl = create_control_app(app, plane)
    print(f"[*] {server_name}: data plane http://{host}:{port}  control plane http://{host}:{control_port}")
    ctrl.run(host=host, port=control_port, debug=False, threaded=True)
