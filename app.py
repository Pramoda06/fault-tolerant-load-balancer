"""
Web Application and Entry Point for Load Balancer & Monitoring Dashboard.

Exposes REST APIs for routing, simulation controls, telemetry, and serves the
real-time monitoring dashboard.

Subsystems (all in this process, one instance each):
  registry            server state (HEALTHY / UNHEALTHY / RECOVERING), origin, draining
  health_monitor      active GET /health probes            -> registry state
  breakers            per-backend circuit breaker            (passive, request outcomes)
  load_balancer       reverse proxy forwarding, retries, admission control
  faults              fault injection + lifecycle (backend faults via the control plane,
                      network faults via the proxy's network simulator NETSIM)
  auto_recovery       stage 1: one minimum-viable replacement when no backend is usable
  capacity            stage 2: load-aware desired capacity, scale out / drain + scale in
  datastore           durable data tier (SQLite primary, replica, WAL, snapshots, manifest)
  disaster            total-outage + data-loss scenario and recovery gate
  traffic             server-side HTTP traffic generator
  experiments         repeatable failover / disaster-recovery comparisons
"""

import sys
import os
import time
import atexit
import requests
from flask import Flask, render_template, jsonify, request

import config
from load_balancer.server_registry import ServerRegistry
from load_balancer.scheduler import SchedulerManager
from load_balancer.metrics import MetricsCollector
from load_balancer.health_monitor import HealthMonitor
from load_balancer.balancer import LoadBalancer
from load_balancer.auto_recovery import AutoRecoveryManager
from load_balancer.circuit_breaker import CircuitBreakerRegistry
from load_balancer.netsim import NETSIM
from load_balancer.faults import FaultManager, FaultError, FAULT_TYPES, FAULT_INFO
from load_balancer.provisioning import InstanceAllocator, ProvisionGate, control_port_for
from load_balancer.capacity import CapacityManager
from load_balancer.topology import build_topology
from load_balancer.cache import ResponseCache
from load_balancer.ratelimit import HeavyHitterLimiter
from load_balancer.scenarios import ScenarioRunner
from load_balancer.resilience import ResilienceController
from load_balancer.datastore import DataStore, DataUnavailable
from load_balancer.disaster import DisasterRecovery
from load_balancer.traffic import TrafficGenerator
from load_balancer.experiments import ExperimentRunner

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
BASE_URL = f"http://{config.LOAD_BALANCER_HOST}:{config.LOAD_BALANCER_PORT}"

app = Flask(__name__)

# Initialize core subsystems
registry = ServerRegistry(recovery_threshold=config.RECOVERY_THRESHOLD)

# Register backends from configuration
for s_cfg in config.BACKEND_SERVERS:
    node = registry.register_server(
        server_id=s_cfg["id"],
        name=s_cfg["name"],
        host=s_cfg["host"],
        port=s_cfg["port"],
        weight=s_cfg["weight"],
        url=s_cfg["url"]
    )
    node.control_url = f"http://{s_cfg['host']}:{control_port_for(s_cfg['port'])}"

scheduler = SchedulerManager(initial_algorithm=config.DEFAULT_ALGORITHM)
metrics = MetricsCollector(max_logs=200)
NETSIM.partition_wait = config.SIM_PARTITION_WAIT
NETSIM.loss_wait = config.SIM_LOSS_WAIT

health_monitor = HealthMonitor(
    registry=registry,
    metrics=metrics,
    interval=config.HEALTH_CHECK_INTERVAL,
    timeout=config.HEALTH_CHECK_TIMEOUT
)

capacity = None          # created below; referenced by callbacks
faults = None


def _on_breaker_event(event_type, server_id, message):
    level = {"CIRCUIT_OPENED": "WARN", "CIRCUIT_HALF_OPEN": "INFO", "CIRCUIT_CLOSED": "SUCCESS"}.get(event_type, "INFO")
    metrics.add_log(message, level=level, event_type=event_type)
    if event_type == "CIRCUIT_OPENED" and faults is not None:
        faults.on_circuit_open(server_id)
    if capacity is not None:
        capacity.notify()


breakers = CircuitBreakerRegistry(
    failure_threshold=config.CIRCUIT_FAILURE_THRESHOLD, open_seconds=config.CIRCUIT_OPEN_SECONDS,
    half_open_max_trials=config.CIRCUIT_HALF_OPEN_MAX_TRIALS, half_open_successes=config.CIRCUIT_HALF_OPEN_SUCCESSES,
    on_event=_on_breaker_event)
response_cache = ResponseCache(ttl=config.RESILIENCE_CACHE_TTL_S, stale_max=config.RESILIENCE_CACHE_STALE_MAX_S)
limiter = HeavyHitterLimiter()
load_balancer = LoadBalancer(registry=registry, scheduler=scheduler, metrics=metrics, breakers=breakers,
                             cache=response_cache, limiter=limiter)
faults = FaultManager(registry, metrics, NETSIM, breakers=breakers)

allocator = InstanceAllocator()
gate = ProvisionGate()

# Automatic Backend Recovery (stage 1): one minimum-viable replacement when 0 backends are usable.
auto_recovery = AutoRecoveryManager(
    registry=registry,
    metrics=metrics,
    project_root=PROJECT_ROOT,
    enabled=config.AUTO_RECOVERY_ENABLED,
    host=config.LOAD_BALANCER_HOST,
    port_range=config.REPLACEMENT_PORT_RANGE,
    max_replacements=config.AUTO_RECOVERY_MAX_REPLACEMENTS,
    max_attempts=config.AUTO_RECOVERY_MAX_ATTEMPTS,
    retry_cooldown=config.AUTO_RECOVERY_RETRY_COOLDOWN,
    startup_timeout=config.REPLACEMENT_STARTUP_TIMEOUT,
    health_timeout=config.HEALTH_CHECK_TIMEOUT,
    health_passes=config.RECOVERY_THRESHOLD,
    weight=config.REPLACEMENT_WEIGHT,
    allocator=allocator,
    gate=gate,
)

# Load-aware capacity manager (stage 2).
capacity = CapacityManager(
    registry=registry, metrics=metrics, project_root=PROJECT_ROOT, breakers=breakers, auto_recovery=auto_recovery,
    allocator=allocator, gate=gate, enabled=config.CAPACITY_ENABLED, host=config.LOAD_BALANCER_HOST,
    port_range=config.REPLACEMENT_PORT_RANGE, health_timeout=config.HEALTH_CHECK_TIMEOUT,
    health_passes=config.RECOVERY_THRESHOLD, startup_timeout=config.REPLACEMENT_STARTUP_TIMEOUT,
    weight=config.REPLACEMENT_WEIGHT)

# Durable data tier and disaster recovery
datastore = DataStore(config.DATA_DIR, snapshot_interval=config.SNAPSHOT_INTERVAL,
                      keep_snapshots=config.SNAPSHOTS_KEPT, metrics=metrics)
disaster = DisasterRecovery(registry, faults, datastore, metrics, capacity=capacity, strategies=config.DR_STRATEGIES)
load_balancer.routing_gate = disaster.routing_block_reason
load_balancer.on_request_failure = faults.on_request_failure
load_balancer.on_request_success = faults.on_response

traffic = TrafficGenerator(BASE_URL, max_rps=config.TRAFFIC_MAX_RPS, max_concurrency=config.TRAFFIC_MAX_CONCURRENCY)

# Cost-aware resilience controller: cheaper mitigations before new servers (gates capacity scale-out)
resilience = ResilienceController(metrics, capacity, load_balancer, response_cache, limiter, registry)


def _on_transition(server, event_type, kind):
    """Health-monitor state transitions: fault detection, circuit reset on re-admission, capacity re-check."""
    if event_type == "FAILURE_DETECTED":
        faults.on_health_failure(server.id, kind)
    elif event_type == "SERVER_READMITTED":
        breakers.reset(server.id, reason=f"{server.name} re-admitted after {config.RECOVERY_THRESHOLD} passing health checks")
    capacity.notify()


def _after_health_cycle():
    auto_recovery.evaluate()
    faults.tick()
    disaster.tick()


health_monitor.on_transition = _on_transition
health_monitor.on_cycle = _after_health_cycle         # checked after every health-check cycle
atexit.register(auto_recovery.shutdown)               # stop replacement processes with the proxy
atexit.register(capacity.shutdown)
atexit.register(traffic.stop)
atexit.register(datastore.stop)


def write_record(name, value):
    """Write path for demo records: the request is routed to a backend (POST, not retried blindly
    after delivery); only if a backend processed it is the write committed to the data tier."""
    status, body = load_balancer.handle_request(client_ip="127.0.0.1", method="POST",
                                                payload={"op": "create", "name": name, "value": value},
                                                rate_limit=False)
    if status != 200:
        return status, body
    try:
        rec = datastore.create(name, value)
    except DataUnavailable as ex:
        return 503, {"status": "error", "message": str(ex)}
    return 201, {"status": "created", "record": rec, "served_by": body.get("server_id")}


experiments = ExperimentRunner({
    "base_url": BASE_URL, "registry": registry, "scheduler": scheduler, "faults": faults, "breakers": breakers,
    "netsim": NETSIM, "retry_budget": load_balancer.retry_budget, "capacity": capacity, "traffic": traffic,
    "datastore": datastore, "disaster": disaster, "write_record": write_record, "metrics": metrics,
    "resilience": resilience, "cache": response_cache, "limiter": limiter, "balancer": load_balancer,
}, config.EXPERIMENT_REPORT_DIR)

metrics.add_log(
    f"Reverse proxy started on {config.LOAD_BALANCER_HOST}:{config.LOAD_BALANCER_PORT} "
    f"with {len(config.BACKEND_SERVERS)} backend servers.",
    level="INFO",
    event_type="SYSTEM"
)

# Start background threads
health_monitor.start()
capacity.start()
resilience.start()
scenarios = ScenarioRunner({"traffic": traffic, "faults": faults, "resilience": resilience, "cache": response_cache,
                            "limiter": limiter, "balancer": load_balancer, "capacity": capacity, "metrics": metrics,
                            "auto_recovery": auto_recovery})
datastore.start_periodic(lambda: config.SNAPSHOT_INTERVAL)


def _json_body():
    return request.get_json(silent=True) or {}


@app.route("/", methods=["GET"])
def index():
    """Serve the real-time visual load balancer dashboard."""
    return render_template("dashboard.html", config=config)


@app.route("/api/request", methods=["GET", "POST"])
def client_request():
    """
    Main entrypoint for client traffic.
    Routes incoming request to healthy backend according to active scheduling policy.
    """
    client_ip = request.headers.get("X-Forwarded-For", request.remote_addr or "127.0.0.1")
    payload = request.get_json(silent=True) if request.is_json else None

    trace = None
    if request.headers.get("X-Trace") == "1":            # request tracer (Load Balancing page)
        xff = request.headers.get("X-Forwarded-For")
        trace = [{"step": "receive", "result": f"{request.method} {request.full_path.rstrip('?')}",
                  "detail": f"TCP connection from {request.remote_addr}:{request.environ.get('REMOTE_PORT', '?')}"
                            + (f"; X-Forwarded-For says the client is {xff}, so the proxy uses {client_ip}" if xff else
                               f"; no X-Forwarded-For header, so the client is {client_ip}"), "t_ms": 0.0}]
    status_code, response_data = load_balancer.handle_request(
        client_ip=client_ip,
        method=request.method,
        payload=payload,
        headers={k: v for k, v in request.headers.items() if k != "X-Trace"},
        cache_key=request.args.get("item"),
        trace=trace,
    )
    if trace is not None:
        response_data["trace"] = trace
    if status_code == 200 and response_data.get("cache") is None:
        auto_recovery.note_request_served(response_data.get("server_id"))
        disaster.on_request_served(response_data.get("server_id"))
    headers = {}
    if status_code == 429:
        headers["Retry-After"] = str(response_data.get("retry_after_s", 1))
    if response_data.get("cache"):
        headers["X-Cache"] = response_data["cache"]
        headers["Age"] = str(int(response_data.get("cache_age_s", 0)))
    return jsonify(response_data), status_code, headers


def _server_dicts():
    br = breakers.snapshot()
    active_faults = faults.active_by_server()
    out = []
    for s in registry.get_all_servers():
        d = s.to_dict()
        d["control_url"] = s.control_url
        d["circuit"] = br.get(s.id, {"state": "CLOSED", "consecutive_failures": 0, "opens": 0, "open_remaining_s": 0})
        d["faults"] = [{"type": f["type"], "params": f["params"], "injected_at": f["injected_at"],
                        "detected_at": f["detected_at"], "detected_by": f["detected_by"]} for f in active_faults.get(s.id, [])]
        d["routable"] = d["is_active"] and d["circuit"]["state"] != "OPEN"
        out.append(d)
    return out


@app.route("/api/status", methods=["GET"])
def system_status():
    """
    Returns complete real-time system state:
    - Active server count
    - Server states and latencies
    - Metrics snapshot
    - Active scheduling algorithm
    - Recent routing and system event logs
    """
    # Provisioning state is read BEFORE the registry, so a server registered in between appears once
    # (registered) instead of disappearing for one poll (see load_balancer/topology.py).
    recovery_status = auto_recovery.status()
    cap_status = capacity.status()
    all_servers = _server_dicts()
    # a server on standby is healthy and intentionally out of routing: not a degradation
    active_servers = [s for s in all_servers if s["is_active"] or (s.get("standby") and s["status"] == "HEALTHY")]
    topo = build_topology(all_servers, recovery_status, cap_status)

    # System health status
    total_count = len(all_servers)
    active_count = len(active_servers)
    if disaster.routing_block_reason():
        sys_status = "CRITICAL - DATA RECOVERY"
    elif active_count == total_count:
        sys_status = "HEALTHY"
    elif active_count > 0:
        sys_status = "DEGRADED"
    else:
        sys_status = "CRITICAL - NO HEALTHY SERVERS"

    counts = {k: sum(1 for s in all_servers if s["status"] == k) for k in ("HEALTHY", "UNHEALTHY", "RECOVERING")}
    counts["replacement"] = sum(1 for s in all_servers if s["is_replacement"])
    counts["draining"] = sum(1 for s in all_servers if s["draining"])
    counts["standby"] = sum(1 for s in all_servers if s.get("standby"))
    counts["circuit_open"] = sum(1 for s in all_servers if s["circuit"]["state"] == "OPEN")

    return jsonify({
        "system_status": sys_status,
        "load_balancer": "RUNNING",
        "active_server_count": active_count,
        "total_server_count": total_count,
        "server_counts": counts,
        "active_algorithm": scheduler.current_algorithm,
        "servers": all_servers,
        "metrics": metrics.get_metrics_snapshot(),
        "telemetry": metrics.telemetry(10.0),
        "logs": metrics.get_logs(limit=80),
        "events": metrics.get_events(limit=80),
        "auto_recovery": recovery_status,
        "capacity": cap_status,
        "topology": topo,
        "resilience": resilience.status(),
        "scenarios": scenarios.status(),
        "responses": metrics.response_series(60),
        "faults": {"records": faults.records(30), "types": list(FAULT_TYPES), "info": FAULT_INFO},
        "network_sim": NETSIM.snapshot(),
        "retry_budget": load_balancer.retry_budget.snapshot(),
        "proxy_inflight": load_balancer.inflight,
        "traffic": traffic.status(),
        "data": datastore.status(),
        "disaster": disaster.status(),
        "experiments": {"state": experiments.status()["state"]},
        "server_time": time.time(),
        "config": {
            "health_check_interval": config.HEALTH_CHECK_INTERVAL,
            "health_check_timeout": config.HEALTH_CHECK_TIMEOUT,
            "recovery_threshold": config.RECOVERY_THRESHOLD,
            "request_timeout": config.REQUEST_TIMEOUT,
            "connect_timeout": config.CONNECT_TIMEOUT,
            "max_failover_retries": config.MAX_FAILOVER_RETRIES,
            "circuit_failure_threshold": config.CIRCUIT_FAILURE_THRESHOLD,
            "circuit_open_seconds": config.CIRCUIT_OPEN_SECONDS,
            "proxy_max_inflight": config.PROXY_MAX_INFLIGHT,
            "backend_workers": config.BACKEND_WORKERS,
            "backend_service_time_ms": config.BACKEND_SERVICE_TIME_MS,
        }
    }), 200


@app.route("/api/telemetry", methods=["GET"])
def get_telemetry():
    window = min(120.0, max(2.0, float(request.args.get("window", 10))))
    return jsonify(metrics.telemetry(window)), 200


# ---------------------------------------------------------------------------
# Fault injection
# ---------------------------------------------------------------------------
@app.route("/api/fail/<server_id>", methods=["POST"])
def fail_server(server_id: str):
    """
    Inject a fault on a specific backend server (default: HTTP 503 application failure,
    as before). Optional JSON body: {"type": <fault type>, "loss": 0.3, "delay_ms": 400}.
    """
    server = registry.get_server(server_id)
    if not server:
        return jsonify({"status": "error", "message": f"Server {server_id} not found"}), 404
    body = _json_body()
    ftype = body.get("type", "http_5xx")
    try:
        rec = faults.inject(server_id, ftype, {k: body[k] for k in ("loss", "delay_ms") if k in body})
        return jsonify({
            "status": "success",
            "message": f"Failure injected into {server.name}",
            "fault": rec,
            "backend_response": {"failure_injected_at": rec["injected_at"], "type": ftype}
        }), 200
    except FaultError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Failed to reach {server.name}: {str(e)}"
        }), 500


@app.route("/api/recover/<server_id>", methods=["POST"])
def recover_server(server_id: str):
    """
    Clear every injected fault on a backend. The server must then pass consecutive health
    checks before it rejoins routing.
    """
    server = registry.get_server(server_id)
    if not server:
        return jsonify({"status": "error", "message": f"Server {server_id} not found"}), 404

    try:
        cleared = faults.clear(server_id)
        metrics.add_log(
            f"User triggered recovery on {server.name} ({server.url}). Server will undergo health verification...",
            level="INFO",
            event_type="RECOVERY_TRIGGERED"
        )
        return jsonify({
            "status": "success",
            "message": f"Recovery initiated for {server.name}",
            "cleared": [c["type"] for c in cleared],
            "backend_response": {"recovery_triggered_at": time.time()}
        }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Failed to reach {server.name}: {str(e)}"
        }), 500


@app.route("/api/faults", methods=["GET"])
def list_faults():
    return jsonify({"types": list(FAULT_TYPES), "info": FAULT_INFO, "active": faults.active_by_server(),
                    "records": faults.records(60), "network_sim": NETSIM.snapshot()}), 200


@app.route("/api/faults/inject", methods=["POST"])
def inject_fault():
    body = _json_body()
    try:
        rec = faults.inject(body.get("server_id", ""), body.get("type", ""),
                            {k: body[k] for k in ("loss", "delay_ms") if k in body})
        return jsonify({"status": "success", "fault": rec}), 200
    except FaultError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": f"could not inject: {e}"}), 500


@app.route("/api/faults/clear", methods=["POST"])
def clear_fault():
    body = _json_body()
    sid = body.get("server_id", "")
    try:
        if not sid:
            # No server given: clear every active fault (dashboard "Clear all").
            cleared, errors = [], []
            for server_id in list(faults.active_by_server()):
                if registry.get_server(server_id):
                    try:
                        cleared += faults.clear(server_id, body.get("type") or None)
                    except Exception as e:          # e.g. that backend's control plane is unreachable
                        errors.append(f"{server_id}: {e}")
            NETSIM.clear()
            return jsonify({"status": "partial" if errors else "success", "cleared": cleared,
                            "errors": errors}), 200
        if not registry.get_server(sid):
            return jsonify({"status": "error", "message": f"Server {sid} not found"}), 404
        cleared = faults.clear(sid, body.get("type") or None)
        return jsonify({"status": "success", "cleared": cleared}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": f"could not clear: {e}"}), 500


@app.route("/api/fail-all", methods=["POST"])
def fail_all_servers():
    """Inject a fault (default http_5xx) into every ORIGINAL backend without an active backend fault."""
    ftype = _json_body().get("type", "http_5xx")
    active = faults.active_by_server()
    failed, errors = [], []
    for server in registry.get_all_servers():
        if server.is_replacement or any(f["type"] in ("refused", "timeout", "http_5xx") for f in active.get(server.id, [])):
            continue
        try:
            faults.inject(server.id, ftype)
            failed.append(server.name)
        except Exception as e:
            errors.append(f"{server.name}: {e}")
    return jsonify({"status": "success" if not errors else "partial", "failed": failed, "errors": errors}), 200


@app.route("/api/recover-all", methods=["POST"])
def recover_all_servers():
    """Clear the faults of every backend that has one."""
    recovered, errors = [], []
    for sid in list(faults.active_by_server()):
        server = registry.get_server(sid)
        if server is None:
            continue
        try:
            faults.clear(sid)
            recovered.append(server.name)
        except Exception as e:
            errors.append(f"{server.name}: {e}")
    return jsonify({"status": "success" if not errors else "partial", "recovered": recovered, "errors": errors}), 200


@app.route("/api/circuit/reset", methods=["POST"])
def reset_circuit():
    sid = _json_body().get("server_id")
    breakers.reset(sid, reason="reset by operator")
    return jsonify({"status": "success"}), 200


# ---------------------------------------------------------------------------
# Routing strategy, metrics
# ---------------------------------------------------------------------------
@app.route("/api/set-algorithm", methods=["POST"])
def set_algorithm():
    """Switch the active routing strategy at runtime (see SchedulerManager.ALGORITHMS).
    Only the strategy changes: weights, server health, counters and metrics are kept."""
    data = request.get_json(silent=True) or {}
    algo = data.get("algorithm", "").upper()

    if scheduler.set_algorithm(algo):
        metrics.add_log(f"Scheduling algorithm switched to {algo}", level="INFO", event_type="CONFIG")
        return jsonify({"status": "success", "active_algorithm": algo}), 200
    return jsonify({
        "status": "error",
        "message": f"Invalid algorithm '{algo}'. Choose from: {', '.join(SchedulerManager.ALGORITHMS)}"
    }), 400


@app.route("/api/reset-metrics", methods=["POST"])
def reset_metrics():
    """Reset telemetry metrics and counters."""
    metrics.reset_metrics()
    scheduler.reset()
    for s in registry.get_all_servers():
        s.requests_served = 0
    return jsonify({"status": "success", "message": "Metrics reset successfully"}), 200


# ---------------------------------------------------------------------------
# Capacity, traffic generator, settings
# ---------------------------------------------------------------------------
@app.route("/api/capacity", methods=["GET"])
def capacity_status():
    return jsonify(capacity.status()), 200


@app.route("/api/resilience", methods=["GET"])
def resilience_status():
    return jsonify(resilience.status()), 200


@app.route("/api/resilience/policy", methods=["POST"])
def resilience_policy():
    b = _json_body()
    try:
        resilience.set_policy(str(b.get("policy", "")), b.get("static_servers"))
    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    return jsonify({"status": "success", "resilience": resilience.status()}), 200


@app.route("/api/resilience/allowed", methods=["POST"])
def resilience_allowed():
    try:
        resilience.set_allowed(list(_json_body().get("rungs", [])))
    except (ValueError, TypeError) as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    return jsonify({"status": "success", "resilience": resilience.status()}), 200


@app.route("/api/scenarios", methods=["GET"])
def scenarios_status():
    return jsonify(scenarios.status()), 200


@app.route("/api/scenarios/<action>", methods=["POST"])
def scenarios_action(action):
    if action == "stop":
        scenarios.stop()
        return jsonify({"status": "success"}), 200
    if action == "start":
        if experiments.state.get("running"):
            return jsonify({"status": "error", "message": "an experiment is running"}), 409
        if disaster.state not in ("IDLE", "RESTORED"):
            return jsonify({"status": "error", "message": "a data-recovery scenario is in progress"}), 409
        try:
            scenarios.start(str(_json_body().get("name", "")))
        except ValueError as e:
            return jsonify({"status": "error", "message": str(e)}), 400
        return jsonify({"status": "success", "scenarios": scenarios.status()}), 200
    return jsonify({"status": "error", "message": "unknown action"}), 404


@app.route("/api/topology", methods=["GET"])
def topology_status():
    """Backend nodes of the network diagram: registered + being provisioned + recently retired."""
    rec, cap = auto_recovery.status(), capacity.status()
    return jsonify(build_topology(_server_dicts(), rec, cap)), 200


@app.route("/api/capacity/<action>", methods=["POST"])
def capacity_action(action):
    if action == "pause":
        capacity.pause("paused by operator")
    elif action == "resume":
        capacity.resume()
    else:
        return jsonify({"status": "error", "message": "action must be pause or resume"}), 400
    return jsonify({"status": "success", "capacity": capacity.status()}), 200


@app.route("/api/traffic", methods=["GET"])
def traffic_status():
    return jsonify(traffic.status()), 200


@app.route("/api/traffic/start", methods=["POST"])
def traffic_start():
    b = _json_body()
    try:
        st = traffic.start(rps=b.get("rps", 10), concurrency=b.get("concurrency", 8), clients=b.get("clients", 16),
                           write_ratio=b.get("write_ratio", 0.0), duration=b.get("duration"), seed=b.get("seed", 1),
                           keys=b.get("keys", 0), heavy=b.get("heavy", 0.0), heavy_clients=b.get("heavy_clients", 2),
                           poisson=bool(b.get("poisson", False)))
    except (ValueError, TypeError) as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    metrics.add_log(f"Traffic generator started: {st['params']['rps']:g} req/s, {st['params']['concurrency']} "
                    f"concurrent, {st['params']['clients']} clients, {st['params']['write_ratio']:.0%} writes.",
                    level="INFO", event_type="TRAFFIC")
    return jsonify({"status": "success", "traffic": st}), 200


@app.route("/api/traffic/rate", methods=["POST"])
def traffic_rate():
    try:
        traffic.update_rate(_json_body().get("rps"))
    except (ValueError, TypeError) as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    return jsonify({"status": "success", "traffic": traffic.status()}), 200


@app.route("/api/traffic/stop", methods=["POST"])
def traffic_stop():
    traffic.stop()
    metrics.add_log("Traffic generator stopped.", level="INFO", event_type="TRAFFIC")
    return jsonify({"status": "success", "traffic": traffic.status()}), 200


@app.route("/api/settings", methods=["GET", "POST"])
def settings():
    keys = ["CAPACITY_TARGET_UTILIZATION", "CAPACITY_SCALE_IN_UTILIZATION", "CAPACITY_MIN_SERVERS",
            "CAPACITY_MAX_SERVERS", "CAPACITY_OBSERVATION_WINDOW", "CAPACITY_MIN_SAMPLES",
            "CAPACITY_SCALE_OUT_SUSTAIN", "CAPACITY_SCALE_IN_SUSTAIN", "CAPACITY_SCALE_OUT_COOLDOWN",
            "CAPACITY_SCALE_IN_COOLDOWN", "CAPACITY_PER_SERVER_RPS", "CAPACITY_ORIGINAL_STANDBY", "CIRCUIT_FAILURE_THRESHOLD", "CIRCUIT_OPEN_SECONDS",
            "CIRCUIT_HALF_OPEN_SUCCESSES", "MAX_FAILOVER_RETRIES", "SNAPSHOT_INTERVAL"]
    if request.method == "POST":
        try:
            applied = config.apply_settings(_json_body())
        except ValueError as e:
            return jsonify({"status": "error", "message": str(e)}), 400
        breakers.configure(failure_threshold=config.CIRCUIT_FAILURE_THRESHOLD, open_seconds=config.CIRCUIT_OPEN_SECONDS,
                           half_open_successes=config.CIRCUIT_HALF_OPEN_SUCCESSES)
        load_balancer.max_retries = config.MAX_FAILOVER_RETRIES
        datastore.snapshot_interval = config.SNAPSHOT_INTERVAL
        if applied:
            metrics.add_log("Settings changed: " + ", ".join(f"{k}={v}" for k, v in applied.items()),
                            level="INFO", event_type="CONFIG")
        capacity.notify()
    return jsonify({k: getattr(config, k) for k in keys}), 200


# ---------------------------------------------------------------------------
# Data tier and disaster recovery
# ---------------------------------------------------------------------------
@app.route("/api/records", methods=["GET", "POST"])
def records():
    if request.method == "GET":
        try:
            return jsonify({"records": datastore.list_records(100), "data": datastore.status()["primary"]}), 200
        except DataUnavailable as e:
            return jsonify({"status": "error", "message": str(e)}), 503
    b = _json_body()
    name = str(b.get("name", "")).strip()[:80] or f"record-{int(time.time() * 1000) % 100000}"
    value = str(b.get("value", ""))[:200]
    status, body = write_record(name, value)
    if status == 201:
        disaster.on_request_served(body.get("served_by"))
    return jsonify(body), status


@app.route("/api/records/<int:rid>", methods=["PUT", "DELETE"])
def record_item(rid):
    block = disaster.routing_block_reason()
    if block:
        return jsonify({"status": "error", "message": block}), 503
    try:
        if request.method == "DELETE":
            rec = datastore.delete(rid)
            metrics.add_log(f"Record {rid} deleted (e.g. an accidental delete: it is replicated and logged too).",
                            level="WARN", event_type="DATA")
        else:
            rec = datastore.update(rid, str(_json_body().get("value", ""))[:200])
        return jsonify({"status": "success", "record": rec}), 200
    except KeyError as e:
        return jsonify({"status": "error", "message": str(e)}), 404
    except DataUnavailable as e:
        return jsonify({"status": "error", "message": str(e)}), 503


@app.route("/api/data", methods=["GET"])
def data_status():
    return jsonify(datastore.status()), 200


@app.route("/api/data/map", methods=["GET"])
def data_map():
    """Per-record view: acknowledged version vs. the version held by each copy, and what each
    recovery strategy would restore right now."""
    limit = max(5, min(200, int(request.args.get("limit", 40) or 40)))
    return jsonify(datastore.record_map(limit=limit)), 200


@app.route("/api/data/<action>", methods=["POST"])
def data_action(action):
    b = _json_body()
    try:
        if action == "snapshot":
            return jsonify({"status": "success", "snapshot": datastore.snapshot("manual")}), 200
        if action == "corrupt":
            return jsonify({"status": "success", "message": datastore.corrupt(b.get("target", ""))}), 200
        if action == "seed":
            n = max(1, min(200, int(b.get("count", 10))))
            done = 0
            for i in range(n):
                st, body = write_record(f"order-{int(time.time() * 1000) % 1000000}-{i}", f"amount={100 + i}")
                if st != 201:
                    return jsonify({"status": "partial", "written": done, "message": body.get("message")}), 503
                done += 1
            return jsonify({"status": "success", "written": done}), 200
        if action == "reset":
            if disaster.state not in ("IDLE", "RESTORED"):
                return jsonify({"status": "error", "message": "a disaster scenario is in progress"}), 409
            datastore.reset()
            disaster.reset()
            metrics.add_log("Data tier reset to an empty dataset with empty durable storage.", level="INFO",
                            event_type="DATA")
            return jsonify({"status": "success"}), 200
    except (DataUnavailable, ValueError) as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    return jsonify({"status": "error", "message": "unknown action"}), 404


@app.route("/api/disaster", methods=["GET"])
def disaster_status():
    return jsonify(disaster.status()), 200


@app.route("/api/disaster/start", methods=["POST"])
def disaster_start():
    b = _json_body()
    try:
        st = disaster.start(b.get("strategy", "wal"), b.get("fault", "refused"))
    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except RuntimeError as e:
        return jsonify({"status": "error", "message": str(e)}), 409
    return jsonify({"status": "success", "disaster": st}), 200


@app.route("/api/disaster/acknowledge", methods=["POST"])
def disaster_ack():
    if not disaster.acknowledge():
        return jsonify({"status": "error", "message": "nothing to acknowledge"}), 409
    return jsonify({"status": "success", "disaster": disaster.status()}), 200


# ---------------------------------------------------------------------------
# Experiments
# ---------------------------------------------------------------------------
@app.route("/api/experiments", methods=["GET"])
def experiments_status():
    return jsonify(experiments.status()), 200


@app.route("/api/experiments/<kind>", methods=["POST"])
def experiments_start(kind):
    b = _json_body()
    if kind != "cancel":
        scenarios.stop(wait=True)          # one thing drives the traffic at a time
    try:
        if kind == "failover":
            experiments.start_failover(b)
        elif kind == "dr":
            experiments.start_dr(b)
        elif kind == "utilisation":
            experiments.start_utilisation(b)
        elif kind == "policies":
            experiments.start_policies(b)
        elif kind == "cancel":
            experiments.cancel()
        else:
            return jsonify({"status": "error", "message": "kind must be failover, dr, utilisation, policies or cancel"}), 404
    except ValueError as e:
        return jsonify({"status": "error", "message": str(e)}), 400
    except RuntimeError as e:
        return jsonify({"status": "error", "message": str(e)}), 409
    return jsonify({"status": "success", "experiments": experiments.status()["state"]}), 200


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------
@app.route("/api/reset-simulation", methods=["POST"])
def reset_simulation():
    """
    Return the whole demonstration to its initial state:
    1. Stop traffic and experiments; cancel recovery and scaling work in progress.
    2. Clear every injected fault (backend and simulated network) and circuit breaker.
    3. Stop and unregister every replacement backend.
    4. Probe each backend's /health once and reinitialise the registry from the
       live result (as on a fresh load balancer start).
    5. Restore the default scheduling algorithm and reset scheduler pointers.
    6. Reset the data tier to an empty dataset; clear counters, metrics, logs and events.
    """
    experiments.cancel()
    scenarios.stop(wait=True)
    traffic.stop()
    capacity.reset()                      # cancels provisioning in flight, stops capacity replacements
    resilience.reset()                    # back to rung 0 (policy kept); cost counters cleared
    if resilience.policy != config.RESILIENCE_POLICY:   # the dashboard only shows the cost-aware ladder
        resilience.set_policy(config.RESILIENCE_POLICY)
    response_cache.reset()
    limiter.reset()
    load_balancer.degrade = False
    auto_recovery.reset()                 # stops recovery replacements
    disaster.reset()
    faults.reset()
    NETSIM.reset()
    breakers.reset()
    load_balancer.retry_budget.reset()

    health = {}
    for server in registry.get_all_servers():
        try:
            st = requests.get(f"{server.url}/status", timeout=1.0).json()
            if not st.get("is_healthy", True):
                requests.post(f"{server.url}/simulate/recover", timeout=1.0)
        except Exception:
            pass
        try:
            r = requests.get(f"{server.url}/health", timeout=config.HEALTH_CHECK_TIMEOUT)
            health[server.id] = r.status_code == 200
        except Exception:
            health[server.id] = False

    registry.reset_all(health)
    auto_recovery.resume()
    capacity.resume()
    scheduler.set_algorithm(config.DEFAULT_ALGORITHM)
    scheduler.reset()
    datastore.reset()
    metrics.reset_metrics()
    metrics.clear_logs()

    health_monitor.report_initial_health_again()

    healthy = sum(1 for ok in health.values() if ok)
    metrics.add_log(
        f"Simulation reset. {healthy}/{len(health)} servers healthy and in the active pool.",
        level="INFO",
        event_type="SYSTEM"
    )
    return jsonify({"status": "success", "healthy_servers": healthy, "total_servers": len(health)}), 200


if __name__ == "__main__":
    print(f"[*] Starting Load Balancer & Dashboard on http://{config.LOAD_BALANCER_HOST}:{config.LOAD_BALANCER_PORT}...")
    app.run(
        host=config.LOAD_BALANCER_HOST,
        port=config.LOAD_BALANCER_PORT,
        debug=False,
        threaded=True
    )
