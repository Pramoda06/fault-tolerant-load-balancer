"""
Central Configuration for Load Balancing and Fault-Tolerant Network Monitoring.

All system parameters (ports, intervals, timeouts, thresholds) are defined here
to avoid hardcoding throughout the codebase.
"""

import os

# Load Balancer configuration
LOAD_BALANCER_HOST = "127.0.0.1"
LOAD_BALANCER_PORT = 5000

# Backend Server configurations
# Each server simulates an independent network service on a separate localhost port.
BACKEND_SERVERS = [
    {
        "id": "server-1",
        "name": "Server 1",
        "host": "127.0.0.1",
        "port": 5001,
        "weight": 3,
        "url": "http://127.0.0.1:5001",
    },
    {
        "id": "server-2",
        "name": "Server 2",
        "host": "127.0.0.1",
        "port": 5002,
        "weight": 2,
        "url": "http://127.0.0.1:5002",
    },
    {
        "id": "server-3",
        "name": "Server 3",
        "host": "127.0.0.1",
        "port": 5003,
        "weight": 1,
        "url": "http://127.0.0.1:5003",
    },
]

# Scheduling algorithm the load balancer starts with (and returns to on simulation reset).
# Options: "ROUND_ROBIN", "WEIGHTED_ROUND_ROBIN" (uses each server's "weight" as its capacity),
# "LEAST_CONNECTIONS", "WEIGHTED_LEAST_CONNECTIONS", "LEAST_RESPONSE_TIME", "IP_HASH".
DEFAULT_ALGORITHM = "ROUND_ROBIN"

# Health Monitoring configuration
HEALTH_CHECK_INTERVAL = 2.0     # Time (seconds) between periodic health checks
HEALTH_CHECK_TIMEOUT = 1.0      # Max time (seconds) to wait for a health check response
RECOVERY_THRESHOLD = 2          # Consecutive successful health checks needed before re-admission

# Forwarding / Proxy configuration
REQUEST_TIMEOUT = 3.0           # Max time (seconds) to wait for backend request processing
MAX_FAILOVER_RETRIES = 2        # Retry up to N times on other healthy servers if a backend fails mid-request

# Traffic Generator defaults
DEFAULT_TRAFFIC_INTERVAL = 1.0  # Seconds between automated traffic requests
MIN_TRAFFIC_INTERVAL = 0.2
MAX_TRAFFIC_INTERVAL = 5.0

# Automatic Backend Recovery (dynamic replacement server provisioning)
# When the reverse proxy has 0 healthy backends it starts a new backend process
# (backend_servers/replacement_server.py) on a free port, health-checks it and
# registers it in the routing pool. Set environment variable LB_AUTO_RECOVERY=0 to disable.
AUTO_RECOVERY_ENABLED = os.environ.get("LB_AUTO_RECOVERY", "1") != "0"
AUTO_RECOVERY_MAX_REPLACEMENTS = 2     # max replacement backends alive at the same time
AUTO_RECOVERY_MAX_ATTEMPTS = 2         # max failed provisioning attempts per outage
AUTO_RECOVERY_RETRY_COOLDOWN = 5.0     # seconds between failed attempts
REPLACEMENT_PORT_RANGE = (5004, 5050)  # ports searched for a free one
REPLACEMENT_WEIGHT = 2                 # capacity weight given to a replacement backend
REPLACEMENT_STARTUP_TIMEOUT = 15.0     # seconds allowed to start and pass health checks

# =============================================================================
# Backend service-capacity model (SIMULATED, configurable)
# Each backend handles /process requests with BACKEND_WORKERS worker slots and a
# fixed simulated service time. Requests that find all workers busy wait in a
# bounded queue (BACKEND_QUEUE_LIMIT); beyond that the backend answers
# HTTP 503 + header X-Backend-Overloaded (explicit overload, no unbounded queuing).
# Theoretical capacity per backend = workers / service time = 1 / 0.050 s = 20 req/s.
# =============================================================================
BACKEND_WORKERS = int(os.environ.get("LB_BACKEND_WORKERS", "1"))
BACKEND_SERVICE_TIME_MS = float(os.environ.get("LB_BACKEND_SERVICE_MS", "50"))
BACKEND_QUEUE_LIMIT = 20
BACKEND_HANG_SECONDS = 5.0             # "timeout" fault: backend accepts but answers only after this delay
BACKEND_SLOW_DELAY_MS = 400            # default extra delay of the "slow" fault
CONTROL_PORT_OFFSET = 1000             # management (control-plane) port = data port + offset (5001 -> 6001)

# =============================================================================
# Proxy forwarding: timeouts, retry policy, admission control
# =============================================================================
CONNECT_TIMEOUT = 0.5                  # TCP connect timeout for forwarded requests (seconds)
RETRY_BACKOFF_BASE_MS = 20             # exponential backoff before a retry: 20, 40, 80 ... ms (+ jitter)
RETRY_BACKOFF_CAP_MS = 200
RETRY_BUDGET_RATIO = 0.2               # retries may add at most 20 % extra requests ...
RETRY_BUDGET_MIN_TOKENS = 5            # ... plus this reserve (token bucket)
RETRY_BUDGET_MAX_TOKENS = 20
PROXY_MAX_INFLIGHT = 64                # admission control: more concurrent client requests -> HTTP 503 (shed)
IDEMPOTENT_METHODS = ("GET", "HEAD", "PUT", "DELETE", "OPTIONS")

# =============================================================================
# Circuit breaker (passive health, per backend, in the proxy)
# =============================================================================
CIRCUIT_FAILURE_THRESHOLD = 3          # consecutive failed requests -> OPEN
CIRCUIT_OPEN_SECONDS = 5.0             # OPEN -> HALF_OPEN after this time
CIRCUIT_HALF_OPEN_MAX_TRIALS = 1       # concurrent trial requests allowed while HALF_OPEN
CIRCUIT_HALF_OPEN_SUCCESSES = 2        # successful trials needed to CLOSE

# =============================================================================
# Simulated network faults (applied in the proxy's outbound network layer)
# =============================================================================
SIM_PARTITION_WAIT = 0.5               # a partitioned backend: connect attempt fails after this wait (s)
SIM_LOSS_WAIT = 1.0                    # a lost exchange: no reply; caller gives up after this wait (s)
SIM_DEFAULT_LOSS = 0.3                 # default simulated loss probability
SIM_SLOW_DETECT_MS = 250               # proxied response slower than this marks a "slow" fault detected

# =============================================================================
# Load-aware capacity management (after recovery and continuously)
#   required = ceil(demand_rps / (capacity_rps_per_server * TARGET_UTILIZATION))
# =============================================================================
CAPACITY_ENABLED = os.environ.get("LB_CAPACITY", "1") != "0"
CAPACITY_TARGET_UTILIZATION = 0.7      # plan servers to run at <= 70 % of their capacity
CAPACITY_SCALE_IN_UTILIZATION = 0.5    # remove a replacement only if the rest would run below 50 %
CAPACITY_MIN_SERVERS = 1               # minimum routable servers
CAPACITY_MAX_SERVERS = 6               # maximum routable servers (originals + replacements)
CAPACITY_OBSERVATION_WINDOW = 10.0     # seconds of demand measured before a decision
CAPACITY_MIN_SAMPLES = 20              # requests needed in the window before scaling on it
CAPACITY_SCALE_OUT_SUSTAIN = 5.0       # desired > actual continuously for this long
CAPACITY_SCALE_IN_SUSTAIN = 8.0        # low demand continuously for this long (demo-length; configurable)
CAPACITY_SCALE_OUT_COOLDOWN = 5.0      # after a server is added (demo-length; configurable)
CAPACITY_SCALE_IN_COOLDOWN = 4.0       # after a server is removed
CAPACITY_EVAL_INTERVAL = 1.0
# Capacity per backend used by the formula. 0 = derive it from the measured backend service time
# (workers / mean service time); if too few measurements exist, the configured service model is used
# and labelled "simulated". A value > 0 is an explicitly configured capacity (req/s per backend).
CAPACITY_PER_SERVER_RPS = float(os.environ.get("LB_CAPACITY_PER_SERVER_RPS", "0"))
# Optional: scale-in may also take ORIGINAL servers out of routing when demand is low. They are
# put on STANDBY (process running, still health-checked, no requests), never stopped, and are the
# first capacity used when demand rises again. Default 0: the 3 original servers always serve and
# only replacement servers are scaled in. Enable with 1 (or LB_ORIGINAL_STANDBY=1 / settings panel).
CAPACITY_ORIGINAL_STANDBY = int(os.environ.get("LB_ORIGINAL_STANDBY", "0"))
TOPOLOGY_RETIRED_DISPLAY_S = 10.0      # a retired replacement stays visible (greyed, no link) this long
CAPACITY_DRAIN_TIMEOUT = 10.0          # max wait for in-flight requests on a draining replacement
CAPACITY_FAILED_REPLACEMENT_GRACE = 15.0   # a replacement UNHEALTHY this long is retired
CAPACITY_MAX_PROVISION_FAILURES = 3    # consecutive failed provisions before backing off
CAPACITY_PROVISION_BACKOFF = 10.0

# =============================================================================
# Cost-aware resilience controller (load_balancer/resilience.py)
#   SLO measured at the proxy; cheaper mitigations are tried before servers are added.
# =============================================================================
RESILIENCE_POLICY = os.environ.get("LB_RESILIENCE_POLICY", "ladder")   # ladder | scale_only | static
RESILIENCE_SLO_P95_MS = 200.0          # SLO: p95 end-to-end latency of answered requests
RESILIENCE_SLO_ERROR = 0.01            # SLO: error rate (5xx / no backend) of answered requests
RESILIENCE_WINDOW_S = 5.0              # measurement window of the controller
RESILIENCE_UTIL_HIGH = 0.85            # escalate before the SLO breaks when utilisation exceeds this
RESILIENCE_UTIL_LOW = 0.5              # relax only when utilisation is below this ...
RESILIENCE_RELAX_S = 10.0              # ... and the SLO has been met with margin for this long
RESILIENCE_DWELL_S = 5.0               # minimum time between decisions (effect is measured after it)
RESILIENCE_CACHE_TTL_S = 2.0           # freshness of cached popular items
RESILIENCE_CACHE_STALE_MAX_S = 60.0    # stale-if-error: oldest cached answer that may be served
RESILIENCE_HEAVY_FACTOR = 2.0          # a client is limited above this multiple of its fair share
RESILIENCE_MIN_RELIEF = float(os.environ.get("LB_RESILIENCE_MIN_RELIEF", "0.10"))          # escalate to a rung only if it is predicted to remove >= 10 % of load
RESILIENCE_STATIC_SERVERS = 4          # servers kept by the 'static' baseline policy
RESILIENCE_COST_WEIGHTS = {"server_second": 1.0, "slo_violation_second": 2.0}
BACKEND_DEGRADED_FACTOR = 0.4          # brownout: a degraded request costs 40 % of the service time

# =============================================================================
# Durable data tier and disaster recovery (SQLite, local files)
# =============================================================================
DATA_DIR = os.environ.get("LB_DATA_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
SNAPSHOT_INTERVAL = 30.0               # periodic backup interval (seconds); 0 = manual only
SNAPSHOTS_KEPT = 5
REPLICATION_MODE = "sync"              # replica updated before the write is acknowledged
DR_STRATEGIES = ("backup", "replica", "wal", "none")

# =============================================================================
# Experiments and server-side traffic generator
# =============================================================================
TRAFFIC_MAX_RPS = 100
TRAFFIC_MAX_CONCURRENCY = 32
EXPERIMENT_REPORT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")


def validate_settings(values: dict) -> dict:
    """Validate runtime-editable settings (dashboard settings panel). Returns cleaned values."""
    rules = {
        "CAPACITY_TARGET_UTILIZATION": (float, 0.1, 1.0),
        "CAPACITY_SCALE_IN_UTILIZATION": (float, 0.05, 0.95),
        "CAPACITY_MIN_SERVERS": (int, 1, 10),
        "CAPACITY_MAX_SERVERS": (int, 1, 12),
        "CAPACITY_OBSERVATION_WINDOW": (float, 2.0, 120.0),
        "CAPACITY_MIN_SAMPLES": (int, 1, 1000),
        "CAPACITY_SCALE_OUT_SUSTAIN": (float, 0.0, 120.0),
        "CAPACITY_SCALE_IN_SUSTAIN": (float, 1.0, 600.0),
        "CAPACITY_SCALE_OUT_COOLDOWN": (float, 0.0, 600.0),
        "CAPACITY_SCALE_IN_COOLDOWN": (float, 0.0, 600.0),
        "CAPACITY_PER_SERVER_RPS": (float, 0.0, 10000.0),
        "CAPACITY_ORIGINAL_STANDBY": (int, 0, 1),
        "CIRCUIT_FAILURE_THRESHOLD": (int, 1, 50),
        "CIRCUIT_OPEN_SECONDS": (float, 0.5, 120.0),
        "CIRCUIT_HALF_OPEN_SUCCESSES": (int, 1, 20),
        "MAX_FAILOVER_RETRIES": (int, 0, 5),
        "SNAPSHOT_INTERVAL": (float, 0.0, 3600.0),
    }
    out, errors = {}, []
    for key, raw in values.items():
        if key not in rules:
            errors.append(f"{key}: not a runtime setting")
            continue
        typ, lo, hi = rules[key]
        try:
            v = typ(raw)
        except (TypeError, ValueError):
            errors.append(f"{key}: expected {typ.__name__}")
            continue
        if not lo <= v <= hi:
            errors.append(f"{key}: must be between {lo} and {hi}")
            continue
        out[key] = v
    merged = {k: out.get(k, globals()[k]) for k in rules}
    if merged["CAPACITY_MIN_SERVERS"] > merged["CAPACITY_MAX_SERVERS"]:
        errors.append("CAPACITY_MIN_SERVERS must not exceed CAPACITY_MAX_SERVERS")
    if merged["CAPACITY_SCALE_IN_UTILIZATION"] >= merged["CAPACITY_TARGET_UTILIZATION"]:
        errors.append("CAPACITY_SCALE_IN_UTILIZATION must be lower than CAPACITY_TARGET_UTILIZATION (hysteresis)")
    if errors:
        raise ValueError("; ".join(errors))
    return out


def apply_settings(values: dict) -> dict:
    """Validate and apply runtime settings to this module (read at use time by the components)."""
    cleaned = validate_settings(values)
    globals().update(cleaned)
    return cleaned
