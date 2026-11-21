> **Note:** these are the detailed technical notes. The dashboard no longer has an *Experiments* page and the *Data Recovery* page no longer has the comparison and advanced panels; the experiment code and API described here are still in the repository. See the main [README](../README.md) for the overview.

# Load Balancing and Fault-Tolerant Network Monitoring Using a Reverse Proxy

An academic Computer Networks laboratory project. A Flask reverse proxy distributes HTTP requests across
backend servers. It monitors them actively and passively, routes around network and application failures,
restores service after a total outage, and decides how many servers the measured demand needs. It also shows
that restoring servers is not the same as restoring data.

Everything runs on one machine over loopback (`127.0.0.1`). Some faults are **real** (closed TCP ports, real
timeouts, real HTTP 503s, real slow responses). Others are **simulated** inside the proxy (packet loss,
network partition), and the dashboard and API label them so. Nothing here is a production system.

---

## 1. Project Overview

**Domain**: Computer Networks / Distributed Systems
**Implementation**: Python 3, Flask, Requests, SQLite (standard library), HTML5, CSS3, JavaScript, Chart.js (bundled)

A single server is a single point of failure and cannot scale with demand. This project puts a **reverse proxy**
(port 5000) between clients and three backend servers (ports 5001–5003). The proxy:

- **load-balances** with six strategies;
- **health-checks** every backend (active probes) and watches real traffic (passive checks: circuit breakers);
- **fails over** to healthy servers with bounded, idempotency-aware retries;
- **restores service** after a total outage by starting one minimum-viable replacement server (stage 1);
- **scales capacity** to the measured demand, then retires replacements safely by draining them (stage 2);
- **recovers data** after a total failure from durable storage kept outside the backends (backup, replica,
  write-ahead log), and reports the measured data loss (RPO) and recovery time (RTO).

## 2. Problem Statement and Objectives

When clients talk to one server directly, overload degrades it, and a crash, a hung process or a network
partition gives the client connection errors. Static routing cannot react to these events.

Objectives:
1. **Load balancing** – distribute requests over healthy servers (six strategies).
2. **Health monitoring** – active `GET /health` probes with strict timeouts, plus passive monitoring of real
   requests (circuit breaker).
3. **Failure detection vs. recovery verification** – remove a server quickly after a failure; re-admit it only
   after `RECOVERY_THRESHOLD` (2) consecutive passing checks, to prevent flapping.
4. **Failover** – retry a failed request on another server when it is safe to do so.
5. **Network fault handling** – connection refused, timeout, slow responses, HTTP 5xx, plus simulated packet
   loss and partition, each with measured detection and recovery.
6. **Load-aware capacity** – after an outage restore service first, then provision servers to match the
   *measured* demand, with hysteresis, cooldowns and safe draining.
7. **Data durability** – separate application availability from data durability, and measure the RPO and RTO
   of four recovery strategies.
8. **Measured telemetry** – every number comes from runtime measurement and is labelled with its source.

---

## 3. System Architecture

```
                         Client (browser, traffic generator, experiments, curl)
                                             │  HTTP :5000
          ┌──────────────────────────────────▼───────────────────────────────────┐
          │                      Reverse proxy (app.py, Flask)                    │
          │                                                                      │
          │  admission control ─► data-recovery gate ─► scheduler (6 strategies)  │
          │        │                                     │                       │
          │        │                          circuit-breaker filter             │
          │        │                                     │                       │
          │        │                    forward (connect 0.5 s / read 3 s)       │
          │        │              ◄── network simulator (partition, loss) ──►    │
          │        │                    retry policy + retry budget               │
          │                                                                      │
          │  Health monitor ──► Server registry ◄── Auto-recovery (stage 1)       │
          │  (GET /health      (state machine,     ◄── Capacity manager (stage 2) │
          │   every 2 s)        origin, draining)  ◄── Fault manager              │
          │  Metrics/telemetry · Data store (SQLite) · Disaster recovery ·        │
          │  Traffic generator · Experiment runner                                │
          └───────┬───────────────────────┬──────────────────────┬───────────────┘
                  │ data plane :5001      │ :5002                │ :5003   (+ replacements :5004…)
            ┌─────▼──────┐          ┌─────▼──────┐         ┌─────▼──────┐
            │  Server 1  │          │  Server 2  │         │  Server 3  │
            │ /health    │          │            │         │            │
            │ /process   │          │  1 worker, 50 ms service time,   │
            │ control    │          │  bounded queue (20) -> 503 when  │
            │  :6001     │          │  full; control plane :600x       │
            └────────────┘          └────────────┘         └────────────┘

   Durable storage (separate from the backends):  data/durable/{wal.jsonl, replica.sqlite, snapshots/, manifest.json}
   Backend-local data (lost in a total failure):  data/backend_local/primary.sqlite
```

Each backend has two listeners:
- a **data plane** (`500x`) that serves client traffic and `/health`;
- a **control plane** (`600x` = data port + 1000) that the proxy uses to inject and clear faults.

The control plane stays up when the data port is closed. That is how a "connection refused" fault can be
undone without restarting the process.

### Request-routing flow

1. **Admission control.** More than `PROXY_MAX_INFLIGHT` (64) concurrent client requests are shed with HTTP 503.
   This is bounded overload instead of an unbounded queue.
2. **Data-recovery gate.** While a disaster recovery is restoring data, requests get HTTP 503 with a clear message.
3. **Server choice.** The scheduler picks a server from the active pool: HEALTHY, not draining, circuit not OPEN.
   With no usable server the proxy returns HTTP 503 (`no_backend_503`).
4. **Forwarding.** The request is forwarded with a connect timeout of 0.5 s and a read timeout of 3 s.
5. **Failure classification.** A failure is classified as refused, connect timeout, read timeout, HTTP 5xx,
   backend overloaded, simulated partition or simulated loss. It is recorded on the server's circuit breaker.
6. **Retry decision.** The request is retried on a different server only if all three hold:
   - the method is idempotent (GET, HEAD, PUT, DELETE, OPTIONS), **or** the request provably never reached the
     backend (refused, connect timeout, partition, backend-overloaded rejection);
   - the retry budget has a token;
   - `MAX_FAILOVER_RETRIES` (2) is not exceeded.

   Backoff between retries is exponential: 20 ms, doubling, capped at 200 ms, with ±50 % jitter.
7. **Response.** The response carries `server_id`, `attempts`, `failover_retries` and `proxy_latency_ms`.

### Health monitoring and server-state transitions

```
                            probe fails
          HEALTHY ───────────────────────────────────► UNHEALTHY
             ▲                                             │ probe passes
             │  RECOVERY_THRESHOLD (2) consecutive passes  ▼
             └──────────────────────────────────────── RECOVERING ──(probe fails)──► UNHEALTHY
```

- **HEALTHY** – passes probes; in the routing pool.
- **UNHEALTHY** – failed a probe; removed from the pool and receives no requests.
- **RECOVERING** – answering again but on probation. It receives no traffic until it passes
  `RECOVERY_THRESHOLD` checks.
- **Draining** (replacement servers only) – receives no new requests; in-flight requests finish, then the
  server is stopped and removed.

All probes of one cycle run in parallel (one probe per server; the next cycle starts after all have finished).
Each probe carries the registry epoch it started in. A simulation reset increments the epoch, so a probe that
was in flight during a reset cannot overwrite the fresh state. Only the health monitor changes a server's
health state; real traffic acts through the circuit breaker. On re-admission the server's breaker is reset.

**Failure detection vs. recovery verification.** A failure is acted on immediately:
- one failed probe removes the server from the pool;
- a failed forwarded request is retried on another server and counts towards the circuit breaker, which stops
  routing to the server after 3 consecutive failures, usually before the next probe.

Recovery needs repeated evidence: two consecutive passing probes. The asymmetry
is deliberate. Wrongly removing a healthy server only costs capacity for a few seconds, while wrongly re-admitting
a flapping server sends client requests into failures.

---

## 4. Technologies Used

- **Backend:** Python 3.9+ (tested with 3.11–3.13), Flask 3, Requests 2, Werkzeug `make_server` (closable data
  plane), `threading`, `subprocess`, `sqlite3`, `hashlib`.
- **Frontend:** HTML5, CSS3 (grid, custom properties, dark/light theme), vanilla JavaScript, inline SVG topology,
  Chart.js 4.4 bundled in `static/vendor` (works offline).
- **Testing:** `unittest` (unit tests with deterministic clocks and fake processes), live integration scripts
  (`verify_*.py`), Playwright/Chromium for the dashboard check (optional, not a project dependency).

No cloud services, paid APIs or extra infrastructure: `requirements.txt` is still `flask` and `requests`.

---

## 5. Folder Structure

```
CN_project/
├── app.py                        # Reverse proxy, REST API, dashboard server (port 5000); wires all components
├── config.py                     # All settings + validate_settings / apply_settings for the settings panel
├── run_all.py                    # All-in-one launcher (3 backends + proxy)
├── requirements.txt              # flask, requests
├── verify_live_system.py         # Live: original 6 networking scenarios
├── verify_auto_recovery.py       # Live: stage-1 automatic backend recovery
├── verify_routing_strategies.py  # Live: six routing strategies with real traffic
├── verify_network_faults.py      # Live: six fault types, circuit breaker, retries          (new)
├── verify_capacity_recovery.py   # Live: outage -> 1 replacement -> observe -> scale -> drain (new)
├── verify_experiments.py         # Live: DR comparison + failover comparison                (new)
├── verify_load_scaling.py        # Live: one healthy backend -> scale-out -> scale-in -> reset (new)
├── verify_resilience.py         # Live: static vs scale_only vs ladder on the same trace    (new)
│
├── backend_servers/
│   ├── base_server.py            # Backend app: service model, faults, DataPlane, control plane
│   ├── server1.py … server3.py   # Original backends (5001-5003, control 6001-6003)
│   └── replacement_server.py     # Backend started on demand (--id --port --control-port ...)
│
├── load_balancer/
│   ├── server_registry.py        # Thread-safe registry, state machine, origin, draining
│   ├── scheduler.py              # Six routing strategies
│   ├── health_monitor.py         # Parallel active probes, transition hooks
│   ├── balancer.py               # Forwarding, admission control, breaker filter, retries
│   ├── metrics.py                # Counters, latency percentiles, telemetry with source labels
│   ├── auto_recovery.py          # Stage 1: one minimum-viable replacement after a total outage
│   ├── capacity.py               # Stage 2: load-aware scale-out / drained scale-in      (new)
│   ├── topology.py               # Diagram nodes: registered + provisioning + retired     (new)
│   ├── provisioning.py           # Port allocation, provisioning gate, process start/stop (new)
│   ├── circuit_breaker.py        # CLOSED / OPEN / HALF_OPEN per backend                  (new)
│   ├── retry_policy.py           # Failure classes, idempotency rules, backoff, budget    (new)
│   ├── netsim.py                 # Simulated partition and packet loss (proxy side)       (new)
│   ├── faults.py                 # Fault injection, detection and recovery timeline       (new)
│   ├── datastore.py              # SQLite primary, WAL, replica, snapshots, manifest      (new)
│   ├── disaster.py               # Total-failure + data-recovery orchestration            (new)
│   ├── traffic.py                # Server-side open-loop traffic generator                (new)
│   ├── resilience.py             # Cost-aware mitigation ladder (section 9)               (new)
│   ├── scenarios.py              # One-click demonstrations of each method (section 9)     (new)
│   ├── cache.py                  # Proxy response cache + stale-if-error                  (new)
│   ├── ratelimit.py              # Per-client token bucket for heavy hitters (HTTP 429)    (new)
│   └── experiments.py            # Repeatable failover and DR comparisons -> reports/     (new)
│
├── templates/dashboard.html      # 7 pages: Overview, Load Balancing, Fault Tolerance, Monitoring,
│                                 #          Resilience & Capacity, Data Recovery, Experiments
├── static/css/style.css
├── static/js/dashboard.js        # Original dashboard (+ a small extension bridge)
├── static/js/dashboard_ext.js    # New panels and pages                                    (new)
├── tests/                        # 179 unit tests (50 original + 129 new)
├── data/                         # Created at runtime: backend_local/ and durable/  (LB_DATA_DIR overrides)
└── reports/                      # Created at runtime: experiment reports (JSON)
```

---

## 6. Installation and Running (Windows)

Prerequisites: Python 3.9 or newer and `pip`.

```powershell
cd F:\CN_project
pip install -r requirements.txt
```

**Option A: all in one**
```powershell
python run_all.py
```
Open **http://127.0.0.1:5000**. Press `Ctrl+C` to stop; replacement servers stop with the proxy.

**Option B: one terminal per process** (shows that each server is a separate OS process on its own port)
```powershell
python backend_servers/server1.py
python backend_servers/server2.py
python backend_servers/server3.py
python app.py
```

**Environment switches** (optional):

| Variable | Default | Effect |
|---|---|---|
| `LB_AUTO_RECOVERY` | `1` | `0` disables stage-1 recovery |
| `LB_CAPACITY` | `1` | `0` disables the capacity manager (stage 2) |
| `LB_DATA_DIR` | `.\data` | location of the data tier |
| `LB_BACKEND_WORKERS` | `1` | worker slots per backend |
| `LB_BACKEND_SERVICE_MS` | `50` | simulated service time per request |
| `LB_RESILIENCE_POLICY` | `ladder` | `scale_only` or `static` (section 9) |
| `LB_RESILIENCE_MIN_RELIEF` | `0.10` | minimum predicted relief for a rung; `0` disables effect-aware escalation (rungs strictly in order — the ablation in the paper) |
| `LB_ORIGINAL_STANDBY` | `0` | `1` lets low demand put original servers on standby (off: the 3 originals always serve) |

Example in PowerShell: `$env:LB_CAPACITY="0"; python app.py`.

---

## 7. Dashboard

| Page | Contents |
|---|---|
| **Overview** | Topology Client → Reverse Proxy → Servers with live request dots, system status |
| **Load Balancing** | Strategy switch, actual vs. expected share, routing sequence, **request tracer** (one request, every hop, real headers and timings), strategy comparison |
| **Fault Tolerance** | One *Simulate failure* panel: choose the fault (HTTP 503, refused, timeout, slow, partition, packet loss, with its parameter), then Fail / Recover per server. Auto-recovery pipeline, failover sequence and timings for any fault type, circuit-breaker state per server, fault timeline (injected → detected after → detected by → cleared → back in routing → outcome) |
| **Monitoring** | Charts (at the top), **what clients received** (HTTP outcome per second: 200 from a backend / lighter / from the cache, 429, 5xx), health table, distribution, event timeline, request log. **New:** networking telemetry, each value tagged *proxy-observed*, *backend-reported* or *simulated* |
| **Resilience & Capacity** (new) | **See each method in action**: five one-click scenarios (popular items → cache, two flooding clients → rate limit, all-different requests → brownout, checkout traffic → add servers, server crash during a spike → controller's choice), each 80 s (normal → spike → calm) with *Expected* vs *Happening now* and the HTTP outcome chart. **Resilience controller** (section 9): policy switch, SLO meters, the four methods with live effect, predicted relief and an *allowed for this service* switch, and one history of everything the system did (mitigations and server changes, with before → after). Then the **load-based allocation topology**, capacity management, the current decision, and folded *Custom traffic* and *Advanced settings* |
| **Data Recovery** (new) | Rebuilt for first-time viewers: an idea strip (servers are replaceable → a server's disk is lost → only copies kept elsewhere bring data back), then three steps: **1** make changes (or **▶ Play the story**, which narrates writes → backup → more writes, a change and a delete), with the measured write path; a **timeline** of every confirmed change with a *backup* line and one bar per strategy (green = comes back, red = lost); **2** pick what was kept (exact preview per card) and fail every server, with live progress; **3** what came back: the same timeline with a *failure* line, RPO, RTO, checksums. Record-by-record view, *Measure all four for real* and *Advanced* are folded. |
| **Experiments** (new) | Each card says *What this shows* and *What to look for*. Resilience policies (static / scale_only / ladder, with charts and costs), utilisation vs latency, failover comparison (6 strategies), each with a results table, saved report path and limitations. The DR comparison is on the Data Recovery page. |

There is **one traffic generator**. It runs inside the proxy process as a real HTTP client (up to 100 req/s, many
client IPs via `X-Forwarded-For`). Start it from the sidebar (2, 5, 10, 20 or 40 req/s). Its shape (exact rate,
popular or all-different items, heavy clients, random or even arrivals, client IPs, connections, writes) is set in
the folded *Custom traffic* panel on the Resilience & Capacity page (the scenarios set it for you). The routing sequence and the request dots on the
diagrams are replayed from the routes the generator reports, so they show real routed requests. **Reset
simulation** returns everything to the initial state: traffic, experiments, capacity work, replacements, disaster
state, faults, breakers, data tier and metrics.

**Cleanup in this version** (nothing was removed without a replacement):
- The browser-side and server-side traffic generators were merged into the one above.
- The two fault panels on Fault Tolerance were merged: the fault type is chosen once and used by every Fail
  button. The failover sequence now also follows non-HTTP faults (it previously listened only for the
  `FAILURE_INJECTED` event, which the fault module no longer emits, so it stayed empty).
- The Capacity page's server table (a copy of Monitoring and of the diagram) and its duplicate Fail all / Recover
  all buttons were removed; those stay on Fault Tolerance.
- The DR comparison moved from Experiments to the Data Recovery page.

---

## 8. Load-Aware Capacity (recovery and scaling as separate stages)

### Stage 1: restore service (auto_recovery.py)
When the proxy has **0 usable backends**, it starts **one** replacement server, which is the minimum viable
capacity. The replacement must pass 2 consecutive health checks before it is registered. Stage 1 never starts
more than one server at a time.

### Stage 2: match capacity to measured demand (capacity.py)
After service is restored, the capacity manager **observes** demand for a full window
(`CAPACITY_OBSERVATION_WINDOW` = 10 s, at least `CAPACITY_MIN_SAMPLES` = 20 requests) before deciding anything.

```
capacity per server  = workers / service time            (measured from backend reports; config fallback)
                       or CAPACITY_PER_SERVER_RPS if set > 0  (explicitly configured)
safe capacity        = capacity per server × TARGET_UTILIZATION (0.7)
required             = ceil(demand_rps / safe capacity)
desired              = clamp(required, MIN_SERVERS = 1, MAX_SERVERS = 6)
available            = routable + RECOVERING originals + replacements being started
```

The capacity per server is labelled with its source everywhere it is shown:
- **measured**: from at least 5 backend reports in the window. The backend's work is itself a simulated
  service time, and the label says so.
- **configured**: `CAPACITY_PER_SERVER_RPS` is set.
- **simulated**: too few measurements, so the configured service model is used.

**Example with a configured capacity** (as in the task brief): with `CAPACITY_PER_SERVER_RPS = 50`, a target of
70 % and 120 req/s, desired = ceil(120 / (50 × 0.7)) = ceil(3.43) = **4**. A unit test checks this case.

**Worked example** (default settings, numbers as measured in the verification run):
- One worker with a measured service time of about 50 ms gives about 20 req/s per server.
- The safe capacity is 20 × 0.7 = 14 req/s per server.
- At 42 req/s: required = ceil(42 / 14) = **3** servers.
- At 30 req/s: ceil(30 / 14) = ceil(2.14) = **3**.
- At 8 req/s: ceil(8 / 14) = **1**.

So when demand drops to 8 req/s, every replacement can be retired while the three originals stay.

**Scale-out** requires all of the following:
- `desired > available` in both the 10 s window and the most recent 3 s, so a short spike does not trigger it;
- the condition sustained for `CAPACITY_SCALE_OUT_SUSTAIN` (5 s);
- no scale-out cooldown running (5 s).

**Scale-out order:**
1. Cancel a drain in progress, if any.
2. Reactivate an original server on **standby**. It is already running and health-checked, so this is instant,
   and the highest-weight server goes first.
3. Only then provision a new replacement.

Replacements are added **one at a time**. Each must pass health checks before registration. Just before
registering, the need is re-checked: if an original recovered meanwhile, the new server is stopped instead.

**Scale-in** requires all of the following:
- candidates are **replacements first** (newest first). Once none is left, **original servers** are candidates,
  lowest weight first (Server 3, then Server 2). At least `CAPACITY_MIN_SERVERS` (1) servers stay routable;
- utilisation with one server fewer must stay below `CAPACITY_SCALE_IN_UTILIZATION` (0.5), which is lower than
  the 0.7 target. The gap between the two values is the hysteresis band;
- the condition sustained for 8 s, and no scale-in cooldown running (4 s).

These timings are set for a practical classroom demonstration. They are in `config.py` and can be changed at
runtime on the Capacity page's settings panel.

The chosen server is then **drained**: it gets no new requests, and its in-flight requests finish (at most 10 s).
Then:
- a **replacement** is stopped and unregistered (`BACKEND_RETIRED`);
- an **original** is put on **standby** (`BACKEND_STANDBY`). Its process keeps running, it stays registered, and
  it is still health-checked, but it receives no requests until load rises.

Originals are therefore never terminated. Set `CAPACITY_ORIGINAL_STANDBY = 0` (or use the settings panel) to scale
in replacements only.

Example: at 5 req/s, desired = ceil(5 / 14) = 1, so Server 3 and then Server 2 go to standby and Server 1 serves
alone. At 36 req/s, desired = 3, so Server 2 and Server 3 come back first, and no new process is started.

**Safety:**
- Stage 1 and stage 2 share one port allocator and a provisioning gate, so they never provision in parallel and
  never take the same port. The allocator checks both the data port and the control port.
- A failed start is never registered. After 3 failures in a row the manager backs off for 10 s.
- A replacement that stays UNHEALTHY for 15 s is retired.
- Recovering originals count as available capacity.
- **Standby never causes an outage.** If no routable server is left, the registry immediately serves from a
  healthy standby server, and the capacity manager reactivates it (`BACKEND_ACTIVATED`). Standby servers stay
  HEALTHY, so stage-1 recovery does not start a replacement while one exists.
- A total failure (Data Recovery) and the experiments first return every standby server to routing.
- `AT_MAXIMUM` is reported when demand needs more than `MAX_SERVERS`.
- Reset and shutdown cancel provisioning in flight and stop every replacement process.

The API (`/api/capacity`, `/api/status` → `capacity`) and the Capacity page show:
demand (10 s and 3 s), capacity per server and its source, desired vs. routable, the formula with numbers,
phase, decision, reason, sustain/cooldown timers and the decision history.

### Allocation in the topology diagram (topology.py)
The diagram is drawn from `/api/status` → `topology` (also `GET /api/topology`). That view is built from three
real sources, and nothing is added in the browser:

| Node | Source | Drawn as |
|---|---|---|
| Registered server | the proxy's server registry | solid box with its role tag (ORIGINAL, RECOVERY REPLACEMENT, SCALE-OUT REPLACEMENT), state, port and measured load share; link to the proxy; caption **✓ In routing pool** only when routable |
| Candidate | a replacement process that stage 1 or stage 2 is starting right now | dashed amber box: PROVISIONING → STARTING → HEALTH CHECK → REGISTERING, with port and PID; dashed link labelled *not routed*; caption *Not registered · no traffic* |
| Draining | a registered server chosen for scale-in | DRAINING, dotted link, *no new requests* |
| Standby | an original server scaled in at low demand | blue dashed box STANDBY, dashed link labelled *standby*, *Standby · returns when load rises*; not counted in *In routing pool* |
| Retired | a replacement stopped in the last `TOPOLOGY_RETIRED_DISPLAY_S` (10 s) | grey dotted box, **no link**, *Removed from routing*; then it disappears |

- A desired count larger than the registered count is shown only as a number ("target only — 2 more needed"),
  never as nodes.
- Failed and recovering servers show why they are not routable.
- The proxy box shows *In routing pool x/y* and the current scaling action.
- The layout widens for more than 3 nodes. Above 5 nodes each node keeps a minimum on-screen width and the panel
  scrolls sideways on small screens.
- Requests sent by the server-side traffic generator are shown as dots, sampled from each server's real request
  counter.

**Timeline events** are emitted on transitions only, never once per evaluation. Each carries
`details = {backend_id, previous_healthy, desired, observed_rps, reason}`:

`CAPACITY_EVALUATION_STARTED` → `CAPACITY_TARGET_UPDATED` (once the new count has held for 3 evaluations) →
`SCALE_OUT_STARTED` → `REPLACEMENT_PROVISIONED` → `REPLACEMENT_HEALTHY` → `BACKEND_REGISTERED` …
`SCALE_IN_STARTED` → `BACKEND_DRAINING` → `BACKEND_RETIRED` (replacement) or `BACKEND_STANDBY` (original) →
`CAPACITY_STABILIZED`. `BACKEND_ACTIVATED` is emitted when a standby original returns to routing.

---

## 9. Cost-Aware Resilience Controller (research contribution)

### Problem
Section 8 reacts to overload and failures the usual way: it adds servers. Servers cost money for as long as
they run, and a new server needs seconds to start and pass health checks, so the clients still see slow
responses in between. Production proxies have cheaper tools: response caching, per-client rate limiting and
*brownout* (serving a lighter response). Each one is normally switched on by hand, one at a time, with no
record of what it actually achieved.

**Question:** can a reverse proxy pick the *cheapest* mitigation that keeps the latency SLO, using only
what it measures itself, and use extra servers only as the last resort?

### The mitigation ladder (`load_balancer/resilience.py`)
Every second the controller measures, over the last 5 s: client p95 latency, error rate, backend
utilisation (backend req/s ÷ (routable servers × capacity per server)), and the share of requests
answered from the cache, rejected with 429, or degraded.

| Rung | Mitigation | What the client gets | Cost | Module |
|---|---|---|---|---|
| 0 | none | full response | — | |
| 1 | **cache**: popular keyed GETs answered by the proxy for 2 s (`X-Cache: HIT`), plus stale-if-error (RFC 5861) instead of 503 | data up to 2 s old | freshness | `cache.py` |
| 2 | **rate limit heavy hitters**: token bucket per client, limit = 2 × fair share; above it HTTP 429 + `Retry-After` | only the heaviest clients are slowed | fairness to heavy clients | `ratelimit.py` |
| 3 | **brownout**: header `X-Degrade: 1`, the backend skips optional work (0.4 × service time) | a lighter response, marked `degraded` | quality | `base_server.py` |
| 4 | **scale out**: the capacity manager of section 8 may add servers | full response | money (server-seconds) | `capacity.py` |

Rules:
- **Escalate** when the SLO (p95 ≤ 200 ms, errors ≤ 1 %) is broken or utilisation is above 85 %, and at
  least 5 s (*dwell*) have passed since the last change.
- **Effect-aware escalation** (the main novelty): before switching a rung on, the controller *predicts* its
  relief from the live traffic, also while the rung is off:
  - cache: the share of requests whose key was already seen within the TTL (would-be hits);
  - rate limit: the share of traffic above the per-client limit;
  - brownout: 1 − 0.4 of the remaining backend work.

  Rungs with less than 10 % predicted relief are skipped, and the reason is logged
  ("skipped cache: predicted relief 0 %"). So a workload with no repeated items does not waste 5 s on a
  useless cache.
- **Counterfactual relaxation**: a rung is switched off only if utilisation *without* it would be below 50 %
  (for example, backend req/s + req/s currently answered by the cache). Removing a rung because the
  backends look idle — when they look idle *because of* that rung — would make it oscillate; this test
  prevents that.
- **Servers last**: the capacity manager asks `scale_out_gate()` before starting a replacement. Under the
  ladder this is allowed only at rung 4. Stage-1 recovery after a total outage is never blocked.
- Every decision is logged with its reason, the metrics before, and the metrics measured one dwell later
  ("after"), so the effect of each step is visible on the *Resilience & Capacity* page.
- **Only the chosen method is switched on.** A skipped rung (no predicted relief) is never activated, and
  relaxing jumps over it: escalating straight to brownout and relaxing later produce exactly two log lines.
- **Allowed mitigations.** Not every service tolerates every method: checkout traffic must not be served stale,
  rejected or degraded. Each method has an *allowed for this service* switch (`POST /api/resilience/allowed
  {"rungs": [...]}`); the controller skips methods that are not allowed and says so ("skipped cache (not
  allowed)"). Adding servers is always allowed.

### One-click scenarios (Resilience & Capacity → *See each method in action*)
Each scenario resets the controller, retires leftover replacement servers (so it starts from exactly the 3
originals), and plays 10 s at 8 req/s → 45 s at 55 req/s (about 92 % of three servers) → 25 s at 8 req/s. Only
the traffic shape (and, for checkout, the allowed methods) differs; the controller decides on its own. Measured
on 2026-10-28:

| Scenario | Traffic | Method the controller chose | Extra server-s | SLO broken |
|---|---|---|---|---|
| Many users want the same few items | 200 items, Zipf | cache (predicted relief 68 %) | 0 | 0 s |
| Two clients flood the service | all different, 2 clients send 60 % | rate limit (52 %), cache skipped; 1,100 × 429 | 0 | 0 s |
| Many users, every request different | all different, 32 equal clients | brownout (60 %), cache and rate limit skipped | 0 | 0 s |
| Checkout traffic: no shortcuts allowed | as above, only servers allowed | add servers | 46 | 0 s |
| A server crashes during a spike | popular items + heavy clients, 40 req/s, Server 2 refused | cache (66 %) | 0 | 0 s |

In each case the method was switched off again in the calm phase (counterfactual check). The scenario result
also shows who received the 429s (the two flooding clients).

### Policies compared
| Policy | Behaviour |
|---|---|
| `static` | always 4 servers (over-provisioned), no mitigations |
| `scale_only` | the section 8 behaviour: add servers when demand needs them |
| `cache_always` | baseline: the proxy cache (+ stale-if-error) is always on; servers are added like `scale_only` |
| `ladder` (default) | the cost-aware ladder above |

Set it on the dashboard (Resilience & Capacity page), with `POST /api/resilience/policy {"policy": ...}`, or at
start-up with `LB_RESILIENCE_POLICY`.

**Cost model.** Extra server-seconds (servers above the 3 originals × time) cost 1 unit each, and each second
with the SLO broken costs 2 units. The weights are settings (`RESILIENCE_COST_WEIGHTS`); the raw numbers are
reported too, so a reader can apply other weights.

### Measured results (Experiments page → *Resilience policies*, saved in `reports/`)
Same trace for every policy: 15 s at 12 req/s, 45 s at 40 req/s, 20 s at 12 req/s, Poisson arrivals. Server 2
refuses connections from 20 s to 60 s, during the spike. 3 servers × about 20 req/s each.

Rerun on 2026-10-28 after the brownout accounting fix (see below).

Workload A: 200 items with Zipf popularity (cacheable); two heavy clients send 40 % of all requests.

| Policy | SLO broken | p95 | Extra server-s | Weighted cost | Notes |
|---|---|---|---|---|---|
| static | 0 s | 76 ms | 124 | 124 | |
| scale_only | 6 s | 148 ms | 45 | 57 | |
| **ladder** | **0 s** | 96 ms | **0** | **0** | one step (cache, predicted relief 70 %); 40 % answered from cache; relaxed after the spike |

Workload B: every request asks for a different item (nothing cacheable); same two heavy clients.

| Policy | SLO broken | p95 | Extra server-s | Weighted cost | Notes |
|---|---|---|---|---|---|
| static | 0 s | 84 ms | 125 | 125 | |
| scale_only | 10 s | 299 ms | 44 | 64 | |
| **ladder** | 5 s | 120 ms | **0** | **10** | skipped the cache (0 % relief), then rate limit and brownout; 22.8 % got 429 (501 of the 507 rejections hit the two heavy clients), 36.5 % degraded |

**Brownout accounting fix.** A degraded response does 40 % of the work, so it reports a shorter service time.
Those responses used to enter the measured capacity per server, which then looked about twice as large while
brownout was on, and both the controller and the autoscaler underestimated their load. Now the proxy records
a degraded response's service time as its full-work equivalent (÷ 0.4), and the controller computes utilisation
as *work*: degraded requests count 0.4. Its counterfactual "without brownout" is then simply the full-work
utilisation. Before the fix, workload B gave the ladder 4.0 s of SLO violation (15.7 % 429, 50 % degraded); the
numbers above are from the corrected code. Without effect-aware escalation (rungs tried strictly in order) the
ladder broke the SLO for 10.3 s in workload B (measured before the fix).

What the numbers show:
- Static over-provisioning meets the SLO, but pays for a fourth server all the time.
- Reactive scaling pays less, but breaks the SLO while new servers start.
- The ladder used **no extra server** in either workload. In workload A it kept the SLO completely. In
  workload B it halved the SLO violation of scale_only (5 s vs 10 s) by trading quality: 429s, almost all for
  the heavy clients, and lighter responses. Whether that is
  acceptable is a policy choice, which is why the costs are reported separately.

### Is 100 % utilisation good?
No. Measured with `scale_only`, 3 servers, Poisson arrivals, 12 s per level (Experiments → *Utilisation vs
latency*, run on 2026-10-28):

| Utilisation | p50 | p95 | Mean queue wait | Success |
|---|---|---|---|---|
| 20 % | 56 ms | 72 ms | 1.6 ms | 100 % |
| 40 % | 56 ms | 84 ms | 3.9 ms | 100 % |
| 60 % | 57 ms | 95 ms | 7.8 ms | 100 % |
| 70 % | 60 ms | 138 ms | 18.6 ms | 100 % |
| 80 % | 67 ms | 127 ms | 20.6 ms | 100 % |
| 90 % | 210 ms | 436 ms | 174 ms | 100 % |
| 100 % | 345 ms | 581 ms | 295 ms | 100 % |
| 110 % | 573 ms | 1048 ms | 537 ms | 98.6 % |

The 70 % and 80 % rows are within run-to-run noise of each other (12 s of random arrivals per level).
Queueing delay grows sharply near 100 % (as queueing theory predicts for M/D/1), so planning targets 70 %.
With even (non-Poisson) arrivals, p95 at 75 % stayed at 53 ms: burstiness, not the average, causes the
queue. That is why the ladder looks at the measured p95 and not only at utilisation.

### Limitations and related work
- One machine; brownout is a simulated reduction of service time; the predicted relief of brownout uses
  the configured factor, not a measurement.
- The cache is only correct for read requests whose answer may be up to 2 s old; writes are never cached
  and stale data is never served during data recovery.
- Results come from short runs and vary between runs; repeat them for statistics before publishing.
- Related: brownout (Klein et al., ICSE 2014), overload control in WeChat's DAGOR (Zhou et al., SoCC 2018),
  stale-if-error (RFC 5861), gray failure (Huang et al., HotOS 2017). The difference here is that one
  controller orders these mitigations by cost, predicts each one's relief before using it, relaxes by
  counterfactual, and gates the autoscaler behind them.

---

## 10. Networking Features

### Fault types (inject from Fault Tolerance page or `POST /api/faults/inject`)

| Type | Layer | Real or simulated | What happens | Detected by |
|---|---|---|---|---|
| `refused` | Transport (TCP) | **Real** | the backend closes its data-port listener → TCP RST, "connection refused" | first forwarded request (ms) or next probe |
| `timeout` | Transport/app | **Real** | the backend accepts the connection but answers only after 5 s | probe timeout (1 s) or read timeout (3 s) |
| `slow` | Application | **Real** | every response is delayed (`delay_ms`, default 400) but correct | response slower than 250 ms |
| `http_5xx` | Application (HTTP) | **Real** | the backend is reachable but answers HTTP 503 | first forwarded request or probe |
| `packet_loss` | Network | **Simulated** in the proxy | with probability `loss` an exchange gets no reply (seeded RNG) | forwarded request / probe |
| `partition` | Network | **Simulated** in the proxy | the backend is running and directly reachable, but the proxy's path to it is cut | forwarded request / probe |

Clearing a fault (`POST /api/faults/clear`) does not put the server back immediately: it must pass the normal
recovery checks. The fault timeline records injection time, detection time and method, clear time, the time
until the server was back in routing, and the outcome.

### Circuit breaker (per backend, passive health)
- **CLOSED → OPEN** after 3 consecutive failed requests. While OPEN, requests skip the server
  (`circuit_rejected`) without waiting for the next probe.
- **OPEN → HALF_OPEN** after 5 s. One trial request at a time is allowed.
- **HALF_OPEN → CLOSED** after 2 successful trials; any failed trial returns it to OPEN.
- A backend's explicit overload rejection (503 + `X-Backend-Overloaded`) does **not** count as a failure.
  Overload is not a health problem.
- When the health monitor re-admits a server, its breaker is reset.

### Retry control (no retry storms)
- GET/HEAD/PUT/DELETE/OPTIONS are retried on another server.
- **POST is retried only when the request never reached a backend** (refused, connect timeout, partition,
  overload rejection). A POST that timed out after being sent is not retried, because it may already have been
  executed (counter `not_retried_non_idempotent`).
- Exponential backoff with jitter (20 → 200 ms).
- **Retry budget:** a token bucket. Each request adds 0.2 tokens, each retry costs 1, at most 20 tokens. Retries
  can therefore add at most about 20 % extra load; denied retries are counted.

### Telemetry and sources
`/api/telemetry` and the Monitoring page report the following, each with its source label:
- incoming rate, throughput, success, error and timeout rates;
- end-to-end latency p50/p95/p99 and health-probe RTT;
- backend-reported service and queue time;
- the counters for every failure class, retry outcome, shed request and simulated drop.

- **proxy-observed:** measured by the proxy.
- **backend-reported:** measured inside the backend and returned in its response.
- **simulated:** produced by the network simulator.

No CPU utilisation or OS-level packet loss is reported, because none is measured.

### Request tracer (Load Balancing page)
Like `curl -v` for the proxy. The browser sends one real request with `X-Trace: 1` (and optionally an
`X-Forwarded-For` client address). The proxy records each hop with its time since arrival: the TCP source
address and the client address it uses, the rate-limit check, the cache check (HIT/MISS/off), the healthy pool
(port, open connections, weight, circuit state), which server the strategy picked, every forward attempt with
its HTTP status or failure class and time, retries and backoff, and the backend's queue and service time. The
page then shows the real response status line and headers (`Content-Type`, `X-Cache`, `Age`, `Retry-After`, …)
and the round trip seen by the browser vs. inside the proxy. Sending twice from the same address under IP Hash
shows stickiness; tracing during a fault shows the failed attempt and the retry.

### What clients received (HTTP outcomes per second)
Monitoring page and the scenario panel: a stacked chart of the last 60 s — 200 from a backend, 200 lighter
(brownout), 200 from the proxy cache (fresh or stale-if-error), 429 Too Many Requests, and 5xx / failed. Every
mitigation is visible at the HTTP level.

### Failover comparison experiment
The same seeded workload runs against each of the six strategies. A chosen fault is injected on a chosen server
at 30 % of the run and cleared at 70 %. The experiment reports:
- throughput, success rate, latency percentiles and retries;
- per-server distribution;
- requests during the fault, and how many the faulty server served.

Each run is preceded by a clean baseline (faults cleared, replacements retired, breakers and budget reset).

---

## 11. Total Backend Failure and Data Recovery

**A replacement server restores computing capacity, not lost data.** Backends serve from a backend-local SQLite
file (`data/backend_local/primary.sqlite`), and a total failure deletes it. Durable copies live separately in
`data/durable/`.

**Write path:**
1. Append to the write-ahead log (`wal.jsonl`), with a CRC per entry and fsync.
2. Apply to the primary.
3. Apply to the synchronous replica.
4. Acknowledge (HTTP 201).

Every record has a SHA-256 checksum. Every backup (`snapshots/snap_NNNN.sqlite`, taken every 30 s and on demand)
is listed in `manifest.json` with its SHA-256 and LSN.

**Total-failure workflow** (Data Recovery page or `POST /api/disaster/start`):
1. Replacement servers left from earlier scaling are lost too: they are stopped and removed first, so recovery
   starts clean. Every original backend then fails (a real `refused` fault or HTTP 503), and backend-local data
   is deleted. State becomes **OUTAGE**.
2. The proxy **blocks routing** with an explicit HTTP 503 ("data recovery pending"), so no request can be served
   from an empty database.
3. Stage-1 recovery provides a backend (or an original recovers). This is measured as *backend_ready*.
4. **RESTORING_DATA**: restore with the chosen strategy, then verify integrity (checksums, LSN continuity).
5. If the restore succeeded: **RESTORED**. Routing is reopened, and RTO is measured at the first client request
   served. If no traffic is running, the Data Recovery page sends that one request itself.
6. If nothing valid exists: **DATA_LOSS_ACK_REQUIRED**. Routing stays closed until an operator acknowledges the
   loss. The dashboard never shows a false success.

**Strategies:**

| Strategy | Restores from | Data loss | Caveats |
|---|---|---|---|
| `backup` | newest backup whose SHA-256 matches | everything after that backup (RPO ≈ backup interval) | a corrupted newest backup falls back to an older valid one |
| `replica` | synchronous replica | no acknowledged write | **copies accidental deletes and corruption too**; corrupted records are excluded and reported |
| `wal` | newest valid backup + log replay | none if the log is intact | replay stops at the first bad CRC or LSN gap; later writes are reported as lost; deletes are replayed too |
| `none` | nothing | everything | requires operator acknowledgement |

**RPO** is measured against a ledger of acknowledged writes that is used *only for measurement, never for
restore*. The report lists:
- missing, stale and resurrected record IDs;
- lost records and writes;
- RPO in seconds;
- the integrity result.

**RTO** is the wall-clock time from the disaster start to the first served client request. Zero data loss is
claimed only when no acknowledged record is missing or stale.

**Write timeline** (`GET /api/data/map` → `timeline`, shown on the Data Recovery page). Every confirmed change
of the current log in order (`+3` created, `✎3` changed, `✕3` deleted), with a *backup* line at the newest
**valid** backup, and one bar per strategy: green up to how far that strategy's restore reaches, red after it.
The reach is computed with exactly the restore's rules (newest valid backup; log replay from the backup,
stopping at the first damaged or missing entry), and a unit test checks that it equals the real restore for
every strategy, with and without corruption. After a failure, the result repeats the timeline for the chosen
strategy with a *failure* line, so "what came back" is the same picture.

**Record map** (folded, "Record by record"). One square per record and copy: green (current version), amber
(older version), red (missing), purple (deleted, but still in this copy). Each strategy card says exactly what it
would bring back right now, e.g. "8 of 14 come back exactly · loses #11–#15 · old version of #2 · deleted #1
would come back"; a unit test checks this equals the real restore.

**Write path (networking view).** Every write is timed: change log (append + fsync), own disk, live copy; the
page shows client → proxy → backend → log → disk → live copy → `201 Created` with the measured milliseconds.
Here the live copy is local; across a network it would add one round trip to every write, which is why many
systems replicate asynchronously and accept a small RPO.

**Page flow:**
1. Make changes — or **▶ Play the story**: it starts from empty data, writes 10 records, takes a backup, writes 5
   more, changes one and deletes one, narrating each step.
2. Pick what was kept (the cards show the exact preview), then fail every server; a progress list shows each
   stage with its time.
3. What came back: the verdict, the timeline with the failure line, RPO, RTO and checksums; repair the original
   servers or start over.

*Measure all four for real* (folded) runs one real failure per strategy on the same data (the DR comparison).

**Limitations:**
- All "durable" storage is on the same disk (a simulation of separate storage).
- The replica is in the same process and is synchronous, with no network replication or lag.
- There is no point-in-time restore and no protection from logical errors (an accidental delete is durable).
- RTO numbers are from this machine, not production figures.

---

## 12. API Reference

| Method & path | Purpose |
|---|---|
| `GET /api/request` / `POST /api/request` | Client request through the proxy (POST writes a record) |
| `GET /api/status` | Full system state (servers, circuits, faults, capacity, data, disaster, telemetry, traffic) |
| `GET /api/telemetry` | Networking telemetry with source labels |
| `POST /api/fail/<id>` `{type}` · `POST /api/recover/<id>` | Original fail/recover buttons (default type `http_5xx`) |
| `GET /api/faults` · `POST /api/faults/inject` `{server_id, type, delay_ms?, loss?}` · `POST /api/faults/clear` `{server_id?}` (no id = clear all) | Fault injection and timeline |
| `POST /api/fail-all` `{type}` · `POST /api/recover-all` | All originals |
| `POST /api/circuit/reset` | Reset all circuit breakers |
| `POST /api/set-algorithm` `{algorithm}` · `POST /api/reset-metrics` | Routing strategy, metrics |
| `GET /api/capacity` · `POST /api/capacity/pause` · `/resume` | Capacity manager |
| `GET /api/topology` | Diagram nodes: registered servers, replacements being provisioned, recently retired replacements |
| `GET /api/traffic` · `POST /api/traffic/start` `{rps, concurrency, clients, write_ratio, keys?, heavy?, heavy_clients?, poisson?}` · `/rate` · `/stop` | Traffic generator |
| `GET/POST /api/settings` | Validated runtime settings |
| `GET/POST /api/records` · `PUT/DELETE /api/records/<id>` | Data records |
| `GET /api/data` · `GET /api/data/map?limit=40` · `POST /api/data/snapshot` · `/seed` · `/corrupt` `{target}` · `/reset` | Data tier; record map with the exact per-strategy preview |
| `GET /api/disaster` · `POST /api/disaster/start` `{strategy, fault}` · `POST /api/disaster/acknowledge` | Total failure + data recovery |
| `GET /api/resilience` · `POST /api/resilience/policy` `{policy, static_servers?}` | Resilience controller state, decisions, costs; policy switch |
| `GET /api/scenarios` · `POST /api/scenarios/start` `{name}` (`cache`, `rate_limit`, `degrade`, `scale_out`, `failure`) · `POST /api/scenarios/stop` | One-click demonstrations |
| `POST /api/resilience/allowed` `{rungs}` | Methods this service allows (servers always allowed) |
| `GET /api/request` with header `X-Trace: 1` | Same request, plus a `trace` list of every hop and decision |
| `GET /api/experiments` · `POST /api/experiments/failover` · `/dr` · `/policies` · `/utilisation` · `/cancel` | Experiments |
| `POST /api/reset-simulation` | Return everything to the initial state |

Backend control plane (used by the proxy): `GET /control/status`, `POST /control/fault {type, delay_ms}`,
`POST /control/clear` on port `600x`.

---

## 13. How Metrics Are Calculated

| Metric | Calculation |
|---|---|
| Failure detection time | detected_at − fault injected_at |
| Failover (redirection) time | first request served by another server − detection |
| Recovery detection / re-admission | first passing probe − recovery; re-admission − first passing probe |
| Incoming rate / throughput | proxy arrivals (resp. completions) in the last 10 s ÷ 10 |
| End-to-end latency p50/p95/p99 | proxy-measured time per client request, last 10 s |
| Backend service / queue time | reported by the backend in each response |
| Capacity per server | workers ÷ mean backend-reported service time (needs ≥ 20 samples; else config) |
| RPO (records / seconds) | acknowledged records missing or stale after restore / time span of lost acknowledged writes |
| RTO | disaster start → first client request served after recovery |

---

## 14. Testing

### Commands (Windows)
```powershell
python -m unittest discover tests        # 179 unit tests (no servers needed, about 15 s)
python verify_live_system.py             # live integration scripts: each starts and stops its own processes
python verify_auto_recovery.py
python verify_routing_strategies.py
python verify_network_faults.py
python verify_capacity_recovery.py       # about 4 minutes
python verify_experiments.py             # about 4 minutes
python verify_load_scaling.py            # load-based allocation scenario, about 3 minutes
python verify_resilience.py              # three resilience policies on the same trace, about 4 minutes
```
The integration scripts need ports 5000–5010 and 6001–6010 free (stop `run_all.py` first). The unit tests use
deterministic fake clocks and fake processes; only two of them start a real short-lived process to check cleanup.

### Coverage of the required cases

| # | Case | Tests |
|---|---|---|
| 1 | One backend fails, others serve | `test_resilience.test_one_backend_fails_others_serve`, `test_load_balancer_routing.test_failover_when_server_fails_mid_request` |
| 2 | Multiple backends fail | `test_resilience.test_multiple_backends_fail` |
| 3 | Every original fails | `test_auto_recovery.test_replacement_provisioned_and_registered_when_all_failed`, `test_capacity.test_waits_for_stage1_then_observes_before_deciding` |
| 4 | Recovery with consecutive checks | `test_registry_and_monitor.test_recovery_lifecycle_and_readmission`, `test_recovering_interrupted_by_failure` |
| 5 | Backend fails while replacement starts | `test_capacity.test_backend_fails_while_replacement_starting` |
| 6 | Scale-out | `test_capacity.test_sustained_demand_scales_out_with_readiness_checks` |
| 7 | Safe scale-in | `test_capacity.test_low_demand_drains_then_removes_replacement`, `test_drain_timeout_forces_removal`, `test_originals_never_removed` |
| 8 | Stable around a threshold | `test_capacity.test_stable_around_threshold`, `test_transient_spike_does_not_scale` |
| 9 | Recovering original reduces need | `test_capacity.test_recovering_original_reduces_need`, `test_provisioning_cancelled_when_original_recovers` |
| 10 | Maximum capacity | `test_capacity.test_maximum_capacity_reached` |
| 11 | Startup failure / port unavailable | `test_capacity.test_startup_failure_not_registered_and_backoff`, `test_no_free_port`, `test_replacement_process_dies_during_startup`, `test_auto_recovery.test_no_free_port` |
| 12 | Circuit breaker | `test_resilience.TestCircuitBreaker` (4 tests), `test_all_circuits_open_fail_fast` |
| 13 | Partition / packet loss | `test_resilience.TestNetworkSimulator`, `test_partition_bypassed_and_counted`, `test_packet_loss_retried_for_get` |
| 14 | All backends fail, durable data available | `test_data_recovery.test_total_outage_with_durable_data` |
| 15 | Snapshot + valid log | `test_data_recovery.test_snapshot_plus_wal_replay_restores_everything` |
| 16 | No backup | `test_data_recovery.test_no_backup_means_permanent_loss`, `test_no_durable_copy_requires_acknowledgement` |
| 17 | Corrupted backup / invalid log | `test_corrupted_backup_detected_and_older_used`, `test_invalid_wal_entry_stops_replay`, `test_corrupted_replica_record_excluded` |
| 18 | Reset during recovery or scaling | `test_capacity.test_reset_during_provisioning_cancels`, `test_data_recovery.test_reset_during_recovery`, `test_simulation_reset.test_stale_probe_from_before_reset_is_ignored` |
| 19 | Routing strategies still work | `test_routing_strategies` (18 tests, unchanged), `verify_routing_strategies.py` |
| 20 | Shutdown cleans up | `test_capacity.test_shutdown_stops_thread_and_processes`, `test_real_replacement_process_stopped_on_shutdown`, `verify_capacity_recovery.py` step 6 |
| 21 | Cost-aware resilience controller | `test_resilience_controller` (13 tests: cache TTL and stale-if-error, token bucket, 429 without a backend, one rung per dwell, skipping rungs without relief, counterfactual relaxation, scale-out gate, policies, costs), `verify_resilience.py` |
| 22 | Scenarios, request tracer, write timeline | `test_network_view` (tracer hops for routed / cached / 429 requests, HTTP outcome series, scenario phases, allowed methods, fault injection, stop), `test_record_map` (timeline reach = real restore, preview = real restore, write-path timing), `test_resilience_controller.test_disallowed_rungs_are_skipped` |

### Actual results
Final run on 2026-10-20 (Linux, Python 3, one machine), including the load-based allocation update. The numbers vary between runs and machines.

| Suite | Result |
|---|---|
| Unit tests (`python -m unittest discover tests`) | **150 tests, OK**. The 50 original tests are unchanged and all pass; 100 are new, including 20 in `test_topology_allocation.py` and 9 in `test_original_standby.py`. |
| `verify_live_system.py` | **passed**. Its 1 failed request is the intended HTTP 503 of the all-servers-down scenario. |
| `verify_auto_recovery.py` | **passed** |
| `verify_routing_strategies.py` | **passed** (6 strategies, IP-hash stickiness, failover) |
| `verify_network_faults.py` | **passed** |
| `verify_capacity_recovery.py` | **passed** |
| `verify_experiments.py` | **passed** |
| Rerun on 2026-10-23 (original servers to standby) | 150 unit tests OK; all 7 live scripts passed. `verify_load_scaling.py`: at 10 req/s Server 3 went to standby and Server 2 was kept (one server would be at 50 %); at 5 req/s Server 2 went to standby too, and Server 1 served alone; at 36 req/s both came back first with no new process; 481 API samples consistent |
| Rerun on 2026-10-23 (after the scale-in, comparison-table and Data Recovery fixes) | 141 unit tests OK; all 7 live scripts passed (`verify_experiments.py` twice in a row); Data Recovery flow clicked through in a browser with leftover replacements present: backup strategy restored 10 of 20 (10 lost, as previewed); *No copy* required acceptance, then resumed; RTO measured both times |
| Rerun on 2026-10-28 (cost-aware resilience controller) | **164 unit tests OK; all 8 live scripts passed** (the 7 earlier ones run with `LB_RESILIENCE_POLICY=scale_only` so they test section 8 unchanged). `verify_resilience.py` (shorter trace, workload A): static 0 s SLO broken / 96 extra server-s; scale_only 12 s / 36; ladder 4 s / 0 (cache answered 41 %, one escalate and one relax). The ladder's SLO time differs from the 0 s in section 9 because runs vary. |
| Rerun on 2026-10-28 (Data Recovery rework, one traffic generator, merged fault panels, brownout accounting fix) | **170 unit tests OK; all 8 live scripts passed.** `verify_resilience.py`: static 0 s / 96 extra server-s; scale_only 13 s / 36; ladder 5 s / 0 (cache 29 %, 429 for 19 %). Browser: Data Recovery clicked through (preview "9 of 14" = restored 9 of 14, same record IDs), traffic at 20 req/s from the sidebar with the routing sequence filled from the generator, *refused* fault from the merged panel with a complete failover sequence, demo mode completed; no JavaScript errors. |
| Rerun on 2026-10-28 (scenarios, allowed methods, Data Recovery timeline, request tracer, HTTP outcome chart) | **179 unit tests OK; all 8 live scripts passed.** All five scenarios chose the expected method (table in section 9). Browser: tracer showed every hop for a routed request; *Play the story* + *Backup only* failure: timeline 10/17 changes kept, result 8 of 14 records exactly as previewed; rate-limit scenario run right after a Data Recovery run started from 3 servers (leftover replacement retired), 1,100 × 429; light and dark themes; no JavaScript errors. |
| `verify_load_scaling.py` | **passed**: all 10 steps; 360 API samples, topology always equal to the registry, no candidate or retired node ever routable |
| Diagram vs. API (Playwright, Capacity page, same scenario) | 109 checks, 0 mismatches between the SVG nodes and `/api/status` topology, no unregistered node drawn as routable or healthy, no JavaScript errors |
| Dashboard (Playwright + Chromium, all 7 pages) | no JavaScript errors. A fault was injected, a total failure was recovered and an experiment was run from the UI. |

Load-based allocation (`verify_load_scaling.py`):
1. At 10 req/s with Server 2/3 failed, desired = 1. Nothing was provisioned for 12 s, and 122/122 client
   requests succeeded on Server 1 alone.
2. At 36 req/s: `ceil(36.0 / (19.9 × 0.7)) = 3`. Scale-out started 7.0 s after the increase. Server 4 and then
   Server 5 each went from candidate (not routable) to health checks to registered, never two at once.
   Server 2/3 served 0 requests while failed.
3. At 6 req/s both replacements were drained and retired, with 0 client errors. They were shown greyed without a
   link and then removed.
4. A reset during a later scale-out left only the 3 originals and no replacement process.

Events counted in that run:

| Event | Count |
|---|---|
| `CAPACITY_EVALUATION_STARTED` | 1 |
| `CAPACITY_TARGET_UPDATED` | 5 |
| `SCALE_OUT_STARTED` | 2 |
| `REPLACEMENT_PROVISIONED` | 2 |
| `REPLACEMENT_HEALTHY` | 2 |
| `BACKEND_REGISTERED` | 2 |
| `SCALE_IN_STARTED` | 2 |
| `BACKEND_DRAINING` | 2 |
| `BACKEND_RETIRED` | 2 |
| `CAPACITY_STABILIZED` | 2 |

In 8 s of steady state, 0 capacity events were emitted.

Network faults (Round Robin, 15 req/s, fault on Server 2, 6 s observed under each fault):

| Fault | Detected after | Detected by | Client success | Back in routing after clear |
|---|---|---|---|---|
| refused (real) | 61 ms | forwarded request | 90/90 | 2.4 s |
| timeout (real) | 2819 ms | health probe | 104/104 | 4.0 s |
| http_5xx (real) | 117 ms | forwarded request | 91/91 | 3.7 s |
| slow 400 ms (real) | 558 ms | slow response | 76/76 | 8.3 s |
| partition (simulated) | 623 ms | forwarded request | 92/92 | 4.6 s |
| packet loss 50 % (simulated) | 1109 ms | forwarded request | 91/91 | 3.7 s |

Capacity after a total outage, with 30 req/s from the traffic generator:
1. Before the outage: `ceil(28.9 / (19.9 × 0.7)) = 3`. The service time was measured at 50.16 ms from 287
   backend reports, and no replacement was needed.
2. All originals failed (refused). Stage 1 restored service with **one** replacement (Server 4) **2.7 s** after
   the failure.
3. No capacity decision was made during the 10 s observation window.
4. Server 5 and Server 6 were added one at a time, reaching desired = 3.
5. The originals recovered (UNHEALTHY → RECOVERING → HEALTHY). At 8 req/s, 3 replacements were drained and
   removed over 39 s with **0 client errors**. The originals were kept, and the replacement processes exited.
6. When the proxy was killed, the replacement process exited too, leaving no orphan.

Disaster-recovery comparison (20 records before the backup, 10 after it, 3 updates after it):

| Strategy | Restored | Lost | RPO | RTO | Notes |
|---|---|---|---|---|---|
| backup | 20/30 | 13 (10 later records + 3 later updates) | 0.60 s | 3895 ms | |
| replica | 30/30 | 0 | 0 s | 5413 ms | |
| wal | 30/30 | 0 | 0 s | 5368 ms | |
| none | 0/30 | 30 | 1.73 s | 5391 ms | operator acknowledgement required |

A corrupted (only) backup was detected by SHA-256 mismatch, and the backup strategy then correctly reported
that nothing valid was left.

Failover comparison (90 requests at 15 req/s, `refused` on Server 2 from 30 % to 70 % of the run): all six
strategies reached about 15.0 req/s, 100 % success, p50 about 55–56 ms and p95 57–59 ms. Server 2 served
**0** requests while its port was closed. Reports are saved in `reports/`.

**Pre-existing failures:** none. The baseline before the changes was 50/50 unit tests and 3/3 live scripts
passing.

---

## 15. Viva Demonstration Sequence (about 10 minutes)

1. **Overview** – explain Client → Reverse Proxy → Servers. Click **Start traffic**.
2. **Load Balancing** – Round Robin (1 2 3), then Weighted RR (3 : 2 : 1), then IP Hash (sticky clients).
3. **Fault Tolerance → Simulate failure** – choose *Connection refused*, then **Fail** on Server 2:
   - detected within milliseconds by a forwarded request; the circuit opens;
   - the failover sequence and the fault timeline show the detection time;
   - **Recover**: RECOVERING 1/2, then HEALTHY.

   Repeat with *Network partition* (simulated). Server 2 still answers directly
   (`http://127.0.0.1:5002/health`); only the proxy's path is cut.
4. **Monitoring** – charts at the top; networking telemetry: retries, connection refused, labels proxy-observed /
   simulated.
5. **Resilience & Capacity** — first click **Scale-out only** (the classic autoscaler). The load-based allocation
   diagram is below the controller.
   1. Sidebar: traffic at 10 req/s. Desired = 1, and the three originals are in the routing pool.
   2. On Fault Tolerance, Fail Server 2 and Server 3 with *Connection refused*. Only Server 1 stays routable, and
      nothing is provisioned because 10 req/s fits one server.
   3. Sidebar: 40 req/s. The panel shows desired 3, healthy 1, "2 more needed". After the window and 5 s of
      sustained overload, Server 4 appears as a dashed **SCALE-OUT** candidate (STARTING → HEALTH CHECK). It becomes
      a routable server only after it passes 2 health checks. Server 5 follows.
   4. Sidebar: 5 req/s. The replacements turn DRAINING, then RETIRED (grey, no link), then disappear. Server 2/3
      are never removed.
   5. Recover the faults: the originals rejoin. The Monitoring timeline lists every step with backend, counts,
      traffic and reason.

   The same run is automated by `python verify_load_scaling.py`.
6. **Data Recovery** – read the idea strip, then press **▶ Play the story** and let it narrate.
   1. On the timeline, *Backup only* turns red after the backup line; the live copy and the change log reach the end.
   2. Point at the write path: the client gets `201 Created` only after the log and the live copy have the change.
   3. Pick *Backup only* and fail every server. The progress list fills in; *What came back* shows the same
      timeline with a failure line: exactly the changes after the backup are gone.
   4. Repair the original servers. Optionally repeat with *Backup + change log* (nothing lost) and *No copy*
      (you must accept the loss).
7. **Resilience & Capacity → See each method in action** – run *Two clients flood the service*: during the spike
   the chart turns amber (429s), *Happening now* says "Limit heavy clients", and the result shows the 429s went
   to the two flooding clients — no server added. Then *Checkout traffic*: the same spike, but only servers are
   allowed, so Server 4 is started. (*Many users want the same few items* shows the cache; *every request
   different* shows brownout.)
   On **Load Balancing**, use *Trace one request* to show the hops, headers and timings of one request.
8. **Experiments** – run *Resilience policies* (about 4 min) or show a saved comparison and its limitations.
9. **Reset simulation**.

---

## 16. Computer Networks Viva Notes

- **Reverse proxy** – a server that accepts client connections and forwards each request to one of several
  backends. The client only knows the proxy's address (here `127.0.0.1:5000`), so backends can change without
  clients noticing.
- **Client–server communication / HTTP** – a request/response protocol over TCP. The proxy is an HTTP client
  towards the backends and an HTTP server towards the clients. Status codes carry application results: 200 ok,
  201 created, 503 service unavailable.
- **TCP connections** – a connection needs a three-way handshake.
  - If nothing listens on the port, the host answers with RST: **connection refused**, detected immediately.
  - If the SYN gets no answer, the client waits for its **connect timeout**.
  - After connecting, the **read timeout** bounds the wait for the response.
- **Timeouts** – the only way to detect silence. If they are too short, slow-but-working servers are marked
  failed; if too long, clients wait. Here: connect 0.5 s, read 3 s, health probe 1 s.
- **Health probes** – active, periodic `GET /health` requests (here every 2 s). Passive checks observe real
  traffic; the circuit breaker is a passive check.
- **Network partition** – both sides are running but cannot reach each other. From the proxy it looks like a
  timeout, not like a crashed server. Here it is simulated in the proxy, and the backend still answers directly.
- **Packet loss** – exchanges that get no reply. TCP retransmits lost segments, so loss mostly shows up as delay
  or timeouts. Here it is simulated per exchange.
- **Load balancing** – spreading requests (RR, WRR, least connections, least response time, IP hash) to use
  capacity and avoid overload.
- **Fault tolerance** – the service continues when components fail, through redundancy, detection, failover and
  recovery verification.
- **Application failure vs. network failure:**
  - An **application failure** means the network path works and the server answers, but with an error
    (HTTP 503) or slowly.
  - A **network failure** means the path or the transport is broken: refused, timeout, partition, loss.

  They are detected differently (status code vs. exception or timeout), and retrying is safe in different
  cases: a refused connection never reached the server, while a timeout after sending may already have been
  executed.
- **Idempotency** – repeating a GET is harmless; repeating a POST may create a duplicate, so POST is retried
  only when it provably never reached a backend.
- **Circuit breaker** – stops sending traffic to a server that keeps failing, then probes it carefully
  (HALF_OPEN).
- **Retry storm** – many clients retrying at once multiply load on an already weak system. Prevented with a
  retry budget and backoff with jitter.
- **Capacity / utilisation** – a server with service time S and one worker can serve 1/S req/s. Planning at 70 %
  leaves headroom because queueing delay grows sharply near 100 % utilisation.
- **Hysteresis** – different thresholds for adding (70 %) and removing (50 %) capacity, plus sustain times and
  cooldowns, to avoid oscillation.
- **Draining** – stop sending new requests to a server, let in-flight requests finish, then stop it.
- **RPO / RTO** – RPO is how much acknowledged data can be lost; RTO is how long until service is restored.
  Backups trade RPO for simplicity; synchronous replication gives RPO 0 for crashes but copies logical errors.

---

## 17. Limitations and Future Work

**Limitations (honest scope):**
- One machine and loopback only. There is no real network between the nodes, so latency is mostly simulated
  service time.
- Packet loss and partition are simulated in the proxy, not in the OS network stack (no `tc`/netem on Windows).
- The traffic generator and experiments share the CPU with the proxy and backends; results vary between runs
  and machines.
- Durable storage is on the same disk; the replica is synchronous and local; there is no point-in-time restore.
- The capacity per server comes from a simulated service model (1 worker, 50 ms), not from measured CPU.
- The proxy itself is a single point of failure: there is no proxy redundancy and no VRRP.
- Flask's development server is used; this is not a production deployment.

**Future work:**
- Real network emulation with Linux `tc netem` in containers.
- Two proxies with a floating IP (VRRP / keepalived).
- Asynchronous replication with measured lag, point-in-time recovery, and backups on a separate host.
- TLS termination at the proxy.
- Predictive scaling from demand trends.
- Learning the cost weights and the brownout factor from measurements; repeated runs with confidence intervals for the policy comparison.
- Persisting metrics in a time-series store.

---

## 18. Note on the Source Used

This version was built from the project folder `F:\CN_project`, which was the last delivered version. The file
named in the task, `CN_project_zip_2.zip`, was not available in the session. The original state was backed up
before the changes.
