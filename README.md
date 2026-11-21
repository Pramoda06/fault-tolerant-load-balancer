# Fault-Tolerant Load Balancer

**Load Balancing and Fault-Tolerant Network Monitoring Using a Reverse Proxy, with Cost-Aware Overload Mitigation**

A Layer 7 reverse-proxy load balancer written in Python/Flask. It spreads HTTP requests over backend servers,
detects failed servers with health checks and real request errors, moves traffic away without client errors,
brings recovered servers back safely, and handles overload by trying cheap proxy-side mitigations before
adding servers. A live web dashboard shows everything as it happens.

Built as a Computer Networks project at Chaitanya Bharathi Institute of Technology (CBIT), Hyderabad.

> All components run on one machine over loopback (`127.0.0.1`). Backend work is simulated (a 50 ms service
> time). This is a research and teaching system, not a production load balancer.

<!-- Screenshot placeholder: add a dashboard screenshot as screenshots/dashboard.png -->
![Dashboard](screenshots/dashboard.png)

---

## Architecture

```
                    Clients (browser, built-in traffic generator, curl)
                                        │  HTTP :5000
        ┌───────────────────────────────▼────────────────────────────────┐
        │                 Reverse proxy  (Flask, app.py)                  │
        │  rate limit → cache → admission control → scheduler → forward   │
        │                 (retries, circuit breaker per server)           │
        │                                                                 │
        │  health monitor · server registry · overload controller ·       │
        │  capacity manager · auto-recovery · metrics + web dashboard     │
        └──────────┬──────────────────────┬─────────────────────┬─────────┘
                   │ :5001                │ :5002               │ :5003   (+ replacements :5004…)
             ┌─────▼─────┐          ┌─────▼─────┐         ┌─────▼─────┐
             │ Server 1  │          │ Server 2  │         │ Server 3  │
             └───────────┘          └───────────┘         └───────────┘
              each backend has a control port (6001–6003) used to inject faults
```

- **Reverse proxy** (port 5000): the only address clients use; it chooses a server for every request.
- **3 backend servers** (ports 5001–5003): one worker each, 50 ms simulated service time, queue of 20.
- **Monitoring dashboard**: live routing, server health, latency percentiles, fault timelines and controller decisions.

## Load-balancing algorithms

| Algorithm | Rule |
|---|---|
| Round Robin | take turns: 1, 2, 3, 1, 2, 3 |
| Smooth Weighted Round Robin | weights 3:2:1 give the interleaved order 1, 2, 1, 3, 2, 1 (the Nginx method) |
| Least Connections | fewest requests in progress |
| Weighted Least Connections | lowest (requests in progress ÷ weight) |
| Least Response Time | fastest recent response times |
| IP Hash (rendezvous hashing) | the same client always reaches the same server; only a removed server's clients move |

All algorithms only choose servers that are currently healthy.

## Failover and health checks

- **Active checks:** `GET /health` to every server every 2 s, with a 1 s timeout.
- **Passive checks:** errors on real forwarded requests (connection refused, timeouts, HTTP 5xx), plus a
  per-server circuit breaker (3 failures → open for 5 s → 2 successful trials to close).
- **Failover:** a failed request is retried on another server (at most 2 retries, only when safe to repeat,
  limited by a retry budget to avoid retry storms).
- **Flap prevention:** a recovered server enters a RECOVERING state and is readmitted only after
  2 consecutive successful health checks.
- **Total outage:** the proxy answers HTTP 503 immediately and automatically starts a replacement server.
- **Fault injection:** connection refused, read timeout, HTTP 503 and slow responses (real), plus network
  partition and 50 % packet loss (simulated inside the proxy).

## Cost-aware overload mitigation

When the servers get too busy, adding servers costs money and takes time. The overload controller treats
proxy-side mitigations as a ladder ordered by cost and puts scale-out last:

1. **Cache** popular items for 2 s
2. **Rate-limit** clients far above their fair share (HTTP 429)
3. **Brownout** — lighter responses that cost 40 % of the normal work
4. **Scale-out** — add a server

Before switching a rung on, the controller predicts how much load it would remove and skips rungs that
cannot help (for example, the cache when no request repeats). It switches a rung off only when the load
would stay low without it. Service target: p95 latency ≤ 200 ms and errors ≤ 1 %.

## Key results

Measured on a single machine with one proxy and three local backend servers. Values are means of 5 runs
unless noted.

| Measurement | Result |
|---|---|
| Failure detection (health checks only, no traffic) | 1.88 s |
| Traffic moved to another server after detection | 2.03 s |
| Recovered server readmitted (after the first good check) | 2.01 s |
| Service restored after **all** servers failed (single run) | 2.7 s |
| Client requests served during 6 injected fault types (15 req/s) | **547 / 547** |
| Detection time under live traffic, by fault type | 58 ms (connection refused) to 2.8 s (silent timeout) |
| Mean request latency (50 ms simulated work + proxy) | 53 ms |

**Overload test** (traffic spike 12 → 40 → 12 req/s with one server failed), cost-aware controller compared
with scale-out only:

| Workload | Time over latency target: scale-out only → controller | Extra servers used by the controller |
|---|---|---|
| Non-cacheable | 11.2 s → 3.2 s (**71 % less**) | 0 |
| Cacheable | 10.8 s → 1.6 s (**85 % less**) | 0 |

The trade-off is reported openly: on non-cacheable traffic, 22 % of requests were rate-limited (98.2 % of them
from the two heavy clients) and 20 % received lighter responses.

## How to run

Requirements: Python 3.10 or newer.

```powershell
git clone https://github.com/Pramoda06/fault-tolerant-load-balancer.git
cd fault-tolerant-load-balancer
pip install -r requirements.txt
python run_all.py
```

Open **http://127.0.0.1:5000** for the dashboard. Press `Ctrl+C` to stop all servers.

To start the components separately:

```powershell
python backend_servers/server1.py
python backend_servers/server2.py
python backend_servers/server3.py
python app.py
```

## Tests

```powershell
python -m unittest discover tests      # 180 unit tests, no servers needed
python verify_live_system.py           # live integration test: starts real servers and injects failures
python verify_network_faults.py        # six fault types under live traffic
```

Other `verify_*.py` scripts check auto-recovery, routing strategies, capacity scaling and the overload controller.

## Project structure

```
app.py                  reverse proxy, REST API and dashboard
run_all.py              starts the 3 backends and the proxy
config.py               all settings (intervals, thresholds, ports)
load_balancer/          scheduler, health monitor, registry, circuit breaker, retries,
                        cache, rate limiter, overload controller, capacity, auto-recovery
backend_servers/        backend server and replacement server
templates/, static/     dashboard (HTML, CSS, JavaScript, Chart.js)
tests/                  unit tests
verify_*.py             live integration scripts
docs/TECHNICAL_NOTES.md detailed technical notes
```

## Tech stack

Python · Flask · Requests · SQLite · HTML/CSS/JavaScript · Chart.js · unittest

## Limitations

- Everything runs on one machine over loopback; real TCP is used, but there is no physical network.
- Server work is a simulated 50 ms wait; partition and packet loss are simulated inside the proxy.
- A single proxy instance (itself a single point of failure) and a small scale: 3 servers, tens of requests per second.

## Authors

Pramoda Challa · Jannu Shiny — Department of Information Technology, CBIT, Hyderabad
