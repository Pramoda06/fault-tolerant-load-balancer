/**
 * Dashboard extension: network faults & circuit breakers, networking telemetry,
 * load-aware capacity, data recovery and repeatable experiments.
 *
 * Every number comes from the proxy's API. Each value carries a source tag:
 *   proxy-observed  measured by the reverse proxy
 *   backend-reported measured inside a backend and returned in its response
 *   simulated       produced by the proxy's network simulator (packet loss, partition)
 *   derived/configured  computed from the above or taken from config.py
 */
(() => {
"use strict";
const L = window.LBM;
if (!L) return;
const { S, $, $$, esc, api, poll, toast, fmtNum, fmtMs, nameOf, stateOf, stateLabel } = L;

["capacity", "data-recovery"].forEach(L.addPage);   // Experiments page removed from the dashboard (API kept for scripts)

const src = (t) => `<span class="src src-${t.split(" ")[0].replace(/[^a-z-]/g, "")}">${esc(t)}</span>`;
const pct = (v, d = 0) => (v === null || v === undefined ? "–" : `${fmtNum(v * 100, d)} %`);
const clock = (t) => (t ? new Date(t * 1000).toLocaleTimeString("en-GB") : "–");
const dur = (a, b) => (a && b ? `${fmtNum((b - a) * 1000)} ms` : "–");
const dot = (s) => `<span class="dot ${stateOf(s)}"></span>`;
const ROLE = { original: "Original", recovery: "Replacement (recovery)", capacity: "Replacement (capacity)" };

async function req(method, path, body) {
    const res = await fetch(path, {
        method, headers: body ? { "Content-Type": "application/json" } : {},
        body: body ? JSON.stringify(body) : undefined, cache: "no-store",
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.message || "HTTP " + res.status);
    return data;
}
async function act(fn, okMsg) {
    try { const r = await fn(); if (okMsg) toast(typeof okMsg === "function" ? okMsg(r) : okMsg); }
    catch (e) { toast(e.message); }
    finally { poll(); }
}
function setHTML(el, html) {           // avoid needless DOM churn (keeps text selectable)
    if (el && el.__html !== html) { el.innerHTML = html; el.__html = html; }
}
function circuitBadge(c) {
    if (!c) return "–";
    const cls = { CLOSED: "ok", OPEN: "bad", HALF_OPEN: "rec" }[c.state] || "";
    const extra = c.state === "OPEN" ? ` · retry in ${fmtNum(c.open_remaining_s, 1)} s`
        : c.state === "CLOSED" && c.consecutive_failures ? ` · ${c.consecutive_failures} failure(s)` : "";
    return `<span class="badge badge-${cls}">${c.state.replace("_", "-")}</span><span class="sub">${extra}</span>`;
}

/* ================================================================== */
/* Fault Tolerance page: fault injection, breakers, fault timeline     */
/* ================================================================== */
const PARAM = {
    slow: { label: "Delay (ms)", key: "delay_ms", value: 400, min: 1, max: 10000, step: 50 },
    packet_loss: { label: "Loss (0–1)", key: "loss", value: 0.3, min: 0.01, max: 1, step: 0.05 },
};
let fxBuilt = false;
/** Fault chosen in "Simulate failure": used by every Fail button on the Fault Tolerance page. */
L.faultBody = () => {
    const type = ($("#fx-type") && $("#fx-type").value) || "http_5xx", p = PARAM[type], body = { type };
    if (p) body[p.key] = +$("#fx-param").value;
    return body;
};
function buildFaultControls() {
    const st = S.status;
    if (fxBuilt || !st.faults || !st.faults.types) return;
    fxBuilt = true;
    $("#fx-type").innerHTML = st.faults.types.map((t) =>
        `<option value="${t}"${t === "http_5xx" ? " selected" : ""}>${FAULT_NAME[t] || t}${st.faults.info[t].simulated ? " (simulated)" : ""}</option>`).join("");
    const paramUi = () => {
        const p = PARAM[$("#fx-type").value];
        $("#fx-param-wrap").hidden = !p;
        if (p) {
            $("#fx-param-wrap").firstChild.textContent = p.label;
            Object.assign($("#fx-param"), { value: p.value, min: p.min, max: p.max, step: p.step });
        }
        const info = st.faults.info[$("#fx-type").value];
        $("#fx-help").innerHTML = `<b>${esc(info.layer)}</b> — ${esc(info.summary)} ${info.simulated ? src("simulated") : src("real")}`;
    };
    $("#fx-type").addEventListener("change", paramUi);
    paramUi();
    $("#fx-clear-all").addEventListener("click", () => act(() => api("/api/faults/clear", {}), "All faults cleared"));
    $("#fx-cb-reset").addEventListener("click", () => act(() => api("/api/circuit/reset"), "Circuit breakers reset to CLOSED"));
}
const FAULT_NAME = { http_5xx: "HTTP 503 errors", refused: "Connection refused", timeout: "No response (timeout)",
    slow: "Slow responses", partition: "Network partition", packet_loss: "Packet loss" };

function renderFaults() {
    buildFaultControls();
    setHTML($("#fx-table"), S.servers.map((s) => `<tr>
        <td><span class="srv-dot" style="background:${L.serverColor(s.id)}"></span>${esc(s.name)}<span class="sub"> ${esc(ROLE[s.origin] || "")}</span></td>
        <td>${dot(s)} ${esc(stateLabel(s))}${s.draining ? ' <span class="badge">DRAINING</span>' : ""}</td>
        <td>${circuitBadge(s.circuit)}</td>
        <td>${(s.faults || []).length ? s.faults.map((f) => `<span class="badge badge-bad">${esc(f.type)}</span>${f.simulated ? src("simulated") : ""}<span class="sub"> ${f.detected_at ? "detected" : "not detected yet"}</span>`).join(" ") : '<span class="muted">none</span>'}</td>
        <td class="num">${s.health_latency_ms != null ? fmtMs(s.health_latency_ms) + " ms" : "–"}</td>
        <td class="num">${s.circuit ? s.circuit.opens : "–"}</td></tr>`).join(""));
    const recs = (S.status.faults.records || []).slice().reverse();
    setHTML($("#fx-records"), recs.length ? recs.map((r) => `<tr>
        <td><span class="badge ${r.cleared_at ? "" : "badge-bad"}">${esc(r.type)}</span>${r.simulated ? src("simulated") : ""}</td>
        <td>${esc(r.server_name)}</td>
        <td class="mono">${clock(r.injected_at)}</td>
        <td class="num">${r.detected_at ? dur(r.injected_at, r.detected_at) : '<span class="muted">not yet</span>'}</td>
        <td class="sub">${esc(r.detected_by || "–")}</td>
        <td class="mono">${clock(r.cleared_at)}</td>
        <td class="num">${r.recovered_at ? dur(r.cleared_at, r.recovered_at) : "–"}</td>
        <td class="sub wrap">${esc(r.outcome)}</td></tr>`).join("")
        : `<tr><td colspan="8" class="muted">No fault injected yet. Inject one above while traffic is running.</td></tr>`);
}

/* ------------------------------------------------------------------ */
/* What clients received: HTTP outcome per second (Monitoring + scenarios) */
/* ------------------------------------------------------------------ */
const RESP_SERIES = [["backend_full", "200 from a backend", "--green"], ["backend_light", "200, lighter (brownout)", "--s5"],
    ["cache", "200 from the proxy cache", "--blue"], ["rejected_429", "429 Too Many Requests", "--amber"], ["errors", "5xx / failed", "--red"]];
const respCharts = {};
function drawResponses(id) {
    const r = S.status && S.status.responses, el = document.getElementById(id);
    if (!r || !el || typeof Chart === "undefined" || !el.offsetParent) return;
    let c = respCharts[id];
    if (!c || c.canvas !== el) {
        if (c) c.destroy();
        const grid = cssv("--border"), tick = cssv("--muted");
        c = respCharts[id] = new Chart(el, {
            type: "bar",
            data: { labels: r.t, datasets: RESP_SERIES.map(([k, label, color]) => ({ label, data: r[k], backgroundColor: cssv(color), borderWidth: 0, barPercentage: 1, categoryPercentage: 0.92 })) },
            options: { responsive: true, maintainAspectRatio: false, animation: false,
                plugins: { legend: { labels: { color: cssv("--text-2"), boxWidth: 10, font: { size: 11 } } } },
                scales: { x: { stacked: true, ticks: { color: tick, maxTicksLimit: 7 }, grid: { display: false } },
                          y: { stacked: true, beginAtZero: true, ticks: { color: tick }, grid: { color: grid }, title: { display: true, text: "responses / s", color: tick } } } },
        });
        return;
    }
    c.data.labels = r.t;
    RESP_SERIES.forEach(([k], i) => { c.data.datasets[i].data = r[k]; });
    c.update("none");
}

/* ================================================================== */
/* Load Balancing page: request tracer                                 */
/* ================================================================== */
const TR_STEP = { receive: "Proxy receives", rate_limit: "Rate-limit check", cache: "Cache check", pool: "Healthy servers",
    select: "Load balancer picks", forward: "Forward to backend", backend: "Backend work", retry: "Retry",
    "stale-if-error": "Stale copy", routing: "Routing" };
async function sendTrace() {
    const ip = $("#tr-ip").value, item = $("#tr-item").value;
    const headers = { "X-Trace": "1" };
    if (ip) headers["X-Forwarded-For"] = ip;
    const url = "/api/request" + (item ? `?item=${item}` : "");
    $("#tr-send").disabled = true;
    const t0 = performance.now();
    try {
        const res = await fetch(url, { headers, cache: "no-store" });
        const ms = performance.now() - t0;
        const body = await res.json().catch(() => ({}));
        const hdr = ["Content-Type", "Content-Length", "X-Cache", "Age", "Retry-After", "Server", "Date"]
            .map((h) => [h, res.headers.get(h)]).filter(([, v]) => v != null);
        const steps = (body.trace || []).map((t) => {
            const bad = /^(429|blocked|stopped)|refused|timeout|5\d\d|connection|partition|loss/i.test(t.result) && t.step !== "pool";
            const extra = t.step === "pool" && t.servers ? `<div class="tr-servers">${t.servers.map((x) => `<span class="badge${x.circuit_ok ? "" : " badge-bad"}">${esc(x.name)} :${x.port} · ${x.active_connections} conn · w${x.weight}${x.circuit_ok ? "" : " · circuit open"}</span>`).join(" ")}</div>` : "";
            return `<li class="tr-step${bad ? " bad" : ""}"><span class="tr-t mono">${fmtNum(t.t_ms, 1)} ms</span>
                <span class="tr-name">${esc(TR_STEP[t.step] || t.step)}</span>
                <span class="tr-res">${esc(t.result)}${t.ms != null ? ` <em>${fmtNum(t.ms, 1)} ms</em>` : ""}</span>
                <span class="tr-detail">${esc(t.detail || "")}${extra}</span></li>`;
        }).join("");
        const SIMPLE = { receive: "Arrived", rate_limit: "Limit check", cache: "Cache", pool: "Healthy servers", select: "Picked",
            forward: "Sent to server", backend: "Server work", retry: "Retry", "stale-if-error": "Old copy", routing: "Routing" };
        const chips = (body.trace || []).map((t) => {
            const bad = /^(429|blocked|stopped)|refused|timeout|5\d\d|connection|partition|loss/i.test(t.result) && t.step !== "pool";
            let val = t.result;
            if (t.step === "receive") val = ip || "this browser";
            if (t.step === "backend") { const w = /worked ([\d.]+) ms/.exec(t.detail || ""); val = w ? `${w[1]} ms` : t.result; }
            return `<span class="tf-chip${bad ? " bad" : ""}"><span class="tf-k">${esc(SIMPLE[t.step] || t.step)}</span><span class="tf-v">${esc(val)}</span></span>`;
        }).join('<span class="tf-arr">→</span>');
        const answer = `<span class="tf-chip ${res.ok ? "ok" : "bad"}"><span class="tf-k">Answer</span><span class="tf-v">${res.status} ${res.ok ? "OK" : esc(res.statusText || "")}</span></span>`;
        const who = body.server ? `answered by <b>${esc(body.server)}</b>` : (res.status === 429 ? "stopped by the rate limit" : body.cache ? "answered from the proxy cache" : "not answered by a server");
        setHTML($("#tr-out"), `<div class="tf-flow">${chips}<span class="tf-arr">→</span>${answer}</div>
            <div class="tf-sum">${res.ok ? "✔" : "✖"} ${who} in <b>${fmtNum(ms, 0)} ms</b> <span class="muted">(${body.proxy_latency_ms != null ? fmtNum(body.proxy_latency_ms, 0) + " ms inside the proxy" : "proxy time not reported"})</span></div>
            <details class="tf-more"><summary>Show technical details (every step, timings and HTTP headers)</summary>
            <ol class="tr-list"><li class="tr-step"><span class="tr-t mono">0 ms</span><span class="tr-name">Browser sends</span>
            <span class="tr-res">GET ${esc(url)}</span><span class="tr-detail">${ip ? `X-Forwarded-For: ${esc(ip)} · ` : ""}X-Trace: 1</span></li>${steps}</ol>
            <div class="tr-resp"><div class="mono tr-status ${res.ok ? "ok" : "bad"}">HTTP ${res.status} ${esc(res.statusText || "")}</div>
            ${hdr.map(([h, v]) => `<div class="mono"><span class="muted">${h}:</span> ${esc(v)}</div>`).join("")}</div></details>`);
    } catch (e) {
        setHTML($("#tr-out"), `<p class="bad">Request failed: ${esc(e.message)}</p>`);
    } finally {
        $("#tr-send").disabled = false;
        poll();
    }
}

/* ================================================================== */
/* Monitoring page: networking telemetry                               */
/* ================================================================== */
const COUNTERS = [
    ["retries_attempted", "Retries attempted", "proxy-observed"], ["retries_succeeded", "Retries that succeeded", "proxy-observed"],
    ["retries_denied_budget", "Retries denied (budget)", "proxy-observed"], ["not_retried_non_idempotent", "Not retried (non-idempotent)", "proxy-observed"],
    ["connection_refused", "Connection refused", "proxy-observed"], ["connect_timeouts", "Connect timeouts", "proxy-observed"],
    ["timeouts", "Read timeouts", "proxy-observed"], ["http_5xx", "HTTP 5xx from backend", "proxy-observed"],
    ["backend_overloaded", "Backend queue full (503)", "backend-reported"], ["circuit_rejected", "Skipped: circuit open", "proxy-observed"],
    ["shed_proxy_overload", "Shed: proxy overload", "proxy-observed"], ["no_backend_503", "No backend available (503)", "proxy-observed"],
    ["blocked_data_recovery", "Blocked: data recovery", "proxy-observed"],
    ["simulated_packet_loss", "Packet loss", "simulated"], ["simulated_partition", "Partition drops", "simulated"],
    ["cache_hits", "Answered from proxy cache", "proxy-observed"], ["cache_stale_served", "Stale answer instead of error", "proxy-observed"],
    ["rate_limited", "Rate-limited (HTTP 429)", "proxy-observed"], ["degraded_responses", "Degraded (brownout) responses", "backend-reported"],
];
function renderTelemetry() {
    const st = S.status, t = st.telemetry;
    if (!t) return;
    const po = t.proxy_observed, e2e = po.e2e_latency_ms, hp = po.health_probe_ms, c = st.server_counts, rb = st.retry_budget;
    const tile = (label, value, unit, source, sub = "") =>
        `<div class="tm"><div class="tm-label">${label} ${src(source)}</div><div class="tm-value">${value}<span class="unit">${unit}</span></div><div class="tm-sub">${sub}</div></div>`;
    setHTML($("#tm-grid"), [
        tile("Incoming", fmtNum(po.incoming_rps, 1), "req/s", "proxy-observed", `${po.requests_in_window} requests in ${t.window_s} s`),
        tile("Throughput", fmtNum(po.throughput_rps, 1), "req/s", "proxy-observed", `success ${pct(po.success_rate, 1)} · timeouts ${pct(po.timeout_rate, 1)}`),
        tile("End-to-end latency", `${fmtMs(e2e.p50)} / ${fmtMs(e2e.p95)} / ${fmtMs(e2e.p99)}`, "ms", "proxy-observed",
            `p50 / p95 / p99 · ${e2e.samples} samples${e2e.enough_samples ? "" : " (few samples)"}`),
        tile("Health probe RTT", `${fmtMs(hp.p50)} / ${fmtMs(hp.p95)}`, "ms", "proxy-observed", `p50 / p95 · ${hp.probes} probes, ${hp.failed} failed`),
        tile("Proxy in-flight", fmtNum(st.proxy_inflight), `/ ${st.config.proxy_max_inflight}`, "proxy-observed", "admission limit; excess is shed with 503"),
        tile("Retry budget", fmtNum(rb.tokens, 1), `/ ${rb.max_tokens} tokens`, "proxy-observed", `${rb.retries_spent} spent · ${rb.retries_denied} denied`),
        tile("Servers", `${c.HEALTHY}`, `healthy / ${S.servers.length}`, "proxy-observed",
            `${c.UNHEALTHY} failed · ${c.RECOVERING} recovering · ${c.draining} draining · ${c.circuit_open} circuit open · ${c.replacement} replacement`),
        tile("Simulated network", fmtNum(st.network_sim.stats.lost + st.network_sim.stats.partition_drops), "drops", "simulated",
            `${st.network_sim.stats.exchanges} exchanges passed through the simulator`),
    ].join(""));
    const br = t.backend_reported;
    setHTML($("#tm-backend"), Object.keys(br).length ? Object.entries(br).map(([id, b]) => `<tr>
        <td>${esc(nameOf(id))} ${src("backend-reported")}</td><td class="num">${fmtMs(b.service_ms_avg)} ms</td>
        <td class="num">${fmtMs(b.queue_ms_avg)} ms</td><td class="num">${fmtMs(b.queue_ms_p95)} ms</td><td class="num">${b.samples}</td></tr>`).join("")
        : `<tr><td colspan="5" class="muted">No requests in the last ${t.window_s} s.</td></tr>`);
    setHTML($("#tm-counters"), COUNTERS.map(([k, label, s]) => `<tr class="${t.counters_window[k] ? "" : "row-dim"}">
        <td>${label}</td><td class="num">${fmtNum(t.counters_window[k])}</td><td class="num">${fmtNum(t.counters_total[k])}</td><td>${src(s)}</td></tr>`).join(""));
}

/* ================================================================== */
/* Capacity page                                                       */
/* ================================================================== */
const SETTINGS = [
    ["CAPACITY_TARGET_UTILIZATION", "Target utilisation", 0.1, 1.0, 0.05],
    ["CAPACITY_SCALE_IN_UTILIZATION", "Scale-in utilisation", 0.05, 0.95, 0.05],
    ["CAPACITY_MIN_SERVERS", "Min servers", 1, 10, 1],
    ["CAPACITY_MAX_SERVERS", "Max servers", 1, 12, 1],
    ["CAPACITY_OBSERVATION_WINDOW", "Observation window (s)", 2, 120, 1],
    ["CAPACITY_MIN_SAMPLES", "Min samples", 1, 1000, 1],
    ["CAPACITY_SCALE_OUT_SUSTAIN", "Scale-out sustain (s)", 0, 120, 1],
    ["CAPACITY_SCALE_IN_SUSTAIN", "Scale-in sustain (s)", 1, 600, 1],
    ["CAPACITY_SCALE_OUT_COOLDOWN", "Scale-out cooldown (s)", 0, 600, 1],
    ["CAPACITY_SCALE_IN_COOLDOWN", "Scale-in cooldown (s)", 0, 600, 1],
    ["CAPACITY_PER_SERVER_RPS", "Capacity per backend (req/s, 0 = measure)", 0, 10000, 1],
    ["CAPACITY_ORIGINAL_STANDBY", "Originals to standby at low load (1 = yes, 0 = no)", 0, 1, 1],
    ["CIRCUIT_FAILURE_THRESHOLD", "Circuit: failures to open", 1, 50, 1],
    ["CIRCUIT_OPEN_SECONDS", "Circuit: open time (s)", 0.5, 120, 0.5],
    ["CIRCUIT_HALF_OPEN_SUCCESSES", "Circuit: successes to close", 1, 20, 1],
    ["MAX_FAILOVER_RETRIES", "Max retries per request", 0, 5, 1],
    ["SNAPSHOT_INTERVAL", "Backup interval (s, 0 = off)", 0, 3600, 5],
];
let settingsDirty = false, settingsAt = 0;
async function loadSettings(force) {
    if (!force && (settingsDirty || Date.now() - settingsAt < 5000)) return;
    settingsAt = Date.now();
    try {
        const v = await req("GET", "/api/settings");
        SETTINGS.forEach(([k]) => { const el = $(`#set-${k}`); if (el) el.value = v[k]; });
        settingsDirty = false;
        $("#set-msg").textContent = "";
    } catch (e) { /* surfaced by poll */ }
}
function buildCapacity() {
    $("#set-form").innerHTML = SETTINGS.map(([k, label, lo, hi, step]) =>
        `<label title="${lo} – ${hi}">${label}<input type="number" id="set-${k}" min="${lo}" max="${hi}" step="${step}"></label>`).join("");
    $("#set-form").addEventListener("input", () => { settingsDirty = true; $("#set-msg").textContent = "unsaved changes"; });
    $("#set-save").addEventListener("click", async () => {
        const body = {};
        SETTINGS.forEach(([k]) => { body[k] = +$(`#set-${k}`).value; });
        try {
            await api("/api/settings", body);
            settingsDirty = false;
            $("#set-msg").className = "set-msg ok";
            $("#set-msg").textContent = "applied";
            toast("Settings applied");
            loadSettings(true);
        } catch (e) {
            $("#set-msg").className = "set-msg bad";
            $("#set-msg").textContent = e.message;
        }
    });
    loadSettings(true);

    $("#tg-rate").addEventListener("input", () => { $("#tg-rate-val").textContent = $("#tg-rate").value; });
    $("#tg-rate").addEventListener("change", () => L.setTrafficRate(+$("#tg-rate").value));
    const readShape = () => {
        S.traffic.rate = +$("#tg-rate").value;
        S.traffic.shape = { concurrency: +$("#tg-conc").value, clients: +$("#tg-clients").value, write_ratio: +$("#tg-writes").value,
            keys: +$("#tg-keys").value, heavy: +$("#tg-heavy").value, heavy_clients: 2, poisson: $("#tg-poisson").value === "1" };
    };
    $("#tg-start").addEventListener("click", async () => { readShape(); await L.startTraffic(); toast(`Traffic: ${S.traffic.rate} req/s`); });
    $("#tg-stop").addEventListener("click", async () => { await L.stopTraffic(); poll(); toast("Traffic stopped"); });
}

/* ------------------------------------------------------------------ */
/* Cost-aware resilience controller                                    */
/* ------------------------------------------------------------------ */
const RUNG_SHORT = { cache: "Cache popular items", rate_limit: "Limit heavy clients", degrade: "Brownout (lighter responses)", scale_out: "Add servers" };
const RUNG_COST = { cache: "data up to 2 s old", rate_limit: "429 for heavy clients only", degrade: "optional content dropped", scale_out: "server cost" };
let rsWired = false;
function renderResilience() {
    const r = S.status.resilience;
    if (!r) return;
    if (!rsWired) {
        rsWired = true;
        $("#rs-policy").addEventListener("click", (e) => {
            const b = e.target.closest("button[data-policy]");
            if (b) act(() => api("/api/resilience/policy", { policy: b.dataset.policy }), `Policy: ${b.textContent}`);
        });
        $("#rs-ladder").addEventListener("change", (e) => {
            const cb = e.target.closest("input[data-allow]");
            if (!cb) return;
            const now = new Set(S.status.resilience.allowed);
            if (cb.checked) now.add(cb.dataset.allow); else now.delete(cb.dataset.allow);
            act(() => api("/api/resilience/allowed", { rungs: Array.from(now) }),
                `${RUNG_SHORT[cb.dataset.allow]} ${cb.checked ? "allowed" : "not allowed"}`);
        });
        buildScenarios();
    }
    $$("#rs-policy button").forEach((b) => b.classList.toggle("on", b.dataset.policy === r.policy));
    const o = r.observed || {}, slo = r.slo;
    const p95ok = o.p95_ms == null || o.p95_ms <= slo.p95_ms, errOk = (o.error_rate || 0) <= slo.error_rate;
    const util = o.utilization || 0;
    const meter = (label, value, ok, sub) => `<div class="rs-m ${ok ? "ok" : "bad"}"><div class="rs-m-label">${label}</div><div class="rs-m-value">${value}</div><div class="rs-m-sub">${sub}</div></div>`;
    setHTML($("#rs-slo"), [
        meter(`p95 latency ${src("proxy-observed")}`, o.p95_ms != null ? `${fmtNum(o.p95_ms)} ms` : "–", p95ok, `SLO ≤ ${fmtNum(slo.p95_ms)} ms · last ${fmtNum(slo.window_s)} s`),
        meter("Error rate", pct(o.error_rate || 0, 1), errOk, `SLO ≤ ${pct(slo.error_rate)}`),
        meter("Server utilisation", pct(util), util <= r.thresholds.util_high, `escalate above ${pct(r.thresholds.util_high)} · relax below ${pct(r.thresholds.util_low)}`),
        meter("Extra servers (since reset)", `${fmtNum(r.costs.extra_server_seconds, 0)} s`, true, `${fmtNum(r.costs.slo_violation_seconds, 0)} s in SLO violation`),
    ].join(""));
    const ladderOn = r.policy === "ladder";
    const ladderHTML = r.rungs.map((g, i) => `<div class="rs-rung ${g.active ? "on" : ""} ${(!ladderOn || !g.allowed) && g.key !== "scale_out" ? "off" : ""}">
        <span class="rs-step">${i + 1}</span><span class="rs-name">${RUNG_SHORT[g.key]}</span>
        <span class="rs-cost">cost: ${RUNG_COST[g.key]}</span>
        ${g.key !== "scale_out" ? `<label class="rs-allow"><input type="checkbox" data-allow="${g.key}" ${g.allowed ? "checked" : ""}${scenarioRunning() ? " disabled" : ""}> allowed for this service</label>` : `<span class="rs-allow muted">always allowed</span>`}
        ${ladderOn && g.allowed && o.relief && g.key !== "scale_out" ? `<span class="rs-relief ${o.relief[g.key] >= 0.1 ? "" : "low"}">would remove ≈ ${pct(o.relief[g.key])} of load${o.relief[g.key] >= 0.1 ? "" : " · skipped"}</span>` : ""}
        ${g.key === "cache" && g.active ? `<span class="rs-live">${pct(o.cache_share || 0)} answered by proxy</span>` : ""}
        ${g.key === "rate_limit" && g.active ? `<span class="rs-live">limit ${fmtNum(r.rate_limit.limit_rps, 1)} req/s per client · ${pct(o.rejected_share || 0)} rejected</span>` : ""}
        ${g.key === "degrade" && g.active ? `<span class="rs-live">${pct(o.degraded_share || 0)} degraded</span>` : ""}
        ${g.key === "scale_out" && g.active ? `<span class="rs-live">${o.replacements || 0} replacement(s) running</span>` : ""}
      </div>`).join('<span class="rs-arrow">→</span>');
    const lad = $("#rs-ladder");
    if (!lad.contains(document.activeElement)) setHTML(lad, ladderHTML);
    setHTML($("#rs-note"), ladderOn
        ? `Active now: <b>${esc(r.rungs.filter((g) => g.active).map((g) => RUNG_SHORT[g.key]).join(" + ") || "nothing (the SLO is met)")}</b>. The controller jumps to the cheapest step predicted to remove at least 10 % of the load, measures its effect for ${fmtNum(r.thresholds.dwell_s)} s, and only then considers the next. A step is removed only when utilisation would stay below ${pct(r.thresholds.util_low)} without it.`
        : r.policy === "scale_only" ? "Baseline: no mitigations — the capacity manager adds servers when the 70 % rule asks for them."
        : r.policy === "cache_always" ? "Baseline: the proxy cache is always on (no other mitigations); servers are added when the 70 % rule asks for them."
        : `Baseline: a fixed ${r.static_servers} servers are kept running; no mitigations and no reactive scaling.`);
    const fx = (b, a, k, unit, d = 0) => (b && b[k] != null ? fmtNum(b[k], d) + unit : "–") + " → " + (a && a[k] != null ? fmtNum(a[k], d) + unit : "measuring…");
    // one history: the controller's mitigations + the capacity manager's server changes
    const rows = r.decisions.map((d) => ({ time: d.time, html: `<tr>
        <td class="mono">${esc(d.time)}</td>
        <td>${d.action === "escalate" ? '<span class="badge badge-rec">+ ' : '<span class="badge badge-ok">− '}${esc(RUNG_SHORT[d.rung] || d.rung)}</span></td>
        <td class="sub wrap">${esc(d.reason)}</td>
        <td class="num">${fx(d.before, d.after, "p95_ms", " ms")}</td>
        <td class="num">${fx(d.before, d.after, "backend_rps", " req/s", 1)}</td></tr>` }));
    ((S.status.capacity || {}).history || []).forEach((h) => {
        const lbl = CAP_ACTION[h.decision] || (["AT_MAXIMUM", "BACKOFF"].includes(h.phase) ? h.phase.replace("_", " ").toLowerCase() : null);
        if (!lbl) return;
        rows.push({ time: h.time, html: `<tr><td class="mono">${esc(h.time)}</td>
            <td><span class="badge ${lbl[0] === "+" ? "badge-info" : ""}">${esc(lbl)}</span></td>
            <td class="sub wrap">${esc(h.reason)}</td><td class="num">–</td><td class="num">${h.demand_rps != null ? fmtNum(h.demand_rps, 1) + " req/s" : "–"}</td></tr>` });
    });
    rows.sort((x, y) => (x.time < y.time ? 1 : x.time > y.time ? -1 : 0));
    setHTML($("#rs-decisions"), rows.length ? rows.slice(0, 16).map((x) => x.html).join("")
        : `<tr><td colspan="5" class="muted">Nothing yet — run a scenario above, or start traffic.</td></tr>`);
    renderScenarios();
}
const CAP_ACTION = { scale_out: "+ Starting a server", registered: "+ Server added", scale_in: "− Draining a server",
    removed: "− Server removed", standby: "Original server on standby", activate_standby: "+ Original back from standby",
    cancel_drain: "Drain cancelled" };

/* ------------------------------------------------------------------ */
/* One-click scenarios                                                 */
/* ------------------------------------------------------------------ */
const SC_ICON = { cache: "⚡", rate_limit: "✋", degrade: "◐", scale_out: "＋", failure: "✕" };
const PHASE_TXT = { preparing: "Preparing a clean start", normal: "Normal traffic", spike: "Traffic spike", calm: "Calm again" };
function scenarioRunning() { const sc = S.status && S.status.scenarios; return !!(sc && sc.state.running); }
function buildScenarios() {
    $("#sc-grid").addEventListener("click", (e) => {
        const b = e.target.closest("button[data-sc]");
        if (b) act(() => api("/api/scenarios/start", { name: b.dataset.sc }), "Scenario started — watch the controller below");
    });
    $("#sc-live").addEventListener("click", (e) => {
        if (e.target.closest("#sc-stop")) act(() => api("/api/scenarios/stop"), "Scenario stopped");
    });
}
function renderScenarios() {
    const sc = S.status.scenarios;
    if (!sc) return;
    const run = sc.state, last = sc.last;
    setHTML($("#sc-grid"), sc.scenarios.map((x) => `<div class="sc-card${run.running && run.name === x.key ? " on" : ""}">
        <div class="sc-top"><span class="sc-ico">${SC_ICON[x.key] || "•"}</span><b>${esc(x.title)}</b></div>
        <div class="sc-expect">${esc(x.expect)}</div>
        <button class="btn btn-sm" data-sc="${x.key}"${run.running || ((S.status.experiments || {}).state || {}).running ? " disabled" : ""}>${run.running && run.name === x.key ? "Running…" : "Run"}</button>
      </div>`).join(""));
    const r = S.status.resilience, o = r.observed || {};
    if (run.running) {
        const total = run.total_s || 80, el = run.elapsed_s || 0;
        const bands = sc.phases.map((p) => `<span class="sc-band sc-${p.name}" style="width:${(p.seconds / total) * 100}%">${PHASE_TXT[p.name]}</span>`).join("");
        const active = r.rungs.filter((g) => g.active).map((g) => RUNG_SHORT[g.key]);
        const lastD = r.decisions[0];
        const def = sc.scenarios.find((x) => x.key === run.name) || {};
        setHTML($("#sc-live"), `<div class="sc-now">
            <div class="sc-head"><b>${esc(def.title || "")}</b><span class="muted"> · ${PHASE_TXT[run.phase] || run.phase}${run.rate ? ` · ${fmtNum(run.rate)} req/s` : ""}${run.phase_left_s != null ? ` · ${fmtNum(run.phase_left_s)} s left in this phase` : ""}</span>
              <button class="btn btn-sm btn-ghost" id="sc-stop">Stop</button></div>
            <div class="sc-bar">${bands}<span class="sc-cursor" style="left:${Math.min(100, (el / total) * 100)}%"></span></div>
            <div class="sc-cols">
              <div><span class="sc-k">Expected</span>${esc(def.expect || "")}</div>
              <div><span class="sc-k">Happening now</span>${active.length ? `<b>${esc(active.join(" + "))}</b>` : "No mitigation"} · p95 ${o.p95_ms != null ? fmtNum(o.p95_ms) + " ms" : "–"} · servers busy ${pct(o.utilization || 0)} · ${S.status.resilience.observed.routable || "–"} servers in routing
                ${lastD ? `<div class="sub">Last decision: ${esc(lastD.reason)}</div>` : ""}</div>
            </div></div>`);
    } else if (last) {
        const used = last.rungs_used.map((k) => RUNG_SHORT[k] || k);
        const verdict = last.matched === true ? `<span class="badge badge-ok">as expected</span>`
            : last.matched === false ? `<span class="badge badge-rec">different from expected</span>` : "";
        setHTML($("#sc-live"), `<div class="sc-result">
            <div class="sc-head"><b>Last run: ${esc(last.title)}</b> ${verdict} <span class="muted">· ${esc(last.outcome)} at ${esc(last.finished_at)}</span></div>
            <div class="sc-facts">
              <div><span>Methods used</span><b>${used.length ? esc(used.join(" → ")) : "none"}</b></div>
              <div><span>Extra server time</span><b>${fmtNum(last.costs.extra_server_seconds, 0)} s</b></div>
              <div><span>Seconds over the latency target</span><b>${fmtNum(last.costs.slo_violation_seconds, 0)} s</b></div>
              <div><span>Requests</span><b>${fmtNum(last.ok)} served · ${fmtNum(last.rate_limited)} got 429 · ${fmtNum(last.failed)} failed</b></div>
              ${last.rejected_total ? `<div><span>Who got the 429s</span><b>${pct(last.rejected_heavy / last.rejected_total)} the two flooding clients</b></div>` : ""}
            </div></div>`);
    } else setHTML($("#sc-live"), "");
}

function renderCapacity() {
    const st = S.status, c = st.capacity;
    if (!c) return;
    const topo = st.topology || {};
    const cand = (topo.nodes || []).find((n) => n.kind === "candidate");
    const gap = c.desired - (topo.routable ?? c.routable);
    const cell = (label, value, unit, sub, source, wide) =>
        `<div class="cm${wide ? " cm-wide" : ""}"><div class="cm-label">${label}${source ? " " + src(source) : ""}</div>
         <div class="cm-value">${value}${unit ? `<span class="unit">${unit}</span>` : ""}</div><div class="cm-sub">${sub}</div></div>`;
    setHTML($("#cm-grid"), [
        cell("Incoming requests", fmtNum(c.demand_rps, 1), "req/s", `mean of last ${fmtNum(c.window_s)} s · last ${fmtNum(c.recent_window_s)} s: ${fmtNum(c.demand_recent_rps, 1)}`, "proxy-observed"),
        cell("Capacity per backend", fmtNum(c.capacity_rps_per_server, 1), "req/s", esc(c.capacity_source || ""), c.capacity_kind || "derived"),
        cell("Target utilisation", pct(c.target_utilization), "", `safe load per backend ${fmtNum(c.safe_rps_per_server, 1)} req/s`, "configured"),
        cell("Healthy backends", `${topo.routable ?? c.routable}`, "in routing pool", `${topo.healthy ?? "–"} healthy · ${c.originals_routable} original + ${c.replacements_routable} replacement routable${c.recovering ? ` · ${c.recovering} recovering` : ""}`, "proxy-observed"),
        cell("Desired backends", c.desired, "", c.enough_window ? (gap > 0 ? `target only — ${gap} more needed; not yet running` : gap < 0 ? `${-gap} more than needed` : "matches routable count") : `still measuring (${fmtNum(c.observed_s)}/${fmtNum(c.window_s)} s)`, "derived"),
        cell("Pending provisions", topo.pending_provisions ?? c.pending, "", cand ? `${esc(cand.name)} · ${esc(cand.state)}${cand.port ? " · :" + cand.port : ""} · not routed` : "none in progress", ""),
        cell("Replacements · standby", `${topo.replacements_active ?? 0} · ${topo.standby ?? 0}`, "", `${topo.replacements_active ?? 0} replacement process${(topo.replacements_active ?? 0) === 1 ? "" : "es"} · ${topo.standby ?? 0} original${(topo.standby ?? 0) === 1 ? "" : "s"} on standby · ${topo.draining || 0} draining`, ""),
        cell("Scaling state", esc(c.paused ? "PAUSED" : c.phase || "–"), "", esc(c.decision || ""), ""),
        cell("Latest decision", "", "", esc(c.reason || ""), "", true),
    ].join(""));
    const ph = $("#cap-phase");
    ph.textContent = c.paused ? `PAUSED · ${c.phase}` : c.phase;
    ph.className = "cap-phase badge " + ({ STABLE: "badge-ok", OBSERVING: "badge-info", STARTING: "badge-info", COOLDOWN: "badge-info", PROVISIONING: "badge-rec",
        SCALE_OUT: "badge-rec", SCALE_OUT_PENDING: "badge-rec", SCALE_IN: "badge-rec", SCALE_IN_PENDING: "badge-rec", DRAINING: "badge-rec",
        WAITING_FOR_SERVICE: "badge-bad", AT_MAXIMUM: "badge-bad", BACKOFF: "badge-bad", ERROR: "badge-bad" }[c.phase] || "");
    setHTML($("#cap-formula"), `required = ceil(demand / (capacity per server × target utilisation))<br>${esc(c.formula)}`);
    const extras = [];
    if (c.scale_out_pending_s) extras.push(`overload sustained ${fmtNum(c.scale_out_pending_s, 1)} / ${fmtNum(c.scale_out_sustain_s)} s`);
    if (c.scale_in_pending_s) extras.push(`low demand sustained ${fmtNum(c.scale_in_pending_s, 1)} / ${fmtNum(c.scale_in_sustain_s)} s`);
    if (c.cooldown_remaining_s) extras.push(`${c.cooldown_kind} cooldown ${fmtNum(c.cooldown_remaining_s, 1)} s`);
    if (c.provisioning) extras.push(`provisioning ${esc(c.provisioning.name || "")} (${esc(c.provisioning.stage || "")})`);
    if (c.provision_failures) extras.push(`${c.provision_failures} provisioning failure(s)`);
    setHTML($("#cap-reason"), `<b>${esc(c.decision || "")}</b> — ${esc(c.reason || "")}${extras.length ? `<div class="sub">${extras.join(" · ")}</div>` : ""}
        <div class="sub">demand ${src("proxy-observed")} · service time ${src(c.service_source.startsWith("measured") ? "backend-reported" : "configured")} ${esc(c.service_source)} · workers ${src("configured")}</div>`);
    const slots = [];
    for (let i = 1; i <= c.max_servers; i++) {
        const cls = i <= c.routable ? "on" : i <= c.routable + c.pending ? "pending" : "";
        slots.push(`<span class="slot ${cls} ${i <= c.desired ? "want" : ""}" title="${i <= c.desired ? "desired" : ""}"></span>`);
    }
    const capTotal = c.routable * c.safe_rps_per_server;
    setHTML($("#cap-bars"), `<div class="cap-row"><span class="cap-k">Servers (filled = routable, outlined = desired)</span><span class="slots">${slots.join("")}</span></div>
        <div class="cap-row"><span class="cap-k">Demand vs safe capacity of routable servers</span>
        <span class="meter"><span style="width:${Math.min(100, capTotal ? (c.demand_rps / capTotal) * 100 : 100)}%" class="${capTotal && c.demand_rps <= capTotal ? "" : "over"}"></span></span>
        <span class="mono">${fmtNum(c.demand_rps, 1)} / ${fmtNum(capTotal, 1)} req/s</span></div>
        <div class="cap-row"><span class="cap-k">Utilisation of routable servers ${src("derived")}</span>
        <span class="meter"><span style="width:${Math.min(100, (c.utilization || 0) * 100)}%" class="${c.utilization > c.target_utilization ? "over" : ""}"></span></span>
        <span class="mono">${pct(c.utilization)}</span></div>`);
    const tr = st.traffic;
    const rate = $("#tg-rate");
    if (document.activeElement !== rate && rate.__synced !== S.traffic.rate) {
        rate.value = rate.__synced = S.traffic.rate;
        $("#tg-rate-val").textContent = S.traffic.rate;
    }
    $("#tg-start").textContent = tr.running ? "Apply shape (restarts traffic)" : "Start traffic";
    setHTML($("#tg-stats"), `
        <div><dt>Status</dt><dd><span>${tr.running ? "running" : "stopped"}</span>${tr.running ? ` ${fmtNum(tr.params.rps)} req/s target` : ""}</dd></div>
        <div><dt>Achieved (last 5 s) <span>client-observed</span></dt><dd><span>${fmtNum(tr.achieved_rps_5s, 1)}</span>req/s</dd></div>
        <div><dt>Client success (last 5 s)</dt><dd><span>${pct(tr.success_rate_5s, 1)}</span></dd></div>
        <div><dt>Client latency p50 / p95</dt><dd><span>${fmtMs(tr.client_latency_ms_5s.p50)} / ${fmtMs(tr.client_latency_ms_5s.p95)}</span>ms</dd></div>
        <div><dt>Sent / failed</dt><dd><span>${fmtNum(tr.sent)} / ${fmtNum(tr.failed)}</span></dd></div>`);
}

/* ================================================================== */
/* Data Recovery page                                                  */
/* ================================================================== */
async function lowestRecord(skip = 0) {          // skip=1: second-lowest, so "change" and "delete" hit different records
    const r = await req("GET", "/api/records");
    if (!r.records.length) throw new Error("No record yet — write some first (step 1)");
    const ids = r.records.map((x) => x.id).sort((a, b) => a - b);
    return ids[Math.min(skip, ids.length - 1)];
}
function buildData() {
    $$("[data-dt]").forEach((b) => b.addEventListener("click", async () => {
        const a = b.dataset.dt;
        if (a === "delete") {
            act(async () => { const id = await lowestRecord(); await req("DELETE", `/api/records/${id}`); return id; },
                (id) => `Record #${id} deleted — the replica and the log copy the deletion too`);
        } else if (a === "update") {
            act(async () => { const id = await lowestRecord(1); await req("PUT", `/api/records/${id}`, { value: "changed at " + new Date().toLocaleTimeString("en-GB") }); return id; },
                (id) => `Record #${id} changed — it now has a newer version than the backup`);
        } else if (a === "seed") act(() => api("/api/data/seed", { count: 10 }), (r) => `${r.written} records written through the proxy`);
        else if (a === "seed5") act(() => api("/api/data/seed", { count: 5 }), (r) => `${r.written} more records written`);
        else if (a === "snapshot") act(() => api("/api/data/snapshot"), (r) => `Backup taken: ${r.snapshot.records} records`);
        else if (a === "reset") act(() => api("/api/data/reset"), "Data cleared — start again from step 1");
        drMapAt = 0;
    }));
    $$("[data-corrupt]").forEach((b) => b.addEventListener("click", () => {
        drMapAt = 0;
        act(() => api("/api/data/corrupt", { target: b.dataset.corrupt }), (r) => r.message);
    }));
    $("#dr-strats").innerHTML = DR_STRATS.map((s) => `
        <label class="dr-strat${s.key === drStrategy ? " on" : ""}" data-strat="${s.key}">
            <input type="radio" name="dr-strategy" value="${s.key}"${s.key === drStrategy ? " checked" : ""}>
            <span class="dr-strat-title">${s.title}</span>
            <span class="dr-strat-how">${s.how}</span>
            <span class="dr-strat-now" data-now="${s.key}"></span>
        </label>`).join("");
    $("#dr-strats").addEventListener("change", (e) => {
        drStrategy = e.target.value;
        $$(".dr-strat").forEach((el) => el.classList.toggle("on", el.dataset.strat === drStrategy));
        drMapAt = 0;
        renderData();
    });
    $("#dr-start").addEventListener("click", () => act(() => api("/api/disaster/start", {
        strategy: drStrategy, fault: ($("#dr-fault") || { value: "refused" }).value }), "Total failure: every backend is down and its own disk is wiped"));
    $("#dr-play").addEventListener("click", playStory);
    $("#dr-ack").addEventListener("click", () => act(() => api("/api/disaster/acknowledge"), "Data loss accepted; service resumed"));
    $("#dr-recover").addEventListener("click", () => act(() => api("/api/recover-all"), "Repair started — the original servers rejoin after their health checks"));
}

let drStrategy = "wal";
let drRtoProbe = null;
let drMap = null, drMapAt = 0, drMapBusy = false;
const DR_STRATS = [
    { key: "backup", title: "Backup only", how: "Restore the newest backup snapshot." },
    { key: "replica", title: "Live copy (replica)", how: "Use the second copy that every write updates before it is confirmed." },
    { key: "wal", title: "Backup + change log", how: "Restore the newest backup, then replay every change made after it (write-ahead log)." },
    { key: "none", title: "No copy", how: "Nothing is kept outside the servers." },
];
const TL_ROWS = [
    { key: "backup", label: "Backup only", reach: (t) => (t.backup_lsn == null ? -1 : t.backup_lsn),
      sub: (t) => (t.backup_lsn == null ? "no backup yet" : `backup taken at ${clock(t.backup_time)}`) },
    { key: "replica", label: "Live copy", reach: (t) => (t.replica_lsn == null ? -1 : t.replica_lsn), sub: () => "updated on every change" },
    { key: "wal", label: "Backup + change log", reach: (t) => (t.log_valid_lsn == null ? -1 : t.log_valid_lsn), sub: () => "backup, then replay the log" },
    { key: "none", label: "No copy", reach: () => -1, sub: () => "nothing kept elsewhere" },
];
const OP_GLYPH = { create: "+", update: "✎", delete: "✕", base: "" };
const OP_WORD = { create: "created", update: "changed", delete: "deleted" };

/** Timeline grid: one column per confirmed change, one row per strategy; green = comes back, red = lost. */
function tlHTML(t, rows, opts = {}) {
    if (!t) return `<p class="muted">Loading…</p>`;
    const cols = [];
    if (t.base_lsn > 0) cols.push({ lsn: t.base_lsn, op: "base" });
    (t.events || []).forEach((e) => cols.push(e));
    if (!cols.length) return `<p class="muted">No changes yet. Press <b>▶ Play the story</b> (or write some records): each change appears here as a column.</p>`;
    const items = [];
    let backupPlaced = t.backup_lsn == null;
    cols.forEach((c) => {
        if (!backupPlaced && c.lsn > t.backup_lsn) { items.push({ marker: "backup" }); backupPlaced = true; }
        items.push(c);
    });
    if (!backupPlaced) items.push({ marker: "backup" });
    if (opts.failure) items.push({ marker: "failure" });
    const tmpl = "minmax(150px, 190px) " + items.map((x) => (x.marker ? "16px" : x.op === "base" ? "64px" : "28px")).join(" ") + " 64px";
    const head = items.map((x) => x.marker
        ? `<div class="tl-mk tl-mk-${x.marker}" title="${x.marker === "backup" ? "backup taken here" : "every server fails here"}"><span>${x.marker === "backup" ? "backup" : "failure"}</span></div>`
        : x.op === "base" ? `<i class="tl-chip tl-base" title="data restored by an earlier recovery">earlier data</i>`
        : `<i class="tl-chip tl-${x.op}${x.ok === false ? " tl-bad" : ""}" title="#${x.id} ${OP_WORD[x.op]}${x.ok === false ? " — this log entry is damaged" : ""}">${OP_GLYPH[x.op]}${x.id}</i>`).join("");
    const total = cols.length;
    const body = rows.map((r) => {
        const reach = r.reach(t);
        let kept = 0;
        const cells = items.map((x) => {
            if (x.marker) return `<div class="tl-mk tl-mk-${x.marker}"></div>`;
            const ok = x.lsn <= reach;
            if (ok) kept++;
            return `<i class="tl-cell ${ok ? "kept" : "lost"}" title="${x.op === "base" ? "earlier data" : "#" + x.id + " " + OP_WORD[x.op]}: ${ok ? "comes back" : "lost"}"></i>`;
        }).join("");
        return `<div class="tl-row${r.key === opts.highlight ? " tl-hl" : ""}" style="grid-template-columns:${tmpl}">
            <div class="tl-label"><b>${r.label}</b><span>${esc(r.sub(t))}</span></div>${cells}<div class="tl-count" title="changes that come back">${kept}/${total}</div></div>`;
    }).join("");
    return `<div class="tl-scroll"><div class="tl-row tl-head" style="grid-template-columns:${tmpl}"><div class="tl-label"><span>${t.hidden ? `newest ${total} of ${total + t.hidden} changes` : "changes, oldest first →"}</span></div>${head}<div class="tl-count muted">changes</div></div>${body}</div>`;
}
const DR_ROWS = [
    { key: "primary", label: "Backend's own disk", sub: "destroyed by a total failure" },
    { key: "backup", label: "Newest valid backup", sub: "" },
    { key: "replica", label: "Replica", sub: "updated before every confirmation" },
    { key: "wal", label: "Backup + write-ahead log", sub: "what replaying the log gives" },
];

async function loadMap() {
    if (drMapBusy || Date.now() - drMapAt < 1500) return;
    drMapBusy = true;
    try { drMap = await req("GET", "/api/data/map?limit=40"); drMapAt = Date.now(); }
    catch (e) { /* surfaced by poll */ }
    finally { drMapBusy = false; }
}

const RC_TITLE = { ok: "current version", old: "older version", missing: "missing", extra: "deleted, but still here", none: "" };
function cellState(id, copy, truth) {
    const t = truth[id], c = copy ? copy[id] : undefined;
    if (c === undefined) return t === undefined ? "none" : "missing";
    if (t === undefined) return "extra";
    return c < t ? "old" : "ok";
}
function cells(ids, fn) {
    return ids.map((id) => { const st = fn(id); return `<i class="rc rc-${st}" title="#${id}: ${RC_TITLE[st]}">${id}</i>`; }).join("");
}
function recs(k) { return `${k} record${k === 1 ? "" : "s"}`; }
function idList(a, max = 8) { return a.slice(0, max).map((i) => "#" + i).join(" ") + (a.length > max ? ` … +${a.length - max}` : ""); }

function mapHTML(m, dr) {
    if (!m) return `<p class="muted">Loading…</p>`;
    if (!m.ids.length) return `<p class="muted">No records yet. Write some in step 1 — each one appears here as a square in every copy that holds it.</p>`;
    const truth = m.truth, ids = m.ids.map(Number);
    const head = `<div class="dr-map-row dr-map-truth"><div class="dr-map-label"><b>Confirmed to clients</b><span>the truth: ${recs(m.acknowledged)}</span></div>
        <div class="dr-map-cells">${cells(ids, (id) => (truth[id] !== undefined ? "ok" : "none"))}</div></div>`;
    const wiped = dr && ["OUTAGE", "RESTORING_DATA"].includes(dr.state);
    const rows = DR_ROWS.map((r) => {
        const copy = m.copies[r.key];
        const sub = r.key === "backup" ? (m.backup_file ? `${m.backup_file}` : "no valid backup yet")
            : r.key === "primary" && (copy === null || wiped) ? "wiped by the failure" : r.sub;
        const n = copy ? Object.keys(copy).filter((id) => truth[id] !== undefined && copy[id] === truth[id]).length : 0;
        return `<div class="dr-map-row${r.key === "primary" ? " dr-map-primary" : ""}"><div class="dr-map-label"><b>${r.label}</b><span>${esc(sub)}</span></div>
            <div class="dr-map-cells">${cells(ids, (id) => cellState(id, copy, truth))}</div>
            <div class="dr-map-count">${n}/${m.acknowledged}</div></div>`;
    }).join("");
    return head + rows + (m.hidden ? `<div class="sub">showing the newest ${ids.length} of ${ids.length + m.hidden} records</div>` : "");
}

function previewText(key, m) {
    if (!m || !m.acknowledged) return `<span class="muted">Write some records first.</span>`;
    const p = m.preview[key], n = m.acknowledged;
    const bits = [];
    if (p.lost.length) bits.push(`loses ${idList(p.lost)}`);
    if (p.stale.length) bits.push(`old version of ${idList(p.stale)}`);
    if (p.resurrected.length) bits.push(`deleted ${idList(p.resurrected)} would come back`);
    const cls = key === "none" || p.restored === 0 ? "bad" : bits.length ? "warn" : "ok";
    if (p.restored === 0 && !p.stale.length) return `<b class="bad">All ${recs(n)} would be lost.</b>${key === "none" ? '<span class="muted"> Service resumes only after you accept the loss.</span>' : ""}`;
    const head = p.restored === n && !bits.length ? `All ${recs(n)} come back.` : `${p.restored} of ${n} come back exactly.`;
    const extra = key === "none" ? " Service resumes only after you accept the loss."
        : key === "replica" && !bits.length ? " But it would also copy a mistake, like an accidental delete." : "";
    return `<b class="${cls}">${head}</b>${bits.length ? ` <span>${bits.join(" · ")}.</span>` : ""}<span class="muted">${extra}</span>`;
}

function drSteps(d) {
    if (d.state === "IDLE") return "";
    const t = d.timings_ms, rep = d.data_report;
    const step = (cls, title, detail, time) =>
        `<li class="step ${cls}"><span class="step-ico">${cls.startsWith("done") ? "✓" : cls === "bad" || cls === "warn" ? "!" : "·"}</span>
        <div><div class="step-title">${title}</div><div class="step-detail">${detail}</div></div><span class="step-time">${time != null ? fmtNum(time) + " ms" : ""}</span></li>`;
    const restoredCls = d.state === "FAILED" ? "bad" : d.state === "DATA_LOSS_ACK_REQUIRED" ? "warn"
        : t.data_restored != null ? (rep && !rep.zero_data_loss ? "warn" : "done") : t.backend_ready != null ? "pending current" : "pending";
    return [
        step("done", "Every backend failed and its disk was wiped", `failure: ${esc(d.fault || "")} · strategy: ${esc((DR_STRATS.find((s) => s.key === d.strategy) || {}).title || d.strategy || "")}`, 0),
        step(t.detection != null ? "done" : "pending current", "Proxy detected the outage", "no healthy backend · clients get HTTP 503 instead of reaching an empty database", t.detection),
        step(t.backend_ready != null ? "done" : t.detection != null ? "pending current" : "pending", "A healthy backend is available", "automatic recovery starts a new backend and checks it twice", t.backend_ready),
        step(restoredCls, "Data restored and checked", rep ? `${rep.restored_records} records from ${esc(rep.source || "nothing")} · checksums ${esc(rep.integrity)}` : "copy back from the chosen source, then verify every record's checksum", t.data_restored),
        step(t.rto != null ? "done" : d.state === "RESTORED" ? "pending current" : d.state === "DATA_LOSS_ACK_REQUIRED" ? "warn" : "pending", "Clients served again",
            t.rto != null ? "first client request answered — this is the recovery time (RTO)" : d.state === "DATA_LOSS_ACK_REQUIRED" ? "waiting for you to accept the data loss" : d.state === "RESTORED" ? "routing reopened; sending a test request…" : "routing reopens only after the data is checked", t.rto),
    ].join("");
}

function drResult(dr, d) {
    const r = dr.data_report || d.last_recovery;
    if (!r) return `<p class="muted">Do steps 1 and 2 to see what comes back.</p>`;
    const n = r.expected_records, lost = r.rpo_records || 0;
    let head, cls;
    if (!r.source_available && n) { head = `No copy existed — all ${n} records were lost.`; cls = "bad"; }
    else if (r.zero_data_loss && !(r.unexpected_records || []).length) { head = `All ${n} confirmed records came back.`; cls = "ok"; }
    else { head = `${n - lost} of ${n} records came back exactly — ${lost} lost or out of date.`; cls = "warn"; }
    let grid = "";
    if (r.expected_ids) {
        const miss = new Set(r.missing_records || []), stale = new Set(r.stale_records || []), extra = new Set(r.unexpected_records || []);
        const ids = Array.from(new Set([...r.expected_ids, ...(r.unexpected_records || [])])).sort((a, b) => a - b).slice(-40);
        grid = `<div class="dr-map dr-result-map"><div class="dr-map-row"><div class="dr-map-label"><b>After recovery</b><span>vs. what clients were told</span></div>
            <div class="dr-map-cells">${cells(ids, (id) => (miss.has(id) ? "missing" : stale.has(id) ? "old" : extra.has(id) ? "extra" : "ok"))}</div></div></div>`;
    }
    const why = [];
    if ((r.missing_records || []).length) why.push(`<span class="rc-txt rc-missing-t">Lost:</span> ${idList(r.missing_records, 15)} — written after the copy that was restored`);
    if ((r.stale_records || []).length) why.push(`<span class="rc-txt rc-old-t">Older version:</span> ${idList(r.stale_records, 15)} — changed after the copy was made`);
    if ((r.unexpected_records || []).length) why.push(`<span class="rc-txt rc-extra-t">Came back although deleted:</span> ${idList(r.unexpected_records, 15)}`);
    const row = TL_ROWS.find((x) => x.key === r.strategy);
    const tl = r.timeline && row ? `<div class="dr-tl">${tlHTML(r.timeline, [Object.assign({}, row, { reach: () => (r.source_available ? r.restored_lsn : -1) })], { failure: true })}</div>` : "";
    grid = grid ? `<details class="dr-records"><summary>Record by record</summary>${grid}</details>` : "";
    return `<div class="dr-verdict ${cls}">${head}</div>${tl}${grid}
      ${why.length ? `<div class="dr-ids">${why.join("<br>")}</div>` : ""}
      <div class="dr-facts">
        <div><span>Strategy · restored from</span><b>${esc((DR_STRATS.find((s) => s.key === r.strategy) || {}).title || r.strategy)} · ${esc(r.source || "nothing")}</b></div>
        <div><span>Data lost (RPO)</span><b>${recs(lost)} · ${fmtNum(r.rpo_seconds, 2)} s of writes</b></div>
        <div><span>Recovery time (RTO)</span><b>${dr.timings_ms.rto != null ? fmtNum(dr.timings_ms.rto) + " ms" : "–"}</b></div>
        <div><span>Checksums</span><b>${esc(r.integrity)}</b></div>
      </div>
      ${(r.notes || []).length ? `<ul class="dr-notes">${r.notes.map((x) => `<li>${esc(x)}</li>`).join("")}</ul>` : ""}`;
}

let storyRunning = false;
function story(msg) { const el = $("#dr-story"); el.hidden = !msg; el.innerHTML = msg || ""; }
const pause = (ms) => new Promise((r) => setTimeout(r, ms));
async function playStory() {
    if (storyRunning) return;
    storyRunning = true;
    renderData();
    const step = async (msg, fn, wait = 2200) => { story(msg); await fn(); drMapAt = 0; await poll(); await pause(wait); };
    try {
        await step("Starting with empty data…", () => api("/api/data/reset"), 1200);
        await step("<b>1.</b> Clients write <b>10 records</b>. Each one appears as a <b>+</b> column below.", () => api("/api/data/seed", { count: 10 }));
        await step("<b>2.</b> A <b>backup</b> is taken: a snapshot of those 10 records (the dashed line).", () => api("/api/data/snapshot"));
        await step("<b>3.</b> Life goes on after the backup: <b>5 more records</b> are written…", () => api("/api/data/seed", { count: 5 }));
        await step("<b>4.</b> …one record is <b>changed</b>…", async () => { const id = await lowestRecord(1); await req("PUT", `/api/records/${id}`, { value: "changed" }); });
        await step("<b>5.</b> …and one is <b>deleted</b>.", async () => { const id = await lowestRecord(); await req("DELETE", `/api/records/${id}`); }, 600);
        story("<b>Now read the bars:</b> <i>Backup only</i> stops at the backup line — every change after it is red, so it would be lost. "
            + "The <i>live copy</i> and the <i>change log</i> reach the end. <i>No copy</i> loses everything. "
            + "<b>Pick one in step 2 and fail every server</b> to check it for real.");
    } catch (e) {
        story(`Stopped: ${esc(e.message)}`);
    } finally {
        storyRunning = false;
        renderData();
    }
}
function wpathHTML(w) {
    const hop = (name, ms) => `<span class="wp-hop">${name}${ms != null ? `<em>${fmtNum(ms, 2)} ms</em>` : ""}</span>`;
    return `<div class="wp-line"><span class="sc-k">How one change is confirmed${w ? ` (measured, mean of last ${w.samples})` : ""}</span>
        ${hop("client")}<span class="wp-arr">→</span>${hop("proxy :5000")}<span class="wp-arr">→</span>${hop("backend")}<span class="wp-arr">→</span>
        ${hop("change log (fsync)", w && w.log_ms)}<span class="wp-arr">→</span>${hop("own disk", w && w.primary_ms)}<span class="wp-arr">→</span>${hop("live copy", w && w.replica_ms)}
        <span class="wp-arr">→</span>${hop("201 Created ←")}</div>
        <div class="sub">Here the live copy is on the same machine. In another building it would add one network round trip (RTT) to <i>every</i> write — which is why many systems copy asynchronously and accept losing the last few changes.</div>`;
}

function renderData() {
    const st = S.status, d = st.data, dr = st.disaster;
    if (!d || !dr) return;
    loadMap();
    const es = (st.experiments || {}).state || {};
    if (exWasRunning !== es.running || Date.now() - exLastFetch > 15000) { exWasRunning = es.running; loadExperiments(); }
    const active = ["OUTAGE", "RESTORING_DATA", "DATA_LOSS_ACK_REQUIRED", "FAILED"].includes(dr.state);
    const n = d.acknowledged_records;
    const snap = d.snapshots.slice().reverse().find((s) => s.valid);
    const origs = S.servers.filter((s) => s.origin === "original");
    const downOrig = origs.filter((s) => s.status !== "HEALTHY");

    let cur = 1;                                  // which step to look at now
    if (active) cur = dr.state === "DATA_LOSS_ACK_REQUIRED" || dr.state === "FAILED" ? 3 : 2;
    else if (dr.state === "RESTORED" && (downOrig.length || dr.data_report)) cur = 3;
    else if (n && snap && !storyRunning) cur = 2;
    $$(".dr-step").forEach((el) => el.classList.toggle("now", +el.id.split("-")[1] === cur));

    $("#dr-interval").textContent = d.snapshot_interval_s ? fmtNum(d.snapshot_interval_s) : "–";
    $$("[data-dt], [data-corrupt]").forEach((b) => { b.disabled = active || !!es.running || storyRunning; });
    $("#dr-play").disabled = active || !!es.running || storyRunning;
    if (!active) setHTML($("#dr-tl"), tlHTML(drMap && drMap.timeline, TL_ROWS, { highlight: drStrategy }));
    else setHTML($("#dr-tl"), `<p class="muted">Every server is down and its disk is wiped — the copies kept elsewhere are being used now (step 2).</p>`);
    setHTML($("#dr-map"), mapHTML(drMap, dr));
    setHTML($("#dr-wpath"), wpathHTML(drMap && drMap.write_path_ms));
    $$("[data-now]").forEach((el) => setHTML(el, `If every server failed now: ${previewText(el.dataset.now, drMap)}`));
    $("#dr-start").disabled = active || !!es.running || storyRunning;
    $("#dr-ack").hidden = dr.state !== "DATA_LOSS_ACK_REQUIRED";
    setHTML($("#dr-steps"), drSteps(dr));
    setHTML($("#dr-report"), drResult(dr, d));
    setHTML($("#dr-f6"), !origs.length ? "" : downOrig.length ? `${downOrig.map((s) => s.name).join(", ")} ${downOrig.length === 1 ? "is" : "are"} down` : "All original servers are healthy");
    $("#dr-recover").disabled = !downOrig.length;
    if ($("#ex-run-dr")) $("#ex-run-dr").disabled = active || !!es.running;
    if ($("#dx-note")) setHTML($("#dx-note"), es.running ? `<b>Running:</b> ${esc(es.progress || "")}` : "replaces the data on this page");

    setHTML($("#dt-kv"), `
        <div><dt>On the backend <span>lost in a total failure</span></dt><dd><span>${d.primary.records ?? "–"}</span>records</dd></div>
        <div><dt>Replica <span>separate storage, updated before each write is confirmed</span></dt><dd><span>${d.replica.records ?? "–"}</span>records</dd></div>
        <div><dt>Write-ahead log <span>every write, saved first</span></dt><dd><span>${d.wal.entries}</span>entries</dd></div>
        <div><dt>Backups</dt><dd><span>${d.snapshots.length}</span>newest valid: ${esc(d.manifest.last_valid_snapshot || "none")}</dd></div>`);
    setHTML($("#dt-snaps"), d.snapshots.length ? d.snapshots.slice().reverse().map((s) => `<tr>
        <td class="mono">${esc(s.file)}</td><td class="num">${s.records}</td><td class="num">${s.lsn}</td>
        <td class="mono sub">${esc(s.sha256.slice(0, 16))}…</td>
        <td>${s.valid ? '<span class="badge badge-ok">valid</span>' : '<span class="badge badge-bad">checksum mismatch</span>'}</td>
        <td class="sub">${clock(s.created_at)} · ${esc(s.reason)}</td></tr>`).join("")
        : `<tr><td colspan="6" class="muted">No backup yet.</td></tr>`);
}

/** RTO is measured at the first client request after recovery: send one real request if no traffic does. */
function drAutoProbe() {
    const dr = S.status && S.status.disaster;
    if (!dr || dr.state !== "RESTORED" || dr.timings_ms.rto != null) return;
    const key = String(dr.timings_ms.data_restored);
    if (drRtoProbe === key) return;
    drRtoProbe = key;
    fetch("/api/request", { cache: "no-store" }).catch(() => {}).finally(() => poll());
}

/* ================================================================== */
/* Experiments page                                                    */
/* ================================================================== */
let exReports = {}, exWasRunning = null, exLastFetch = 0;
async function loadExperiments() {
    try {
        const r = await req("GET", "/api/experiments");
        exReports = r.reports || {};
        exLastFetch = Date.now();
        renderExperimentReports();
    } catch (e) { /* ignore */ }
}
function buildExperiments() {
    if (!$("#page-experiments")) return;          // page removed from the dashboard (API still available)
    $("#ex-run-failover").addEventListener("click", () => act(() => api("/api/experiments/failover", {
        requests: +$("#ex-req").value, rps: +$("#ex-rps").value, concurrency: +$("#ex-conc").value,
        clients: +$("#ex-clients").value, fault: $("#ex-fault").value, target: $("#ex-target").value,
    }), "Failover comparison started (about 10 s per strategy)"));
    $("#ex-run-dr").addEventListener("click", () => act(() => api("/api/experiments/dr", {
        records_before_backup: +$("#dx-before").value, records_after_backup: +$("#dx-after").value,
        updates_after_backup: +$("#dx-upd").value, accidental_delete: !!$("#dx-del").value,
        corrupt: $("#dx-corrupt").value || null,
    }), "Comparison started — one real failure per strategy (about 40 s)"));
    $("#exp-progress").addEventListener("click", (e) => {
        if (e.target.closest("#ex-cancel")) act(() => api("/api/experiments/cancel"), "Cancelling after the current step");
    });
    $("#ex-run-policies").addEventListener("click", () => act(() => api("/api/experiments/policies", {
        keys: +$("#px-keys").value, heavy: +$("#px-heavy").value, fault: $("#px-fault").value || null,
    }), "Policy comparison started (about 5 minutes)"));
    $("#ex-run-util").addEventListener("click", () => act(() => api("/api/experiments/utilisation", {}),
        "Utilisation experiment started (about 2 minutes)"));
}

const POLICY_NAME = { static: "Static servers", scale_only: "Scale-out only", cache_always: "Cache always on", ladder: "Cost-aware ladder" };
const POLICY_COLOR = { static: "--s6", scale_only: "--s2", cache_always: "--s1", ladder: "--s3" };
const exCharts = {};
function cssv(name) { return getComputedStyle(document.documentElement).getPropertyValue(name).trim(); }
function drawChart(id, key, config) {
    if (typeof Chart === "undefined") return;
    const el = document.getElementById(id);
    if (!el) return;
    if (exCharts[id] && exCharts[id].key === key && exCharts[id].chart.canvas === el) return;
    if (exCharts[id]) exCharts[id].chart.destroy();
    const grid = cssv("--border"), tick = cssv("--muted");
    config.options = Object.assign({ responsive: true, maintainAspectRatio: false, animation: false,
        interaction: { mode: "index", intersect: false },
        plugins: { legend: { labels: { color: cssv("--text-2"), boxWidth: 12, font: { size: 11 } } } },
        scales: { x: { ticks: { color: tick, maxTicksLimit: 10 }, grid: { color: grid } },
                  y: { ticks: { color: tick }, grid: { color: grid } } } }, config.options || {});
    exCharts[id] = { key, chart: new Chart(el, config) };
}
function policyReportHTML(pr) {
    const rows = pr.results;
    const best = rows.reduce((a, r) => (a == null || r.weighted_cost < a.weighted_cost ? r : a), null);
    const lines = rows.map((r) => `<b>${esc(POLICY_NAME[r.policy])}</b>: ${fmtNum(r.slo_violation_s, 0)} s in SLO violation, ${fmtNum(r.extra_server_seconds, 0)} extra server-seconds`).join(" · ");
    const pp = pr.params;
    return `<div class="sub ex-meta">Run ${esc(pr.finished_at)} · ${pp.keys ? "popular items" : "no repeated items"} · ${pp.heavy ? "2 heavy clients" : "no heavy clients"} · ${pp.fault ? "Server 2 fails during the spike" : "no failure"} · saved to <span class="mono">${esc(pr.file || "")}</span></div>
      <div class="ex-conclusion">${lines}.${best ? ` Lowest weighted cost: <b>${esc(POLICY_NAME[best.policy])}</b>.` : ""}</div>
      <table class="tbl tbl-compact"><thead><tr><th>Policy</th><th class="num">SLO violation</th><th class="num">p50 / p95 / p99 ms</th><th class="num">Errors</th><th class="num">Rejected (429)</th><th class="num">From cache</th><th class="num">Degraded</th><th class="num">Extra server-s</th><th class="num">Weighted cost</th></tr></thead>
      <tbody>${rows.map((r) => `<tr><td><span class="srv-dot" style="background:${cssv(POLICY_COLOR[r.policy])}"></span>${esc(POLICY_NAME[r.policy])}</td>
        <td class="num">${fmtNum(r.slo_violation_s, 0)} s (${fmtNum(r.slo_violation_pct, 1)} %)</td>
        <td class="num">${fmtMs(r.client_latency_ms.p50)} / ${fmtMs(r.client_latency_ms.p95)} / ${fmtMs(r.client_latency_ms.p99)}</td>
        <td class="num">${pct(r.error_rate, 2)}</td><td class="num">${pct(r.rejected_share, 1)}</td><td class="num">${pct(r.cache_share, 1)}</td>
        <td class="num">${pct(r.degraded_share, 1)}</td><td class="num">${fmtNum(r.extra_server_seconds, 0)}</td><td class="num"><b>${fmtNum(r.weighted_cost, 1)}</b></td></tr>`).join("")}</tbody></table>
      <div class="ex-charts"><div><h3>p95 latency over time (2 s window)</h3><div class="ex-chart"><canvas id="ch-pol-p95"></canvas></div></div>
        <div><h3>Routable servers over time</h3><div class="ex-chart"><canvas id="ch-pol-srv"></canvas></div></div></div>
      <div class="tbl-foot">Weighted cost = extra server-seconds × ${pr.cost_weights.server_second} + SLO-violation seconds × ${pr.cost_weights.slo_violation_second}. Rejected, cached and degraded shares are reported separately (their user cost is application-specific). ${esc(pr.note || "")}</div>`;
}
function drawPolicyCharts(pr) {
    const key = pr.file || pr.finished_at;
    const labels = pr.results[0].timeline.map((x) => x.t + " s");
    const ds = (field) => pr.results.map((r) => ({ label: POLICY_NAME[r.policy], data: r.timeline.map((x) => x[field]),
        borderColor: cssv(POLICY_COLOR[r.policy]), backgroundColor: cssv(POLICY_COLOR[r.policy]), pointRadius: 0, borderWidth: 2,
        stepped: field === "routable", spanGaps: true }));
    const slo = { label: `SLO ${pr.slo.p95_ms} ms`, data: labels.map(() => pr.slo.p95_ms), borderColor: cssv("--red"),
        borderDash: [5, 4], pointRadius: 0, borderWidth: 1.5 };
    drawChart("ch-pol-p95", key, { type: "line", data: { labels, datasets: ds("p95_ms").concat([slo]) },
        options: { scales: { y: { title: { display: true, text: "ms", color: cssv("--muted") }, beginAtZero: true } } } });
    drawChart("ch-pol-srv", key, { type: "line", data: { labels, datasets: ds("routable") },
        options: { scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } } } });
}
function utilReportHTML(u) {
    return `<div class="sub ex-meta">Run ${esc(u.finished_at)} · ${u.servers} servers · capacity ${fmtNum(u.capacity_rps_per_server, 1)} req/s per server · saved to <span class="mono">${esc(u.file || "")}</span></div>
      <div class="ex-charts one"><div><h3>Latency vs utilisation</h3><div class="ex-chart"><canvas id="ch-util"></canvas></div></div></div>
      <table class="tbl tbl-compact"><thead><tr><th class="num">Utilisation</th><th class="num">Offered</th><th class="num">Throughput</th><th class="num">p50 ms</th><th class="num">p95 ms</th><th class="num">p99 ms</th><th class="num">Queue wait</th><th class="num">Success</th></tr></thead>
      <tbody>${u.results.map((r) => `<tr><td class="num"><b>${pct(r.target_utilization)}</b></td><td class="num">${fmtNum(r.offered_rps, 1)}</td><td class="num">${fmtNum(r.throughput_rps, 1)}</td>
        <td class="num">${fmtMs(r.latency_ms.p50)}</td><td class="num">${fmtMs(r.latency_ms.p95)}</td><td class="num">${fmtMs(r.latency_ms.p99)}</td>
        <td class="num">${r.queue_wait_ms_mean != null ? fmtMs(r.queue_wait_ms_mean) + " ms" : "–"}</td><td class="num">${pct(r.success_rate, 1)}</td></tr>`).join("")}</tbody></table>
      <div class="tbl-foot">${esc(u.note || "")}</div>`;
}
function drawUtilChart(u) {
    const labels = u.results.map((r) => Math.round(r.target_utilization * 100) + " %");
    const line = (label, field, color) => ({ label, data: u.results.map((r) => r.latency_ms[field]), borderColor: cssv(color),
        backgroundColor: cssv(color), pointRadius: 3, borderWidth: 2 });
    drawChart("ch-util", u.file || u.finished_at, { type: "line", data: { labels, datasets: [line("p50", "p50", "--s1"), line("p95", "p95", "--s3"), line("p99", "p99", "--s4")] },
        options: { scales: { y: { title: { display: true, text: "ms", color: cssv("--muted") }, beginAtZero: true } } } });
}
function renderExperimentReports() {
    const f = exReports.failover_comparison, d = exReports.dr_comparison;
    const pr = exReports.policy_comparison, u = exReports.utilisation;
    setHTML($("#ex-policies"), pr ? policyReportHTML(pr) : `<p class="muted">Not run in this session yet.</p>`);
    if (pr && S.page === "experiments") drawPolicyCharts(pr);
    setHTML($("#ex-util"), u ? utilReportHTML(u) : `<p class="muted">Not run in this session yet.</p>`);
    if (u && S.page === "experiments") drawUtilChart(u);
    const ALGO = { ROUND_ROBIN: "Round Robin", WEIGHTED_ROUND_ROBIN: "Weighted RR", LEAST_CONNECTIONS: "Least Connections",
        WEIGHTED_LEAST_CONNECTIONS: "Weighted LC", LEAST_RESPONSE_TIME: "Least Resp. Time", IP_HASH: "IP Hash" };
    setHTML($("#ex-failover"), f ? `<div class="sub ex-meta">Run ${esc(f.finished_at)} · ${f.params.requests} requests at ${f.params.rps} req/s · fault <b>${esc(f.params.fault)}</b> on ${esc(nameOf(f.params.target))} · saved to <span class="mono">${esc(f.file || "")}</span></div>
      <table class="tbl tbl-compact"><thead><tr><th>Strategy</th><th class="num">Throughput</th><th class="num">Success</th><th class="num">p50 ms</th><th class="num">p95 ms</th><th class="num">p99 ms</th><th class="num">Retries</th><th class="num">During fault</th><th class="num">Target served (fault)</th><th>Distribution</th></tr></thead>
      <tbody>${f.results.map((r) => `<tr><td>${esc(ALGO[r.strategy] || r.strategy)}</td><td class="num">${fmtNum(r.throughput_rps, 1)} req/s</td>
        <td class="num">${pct(r.success_rate, 1)}</td><td class="num">${fmtMs(r.latency_ms.p50)}</td><td class="num">${fmtMs(r.latency_ms.p95)}</td><td class="num">${fmtMs(r.latency_ms.p99)}</td>
        <td class="num">${r.retries}</td><td class="num">${r.during_fault.succeeded}/${r.during_fault.requests}</td><td class="num">${r.during_fault.served_by_target}</td>
        <td class="sub">${Object.entries(r.distribution).map(([k, v]) => `${esc(nameOf(k))}: ${v}`).join(" · ")}</td></tr>`).join("")}</tbody></table>
      <div class="tbl-foot">${esc(f.note || "")}</div>` : `<p class="muted">No failover comparison has run in this session.</p>`);
    setHTML($("#ex-dr"), d ? `<div class="sub ex-meta">Run ${esc(d.finished_at)} · ${d.params.records_before_backup} records before the backup, ${d.params.records_after_backup} after, ${d.params.updates_after_backup} updates after${d.params.accidental_delete ? ", accidental delete" : ""}${d.params.corrupt ? `, corrupted ${esc(d.params.corrupt)}` : ""} · saved to <span class="mono">${esc(d.file || "")}</span></div>
      <table class="tbl tbl-compact"><thead><tr><th>Strategy</th><th class="num">Restored</th><th class="num">Lost</th><th class="num">RPO</th><th class="num">RTO</th><th>Integrity</th><th>Operator ack</th><th>Notes</th></tr></thead>
      <tbody>${d.results.map((r) => `<tr><td><b>${esc(r.strategy)}</b></td><td class="num">${r.restored_records ?? "–"}/${r.acknowledged_records}</td>
        <td class="num">${r.lost_records ?? "–"}</td><td class="num">${r.rpo_seconds != null ? fmtNum(r.rpo_seconds, 2) + " s" : "–"}</td>
        <td class="num">${r.timings_ms.rto != null ? fmtNum(r.timings_ms.rto) + " ms" : "–"}</td><td>${esc(r.integrity || "–")}</td>
        <td>${r.operator_ack_required ? '<span class="badge badge-bad">required</span>' : "no"}</td><td class="sub wrap">${esc((r.notes || []).join(" "))}</td></tr>`).join("")}</tbody></table>
      <div class="tbl-foot">${esc(d.note || "")}</div>` : `<p class="muted">No disaster-recovery comparison has run in this session.</p>`);
    const lim = (f || d || {}).limitations;
    $("#ex-limits").textContent = "Limitations: " + (lim || "single machine over loopback; simulated service time; results vary between runs and are not production measurements.");
}
function renderExperiments() {
    if (!$("#page-experiments")) return;
    const es = (S.status.experiments || {}).state || {};
    const box = $("#exp-progress");
    box.hidden = !es.running && !es.error;
    setHTML(box, es.running ? `<b>Running ${esc(es.kind || "")}</b> — ${esc(es.progress || "")} <button class="btn btn-sm btn-ghost" id="ex-cancel">Cancel</button>`
        : es.error ? `<b>Last experiment stopped:</b> ${esc(es.error)}` : "");
    $("#ex-run-failover").disabled = $("#ex-run-policies").disabled = $("#ex-run-util").disabled = !!es.running;
    if (exWasRunning !== es.running || Date.now() - exLastFetch > 15000) { exWasRunning = es.running; loadExperiments(); }
    if (exReports.policy_comparison) drawPolicyCharts(exReports.policy_comparison);
    if (exReports.utilisation) drawUtilChart(exReports.utilisation);
}

// charts take their colours from the theme when created: rebuild them when the theme changes
new MutationObserver(() => {
    Object.keys(respCharts).forEach((k) => { respCharts[k].destroy(); delete respCharts[k]; });
    Object.keys(exCharts).forEach((k) => { exCharts[k].chart.destroy(); delete exCharts[k]; });
}).observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });

/* ================================================================== */
let built = false;
L.onRender((page) => {
    if (!built) {
        built = true;
        buildCapacity(); buildData(); buildExperiments();
        $("#tr-send").addEventListener("click", sendTrace);
    }
    drAutoProbe();
    if (page === "fault-tolerance") renderFaults();
    else if (page === "monitoring") { renderTelemetry(); }
    else if (page === "capacity") { renderResilience(); renderCapacity(); loadSettings(false); drawResponses("ch-resp-res"); }
    else if (page === "data-recovery") renderData();
    else if (page === "experiments") renderExperiments();
});
})();
