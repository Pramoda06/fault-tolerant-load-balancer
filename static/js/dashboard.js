/**
 * Load Balancing & Fault-Tolerant Network Monitoring — dashboard controller.
 *
 * Every value shown comes from the reverse proxy's /api/status endpoint or from
 * the responses of real requests sent through /api/request. Rates and shares are
 * computed from differences between successive status snapshots.
 */
(() => {
"use strict";

const CFG = window.LBM_CONFIG || { healthInterval: 2, recoveryThreshold: 2, maxRetries: 2, defaultAlgorithm: "ROUND_ROBIN" };
const POLL_MS = 600;
const LOAD_WINDOW = 6;          // seconds of traffic used for "Load" (share of requests)
const SERVER_COLOR_COUNT = 6;    // --s1 … --s6 in style.css
const ALGO_NAMES = {
    ROUND_ROBIN: "Round Robin", WEIGHTED_ROUND_ROBIN: "Weighted Round Robin",
    LEAST_CONNECTIONS: "Least Connections", WEIGHTED_LEAST_CONNECTIONS: "Weighted Least Connections",
    LEAST_RESPONSE_TIME: "Least Response Time", IP_HASH: "IP Hash (sticky)",
};
const ALGO_SHORT = {
    ROUND_ROBIN: "Round Robin", WEIGHTED_ROUND_ROBIN: "Weighted RR", LEAST_CONNECTIONS: "Least Connections",
    WEIGHTED_LEAST_CONNECTIONS: "Weighted LC", LEAST_RESPONSE_TIME: "Least Resp. Time", IP_HASH: "IP Hash",
};
/** Strategies with a fixed target share; the others depend on live connections, response times or client IPs. */
const STATIC_SHARE = { ROUND_ROBIN: true, WEIGHTED_ROUND_ROBIN: true };

const S = {
    status: null,
    servers: [],
    byId: {},
    byName: {},
    hist: [],            // [{t, counts:{id:n}, ok, fail}]
    chart: { labels: [], rate: {}, lat: {}, lastT: 0 },
    routes: [],          // client-side record of the last routed requests
    routesVersion: 0,
    traffic: { running: false, rate: 5, lastN: 0, busy: false,
               shape: { concurrency: 8, clients: 16, write_ratio: 0, keys: 0, heavy: 0, poisson: false } },
    demo: null,
    page: "overview",
    pollBusy: false,
    pollFails: 0,
    incidentBase: {},
    topos: {},
    charts: {},
};

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const cssVar = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

function fmtNum(v, maxFrac = 0) {
    if (v === null || v === undefined || Number.isNaN(v)) return "–";
    return Number(v).toLocaleString("en-US", { maximumFractionDigits: maxFrac });
}
function fmtMs(v) {
    if (v === null || v === undefined) return "–";
    return fmtNum(v, v < 100 ? 1 : 0);
}
function serverColor(id) {
    const n = parseInt((String(id).match(/\d+/) || ["1"])[0], 10);
    return cssVar(`--s${((n - 1) % SERVER_COLOR_COUNT) + 1}`);
}
function shortName(s) { return "S" + (s.name.match(/\d+/) || [""])[0]; }
function nameOf(id) { return (S.byId[id] && S.byId[id].name) || id; }
function capacityLabel(s) {
    const ws = [...new Set(S.servers.map((x) => x.weight))].sort((a, b) => a - b);
    if (ws.length === 1) return "Standard";
    if (s.weight === ws[ws.length - 1]) return "High";
    if (s.weight === ws[0]) return "Low";
    return "Medium";
}
function listNames(names) {
    if (names.length <= 1) return names.join("");
    return names.slice(0, -1).join(", ") + " and " + names[names.length - 1];
}

/* ------------------------------------------------------------------ */
/* Server state helpers                                                */
/* ------------------------------------------------------------------ */
function stateOf(s) {
    if (s.standby && s.status === "HEALTHY") return "stby";
    if (s.status === "UNHEALTHY") return "bad";
    if (s.status === "RECOVERING") return "rec";
    return "ok";
}
function stateLabel(s) {
    if (s.standby && s.status === "HEALTHY") return "STANDBY";
    if (s.status === "UNHEALTHY") return "FAILED";
    if (s.status === "RECOVERING") return `RECOVERING ${Math.min(s.consecutive_successes, CFG.recoveryThreshold)}/${CFG.recoveryThreshold}`;
    return "HEALTHY";
}
/** A failure was injected but the health monitor has not detected it yet. */
function faultPending(s) {
    return s.status === "HEALTHY" && !!s.failure_injected_at &&
        !(s.recovery_injected_at && s.recovery_injected_at > s.failure_injected_at) &&
        !(s.failure_detected_at && s.failure_detected_at >= s.failure_injected_at);
}
/** Recovery was triggered but the next health check has not happened yet. */
function recoveryPending(s) {
    return s.status === "UNHEALTHY" && !!s.recovery_injected_at && s.recovery_injected_at > (s.failure_injected_at || 0);
}

function systemState() {
    const sv = S.servers;
    const healthy = sv.filter((s) => s.status === "HEALTHY");
    // servers that actually receive new requests (HEALTHY, not draining, circuit not open)
    const routing = sv.filter((s) => (s.routable !== undefined ? s.routable : s.status === "HEALTHY"));
    const failed = sv.filter((s) => s.status === "UNHEALTHY");
    const rec = sv.filter((s) => s.status === "RECOVERING");
    if (!sv.length) return { cls: "", title: "CONNECTING", sub: "Waiting for the reverse proxy…" };
    const dr = S.status && S.status.disaster;
    if (dr && dr.state && dr.state !== "IDLE" && dr.state !== "RESTORED") {
        return { cls: "bad", title: dr.state === "DATA_LOSS_ACK_REQUIRED" ? "DATA LOSS" : "DISASTER RECOVERY", sub: dr.message };
    }
    if (healthy.length === 0) {
        const ar = S.status.auto_recovery || {};
        const cand = provisioningCandidate();
        if (cand && cand.stage !== "failed") {
            return { cls: "bad", title: "AUTO-RECOVERY",
                     sub: `No healthy backends — starting replacement ${cand.name}${cand.port ? " on port " + cand.port : ""}` };
        }
        if (ar.blocked_reason) return { cls: "bad", title: "OUTAGE", sub: "No healthy backends — auto-recovery limit reached" };
        if (ar.episode && ar.episode.stage === "failed") return { cls: "bad", title: "OUTAGE", sub: "No healthy backends — replacement provisioning failed" };
        return { cls: "bad", title: "OUTAGE", sub: "No healthy backends — proxy returns HTTP 503" };
    }
    if (failed.length) {
        return {
            cls: "warn", title: "DEGRADED",
            sub: `${listNames(failed.map((s) => s.name))} failed · proxy routing to ${listNames((routing.length ? routing : healthy).map((s) => s.name))}`,
        };
    }
    if (rec.length) {
        return {
            cls: "rec", title: "RECOVERING",
            sub: `${listNames(rec.map((s) => s.name))} on probation before returning to routing`,
        };
    }
    const pending = sv.filter(faultPending);
    if (pending.length) {
        return { cls: "ok", title: "OPERATIONAL", sub: `Fault on ${listNames(pending.map((s) => s.name))} — proxy has not detected it yet` };
    }
    const standby = sv.filter((s) => s.standby && s.status === "HEALTHY");
    if (standby.length) {
        return { cls: "ok", title: "OPERATIONAL", sub: `Low demand · routing to ${listNames(routing.map((s) => s.name))} · ${listNames(standby.map((s) => s.name))} on standby` };
    }
    return { cls: "ok", title: "OPERATIONAL", sub: `Proxy routing to all ${sv.length} backends` };
}

/* ------------------------------------------------------------------ */
/* History / rates                                                     */
/* ------------------------------------------------------------------ */
function pushHistory(st) {
    const counts = {};
    st.servers.forEach((s) => { counts[s.id] = s.requests_served; });
    const last = S.hist[S.hist.length - 1];
    if (last && st.metrics.total_requests < last.total) {
        S.hist = [];      // counters were reset
        resetChartData();
    }
    S.hist.push({ t: performance.now(), counts, ok: st.metrics.successful_requests, fail: st.metrics.failed_requests, total: st.metrics.total_requests });
    const cutoff = performance.now() - 130000;
    while (S.hist.length > 2 && S.hist[0].t < cutoff) S.hist.shift();
}

function deltaOver(sec) {
    const n = S.hist.length;
    if (n < 2) return null;
    const now = S.hist[n - 1];
    const target = now.t - sec * 1000;
    let base = S.hist[0];
    for (let i = n - 2; i >= 0; i--) { if (S.hist[i].t <= target) { base = S.hist[i]; break; } }
    const dt = (now.t - base.t) / 1000;
    if (dt <= 0) return null;
    const per = {};
    let sum = 0;
    Object.keys(now.counts).forEach((id) => {
        per[id] = Math.max(0, now.counts[id] - (base.counts[id] || 0));
        sum += per[id];
    });
    return { dt, per, sum, ok: now.ok - base.ok, fail: now.fail - base.fail };
}

/**
 * Share of recent traffic per server. Servers outside the active pool receive no
 * new requests, so their share is 0 and the remaining shares are normalised over
 * the servers that are actually in the pool.
 */
function shares(sec) {
    const d = deltaOver(sec);
    const out = {};
    const inPool = (s) => s.status === "HEALTHY";
    const sum = d ? S.servers.filter(inPool).reduce((a, s) => a + d.per[s.id], 0) : 0;
    S.servers.forEach((s) => { out[s.id] = sum > 0 && inPool(s) ? d.per[s.id] / sum : 0; });
    return { share: out, active: sum > 0, d };
}

function expectedShares() {
    const algo = S.status ? S.status.active_algorithm : "ROUND_ROBIN";
    if (!STATIC_SHARE[algo]) return null;      // no fixed expected share for dynamic strategies
    const inPool = (s) => (s.routable !== undefined ? s.routable : s.status === "HEALTHY");
    const pool = S.servers.filter(inPool);
    const out = {};
    const totalW = pool.reduce((a, s) => a + s.weight, 0);
    S.servers.forEach((s) => {
        if (!inPool(s)) out[s.id] = 0;
        else out[s.id] = algo === "WEIGHTED_ROUND_ROBIN" ? (totalW ? s.weight / totalW : 0) : 1 / pool.length;
    });
    return out;
}

/* ------------------------------------------------------------------ */
/* Event humanisation                                                  */
/* ------------------------------------------------------------------ */
function dn(e) { return (e.details && e.details.backend_name) || ""; }
/** "desired 3 · 1 routable · 36.0 req/s · reason" from an event's structured details. */
function capDetail(e, fallback) {
    const d = e.details;
    if (!d) return fallback || "";
    const parts = [];
    if (d.desired != null) parts.push(`desired ${d.desired}`);
    if (d.previous_healthy != null) parts.push(`${d.previous_healthy} routable`);
    if (d.observed_rps != null) parts.push(`${fmtNum(d.observed_rps, 1)} req/s`);
    if (d.reason) parts.push(d.reason);
    return parts.join(" · ");
}
function humanize(e) {
    const m = e.message || "";
    const server = (m.match(/Server \d+/) || [""])[0];
    const ms = (re) => { const x = m.match(re); return x ? fmtMs(parseFloat(x[1])) + " ms" : ""; };
    switch (e.event_type) {
        case "HEALTH_MONITOR":
            if (/started/.test(m)) return { cls: "ok", icon: "✓", text: "Health monitoring started", detail: `GET /health every ${fmtNum(CFG.healthInterval, 1)} s` };
            return { cls: "neutral", icon: "●", text: m };
        case "HEALTH_OK":
            return { cls: "ok", icon: "✓", text: `${server} healthy`, detail: "health check passed", server };
        case "FAILURE_INJECTED":
            return { cls: "bad", icon: "✕", text: `Failure injected into ${server}`, server };
        case "FAILURE_DETECTED":
            return { cls: "bad", icon: "⚠", text: `${server} failure detected by proxy — removed from routing`, detail: ms(/Detection time: ([\d.]+) ms/) ? "detected in " + ms(/Detection time: ([\d.]+) ms/) : "", server };
        case "FAILOVER_TRIGGERED": {
            const code = (m.match(/HTTP (\d+)/) || [])[1];
            return { cls: "warn", icon: "↪", text: `Request to ${server} failed${code ? " (HTTP " + code + ")" : ""} — proxy retried it on a healthy server`, server };
        }
        case "FAILOVER": {
            const sid = (m.match(/redirected to (\S+) in/) || [])[1];
            return { cls: "info", icon: "↪", text: `Traffic redirected — proxy sent the next request to ${nameOf(sid)}`, detail: ms(/in ([\d.]+) ms/) ? ms(/in ([\d.]+) ms/) + " after detection" : "" };
        }
        case "RECOVERY_TRIGGERED":
            return { cls: "warn", icon: "●", text: `Recovery started on ${server}`, server };
        case "RECOVERY_DETECTED":
            return { cls: "warn", icon: "●", text: `${server} responding again — on probation`, detail: `needs ${CFG.recoveryThreshold} consecutive passing checks`, server };
        case "READMISSION":
            return { cls: "ok", icon: "✓", text: `${server} recovered — back in routing`, detail: ms(/Verification time: ([\d.]+) ms/) ? "verified in " + ms(/Verification time: ([\d.]+) ms/) : "", server };
        case "CONFIG": {
            const a = (m.match(/to (\w+)/) || [])[1];
            return { cls: "info", icon: "⇄", text: `Routing strategy set to ${ALGO_NAMES[a] || a}` };
        }
        case "NO_HEALTHY_BACKENDS":
            return { cls: "bad", icon: "✕", text: "No healthy backend servers available", detail: (m.match(/\(([^)]+)\)/) || [])[1] || "" };
        case "AUTO_RECOVERY_STARTED":
            return { cls: "warn", icon: "⚙", text: "Automatic backend recovery initiated" };
        case "PROVISIONING":
            return { cls: "warn", icon: "⚙", text: `Provisioning replacement backend ${server}`, detail: (m.match(/port \d+/) || [""])[0], server };
        case "REPLACEMENT_STARTED": {
            const pid = (m.match(/PID (\d+)/) || [])[1];
            return { cls: "warn", icon: "⚙", text: `${server} started on ${(m.match(/port \d+/) || ["a new port"])[0]}`, detail: pid ? `new process, PID ${pid}` : "", server };
        }
        case "REPLACEMENT_HEALTHY":
            return { cls: "ok", icon: "✓", text: `${server} health check passed`, detail: (m.match(/\((\d+\/\d+) consecutive/) || [])[1] ? (m.match(/\((\d+\/\d+) consecutive/))[1] + " consecutive" : "", server };
        case "REPLACEMENT_REGISTERED":
            return { cls: "info", icon: "↻", text: `${server} registered with reverse proxy`, detail: "replacement backend", server };
        case "REPLACEMENT_ADDED":
            return { cls: "ok", icon: "✓", text: `${server} added to routing pool`, detail: (m.match(/\d+\/\d+ healthy backends/) || [""])[0], server };
        case "TRAFFIC_RESTORED":
            return { cls: "ok", icon: "✓", text: "Traffic successfully restored", detail: server ? `request served by ${server}` : "" };
        case "AUTO_RECOVERY_FAILED":
            return { cls: "bad", icon: "✕", text: "Replacement provisioning failed", detail: (m.match(/failed: (.*?)\. Server/) || [])[1] || "" };
        case "AUTO_RECOVERY_LIMIT":
            return { cls: "bad", icon: "⚠", text: /limit reached/.test(m) ? "Auto-recovery limit reached — no further replacements" : "Auto-recovery stopped after failed attempts" };
        case "CAPACITY_EVALUATION_STARTED":
            return { cls: "info", icon: "◔", text: "Capacity evaluation started", detail: capDetail(e, m.replace(/^[^:;]*[:;]\s*/, "")) };
        case "CAPACITY_TARGET_UPDATED":
            return { cls: "info", icon: "⊕", text: `Capacity target ${e.details && e.details.previous_target != null ? e.details.previous_target + " → " : "set to "}${e.details ? e.details.desired : "?"} backend(s)`, detail: capDetail(e) };
        case "SCALE_OUT_STARTED":
            return { cls: "warn", icon: "⇡", text: `Scale-out started${dn(e) ? " — " + dn(e) : ""}`, detail: capDetail(e) };
        case "REPLACEMENT_PROVISIONED":
            return { cls: "warn", icon: "⚙", text: `${dn(e) || server} process started${e.details && e.details.port ? " on port " + e.details.port : ""}`, detail: "not in routing yet · health checks next", server };
        case "BACKEND_REGISTERED":
            return { cls: "ok", icon: "✓", text: `${dn(e) || server} registered — now in routing pool`, detail: e.details ? `routable ${e.details.previous_healthy} → ${e.details.routable_after} · desired ${e.details.desired} · ${fmtNum(e.details.observed_rps, 1)} req/s` : "", server };
        case "SCALE_IN_STARTED":
            return { cls: "info", icon: "⇣", text: `Scale-in started — retiring ${dn(e)}`, detail: capDetail(e) };
        case "BACKEND_DRAINING":
            return { cls: "warn", icon: "◌", text: `${dn(e)} draining — no new requests`, detail: e.details ? `${e.details.in_flight} in flight` : "" };
        case "BACKEND_RETIRED":
            return { cls: e.level === "ERROR" ? "bad" : "neutral", icon: "⊖", text: `${dn(e) || server} retired — removed from routing, process stopped`, detail: e.details ? (e.details.reason || "") + (e.details.drain_ms != null ? ` · drained in ${fmtNum(e.details.drain_ms)} ms` : "") + (e.details.previous_healthy != null ? ` · ${e.details.previous_healthy} routable` : "") : "" };
        case "BACKEND_STANDBY":
            return { cls: "info", icon: "◑", text: `${dn(e) || server} on standby — process kept running, no requests`, detail: e.details ? `low demand · ${e.details.previous_healthy} routable` : "" };
        case "BACKEND_ACTIVATED":
            return { cls: "ok", icon: "◕", text: `${dn(e) || server} back from standby — in routing pool`, detail: e.details ? (e.details.reason || "") + (e.details.observed_rps != null ? ` · ${fmtNum(e.details.observed_rps, 1)} req/s` : "") : "" };
        case "CAPACITY_STABILIZED":
            return { cls: "ok", icon: "✓", text: `Capacity stabilized at ${e.details ? e.details.previous_healthy : "?"} routable backend(s)`, detail: capDetail(e) };
        case "SYSTEM":
            if (/Reverse proxy started/.test(m)) return { cls: "ok", icon: "✓", text: "Reverse proxy started", detail: `port ${CFG.proxyPort}` };
            if (/Simulation reset/.test(m)) return { cls: "neutral", icon: "↻", text: "Simulation reset", detail: (m.match(/\d+\/\d+ servers healthy/) || [""])[0] };
            return { cls: "neutral", icon: "↻", text: m };
        default:
            return { cls: e.level === "ERROR" ? "bad" : "neutral", icon: "●", text: m };
    }
}

/** Collapse consecutive identical events (e.g. several in-flight retries) into one row. */
function groupEvents(events) {
    const out = [];
    events.forEach((e) => {
        const h = humanize(e);
        const prev = out[out.length - 1];
        if (prev && prev.h.text === h.text && prev.e.event_type === e.event_type) {
            prev.count += 1;
            prev.e = e;            // keep the latest timestamp
        } else {
            out.push({ e, h, count: 1 });
        }
    });
    return out;
}

function renderEvents(el, events, limit) {
    const groups = groupEvents(events).reverse().slice(0, limit);
    if (!groups.length) {
        el.innerHTML = `<li class="ev-empty">No events yet.</li>`;
        return;
    }
    el.innerHTML = groups.map(({ e, h, count }) => `
        <li class="ev ${h.cls}">
            <span class="ev-time">${esc(e.timestamp)}</span>
            <span class="ev-ico">${h.icon}</span>
            <span class="ev-msg">${esc(h.text)}${h.detail ? ` <span class="ev-detail">· ${esc(h.detail)}</span>` : ""}</span>
            <span class="ev-count">${count > 1 ? "×" + count : ""}</span>
        </li>`).join("");
}

/* ------------------------------------------------------------------ */
/* Topology (SVG)                                                      */
/* ------------------------------------------------------------------ */
const TOPO = { W: 880, H: 446, clientY: 14, clientH: 54, lbY: 112, lbH: 100, busY: 258, srvY: 312, srvW: 236, srvH: 104, slotMin: 170 };
const ROLE_TAG = {
    original: ["ORIGINAL", "ORIGINAL"],
    recovery: ["RECOVERY REPLACEMENT", "RECOVERY"],
    capacity: ["SCALE-OUT REPLACEMENT", "SCALE-OUT"],
};
const CAND_TAG = { recovery: ["RECOVERY · NEW BACKEND", "RECOVERY"], capacity: ["SCALE-OUT · NEW BACKEND", "SCALE-OUT"] };

/**
 * Backend nodes of the diagram, taken from the proxy's topology view (/api/status -> topology):
 * registered servers (live registry data), replacements being provisioned, and replacements
 * retired in the last few seconds. Nothing is added on the client side.
 */
function topoNodes() {
    const topo = S.status && S.status.topology;
    if (!topo) return S.servers.slice();
    return topo.nodes.map((n) => {
        if (n.kind === "server" && S.byId[n.id]) return Object.assign({}, S.byId[n.id], { kind: "server", role: n.role, routable: n.routable, eligibility: n.eligibility });
        return Object.assign({ pseudo: true, weight: 0 }, n);
    });
}
/** Replacement being provisioned (stage-1 recovery or scale-out), if any. */
function provisioningCandidate() {
    return topoNodes().find((n) => n.kind === "candidate") || null;
}

class Topology {
    constructor(el, mode) {
        this.el = el;
        this.mode = mode;
        this.built = false;
        this.sig = "";
        this.packets = 0;
    }

    build(nodesIn) {
        const T = TOPO;
        const n = Math.max(1, nodesIn.length);
        const W = this.W = Math.max(T.W, 40 + n * T.slotMin);
        const cx = this.cx = W / 2;
        const gap = (W - 40) / n;
        const NW = this.srvW = Math.min(T.srvW, gap - 14);
        const narrow = NW < 220;
        this.xs = nodesIn.map((_, i) => 20 + gap * i + gap / 2);
        const linked = nodesIn.map((s) => s.kind !== "retired");
        const nl = linked.filter(Boolean).length;
        const fan = Math.min(64, 300 / Math.max(1, nl - 1));
        let links = "", nodes = "", cuts = "", labels = "";

        links += `<path class="t-link" data-link="client" d="M${cx} ${T.clientY + T.clientH} V${T.lbY - 6}"/>`;
        links += `<path class="t-arrow" data-arrow="client" d="M${cx - 5} ${T.lbY - 8} L${cx + 5} ${T.lbY - 8} L${cx} ${T.lbY - 1}Z"/>`;

        let li = 0;
        nodesIn.forEach((s, i) => {
            const x = this.xs[i];
            const x0 = x - NW / 2, y = T.srvY;
            const my = T.busY + (T.srvY - T.busY) / 2 - 2;
            if (linked[i]) {
                const sx = cx + (li - (nl - 1) / 2) * fan;
                li++;
                const y0 = T.lbY + T.lbH, r = 10;
                let d;
                if (Math.abs(x - sx) < 1) d = `M${sx} ${y0} V${T.srvY - 6}`;
                else {
                    const dir = x > sx ? 1 : -1;
                    d = `M${sx} ${y0} V${T.busY - r} Q${sx} ${T.busY} ${sx + dir * r} ${T.busY} H${x - dir * r} Q${x} ${T.busY} ${x} ${T.busY + r} V${T.srvY - 6}`;
                }
                links += `<path class="t-link" data-link="${s.id}" d="${d}"/>`;
                links += `<path class="t-arrow" data-arrow="${s.id}" d="M${x - 5} ${T.srvY - 8} L${x + 5} ${T.srvY - 8} L${x} ${T.srvY - 1}Z"/>`;
                cuts += `<g class="t-cut" data-cut="${s.id}" style="display:none">
                    <circle cx="${x}" cy="${my}" r="9"/>
                    <path d="M${x - 3.5} ${my - 3.5} L${x + 3.5} ${my + 3.5} M${x + 3.5} ${my - 3.5} L${x - 3.5} ${my + 3.5}"/></g>`;
                labels += `<text class="t-linklabel" data-label="${s.id}" x="${x + 15}" y="${my + 4}"></text>`;
            }
            const tagSet = s.kind === "candidate" ? CAND_TAG[s.role] : ROLE_TAG[s.role || (s.is_replacement ? "recovery" : "original")];
            const tag = tagSet ? `<text class="t-tag ${s.role === "capacity" ? "scale" : s.role === "original" ? "orig" : ""}" x="${x0 + 2}" y="${y - 8}">${narrow ? tagSet[1] : tagSet[0]}</text>` : "";
            const attrs = `data-node="${s.id}" data-kind="${s.kind || "server"}" data-role="${s.role || ""}"`;
            if (s.pseudo) {
                const retired = s.kind === "retired";
                nodes += `<g class="t-node t-server ${retired ? "retired" : "cand rec"}" ${attrs}>
                    ${tag}
                    <rect class="box" x="${x0}" y="${y}" width="${NW}" height="${T.srvH}" rx="7"/>
                    <text class="t-title" x="${x0 + 16}" y="${y + 26}">${esc(s.name)}</text>
                    <text class="t-meta mono" data-f="port" x="${x0 + NW - 16}" y="${y + 26}" text-anchor="end"></text>
                    <circle class="t-dot" cx="${x0 + 20}" cy="${y + 45}" r="4"/>
                    <text class="t-state" data-f="state" x="${x0 + 30}" y="${y + 49}"></text>
                    <text class="t-meta" data-f="what" x="${x0 + 16}" y="${y + 75}"></text>
                    <text class="t-meta mono" data-f="pid" x="${x0 + NW - 16}" y="${y + 75}" text-anchor="end"></text>
                    <text class="t-pending" data-f="pending" x="${x}" y="${y + T.srvH + 20}" text-anchor="middle"></text>
                </g>`;
                return;
            }
            const cap = narrow ? `w${s.weight}` : `${esc(capacityLabel(s))} capacity · w${s.weight}`;
            nodes += `<g class="t-node t-server ok" ${attrs}>
                ${tag}
                <rect class="box" x="${x0}" y="${y}" width="${NW}" height="${T.srvH}" rx="7"/>
                <text class="t-title" x="${x0 + 16}" y="${y + 26}">${esc(s.name)}</text>
                <text class="t-meta" x="${x0 + NW - 16}" y="${y + 26}" text-anchor="end">${cap}</text>
                <circle class="t-dot" cx="${x0 + 20}" cy="${y + 45}" r="4"/>
                <text class="t-state" data-f="state" x="${x0 + 30}" y="${y + 49}">HEALTHY</text>
                <text class="t-meta mono" x="${x0 + NW - 16}" y="${y + 49}" text-anchor="end">:${s.port}</text>
                <text class="t-meta" x="${x0 + 16}" y="${y + 75}">Load</text>
                <text class="t-strong" data-f="load" x="${x0 + NW - 16}" y="${y + 75}" text-anchor="end">0%</text>
                <rect class="t-bar-bg" x="${x0 + 16}" y="${y + 84}" width="${NW - 32}" height="5" rx="2.5"/>
                <rect class="t-bar" data-f="bar" x="${x0 + 16}" y="${y + 84}" width="0" height="5" rx="2.5"/>
                <text class="t-pending" data-f="pending" x="${x}" y="${y + T.srvH + 20}" text-anchor="middle"></text>
            </g>`;
        });

        const cw = 236, lw = 372;
        const client = `<g class="t-node t-client" data-node="client">
            <rect class="box" x="${cx - cw / 2}" y="${T.clientY}" width="${cw}" height="${T.clientH}" rx="7"/>
            <text class="t-title" x="${cx}" y="${T.clientY + 25}" text-anchor="middle">Client</text>
            <text class="t-sub" data-f="client-sub" x="${cx}" y="${T.clientY + 44}" text-anchor="middle">Traffic generator idle</text>
        </g>`;
        const lx = cx - lw / 2, ly = T.lbY;
        const lb = `<g class="t-node t-lb" data-node="lb">
            <rect class="box" x="${lx}" y="${ly}" width="${lw}" height="${T.lbH}" rx="7"/>
            <text class="t-proxy-title" x="${lx + 18}" y="${ly + 26}">REVERSE PROXY</text>
            <circle class="t-online-dot" data-f="online-dot" cx="${lx + lw - 70}" cy="${ly + 22}" r="4"/>
            <text class="t-online" data-f="online" x="${lx + lw - 18}" y="${ly + 26}" text-anchor="end">Online</text>
            <text class="t-sub" data-f="lb-sub" x="${lx + 18}" y="${ly + 48}"></text>
            <text class="t-sub" x="${lx + lw - 18}" y="${ly + 48}" text-anchor="end">In routing pool <tspan class="t-healthy" data-f="healthy">–</tspan></text>
            <line class="t-divider" x1="${lx + 18}" x2="${lx + lw - 18}" y1="${ly + 63}" y2="${ly + 63}"/>
            <text class="t-role" data-f="role" x="${cx}" y="${ly + 84}" text-anchor="middle">Load balancing  ·  Health monitoring  ·  Failover routing</text>
        </g>`;

        // > 5 nodes: keep each node at least ~115 px wide on screen; the panel scrolls sideways if needed
        const minW = nodesIn.length > 5 ? nodesIn.length * 115 : 0;
        this.el.innerHTML = `<svg class="${narrow ? "narrow" : ""}" viewBox="0 0 ${W} ${T.H}" style="${minW ? `min-width:${minW}px;max-height:none` : ""}" role="img" aria-label="Network topology: client, reverse proxy and ${nodesIn.length} backend nodes">
            <g class="links">${links}</g>
            <g class="packets"></g>
            ${cuts}${labels}
            ${client}${lb}${nodes}
        </svg>`;
        this.svg = $("svg", this.el);
        this.packetLayer = $(".packets", this.svg);
        this.packets = 0;
        this.built = true;
    }

    update() {
        if (!S.servers.length) return;
        const all = topoNodes();
        const sig = all.map((n) => `${n.id}:${n.kind || "server"}:${n.role || ""}:${n.pseudo ? "p" : n.weight}`).join("|")
            + "|" + S.servers.map(capacityLabel).join(",");
        if (!this.built || sig !== this.sig) { this.build(all); this.sig = sig; }
        const st = S.status;
        const sh = shares(LOAD_WINDOW);
        const recent = deltaOver(2);
        const exp = expectedShares();
        const registered = all.filter((n) => !n.pseudo);
        const pool = registered.filter((s) => s.routable !== undefined ? s.routable : s.status === "HEALTHY").length;

        // client + proxy
        const gen = st.traffic && st.traffic.running;
        const clientActive = S.traffic.running || gen || (recent && recent.ok + recent.fail > 0);
        $('[data-node="client"]', this.svg).classList.toggle("active", !!clientActive);
        $('[data-f="client-sub"]', this.svg).textContent = S.traffic.running
            ? `${S.traffic.rate} requests/s → proxy :${CFG.proxyPort}`
            : gen ? `${fmtNum(st.traffic.params.rps)} requests/s (generator) → :${CFG.proxyPort}`
            : clientActive ? `Requests → proxy :${CFG.proxyPort}` : "Idle · start traffic";
        $('[data-link="client"]', this.svg).setAttribute("class", "t-link" + (clientActive ? " active" : ""));
        $('[data-arrow="client"]', this.svg).setAttribute("class", "t-arrow" + (clientActive ? " active" : ""));
        $('[data-f="lb-sub"]', this.svg).textContent = `Port ${CFG.proxyPort} · ${ALGO_SHORT[st.active_algorithm] || st.active_algorithm}`;
        const online = st.load_balancer === "RUNNING";
        $('[data-f="online"]', this.svg).textContent = online ? "Online" : "Offline";
        $('[data-f="online"]', this.svg).setAttribute("class", "t-online" + (online ? "" : " off"));
        $('[data-f="online-dot"]', this.svg).setAttribute("class", "t-online-dot" + (online ? "" : " off"));
        const hEl = $('[data-f="healthy"]', this.svg);
        hEl.textContent = `${pool}/${registered.length}`;
        const onStandby = registered.filter((n) => n.standby && n.status === "HEALTHY").length;
        hEl.setAttribute("class", "t-healthy" + (pool + onStandby >= registered.length ? "" : pool === 0 ? " bad" : " warn"));
        const cand = all.find((n) => n.kind === "candidate");
        const draining = registered.find((n) => n.draining);
        const cap = st.capacity || {};
        const role = $('[data-f="role"]', this.svg);
        let roleText = "Load balancing  ·  Health monitoring  ·  Failover routing", roleCls = "t-role";
        if (cand && cand.state !== "FAILED") {
            roleText = cand.role === "capacity"
                ? `Scale-out: starting ${cand.name} (desired ${cap.desired}, routable ${pool})`
                : `Auto-recovery: provisioning ${cand.name}`;
            roleCls += " warn";
        } else if (draining) {
            roleText = `Scale-in: draining ${draining.name} (desired ${cap.desired}, routable ${pool})`;
            roleCls += " warn";
        } else if (cap.scale_in_candidate && cap.scale_in_eta_s != null) {
            const toStandby = S.byId[cap.scale_in_candidate.id] && !S.byId[cap.scale_in_candidate.id].is_replacement;
            roleText = `Low demand: ${cap.scale_in_candidate.name} not needed — ${toStandby ? "standby" : "scale-in"} in ${Math.ceil(cap.scale_in_eta_s)} s`;
            roleCls += " warn";
        } else if (cap.scale_out_eta_s != null && cap.desired > pool) {
            roleText = `High demand: needs ${cap.desired} servers — scale-out in ${Math.ceil(cap.scale_out_eta_s)} s`;
            roleCls += " warn";
        } else if (cap.phase === "COOLDOWN" && cap.cooldown_remaining_s > 0) {
            roleText = `Capacity: cooldown after ${cap.cooldown_kind} (${Math.ceil(cap.cooldown_remaining_s)} s) · desired ${cap.desired} · routable ${pool}`;
        } else if (pool === 0) {
            roleText = "No routable backends — requests receive HTTP 503";
            roleCls += " bad";
        } else if (cap.hold && cap.demand_rps > 0) {
            roleText = `Desired ${cap.desired} · ${pool} kept: ${cap.routable - 1} would run at ${Math.round(cap.hold.utilization_after * 100)}% (scale-in only below ${Math.round(cap.hold.threshold * 100)}%)`;
        } else if (cap.enabled && cap.desired != null && cap.enough_window && cap.demand_rps > 0) {
            roleText = `Capacity: ${fmtNum(cap.demand_rps, 1)} req/s → desired ${cap.desired} · routable ${pool}`;
            if (cap.desired > pool) roleCls += " warn";
        }
        role.textContent = roleText;
        role.setAttribute("class", roleCls);

        all.forEach((s) => {
            const node = $(`[data-node="${s.id}"]`, this.svg);
            if (!node) return;
            const link = $(`[data-link="${s.id}"]`, this.svg);
            const arrow = $(`[data-arrow="${s.id}"]`, this.svg);
            const lbl = $(`[data-label="${s.id}"]`, this.svg);
            const cut = $(`[data-cut="${s.id}"]`, this.svg);
            node.setAttribute("data-routable", s.routable ? "1" : "0");
            if (s.kind === "retired") {
                node.setAttribute("class", "t-node t-server retired");
                $('[data-f="state"]', node).textContent = "RETIRED";
                $('[data-f="port"]', node).textContent = s.port ? `:${s.port}` : "";
                $('[data-f="what"]', node).textContent = "Process stopped";
                $('[data-f="pid"]', node).textContent = "";
                $('[data-f="pending"]', node).textContent = this.srvW < 220 ? "Removed from routing" : "Drained · removed from routing";
                return;
            }
            if (s.pseudo) {
                const failed = s.state === "FAILED";
                node.setAttribute("class", `t-node t-server cand ${failed ? "bad" : "rec"}`);
                $('[data-f="state"]', node).textContent = s.state;
                $('[data-f="port"]', node).textContent = s.port ? `:${s.port}` : "";
                // narrow nodes: the PID replaces the label so the two texts never overlap
                const narrowNode = this.srvW < 220;
                $('[data-f="what"]', node).textContent = narrowNode ? (s.pid ? `PID ${s.pid}` : "New backend") : "New backend process";
                $('[data-f="pid"]', node).textContent = !narrowNode && s.pid ? `PID ${s.pid}` : "";
                $('[data-f="pending"]', node).textContent = failed ? "Provisioning failed · not registered" : "Not registered · no traffic";
                link.setAttribute("class", `t-link ${failed ? "bad" : "rec"}`);
                arrow.setAttribute("class", `t-arrow ${failed ? "bad" : "rec"}`);
                cut.style.display = failed ? "" : "none";
                lbl.textContent = failed ? "" : "not routed";
                lbl.setAttribute("class", "t-linklabel " + (failed ? "bad" : "rec"));
                return;
            }
            const state = s.draining ? "rec" : stateOf(s);
            const short = this.srvW < 220;
            node.setAttribute("class", `t-node t-server ${state}${s.draining ? " draining" : ""}${state === "stby" ? " standby" : ""}`);
            // narrow nodes: the check count moves to the caption so the state never overlaps the port
            $('[data-f="state"]', node).textContent = s.draining ? "DRAINING" : (short && s.status === "RECOVERING") ? "RECOVERING" : stateLabel(s);
            const pct = Math.round(sh.share[s.id] * 100);
            $('[data-f="load"]', node).textContent = sh.active ? pct + "%" : "0%";
            $('[data-f="bar"]', node).setAttribute("width", ((this.srvW - 32) * (sh.active ? sh.share[s.id] : 0)).toFixed(1));

            let pending = "", pcls = "t-pending";
            if (faultPending(s)) pending = short ? "Awaiting health check" : "Fault injected · awaiting health check";
            else if (recoveryPending(s)) pending = short ? "Awaiting health check" : "Recovery started · awaiting health check";
            else if (s.draining) pending = short ? "Draining · no new requests" : `Draining · ${s.active_connections} in flight · no new requests`;
            else if (state === "stby") { pending = short ? "Standby · not routed" : "Standby · returns when load rises"; pcls += " stby"; }
            else if (s.status === "RECOVERING") pending = short ? `Probation ${Math.min(s.consecutive_successes, CFG.recoveryThreshold)}/${CFG.recoveryThreshold} · no traffic` : "Probation · no traffic until verified";
            else if (s.status === "UNHEALTHY") { pending = short ? "Not routable" : "Not routable · removed from pool"; pcls += " bad"; }
            else if (s.routable === false) { pending = short ? "Circuit open" : "Circuit open · skipped by proxy"; pcls += " bad"; }
            else if (cap.scale_in_candidate && cap.scale_in_candidate.id === s.id && cap.scale_in_eta_s != null) {
                const verb = s.is_replacement ? "retiring" : "standby";
                pending = short ? `${verb === "standby" ? "Standby" : "Retiring"} in ${Math.ceil(cap.scale_in_eta_s)} s` : `Not needed · ${verb} in ${Math.ceil(cap.scale_in_eta_s)} s`;
            }
            else { pending = short ? "✓ Routable" : "✓ In routing pool"; pcls += " okay"; }
            const pe = $('[data-f="pending"]', node);
            pe.textContent = pending;
            pe.setAttribute("class", pcls);

            const receiving = recent && recent.per[s.id] > 0;
            let linkCls = "t-link";
            if (state === "bad") linkCls += " bad";
            else if (s.draining) linkCls += " drain";
            else if (state === "stby") linkCls += " standby";
            else if (state === "rec") linkCls += " rec";
            else if (receiving) linkCls += " active";
            link.setAttribute("class", linkCls);
            arrow.setAttribute("class", linkCls.replace("t-link", "t-arrow"));
            cut.style.display = state === "bad" ? "" : "none";

            let text = "", lcls = "t-linklabel";
            if (state === "bad") { text = short ? "" : "removed from routing"; lcls += " bad"; }
            else if (s.draining) { text = short ? "" : "draining"; lcls += " rec"; }
            else if (state === "stby") { text = short ? "" : "standby"; lcls += " stby"; }
            else if (state === "rec") { lcls += " rec"; }
            else if (this.mode === "lb" || this.mode === "cap") {
                text = sh.active ? Math.round(sh.share[s.id] * 100) + "%"
                    : exp ? Math.round(exp[s.id] * 100) + "%" + (short ? "" : " expected") : "";
            }
            lbl.textContent = text;
            lbl.setAttribute("class", lcls);
        });
    }

    /** Replay a real routed request along the path it actually took. */
    packet(sid, ok) {
        if (!this.built || this.packets > 40 || this.el.offsetParent === null) return;
        const p1 = $('[data-link="client"]', this.svg);
        const p2 = sid ? $(`[data-link="${sid}"]`, this.svg) : null;
        if (!p1) return;
        const c = document.createElementNS("http://www.w3.org/2000/svg", "circle");
        c.setAttribute("r", "4.5");
        c.setAttribute("class", "t-packet" + (ok ? "" : " err"));
        this.packetLayer.appendChild(c);
        this.packets++;
        const L1 = p1.getTotalLength(), L2 = p2 ? p2.getTotalLength() : 0;
        const d1 = 260, d2 = 520;
        const t0 = performance.now();
        const step = (now) => {
            const t = now - t0;
            let pt;
            if (t < d1) pt = p1.getPointAtLength(L1 * (t / d1));
            else if (p2 && t < d1 + d2) {
                const k = (t - d1) / d2;
                pt = p2.getPointAtLength(L2 * (1 - Math.pow(1 - k, 2)));
            } else if (!p2 && t < d1 + 260) {
                pt = p1.getPointAtLength(L1);
                c.setAttribute("opacity", String(1 - (t - d1) / 260));
            } else {
                c.remove();
                this.packets--;
                return;
            }
            c.setAttribute("cx", pt.x.toFixed(1));
            c.setAttribute("cy", pt.y.toFixed(1));
            requestAnimationFrame(step);
        };
        requestAnimationFrame(step);
    }
}

/* ------------------------------------------------------------------ */
/* Rendering                                                           */
/* ------------------------------------------------------------------ */
const ICON_OK = `<svg class="ss-icon" viewBox="0 0 18 18"><circle cx="9" cy="9" r="8" fill="var(--green-soft)"/><circle cx="9" cy="9" r="4" fill="var(--green)"/></svg>`;
const ICON_WARN = `<svg class="ss-icon" viewBox="0 0 18 18"><path d="M9 1.8 17 15.6H1z" fill="var(--amber)"/><path d="M9 6.6v4.2M9 12.8v.4" stroke="#1a1408" stroke-width="1.7" stroke-linecap="round"/></svg>`;
const ICON_REC = `<svg class="ss-icon" viewBox="0 0 18 18"><circle cx="9" cy="9" r="8" fill="var(--amber-soft)"/><circle cx="9" cy="9" r="4" fill="var(--amber)"/></svg>`;
const ICON_BAD = `<svg class="ss-icon" viewBox="0 0 18 18"><circle cx="9" cy="9" r="8" fill="var(--red)"/><path d="m6 6 6 6M12 6l-6 6" stroke="#fff" stroke-width="1.7" stroke-linecap="round"/></svg>`;

function renderStatus() {
    const s = systemState();
    const icon = { ok: ICON_OK, warn: ICON_WARN, rec: ICON_REC, bad: ICON_BAD }[s.cls] || ICON_REC;
    const html = `${icon}<div><div class="ss-title">${s.title}</div><div class="ss-sub">${esc(s.sub)}</div></div>`;
    $$("[data-status]").forEach((el) => {
        el.className = "sys-status " + s.cls;
        if (el.innerHTML !== html) el.innerHTML = html;
    });
}

function renderOverview() {
    const m = S.status.metrics;
    $("#ov-total").textContent = fmtNum(m.total_requests);
    $("#ov-total-sub").textContent = m.total_requests === 0 ? "none yet" : m.failed_requests ? `${fmtNum(m.failed_requests)} failed (HTTP 503)` : "all served successfully";
    const d = deltaOver(5);
    $("#ov-rate").textContent = d ? fmtNum((d.ok + d.fail) / d.dt, 1) : "0";
    $("#ov-healthy").textContent = S.status.active_server_count;
    $("#ov-healthy-of").textContent = "/ " + S.status.total_server_count;
    $("#ov-latency").textContent = m.total_requests ? fmtMs(m.avg_response_latency_ms) : "–";
    renderEvents($("#ov-events"), S.status.events || [], 5);
}

function renderLoadBalancing() {
    const st = S.status;
    const algo = st.active_algorithm;
    $$(".algo-switch button").forEach((b) => b.classList.toggle("on", b.dataset.algo === algo));

    const sh = shares(LOAD_WINDOW);
    const exp = expectedShares();
    const lat = st.metrics.avg_latency_per_server || {};
    const cumTotal = S.servers.reduce((a, s) => a + s.requests_served, 0);
    const live = sh.active;
    const hasAny = live || cumTotal > 0;
    $("#lb-table").innerHTML = S.servers.map((s) => {
        const state = stateOf(s);
        const pct = live ? sh.share[s.id] : (cumTotal ? s.requests_served / cumTotal : 0);
        return `<tr class="${state === "bad" ? "row-bad" : ""}">
            <td><span class="srv"><span class="dot ${state}"></span>${esc(s.name)}${s.is_replacement ? '<span class="repl-tag">REPLACEMENT</span>' : ""}</span></td>
            <td>${esc(capacityLabel(s))} <span class="sub">· weight ${s.weight}</span></td>
            <td class="num">${fmtNum(s.requests_served)}</td>
            <td><div class="share">
                <div class="share-track">
                    <div class="share-fill" style="width:${(pct * 100).toFixed(1)}%;background:${serverColor(s.id)}"></div>
                    ${exp ? `<div class="share-tick" style="left:calc(${(exp[s.id] * 100).toFixed(1)}% - 1px)"></div>` : ""}
                </div>
                <span class="share-pct">${hasAny ? Math.round(pct * 100) + "%" : "–"}</span>
            </div></td>
            <td class="num">${lat[s.id] !== undefined && s.status === "HEALTHY" ? fmtMs(lat[s.id]) + " ms" : "–"}</td>
        </tr>`;
    }).join("");
    $("#lb-dist-note").textContent = live ? "share: live, last 6 s" : cumTotal ? "share: all requests since reset" : "no requests yet";
    $("#lb-legend").innerHTML = exp
        ? `<span class="tick-demo"></span> expected share for ${esc(ALGO_NAMES[algo])} with the current pool`
        : `${esc(ALGO_NAMES[algo])} has no fixed expected share — it depends on ${algo === "IP_HASH" ? "the client IP addresses" : algo === "LEAST_RESPONSE_TIME" ? "measured response times" : "active connections"}`;

    renderSequence();
    renderHowTo(algo);
    renderStrategyInputs(algo);
    renderWorkload();
}

let seqRendered = -1;
function renderSequence() {
    if (seqRendered === S.routesVersion) return;
    seqRendered = S.routesVersion;
    const cells = [];
    const routes = S.routes.slice(-24);
    routes.forEach((r, i) => {
        const fresh = i === routes.length - 1 ? " new" : "";
        if (!r.ok) cells.push(`<div class="seq-cell err${fresh}" title="HTTP 503 — no server available">✕</div>`);
        else {
            const s = S.byId[r.sid];
            cells.push(`<div class="seq-cell${r.retries ? " retry" : ""}${fresh}" style="background:${serverColor(r.sid)}" title="${esc(s ? s.name : r.sid)}${r.retries ? " (after in-flight retry)" : ""}">${s ? shortName(s).slice(1) : "?"}</div>`);
        }
    });
    for (let i = routes.length; i < 24; i++) cells.push(`<div class="seq-cell empty"></div>`);
    $("#lb-seq").innerHTML = cells.join("");
    $("#lb-seq-empty").textContent = routes.length
        ? "Number = server that handled the request · outlined = retried after an in-flight failure · ✕ = rejected (503)"
        : "Start traffic to see each request's destination.";
}

let howtoAlgo = null;
function renderHowTo(algo) {
    const key = algo + S.servers.map((s) => s.weight).join(",");
    if (howtoAlgo === key) return;
    howtoAlgo = key;
    const order = S.servers.map(shortName).join(" → ") + " → " + shortName(S.servers[0]) + " …";
    const weights = S.servers.map((s) => `${shortName(s)} = ${s.weight}`).join(", ");
    const totalW = S.servers.reduce((a, s) => a + s.weight, 0);
    const per = S.servers.map((s) => `${shortName(s)} gets ${s.weight}`).join(", ");
    const LEADS = {
        WEIGHTED_ROUND_ROBIN: `<p><strong>Smooth Weighted Round Robin</strong> (as in Nginx): each server's weight is its capacity (${weights}). Each healthy server's counter grows by its weight; the highest is chosen and reduced by the total weight. Out of every ${totalW} requests, ${per}, interleaved rather than in bursts.</p>`,
        ROUND_ROBIN: `<p><strong>Round Robin:</strong> requests go to healthy servers in a fixed cycle (${order}). Capacity weights are not used, so each healthy server gets an equal share.</p>`,
        LEAST_CONNECTIONS: `<p><strong>Least Connections:</strong> each request goes to the healthy server with the fewest <em>active connections</em> — requests the proxy is forwarding to it right now. Weights are not used; when several servers are tied (for example all idle) they take turns.</p>`,
        WEIGHTED_LEAST_CONNECTIONS: `<p><strong>Weighted Least Connections:</strong> the proxy picks the lowest <code>active connections ÷ weight</code> (${weights}). A weight-3 server may hold three times the concurrent requests of a weight-1 server before it looks equally busy. Ties (for example all idle) are broken by Smooth Weighted Round Robin, so idle traffic follows the weights.</p>`,
        LEAST_RESPONSE_TIME: `<p><strong>Least Response Time:</strong> each request goes to the healthy server with the lowest measured response time — the average of its successful requests through the proxy in the last 10 s. A server without a recent measurement is tried first so it gets measured. The backends do identical work, so differences are small.</p>`,
        IP_HASH: `<p><strong>IP Hash (sticky):</strong> the client's IP address (<code>X-Forwarded-For</code> or the connection address) is hashed with each healthy server (rendezvous hashing) and the highest score wins, so a client always reaches the same server. If that server fails, only its clients move to another server; they move back when it recovers.</p>`,
    };
    const lead = LEADS[algo] || "";
    $("#lb-howto").innerHTML = `<p>Clients only ever talk to the <strong>reverse proxy</strong> on port ${CFG.proxyPort}. For each request it picks a backend, forwards the request to that server's <code>/process</code> endpoint and returns the response to the client.</p>` + lead + `<ol>
        <li>Only <strong>HEALTHY</strong> servers are eligible. The proxy probes <code>GET /health</code> every ${fmtNum(CFG.healthInterval, 1)} s and removes a server on the first failed probe.</li>
        <li>If the chosen server fails mid-request, the proxy retries on another healthy server (up to ${CFG.maxRetries} times).</li>
        <li>A recovered server rejoins only after ${CFG.recoveryThreshold} consecutive successful health checks.</li>
        <li>If no server is healthy, the proxy answers <code>HTTP 503</code>.</li>
    </ol>`;
}

/** Live routing inputs of the dynamic strategies, from /api/status (measured by the proxy). */
function renderStrategyInputs(algo) {
    let el = $("#lb-inputs");
    if (!el) {
        el = document.createElement("p");
        el.id = "lb-inputs";
        el.className = "lb-inputs";
        $("#lb-howto").after(el);
    }
    const pool = S.servers.filter((s) => s.status === "HEALTHY");
    let txt = "";
    if (algo === "LEAST_CONNECTIONS" || algo === "WEIGHTED_LEAST_CONNECTIONS") {
        txt = "Active connections now: " + pool.map((s) => `${shortName(s)} ${s.active_connections}` +
            (algo === "WEIGHTED_LEAST_CONNECTIONS" ? ` (÷${s.weight} = ${fmtNum(s.active_connections / s.weight, 2)})` : "")).join(" · ");
    } else if (algo === "LEAST_RESPONSE_TIME") {
        txt = "Response time, last 10 s: " + pool.map((s) => `${shortName(s)} ${s.recent_response_ms === null ? "not measured" : fmtMs(s.recent_response_ms) + " ms"}`).join(" · ");
    } else if (algo === "IP_HASH") {
        txt = "Dashboard traffic comes from one client IP, so it all goes to one server. Use “Clients: 8” in Strategy comparison to send requests as several clients.";
    }
    el.hidden = !txt;
    if (el.textContent !== txt) el.textContent = txt;
}

/* ------------------------------------------------------------------ */
/* Strategy comparison workload                                        */
/* ------------------------------------------------------------------ */
const WL = { requests: 60, parallel: 1, clients: 1, running: false, runs: [] };

/** Send the same fixed workload through the proxy and record what actually happened. */
async function runWorkload() {
    if (WL.running) return;
    const wasRunning = S.traffic.running;
    if (wasRunning) await stopTraffic(true);
    WL.running = true;
    renderWorkload();
    const algo = S.status ? S.status.active_algorithm : "";
    const total = WL.requests, par = WL.parallel, clients = WL.clients;
    const res = [];
    let next = 0;
    const worker = async () => {
        while (next < total) {
            const i = next++;
            // Simulated clients: distinct client addresses sent in X-Forwarded-For, which the proxy uses as client IP
            const headers = clients > 1 ? { "X-Forwarded-For": `10.0.0.${(i % clients) + 1}` } : {};
            try {
                const r = await fetch("/api/request", { cache: "no-store", headers });
                const d = await r.json().catch(() => ({}));
                const ok = r.status === 200;
                res.push({ ok, sid: ok ? d.server_id : null, ms: ok ? d.roundtrip_latency_ms : null, retries: d.failover_retries || 0 });
                S.routes.push({ sid: ok ? d.server_id : null, ok, retries: d.failover_retries || 0 });
                if (S.routes.length > 60) S.routes.shift();
                S.routesVersion++;
            } catch (e) {
                res.push({ ok: false, sid: null, ms: null, retries: 0 });
            }
        }
    };
    await Promise.all(Array.from({ length: par }, worker));
    const per = {};
    res.filter((r) => r.ok).forEach((r) => {
        const p = (per[r.sid] = per[r.sid] || { n: 0, ms: 0 });
        p.n += 1; p.ms += r.ms;
    });
    // remember the server names now: replacements may be retired before the table is redrawn
    const names = {};
    Object.keys(per).forEach((id) => { names[id] = nameOf(id); });
    WL.runs.unshift({ algo, total, par, clients, per, names, ok: res.filter((r) => r.ok).length,
                      fail: res.filter((r) => !r.ok).length, retries: res.reduce((a, r) => a + r.retries, 0),
                      time: new Date().toLocaleTimeString([], { hour12: false }) });
    WL.runs = WL.runs.slice(0, 8);
    WL.running = false;
    if (wasRunning) startTraffic();
    renderWorkload();
    renderSequence();
    poll();
}

function renderWorkload() {
    const btn = $("#btn-workload");
    btn.disabled = WL.running || !!(S.demo && S.demo.running);
    btn.textContent = WL.running ? "Running…" : "Run workload";
    $$(".wl-seg").forEach((g) => $$("button", g).forEach((b) => b.classList.toggle("on", +b.dataset.v === WL[g.dataset.wl])));
    const ids = [];
    S.servers.forEach((s) => ids.push(s.id));
    WL.runs.forEach((r) => Object.keys(r.per).forEach((id) => { if (!ids.includes(id)) ids.push(id); }));
    const known = {};
    WL.runs.forEach((r) => Object.assign(known, r.names || {}));
    const label = (id) => (S.byId[id] ? S.byId[id].name : known[id] || String(id).replace(/^server-(\d+)$/, "Server $1"));
    const head = `<tr><th>Strategy · workload</th>${ids.map((id) => `<th class="num">${esc(label(id))}</th>`).join("")}<th class="num">Failed</th></tr>`;
    if ($("#wl-head").innerHTML !== head) $("#wl-head").innerHTML = head;
    const rows = WL.runs.map((r) => `<tr>
        <td class="wl-strategy">${esc(ALGO_NAMES[r.algo] || r.algo)}<br><span class="sub">${r.total} req · ${r.par} parallel · ${r.clients} client${r.clients > 1 ? "s" : ""}</span></td>
        ${ids.map((id) => {
            const p = r.per[id];
            return `<td class="num">${p ? `${p.n} <span class="sub">(${Math.round((p.n / r.total) * 100)}%)</span><br><span class="sub">${fmtMs(p.ms / p.n)} ms avg</span>` : '<span class="sub">0</span>'}</td>`;
        }).join("")}
        <td class="num">${r.fail}${r.retries ? `<br><span class="sub">${r.retries} retried</span>` : ""}</td>
    </tr>`).join("");
    const body = rows || `<tr><td colspan="${ids.length + 2}" class="sub">No runs yet.</td></tr>`;
    if ($("#wl-table").innerHTML !== body) $("#wl-table").innerHTML = body;
}

function fmtClock(t) {
    return t ? new Date(t * 1000).toLocaleTimeString([], { hour12: false }) : "";
}

function renderAutoRecovery() {
    const ar = S.status.auto_recovery || {};
    const ep = ar.episode;
    const healthy = S.servers.filter((s) => s.status === "HEALTHY").length;
    const total = S.servers.length;

    const en = $("#ar-enabled");
    en.className = "ar-enabled" + (ar.enabled ? "" : " off");
    en.innerHTML = ar.enabled ? `<span class="dot ok"></span>Enabled` : "Disabled";

    const now = `<span class="${healthy === 0 ? "bad" : "ok"}">${healthy} / ${total}</span>`;
    const avail = ep ? `<span class="bad">${ep.initial_healthy} / ${ep.initial_total}</span><span class="arrow">→</span>${now}` : `${now} <span class="suffix">healthy</span>`;
    const av = $("#ar-avail");
    if (av.innerHTML !== avail) av.innerHTML = avail;

    // status line
    const cand = ep ? ep.candidate : null;
    let msg = "", mcls = "";
    if (!ar.enabled) msg = "Automatic recovery is disabled (LB_AUTO_RECOVERY=0).";
    else if (ar.blocked_reason && healthy === 0) { msg = "⚠ " + ar.blocked_reason; mcls = "bad"; }
    else if (ep && ep.stage === "failed") { msg = `✕ Provisioning failed: ${ep.error}. ${cand.name} was not added to the routing pool.`; mcls = "bad"; }
    else if (ep && ep.outcome === "in_progress") { msg = `⚠ No healthy backends — automatic recovery initiated, starting ${cand.name}…`; mcls = "warn"; }
    else if (ep && ep.stage === "routing") { msg = `✓ ${cand.name} is in the routing pool — waiting for the next client request.`; mcls = "ok"; }
    else if (ep && ep.stage === "restored") { msg = `✓ Traffic restored · ${cand.name} is an active replacement backend (port ${cand.port}).`; mcls = "ok"; }
    else if (healthy === 0) { msg = "⚠ No healthy backends — waiting for the next health check."; mcls = "bad"; }
    else msg = `Standing by. Triggers only when the proxy has 0 healthy backends · max ${ar.max_replacements} replacement backends.`;
    const me = $("#ar-msg");
    me.className = "ar-msg " + mcls;
    if (me.textContent !== msg) me.textContent = msg;

    // pipeline
    const name = cand ? cand.name : "Replacement";
    const st = ep ? ep.stages : {};
    const steps = [
        { key: "detected", label: "Failure detected", detail: ep ? `${ep.initial_healthy}/${ep.initial_total} healthy` : "0 healthy backends" },
        { key: "provisioning", label: "Provisioning replacement", detail: cand && cand.port ? `port ${cand.port}` : "find free port" },
        { key: "started", label: `${name} starting`, detail: cand && cand.pid ? `new process · PID ${cand.pid}` : "new backend process" },
        { key: "health_check", label: "Health check", detail: `GET /health × ${CFG.recoveryThreshold}` },
        { key: "healthy", label: `${name} healthy`, detail: "checks passed" },
        { key: "registered", label: "Registered with proxy", detail: "added to routing pool" },
        { key: "restored", label: "Traffic restored", detail: "client request served" },
    ];
    let currentMarked = false;
    const html = steps.map((stp, i) => {
        let cls = "pending", ico = String(i + 1);
        if (ep && st[stp.key]) { cls = "done"; ico = "✓"; }
        else if (ep && ep.stage === "failed" && !currentMarked) { cls = "bad"; ico = "✕"; currentMarked = true; }
        else if (ep && !currentMarked && ep.stage !== "failed") { cls = "current"; ico = "●"; currentMarked = true; }
        const time = ep && st[stp.key] ? ((ep.stage_times || {})[stp.key] || fmtClock(st[stp.key])) : "";
        return `<li class="pl-step ${cls}"><span class="pl-ico">${ico}</span>
            <div class="pl-label">${esc(stp.label)}</div>
            <div class="pl-detail">${esc(stp.detail)}</div>
            <div class="pl-time">${time}</div></li>`;
    }).join("");
    const pl = $("#ar-pipeline");
    if (pl.innerHTML !== html) pl.innerHTML = html;

    const anyOriginalUp = S.servers.some((s) => !s.is_replacement && s.status === "HEALTHY" && !faultPending(s));
    const anyFailed = S.servers.some((s) => s.status === "UNHEALTHY" || faultPending(s));
    const demoOn = !!(S.demo && S.demo.running);
    $("#btn-fail-all").disabled = demoOn || busy.__all || !anyOriginalUp;
    $("#btn-recover-all").disabled = demoOn || busy.__all || !anyFailed;
}

function renderFaultTolerance() {
    renderControls();
    renderAutoRecovery();
    renderStepper();
    const m = S.status.metrics;
    $("#ft-t-detect").textContent = fmtMs(m.failure_detection_time_ms);
    $("#ft-t-failover").textContent = fmtMs(m.failover_time_ms);
    $("#ft-t-recover").textContent = fmtMs(m.recovery_detection_time_ms);
    $("#ft-t-readmit").textContent = fmtMs(m.readmission_time_ms);
    $("#ft-t-readmit-sub").textContent = `probation (${CFG.recoveryThreshold} checks) before returning to routing`;
}

const busy = {};   // server id -> action in progress (button feedback)
function renderControls() {
    const demoOn = !!(S.demo && S.demo.running);
    const html = S.servers.map((s) => {
        let stateTxt, stateCls = "", btn;
        if (faultPending(s) && !(S.status.server_time - s.failure_injected_at > 4 && (s.faults || []).length)) {
            stateTxt = "Fault injected · awaiting health check"; stateCls = "pending";
            btn = `<button class="btn btn-sm btn-recover" data-act="recover" data-id="${s.id}">Recover</button>`;
        } else if (s.status === "HEALTHY" && (s.faults || []).length) {
            const f = s.faults.map((x) => faultLabel(x.type)).join(", ");
            stateTxt = `<span class="dot rec"></span>${esc(f)} active · still in routing (not detected as a failure yet)`; stateCls = "rec";
            btn = `<button class="btn btn-sm btn-recover" data-act="recover" data-id="${s.id}">Recover</button>`;
        } else if (s.status === "HEALTHY" && s.standby) {
            stateTxt = `<span class="dot stby"></span>Standby · running, not routed (low demand)`;
            btn = `<button class="btn btn-sm btn-fail" data-act="fail" data-id="${s.id}">Fail</button>`;
        } else if (s.status === "HEALTHY") {
            stateTxt = `<span class="dot ok"></span>Healthy · in routing`;
            btn = `<button class="btn btn-sm btn-fail" data-act="fail" data-id="${s.id}">Fail</button>`;
        } else if (s.status === "UNHEALTHY") {
            if (recoveryPending(s)) {
                stateTxt = "Recovery started · awaiting health check"; stateCls = "pending";
                btn = `<button class="btn btn-sm" disabled>Recovering…</button>`;
            } else {
                const f = (s.faults || []).map((x) => faultLabel(x.type)).join(", ");
                stateTxt = `<span class="dot bad"></span>Failed${f ? " (" + esc(f) + ")" : ""} · removed from routing`; stateCls = "bad";
                btn = `<button class="btn btn-sm btn-recover" data-act="recover" data-id="${s.id}">Recover</button>`;
            }
        } else {
            stateTxt = `<span class="dot rec"></span>Probation · health checks ${Math.min(s.consecutive_successes, CFG.recoveryThreshold)}/${CFG.recoveryThreshold}`; stateCls = "rec";
            btn = `<button class="btn btn-sm" disabled>Recovering…</button>`;
        }
        if (busy[s.id]) btn = `<button class="btn btn-sm" disabled>${busy[s.id]}</button>`;
        if (demoOn) btn = btn.replace("<button ", "<button disabled ");
        return `<div class="ctl-row">
            <div><div class="ctl-name">${esc(s.name)} <span class="muted mono">:${s.port}</span>${s.is_replacement ? '<span class="repl-tag">REPLACEMENT</span>' : ""}</div>
            <div class="ctl-state ${stateCls}">${stateTxt}</div></div>
            ${btn}
        </div>`;
    }).join("");
    const el = $("#ft-controls");
    if (el.innerHTML !== html) el.innerHTML = html;
}

const FAULT_DETAIL = {
    http_5xx: "it answers every request, including /health, with HTTP 503", refused: "its port is closed: connections are refused",
    timeout: "it accepts connections but never answers", slow: "it answers, but slowly",
    partition: "the network path from the proxy is cut (simulated)", packet_loss: "some exchanges are lost on the way (simulated)",
};
function faultDetail(name) {
    const recs = ((S.status.faults || {}).records || []).filter((r) => r.server_name === name);
    const t = recs.length ? recs[recs.length - 1].type : "http_5xx";
    return FAULT_DETAIL[t] || t;
}
/** Build the failover story for the most recent failure from real events. */
function renderStepper() {
    const st = S.status;
    const events = st.events || [];
    let inj = null;
    for (let i = events.length - 1; i >= 0; i--) if (events[i].event_type === "FAILURE_INJECTED" || events[i].event_type === "FAULT_INJECTED") { inj = events[i]; break; }
    const healthy = S.servers.filter((s) => s.status === "HEALTHY");
    const total = S.servers.length;
    const steps = [];
    const T = (e) => (e ? e.timestamp : "");
    const pend = (n, title, detail, current) => steps.push({ cls: "pending" + (current ? " current" : ""), icon: String(n), title, detail });

    if (!inj) {
        steps.push({ cls: "done", icon: "✓", title: "Normal operation", detail: `proxy routing to ${healthy.length}/${total} healthy backends` });
        ["Server failure", "Failure detected by proxy", "Removed from routing", "Traffic redirected",
         "Other servers continue serving", "Server recovers", "Server returns to routing"].forEach((t, i) => pend(i + 2, t));
        $("#ft-seq-note").textContent = "fail a server to begin";
        return paintSteps(steps);
    }

    const name = (inj.message.match(/Server \d+/) || [""])[0];
    const srv = S.byName[name];
    const after = events.filter((e) => e.time_raw >= inj.time_raw);
    const isSrv = new RegExp(name.replace(" ", "\\s") + "(?!\\d)");     // "Server 2" but not "Server 21"
    const find = (type, from, sameServer = true) => after.find((e) => (Array.isArray(type) ? type.includes(e.event_type) : e.event_type === type)
        && e.time_raw >= (from ? from.time_raw : 0) && (!sameServer || isSrv.test(e.message)));
    const detected = find(["FAILURE_DETECTED", "FAULT_DETECTED"], inj);
    const retries = after.filter((e) => e.event_type === "FAILOVER_TRIGGERED" && isSrv.test(e.message) && (!detected || e.time_raw <= detected.time_raw)).length;
    const redirected = detected ? find("FAILOVER", detected, false) : null;
    const recTrig = find(["RECOVERY_TRIGGERED", "FAULT_CLEARED"], inj);
    const recDet = find("RECOVERY_DETECTED", recTrig || inj);
    const readmit = find("READMISSION", recDet || inj);

    // Requests served since the failure, counted from the moment this dashboard saw it.
    const key = inj.time_raw;
    if (!S.incidentBase[key] && st.server_time - inj.time_raw < 3) {
        S.incidentBase[key] = { ok: st.metrics.successful_requests, fail: st.metrics.failed_requests };
    }
    const base = S.incidentBase[key];
    const others = S.servers.filter((s) => s.name !== name && s.status === "HEALTHY").map((s) => s.name);

    $("#ft-seq-note").textContent = `${name} · ${inj.timestamp}`;

    // 1 Normal
    steps.push({ cls: "done", icon: "✓", title: "Normal operation", detail: `proxy routing to ${total}/${total} healthy backends` });
    // 2 Failure
    steps.push({ cls: "bad", icon: "✕", title: `${name} fails`, detail: faultDetail(name), time: T(inj) });
    // 3 Detected
    if (detected) {
        const dm = (detected.message.match(/Detection time: ([\d.]+) ms/) || detected.message.match(/([\d.]+) ms after injection/) || [])[1];
        const by = (detected.message.match(/detected by (.+?) \d/) || [])[1];
        steps.push({ cls: "bad", icon: "⚠", title: "Failure detected by proxy", time: T(detected),
            detail: `${by ? "detected by " + by : "failed health check"}${dm ? " · " + fmtMs(parseFloat(dm)) + " ms after the failure" : ""}` });
    } else {
        pend(3, "Failure detected by proxy", `next health check within ${fmtNum(CFG.healthInterval, 1)} s`, true);
    }
    // 4 Removed from routing
    if (detected) {
        const poolNow = readmit ? null : `${healthy.length}/${total} healthy backends`;
        steps.push({ cls: "bad", icon: "−", title: "Removed from routing", time: T(detected),
            detail: [poolNow, retries ? `${retries} in-flight request${retries > 1 ? "s" : ""} retried elsewhere` : ""].filter(Boolean).join(" · ") });
    } else pend(4, "Removed from routing");
    // 5 Redirected
    if (redirected) {
        const sid = (redirected.message.match(/redirected to (\S+) in/) || [])[1];
        const fm = (redirected.message.match(/in ([\d.]+) ms/) || [])[1];
        steps.push({ cls: "info", icon: "↪", title: "Traffic redirected", time: T(redirected),
            detail: `next request sent to ${nameOf(sid)}${fm ? " · " + fmtMs(parseFloat(fm)) + " ms after detection" : ""}` });
    } else if (detected && !readmit) {
        pend(5, "Traffic redirected", healthy.length === 0 ? "no healthy server to redirect to"
            : S.traffic.running ? "waiting for the next request" : "start traffic to see requests rerouted", S.traffic.running && healthy.length > 0);
    } else pend(5, "Traffic redirected");
    // 6 Continue serving
    if (detected) {
        const served = base ? st.metrics.successful_requests - base.ok : 0;
        const lost = base ? st.metrics.failed_requests - base.fail : 0;
        const servedTxt = base && (served || lost) ? `${fmtNum(served)} served, ${fmtNum(lost)} failed since the failure` : "";
        if (healthy.length === 0 && !readmit) {
            steps.push({ cls: "bad", icon: "✕", title: "Service unavailable", detail: "no healthy backends — requests receive HTTP 503" });
        } else {
            const via = readmit ? "" : listNames(others);
            steps.push({ cls: "done", icon: "✓", title: "Other servers continue serving", detail: [via, servedTxt].filter(Boolean).join(" · ") });
        }
    } else pend(6, "Other servers continue serving");
    // 7 Recovers
    if (recDet || (srv && srv.status === "RECOVERING")) {
        const done = !!readmit;
        const k = srv && srv.status === "RECOVERING" ? Math.min(srv.consecutive_successes, CFG.recoveryThreshold) : CFG.recoveryThreshold;
        steps.push({ cls: done ? "done" : "warn current", icon: done ? "✓" : "●", time: T(recDet),
            title: done ? `${name} recovered` : `${name} recovering · checks ${k}/${CFG.recoveryThreshold}`,
            detail: done ? `passed ${CFG.recoveryThreshold} consecutive health checks` : "on probation — receives no traffic yet" });
    } else if (recTrig) {
        steps.push({ cls: "warn current", icon: "●", title: `${name} recovering`, detail: "waiting for the next health check", time: T(recTrig) });
    } else {
        pend(7, "Server recovers", detected ? `press Recover on ${name}` : "");
    }
    // 8 Returns to routing
    if (readmit) {
        steps.push({ cls: "done", icon: "✓", title: "Returns to routing", time: T(readmit),
            detail: `${total}/${total} healthy backends · ${name} receiving traffic again` });
    } else pend(8, "Returns to routing");

    paintSteps(steps);
}

function paintSteps(steps) {
    const html = steps.map((s) => `<li class="step ${s.cls}">
        <span class="step-ico">${s.icon}</span>
        <div><div class="step-title">${esc(s.title)}</div>${s.detail ? `<div class="step-detail">${esc(s.detail)}</div>` : ""}</div>
        <span class="step-time">${s.time || ""}</span>
    </li>`).join("");
    const el = $("#ft-stepper");
    if (el.innerHTML !== html) el.innerHTML = html;
}

function renderMonitoring() {
    const st = S.status;
    const sh = shares(LOAD_WINDOW);
    const d5 = deltaOver(5);
    const lat = st.metrics.avg_latency_per_server || {};
    $("#mon-table").innerHTML = S.servers.map((s) => {
        const state = stateOf(s);
        const ago = s.last_health_check ? Math.max(0, st.server_time - s.last_health_check) : null;
        const rate = d5 ? d5.per[s.id] / d5.dt : 0;
        return `<tr class="${state === "bad" ? "row-bad" : ""}">
            <td><span class="srv"><span class="swatch" style="background:${serverColor(s.id)}"></span>${esc(s.name)} <span class="sub mono">:${s.port}</span>${s.is_replacement ? '<span class="repl-tag">REPLACEMENT</span>' : ""}</span></td>
            <td><span class="health ${state}"><span class="dot ${state}"></span>${esc(stateLabel(s).charAt(0) + stateLabel(s).slice(1).toLowerCase())}</span></td>
            <td class="num">${sh.active ? Math.round(sh.share[s.id] * 100) + "%" : "0%"}</td>
            <td class="num">${fmtNum(rate, 1)} <span class="sub">req/s</span></td>
            <td class="num">${lat[s.id] !== undefined && s.status === "HEALTHY" ? fmtMs(lat[s.id]) + ' <span class="sub">ms</span>' : "–"}</td>
            <td class="num">${fmtMs(s.health_latency_ms)} <span class="sub">ms</span></td>
            <td class="num">${fmtNum(s.requests_served)}</td>
            <td class="num">${ago === null ? "–" : fmtNum(ago, 1) + ' <span class="sub">s ago</span>'}</td>
        </tr>`;
    }).join("");

    // distribution bar
    const total = S.servers.reduce((a, s) => a + s.requests_served, 0);
    const dist = $("#mon-dist");
    if (!total) dist.innerHTML = `<span class="dist-empty">No requests yet</span>`;
    else dist.innerHTML = S.servers.filter((s) => s.requests_served > 0).map((s) => {
        const p = s.requests_served / total;
        return `<div class="dist-seg" style="flex-grow:${s.requests_served};background:${serverColor(s.id)}" title="${esc(s.name)}: ${s.requests_served}">${p > 0.12 ? `${esc(s.name)} · ${Math.round(p * 100)}%` : Math.round(p * 100) + "%"}</div>`;
    }).join("");
    $("#mon-dist-note").textContent = total ? `${fmtNum(total)} requests since last reset` : "since last reset";

    renderEvents($("#mon-events"), st.events || [], 15);

    // raw log (only when expanded)
    if ($(".raw-log").open) {
        $("#mon-log").innerHTML = (st.logs || []).slice().reverse().map((l) =>
            `<div class="log-line ${esc(l.level)}"><span class="lt">${esc(l.timestamp)}</span><span class="lk">${esc(l.event_type)}</span><span class="lm">${esc(l.message.replace(/server-(\d)/g, "Server $1"))}</span></div>`).join("");
    }
    updateCharts();
}

/* ------------------------------------------------------------------ */
/* Charts                                                              */
/* ------------------------------------------------------------------ */
function resetChartData() {
    S.chart = { labels: [], rate: {}, lat: {}, lastT: 0 };
}

function sampleCharts() {
    const now = performance.now();
    if (now - S.chart.lastT < 1000) return;
    S.chart.lastT = now;
    const d = deltaOver(3);
    const lat = S.status.metrics.avg_latency_per_server || {};
    S.chart.labels.push(new Date().toLocaleTimeString([], { hour12: false }));
    const pad = () => new Array(S.chart.labels.length - 1).fill(null);   // series of a backend that appeared later
    S.servers.forEach((s) => {
        const per = d && d.per[s.id] !== undefined ? d.per[s.id] : 0;
        (S.chart.rate[s.id] = S.chart.rate[s.id] || pad()).push(d ? +(per / d.dt).toFixed(2) : 0);
        const receiving = per > 0;
        (S.chart.lat[s.id] = S.chart.lat[s.id] || pad()).push(receiving && lat[s.id] !== undefined ? lat[s.id] : null);
    });
    // backends that were removed (e.g. replacements after a reset) keep their history aligned
    Object.keys(S.chart.rate).forEach((id) => {
        if (!S.byId[id]) { S.chart.rate[id].push(null); S.chart.lat[id].push(null); }
    });
    const MAX = 90;
    if (S.chart.labels.length > MAX) {
        S.chart.labels.shift();
        Object.values(S.chart.rate).forEach((a) => a.shift());
        Object.values(S.chart.lat).forEach((a) => a.shift());
    }
}

function chartOptions(yTitle) {
    const grid = cssVar("--border");
    const tick = cssVar("--muted");
    return {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        interaction: { mode: "index", intersect: false },
        plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => ` ${c.dataset.label}: ${c.parsed.y ?? "–"} ${yTitle}` } } },
        scales: {
            x: { grid: { display: false }, border: { color: grid }, ticks: { color: tick, maxTicksLimit: 6, maxRotation: 0, font: { size: 11 } } },
            y: { beginAtZero: true, grid: { color: grid }, border: { display: false }, ticks: { color: tick, maxTicksLimit: 5, font: { size: 11 } } },
        },
        elements: { point: { radius: 0, hoverRadius: 3 }, line: { borderWidth: 2, tension: 0.3 } },
    };
}

function ensureCharts() {
    if (!window.Chart || S.charts.load || !S.servers.length) return;
    Chart.defaults.font.family = "Inter, system-ui, sans-serif";
    const mk = (id, unit, key) => new Chart($(id), {
        type: "line",
        data: { labels: S.chart.labels, datasets: S.servers.map((s) => ({ label: s.name, data: [], borderColor: serverColor(s.id), backgroundColor: serverColor(s.id), spanGaps: false, _sid: s.id, _key: key })) },
        options: chartOptions(unit),
    });
    S.charts.load = mk("#chart-load", "req/s", "rate");
    S.charts.lat = mk("#chart-latency", "ms", "lat");
    $("#chart-legend").innerHTML = S.servers.map((s) => `<span><span class="swatch" style="background:${serverColor(s.id)}"></span>${esc(s.name)}</span>`).join("");
}

function syncChartSeries() {
    const ids = S.servers.map((s) => s.id).join(",");
    if (S.charts.ids === ids) return;
    S.charts.ids = ids;
    [S.charts.load, S.charts.lat].forEach((ch, i) => {
        const key = i === 0 ? "rate" : "lat";
        ch.data.datasets = S.servers.map((s) => ({ label: s.name, data: [], borderColor: serverColor(s.id), backgroundColor: serverColor(s.id), spanGaps: false, _sid: s.id, _key: key }));
    });
    $("#chart-legend").innerHTML = S.servers.map((s) => `<span><span class="swatch" style="background:${serverColor(s.id)}"></span>${esc(s.name)}</span>`).join("");
}

function updateCharts() {
    ensureCharts();
    if (!S.charts.load) return;
    syncChartSeries();
    [S.charts.load, S.charts.lat].forEach((ch) => {
        ch.data.labels = S.chart.labels;
        ch.data.datasets.forEach((ds) => { ds.data = (S.chart[ds._key][ds._sid] || []).slice(); });
        ch.update("none");
    });
}

function restyleCharts() {
    if (!S.charts.load) return;
    [S.charts.load, S.charts.lat].forEach((ch, i) => {
        ch.options = chartOptions(i === 0 ? "req/s" : "ms");
        ch.data.datasets.forEach((ds) => { ds.borderColor = ds.backgroundColor = serverColor(ds._sid); });
        ch.update("none");
    });
    $("#chart-legend").innerHTML = S.servers.map((s) => `<span><span class="swatch" style="background:${serverColor(s.id)}"></span>${esc(s.name)}</span>`).join("");
}

/* ------------------------------------------------------------------ */
/* Polling                                                             */
/* ------------------------------------------------------------------ */
async function poll() {
    if (S.pollBusy) return;
    S.pollBusy = true;
    try {
        const res = await fetch("/api/status", { cache: "no-store" });
        if (!res.ok) throw new Error("HTTP " + res.status);
        const st = await res.json();
        S.pollFails = 0;
        $("#conn-lost").hidden = true;
        S.status = st;
        S.servers = st.servers;
        S.byId = {}; S.byName = {};
        st.servers.forEach((s) => { S.byId[s.id] = s; S.byName[s.name] = s; });
        pushHistory(st);
        sampleCharts();
        render();
        syncTraffic(st);
    } catch (e) {
        S.pollFails++;
        if (S.pollFails >= 3) $("#conn-lost").hidden = false;
    } finally {
        S.pollBusy = false;
    }
}

function render() {
    if (!S.status) return;
    renderStatus();
    Object.values(S.topos).forEach((t) => t.update());
    // Every page is cheap to render; keeping all current avoids stale views on navigation.
    renderOverview();
    renderLoadBalancing();
    renderFaultTolerance();
    if (S.page === "monitoring") renderMonitoring();
    EXT.renderers.forEach((fn) => { try { fn(S.page); } catch (e) { console.error(e); } });
}

/* ------------------------------------------------------------------ */
/* Actions                                                             */
/* ------------------------------------------------------------------ */
let toastTimer = null;
function toast(msg) {
    const t = $("#toast");
    t.textContent = msg;
    t.hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => { t.hidden = true; }, 2600);
}

async function api(path, body) {
    const res = await fetch(path, {
        method: "POST",
        headers: body ? { "Content-Type": "application/json" } : {},
        body: body ? JSON.stringify(body) : undefined,
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.message || "HTTP " + res.status);
    return data;
}

/**
 * One traffic generator: it runs inside the proxy process (a real HTTP client, up to 100 req/s) and is
 * started from the sidebar or from the traffic-shape panel. The routing sequence and the request dots
 * are replayed from the routes it reports (server, success, retries), so nothing is sent by the browser.
 */
function syncTraffic(st) {
    const tr = st.traffic;
    if (!tr) return;
    if (!S.traffic.busy) {
        const was = S.traffic.running, rate = S.traffic.rate;
        S.traffic.running = !!tr.running;
        if (tr.running && tr.params && tr.params.rps) S.traffic.rate = tr.params.rps;
        if (was !== S.traffic.running || rate !== S.traffic.rate) updateTrafficButton();
    }
    const all = tr.routes || [];
    if (all.length && all[all.length - 1].n < S.traffic.lastN) S.traffic.lastN = 0;     // proxy restarted
    const fresh = all.filter((r) => r.n > S.traffic.lastN);
    if (!fresh.length) return;
    const backlog = S.traffic.lastN === 0;
    S.traffic.lastN = fresh[fresh.length - 1].n;
    const routed = fresh.filter((r) => r.cache !== "HIT");          // answered by the proxy cache: no backend
    routed.forEach((r) => S.routes.push({ sid: r.ok ? r.sid : null, ok: r.ok, retries: r.retries || 0 }));
    while (S.routes.length > 60) S.routes.shift();
    S.routesVersion++;
    if (S.page === "load-balancing") renderSequence();
    const topo = S.topos[visibleTopoKey()];
    if (!topo || backlog) return;
    const show = routed.slice(-8);
    show.forEach((r, i) => setTimeout(() => topo.packet(r.ok ? r.sid : null, r.ok), i * (POLL_MS / Math.max(1, show.length))));
}

function visibleTopoKey() {
    return { overview: "overview", "load-balancing": "lb", "fault-tolerance": "ft", capacity: "cap" }[S.page];
}

async function startTraffic() {
    S.traffic.busy = true;
    S.traffic.running = true;
    paintTrafficButton();
    try {
        await api("/api/traffic/start", Object.assign({ rps: S.traffic.rate }, S.traffic.shape));
    } catch (e) {
        S.traffic.running = false;
        toast(e.message);
    } finally {
        S.traffic.busy = false;
        await poll();
    }
}
async function stopTraffic(silent) {
    S.traffic.busy = true;
    S.traffic.running = false;
    if (!silent) paintTrafficButton();
    try { await api("/api/traffic/stop"); } catch (e) { /* surfaced by poll */ }
    finally { S.traffic.busy = false; }
}
async function setTrafficRate(rps) {
    S.traffic.rate = rps;
    $$(".rate-select[data-traffic] button").forEach((x) => x.classList.toggle("on", +x.dataset.rate === rps));
    if (S.traffic.running) {
        try { await api("/api/traffic/rate", { rps }); } catch (e) { toast(e.message); }
    }
    paintTrafficButton();
}
function updateTrafficButton() {
    const b = $("#btn-traffic");
    b.classList.toggle("running", S.traffic.running);
    $("#traffic-label").textContent = S.traffic.running ? `Traffic running · ${fmtNum(S.traffic.rate)}/s` : "Start traffic";
    b.title = S.traffic.running ? "Stop sending requests" : "Send requests to the reverse proxy";
    $$(".rate-select[data-traffic] button").forEach((x) => x.classList.toggle("on", +x.dataset.rate === S.traffic.rate));
}
function paintTrafficButton() {
    updateTrafficButton();
    if (S.status) render();
}

async function bulkAction(act) {
    busy.__all = true;
    renderAutoRecovery();
    try {
        const r = await api(act === "fail" ? "/api/fail-all" : "/api/recover-all", act === "fail" ? faultBody() : undefined);
        const list = r.failed || r.recovered || [];
        toast(list.length ? `${act === "fail" ? "Failure injected into" : "Recovery started on"} ${listNames(list)}` : "Nothing to do");
    } catch (e) {
        toast(e.message);
    } finally {
        delete busy.__all;
        await poll();
    }
}

function faultBody() {                     // the fault chosen in "Simulate failure" (defaults to HTTP 503)
    return (window.LBM && window.LBM.faultBody) ? window.LBM.faultBody() : { type: "http_5xx" };
}
function faultLabel(t) {
    return { http_5xx: "HTTP 503 fault", refused: "Connection refused", timeout: "Timeout", slow: "Slow-response fault",
             partition: "Network partition", packet_loss: "Packet loss" }[t] || t;
}
async function serverAction(act, id) {
    const s = S.byId[id];
    busy[id] = act === "fail" ? "Failing…" : "Recovering…";
    renderControls();
    try {
        const body = act === "fail" ? faultBody() : undefined;
        await api(`/api/${act}/${id}`, body);
        toast(act === "fail" ? `${faultLabel(body.type)} injected into ${s.name}` : `Recovery started on ${s.name}`);
    } catch (e) {
        toast(`Could not reach ${s ? s.name : id}: ${e.message}`);
    } finally {
        delete busy[id];
        await poll();
    }
}

async function resetSimulation(silent, fromDemo) {
    if (!fromDemo) stopDemo();
    stopTraffic();
    const b = $("#btn-reset");
    b.disabled = true;
    try {
        await api("/api/reset-simulation");
        S.routes = []; S.routesVersion++;
        S.hist = []; S.incidentBase = {};
        resetChartData();
        if (!silent) toast("Simulation reset — all servers healthy, counters cleared");
    } catch (e) {
        toast("Reset failed: " + e.message);
    } finally {
        b.disabled = false;
        await poll();
    }
}

/* ------------------------------------------------------------------ */
/* Demo mode — every step waits for the real state change               */
/* ------------------------------------------------------------------ */
const DEMO_TARGET = "server-2";
const DEMO_STEPS = 8;

function demoSet(i, title, text) {
    $("#demo-step").textContent = i ? `${i}/${DEMO_STEPS} · ${title}` : title;
    $("#demo-text").textContent = text;
    $("#demo-progress").style.width = `${(i / DEMO_STEPS) * 100}%`;
}

function sleep(ms, demo) {
    return new Promise((resolve, reject) => {
        const t = setTimeout(() => { demo.cancel = null; resolve(); }, ms);
        demo.cancel = () => { clearTimeout(t); reject(new Error("stopped")); };
    });
}
async function waitFor(cond, timeoutMs, demo) {
    const end = performance.now() + timeoutMs;
    while (performance.now() < end) {
        if (!demo.running) throw new Error("stopped");
        if (S.status && cond()) return true;
        await sleep(200, demo);
    }
    return false;
}

async function runDemo() {
    if (S.demo && S.demo.running) return;
    const demo = { running: true, cancel: null };
    S.demo = demo;
    $("#demo-bar").hidden = false;
    $("#btn-demo").classList.add("running");
    $("#btn-demo-stop").textContent = "Stop";
    location.hash = "#fault-tolerance";
    const srv = () => S.byId[DEMO_TARGET];
    const name = (S.byId[DEMO_TARGET] || { name: "Server 2" }).name;
    try {
        demoSet(0, "Preparing", "Resetting the simulation to a clean state…");
        const algo = S.status ? S.status.active_algorithm : CFG.defaultAlgorithm;
        await resetSimulation(true, true);
        if (!demo.running) throw new Error("stopped");
        if (algo !== CFG.defaultAlgorithm) { await api("/api/set-algorithm", { algorithm: algo }); await poll(); }
        await waitFor(() => S.servers.every((s) => s.status === "HEALTHY"), 6000, demo);

        demoSet(1, "Normal operation", "The client sends every request to the reverse proxy, which distributes them across all three servers.");
        startTraffic();
        await sleep(7000, demo);

        demoSet(2, "Server failure", `${name} is made to fail. The proxy does not know yet — its next health check will find out.`);
        await api(`/api/fail/${DEMO_TARGET}`);
        await poll();
        await waitFor(() => srv().status === "UNHEALTHY", 8000, demo);

        demoSet(3, "Failure detected", `The proxy's health check to ${name} failed, so ${name} is removed from routing.`);
        await sleep(3500, demo);

        demoSet(4, "Traffic redirected", `The proxy now sends new requests only to the remaining healthy servers.`);
        const detT = srv().failure_detected_at || 0;
        await waitFor(() => (S.status.events || []).some((e) => e.event_type === "FAILOVER" && e.time_raw >= detT), 6000, demo);
        await sleep(3500, demo);

        demoSet(5, "Service continues", `Clients keep getting responses — ${S.status.active_server_count}/${S.servers.length} healthy backends.`);
        await sleep(6000, demo);

        demoSet(6, "Recovery", `${name} comes back. The proxy waits for ${CFG.recoveryThreshold} consecutive passing health checks.`);
        await api(`/api/recover/${DEMO_TARGET}`);
        await poll();
        await waitFor(() => srv().status === "RECOVERING", 6000, demo);

        demoSet(7, "Probation", `${name} is responding again but stays out of routing until verification completes.`);
        await waitFor(() => srv().status === "HEALTHY", 8000, demo);

        demoSet(8, "Back in service", `${name} is back in routing and receiving requests again.`);
        const before = srv().requests_served;
        await waitFor(() => srv().requests_served > before, 6000, demo);
        await sleep(6000, demo);

        stopTraffic();
        demo.running = false;
        demoSet(DEMO_STEPS, "Demo complete", "3/3 healthy backends. Failure, detection, redirection and recovery were all shown live.");
        $("#btn-demo").classList.remove("running");
        $("#btn-demo-stop").textContent = "Close";
        renderControls();
    } catch (e) {
        if (e.message !== "stopped") {
            demoSet(0, "Demo stopped", "Could not complete a step: " + e.message);
            $("#btn-demo-stop").textContent = "Close";
            stopTraffic();
        }
        demo.running = false;
        $("#btn-demo").classList.remove("running");
        renderControls();
    }
}

function stopDemo(keepBar) {
    const d = S.demo;
    if (d) {
        const wasRunning = d.running;
        d.running = false;
        if (d.cancel) d.cancel();
        if (wasRunning) stopTraffic();
    }
    $("#btn-demo").classList.remove("running");
    if (!keepBar) $("#demo-bar").hidden = true;
    S.demo = null;
}

/* ------------------------------------------------------------------ */
/* Navigation, theme, wiring                                           */
/* ------------------------------------------------------------------ */
function showPage() {
    const page = (location.hash || "#overview").slice(1);
    const valid = ["overview", "load-balancing", "fault-tolerance", "monitoring", ...EXT.pages];
    S.page = valid.includes(page) ? page : "overview";
    $$(".page").forEach((p) => { p.hidden = p.dataset.page !== S.page; });
    $$(".nav-item").forEach((a) => a.classList.toggle("active", a.dataset.page === S.page));
    window.scrollTo(0, 0);
    if (S.page === "monitoring" && S.status) renderMonitoring();
    if (S.status) render();
}

function setTheme(t) {
    document.documentElement.setAttribute("data-theme", t);
    try { localStorage.setItem("lbm-theme", t); } catch (e) {}
    $("#theme-label").textContent = t === "dark" ? "Light mode" : "Dark mode";
    restyleCharts();
    seqRendered = -1;
    render();
}

function wire() {
    $$("[data-topo]").forEach((el) => { S.topos[el.dataset.topo] = new Topology(el, el.dataset.topo); });

    $("#btn-traffic").addEventListener("click", () => (S.traffic.running ? stopTraffic() : startTraffic()));
    $$(".rate-select[data-traffic] button").forEach((b) => b.addEventListener("click", () => setTrafficRate(+b.dataset.rate)));
    $("#btn-demo").addEventListener("click", () => (S.demo && S.demo.running ? stopDemo() : runDemo()));
    $("#btn-demo-stop").addEventListener("click", () => stopDemo());
    $("#btn-reset").addEventListener("click", () => resetSimulation());

    $$(".algo-switch button").forEach((b) => b.addEventListener("click", async () => {
        if (S.status && S.status.active_algorithm === b.dataset.algo) return;
        try {
            await api("/api/set-algorithm", { algorithm: b.dataset.algo });
            toast(`Routing strategy: ${ALGO_NAMES[b.dataset.algo]}`);
        } catch (e) { toast(e.message); }
        poll();
    }));

    $$(".wl-seg").forEach((g) => $$("button", g).forEach((b) => b.addEventListener("click", () => {
        if (WL.running) return;
        WL[g.dataset.wl] = +b.dataset.v;
        renderWorkload();
    })));
    $("#btn-workload").addEventListener("click", runWorkload);
    $("#btn-workload-clear").addEventListener("click", () => { WL.runs = []; renderWorkload(); });

    $("#btn-fail-all").addEventListener("click", () => bulkAction("fail"));
    $("#btn-recover-all").addEventListener("click", () => bulkAction("recover"));
    $("#ft-controls").addEventListener("click", (e) => {
        const b = e.target.closest("button[data-act]");
        if (b && !b.disabled) serverAction(b.dataset.act, b.dataset.id);
    });

    $(".raw-log").addEventListener("toggle", () => S.status && renderMonitoring());

    const current = document.documentElement.getAttribute("data-theme") || "dark";
    $("#theme-label").textContent = current === "dark" ? "Light mode" : "Dark mode";
    $("#btn-theme").addEventListener("click", () => setTheme(document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark"));

    window.addEventListener("hashchange", showPage);
    showPage();
    paintTrafficButton();
    poll();
    setInterval(poll, POLL_MS);
}

/* Bridge for dashboard_ext.js (capacity, data recovery, experiments, network faults). */
const EXT = { pages: [], renderers: [] };
window.LBM = {
    S, $, $$, esc, api, poll, toast, fmtNum, fmtMs, nameOf, serverColor, stateOf, stateLabel,
    startTraffic, stopTraffic, setTrafficRate,
    addPage: (name) => EXT.pages.push(name),
    onRender: (fn) => EXT.renderers.push(fn),
};

document.addEventListener("DOMContentLoaded", wire);
})();
