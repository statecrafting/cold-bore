// cold-bore night-shift console: live WebSocket feed, fault console, and
// pipeline accounting. Counters arriving from services are cumulative;
// rates are derived here by differencing successive snapshots.

import { RollingChart, formatNum } from "./charts.js";

const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
const S1 = css("--series-1");
const S2 = css("--series-2");
const S3 = css("--series-3");

// ── charts ────────────────────────────────────────────────────────────
const charts = {
  throughput: new RollingChart(
    document.getElementById("chart-throughput"),
    document.getElementById("legend-throughput"),
    { series: [{ name: "generated", color: S1 }, { name: "confirmed", color: S2 }, { name: "inserted", color: S3 }] }
  ),
  backlog: new RollingChart(
    document.getElementById("chart-backlog"),
    document.getElementById("legend-backlog"),
    { series: [{ name: "broker backlog", color: S1 }, { name: "unacked", color: S2 }, { name: "edge buffer", color: S3 }] }
  ),
  latency: new RollingChart(
    document.getElementById("chart-latency"),
    document.getElementById("legend-latency"),
    { series: [{ name: "p50", color: S1 }, { name: "p99", color: S2 }] }
  ),
  absorbed: new RollingChart(
    document.getElementById("chart-absorbed"),
    document.getElementById("legend-absorbed"),
    { series: [{ name: "dups dropped", color: S1 }, { name: "redeliveries", color: S2 }, { name: "retransmits", color: S3 }] }
  ),
};

// ── stat tiles ────────────────────────────────────────────────────────
const TILES = [
  { id: "generated", label: "generated", from: () => latest.edge?.generated },
  { id: "inserted", label: "rows landed", from: () => latest.ingest?.inserted },
  { id: "dup", label: "dups absorbed", from: () => latest.ingest?.dup_dropped },
  { id: "gaps", label: "open gaps", from: () => latest.ingest?.open_gaps, warnAbove: 0 },
  { id: "buffered", label: "edge buffered", from: () => latest.edge?.buffered, warnAbove: 0 },
  { id: "poison", label: "poison (DLQ)", from: () => latest.ingest?.poison, critAbove: 0 },
  { id: "dropped", label: "frames dropped", from: () => latest.edge?.buffer_dropped, critAbove: 0 },
  { id: "rate", label: "well rate hz", from: () => latest.edge?.rate_hz },
];
const tilesEl = document.getElementById("tiles");
const tileEls = {};
for (const t of TILES) {
  const el = document.createElement("div");
  el.className = "tile";
  el.innerHTML = `<div class="label">${t.label}</div><div class="value">…</div>`;
  tilesEl.append(el);
  tileEls[t.id] = el;
}

function renderTiles() {
  for (const t of TILES) {
    const el = tileEls[t.id];
    const v = t.from();
    el.querySelector(".value").textContent = v == null ? "…" : formatNum(v);
    el.classList.toggle("warn", t.warnAbove != null && v > t.warnAbove && !(t.critAbove != null && v > t.critAbove));
    el.classList.toggle("crit", t.critAbove != null && v > t.critAbove);
  }
}

// ── state / rates ─────────────────────────────────────────────────────
const latest = { edge: null, ingest: null, broker: null };
const prev = { edge: null, ingest: null };

function rate(cur, before, field) {
  if (!before || cur.t_ms <= before.t_ms) return null;
  const dt = (cur.t_ms - before.t_ms) / 1000;
  return Math.max(0, (cur[field] - before[field]) / dt);
}

// Edge and ingest snapshots arrive on independent 1 Hz cadences; charts
// want one point per arrival with every series populated, so the latest
// derived rates persist here between messages.
const rates = {
  gen: null, conf: null, ins: null,
  dup: null, redel: null, retrans: null,
};

function onMetrics(service, m) {
  const before = prev[service];
  latest[service] = m;
  if (service === "edge") {
    rates.gen = rate(m, before, "generated") ?? rates.gen;
    rates.conf = rate(m, before, "confirmed") ?? rates.conf;
    rates.retrans = rate(m, before, "retransmits") ?? rates.retrans;
    // Backlog: classic mode reads the queue; stream mode derives lag from
    // total retained records vs the consumer's committed offset.
    const b = latest.broker ?? {};
    let backlog = b.depth ?? null;
    let unacked = b.unacked ?? null;
    if (latest.ingest?.mode === "stream") {
      const total = b.stream?.messages;
      const committed = latest.ingest?.committed_offset;
      backlog = total != null ? Math.max(0, total - 1 - (committed ?? -1)) : null;
      unacked = null;
    }
    charts.backlog.push(m.t_ms, [backlog, unacked, m.buffered]);
    renderLinks(m.links ?? {});
  } else if (service === "ingest") {
    rates.ins = rate(m, before, "inserted") ?? rates.ins;
    rates.dup = rate(m, before, "dup_dropped") ?? rates.dup;
    rates.redel = rate(m, before, "redeliveries") ?? rates.redel;
    charts.latency.push(m.t_ms, [m.e2e?.p50_ms ?? null, m.e2e?.p99_ms ?? null]);
    document.getElementById("mode-badge").textContent = `mode: ${m.mode}`;
  }
  charts.throughput.push(m.t_ms, [rates.gen, rates.conf, rates.ins]);
  charts.absorbed.push(m.t_ms, [rates.dup, rates.redel, rates.retrans]);
  prev[service] = m;
  renderTiles();
}

// ── pad link buttons ──────────────────────────────────────────────────
const linkButtons = document.getElementById("link-buttons");
const knownPads = new Map(); // pad -> button

function renderLinks(links) {
  for (const [padStr, up] of Object.entries(links)) {
    const pad = Number(padStr);
    let btn = knownPads.get(pad);
    if (!btn) {
      btn = document.createElement("button");
      btn.addEventListener("click", () => {
        const currentlyUp = !btn.classList.contains("link-down");
        sendControl({ cmd: "link", pad, state: currentlyUp ? "down" : "up" });
      });
      knownPads.set(pad, btn);
      [...knownPads.entries()].sort((a, b) => a[0] - b[0]).forEach(([, b]) => linkButtons.append(b));
    }
    btn.textContent = `pad ${pad} ${up ? "▲" : "▼"}`;
    btn.classList.toggle("link-down", !up);
    btn.title = up ? "uplink up: click to sever" : "uplink severed: click to restore";
  }
}

// ── events ────────────────────────────────────────────────────────────
const eventsEl = document.getElementById("events");
function logEvent(kind, service, t_ms, data) {
  const li = document.createElement("li");
  const t = new Date(t_ms || Date.now()).toLocaleTimeString(undefined, { hour12: false });
  li.innerHTML = `<span class="t">${t}</span><span class="k ${kind}">${kind}</span><span class="d">${service}: ${JSON.stringify(data)}</span>`;
  eventsEl.prepend(li);
  while (eventsEl.children.length > 200) eventsEl.lastChild.remove();
}

// ── wells grid ────────────────────────────────────────────────────────
const wellsEl = document.getElementById("wells");
const wellEls = new Map();
function renderWells(wells) {
  const now = Date.now();
  document.getElementById("wells-meta").textContent = `${wells.length} reporting`;
  for (const w of wells) {
    const key = `${w.pad}-${w.well}`;
    let el = wellEls.get(key);
    if (!el) {
      el = document.createElement("div");
      el.className = "well";
      wellsEl.append(el);
      wellEls.set(key, el);
    }
    const age = now - new Date(w.time).getTime();
    el.classList.toggle("stale", age > 5000);
    el.innerHTML = `<div class="id">pad ${w.pad} · well ${w.well}</div>
      <div class="psi">${Math.round(w.pressure_psi)} <span style="font-size:11px;color:var(--muted)">psi</span></div>
      <div class="rest">${w.rate_bpm.toFixed(1)} bpm · ${w.proppant_ppa.toFixed(2)} ppa · ${w.temp_f.toFixed(0)}°F</div>`;
  }
}

// ── control ───────────────────────────────────────────────────────────
async function sendControl(cmd) {
  try {
    const res = await fetch("/api/control", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(cmd),
    });
    if (!res.ok) {
      const body = await res.text();
      logEvent("control_rejected", "console", Date.now(), { cmd, status: res.status, body: body.slice(0, 200) });
    }
  } catch (err) {
    logEvent("control_failed", "console", Date.now(), { cmd, error: String(err) });
  }
}

for (const row of document.querySelectorAll(".btn-row[data-cmd]")) {
  const cmd = row.dataset.cmd;
  row.addEventListener("click", (e) => {
    const btn = e.target.closest("button");
    if (!btn) return;
    row.querySelectorAll("button").forEach((b) => b.classList.toggle("active", b === btn));
    if (cmd === "dup") sendControl({ cmd, rate: Number(btn.dataset.rate) });
    else if (cmd === "reorder") sendControl({ cmd, window: Number(btn.dataset.window) });
    else if (cmd === "rate") sendControl({ cmd, multiplier: Number(btn.dataset.mult) });
  });
}
document.getElementById("kill-ingest").addEventListener("click", () => sendControl({ cmd: "kill", service: "ingest" }));
document.getElementById("kill-edge").addEventListener("click", () => sendControl({ cmd: "kill", service: "edge" }));
document.getElementById("reset-faults").addEventListener("click", () => {
  sendControl({ cmd: "reset" });
  document.querySelectorAll(".btn-row[data-cmd] button.active").forEach((b) => b.classList.remove("active"));
});

// ── websocket ─────────────────────────────────────────────────────────
const connBadge = document.getElementById("conn-badge");
let backoff = 500;

function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.addEventListener("open", () => {
    backoff = 500;
    connBadge.textContent = "ws: live";
    connBadge.classList.replace("badge-bad", "badge-good");
  });
  ws.addEventListener("message", (e) => {
    let msg;
    try { msg = JSON.parse(e.data); } catch { return; }
    if (msg.type === "metrics") onMetrics(msg.service, msg.data);
    else if (msg.type === "broker") { latest.broker = msg.data; }
    else if (msg.type === "wells") renderWells(msg.data);
    else if (msg.type === "event") logEvent(msg.kind, msg.service, msg.t_ms, msg.data);
    else if (msg.type === "hello") {
      for (const [svc, data] of Object.entries(msg.services ?? {})) onMetrics(svc, data);
      if (msg.broker && Object.keys(msg.broker).length) latest.broker = msg.broker;
    }
  });
  ws.addEventListener("close", () => {
    connBadge.textContent = "ws: reconnecting";
    connBadge.classList.replace("badge-good", "badge-bad");
    setTimeout(connect, backoff);
    backoff = Math.min(backoff * 2, 10000);
  });
  ws.addEventListener("error", () => ws.close());
}
connect();

window.addEventListener("resize", () => Object.values(charts).forEach((c) => c.scheduleDraw()));
