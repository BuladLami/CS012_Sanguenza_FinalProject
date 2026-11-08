/* POLARIS Cloud simulation UI.
 *
 * Every step calls the real gateway API, then fetches that request's distributed trace from
 * the collector (through the gateway) and replays it event by event: the topology shows each
 * hop, the map fills in shard by shard, and the narration explains what each node just did.
 * Nothing on this page is mocked; it only renders what the cluster recorded.
 */
"use strict";

const API = "";                         // same origin as the gateway
const CHO_HEADERS = { "X-Role": "cho-planner", "X-Api-Key": "cho-demo-key" };
const SHARDS = ["coastal", "north", "upland"];
const AZS = ["a", "b", "c"];
const CLASSES = ["Highly Suitable", "Moderately Suitable", "Low Suitability", "Not Suitable"];
const CLASS_KEY = { "Highly Suitable": "high", "Moderately Suitable": "mod",
                    "Low Suitability": "low", "Not Suitable": "not" };
const WEIGHTS = [
  ["building_density", "Building density", 0.40, +1],
  ["road_accessibility", "Road accessibility", 0.25, +1],
  ["facility_distance", "Distance from facilities", 0.15, +1],
  ["flood_susceptibility", "Flood", 0.08, -1],
  ["landslide_susceptibility", "Landslide", 0.07, -1],
  ["storm_surge_susceptibility", "Storm surge", 0.05, -1],
];
const HAZARD_LABEL = ["None", "Low", "Moderate", "High"];
const BASE_DELAY = 520;                 // ms per replayed item at 1x

const $ = (id) => document.getElementById(id);
const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
const fmt = (v, d = 4) => (typeof v === "number" ? v.toFixed(d) : v);
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

const state = {
  status: null,
  barangayShard: {},
  sites: new Map(),          // site_id -> {lat, lon, barangay, shard, ...scored fields}
  shardStage: {},            // shard -> none | raw | gated | classified | published
  top: [],                   // final recommendation
  candidates: new Set(),     // shard-local top-k (shown while scatter-gather replays)
  colorMode: "auto",
  lastRun: null,
  busy: false,
  done: new Set(),
};
SHARDS.forEach((s) => (state.shardStage[s] = "none"));

/* --------------------------------------------------------------------------------------
 * HTTP helpers
 * ------------------------------------------------------------------------------------ */
async function call(method, path, body, headers = {}) {
  const res = await fetch(API + path, {
    method, headers: { "Content-Type": "application/json", ...headers },
    body: body ? JSON.stringify(body) : undefined,
  });
  let data = null;
  try { data = await res.json(); } catch (_) { /* empty */ }
  return { ok: res.ok, status: res.status, data, trace: res.headers.get("X-Trace-ID") };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchTrace(traceId) {
  // telemetry is shipped in 0.3 s batches, commit notices arrive just after the response
  await sleep(1200);
  let best = null;
  for (let i = 0; i < 6; i++) {
    const r = await call("GET", `/observability/traces/${traceId}`);
    if (r.ok) {
      if (best && r.data.span_count === best.span_count) return r.data;
      best = r.data;
    }
    await sleep(400);
  }
  return best;
}

/* --------------------------------------------------------------------------------------
 * Topology diagram
 * ------------------------------------------------------------------------------------ */
const POS = {};
const NODE_W = 132, NODE_H = 44;

function buildTopology() {
  const svg = $("topology");
  const ns = "http://www.w3.org/2000/svg";
  const el = (tag, attrs, parent = svg) => {
    const e = document.createElementNS(ns, tag);
    Object.entries(attrs).forEach(([k, v]) => e.setAttribute(k, v));
    parent.appendChild(e);
    return e;
  };
  svg.innerHTML = "";
  POS.client = { x: 80, y: 40, label: "Planner browser", sub: "Internet" };
  POS.gateway = { x: 320, y: 40, label: "gateway", sub: "public subnet · AZ-a" };
  POS["cho-vault"] = { x: 560, y: 40, label: "cho-vault", sub: "CHO private cloud" };
  POS.collector = { x: 560, y: 102, label: "collector", sub: "observability · AZ-b", small: true };
  SHARDS.forEach((s, i) => {
    AZS.forEach((az, j) => {
      POS[`${s}-${az}`] = { x: 120 + i * 200, y: 200 + j * 66, label: `${s}-${az}`, sub: `AZ-${az}`, shard: s };
    });
  });

  const links = el("g", {});
  const groups = el("g", {});
  SHARDS.forEach((s, i) => {
    el("rect", { x: 120 + i * 200 - 78, y: 140, width: 156, height: 248, rx: 10, class: "group" }, groups);
    const t = el("text", { x: 120 + i * 200, y: 160, "text-anchor": "middle", "font-weight": 600 }, groups);
    t.textContent = `shard ${s}`;
    t.setAttribute("style", `fill: var(--${s})`);
  });
  AZS.forEach((az, j) => {
    const t = el("text", { x: 8, y: 204 + j * 66, class: "sub" }, groups);
    t.textContent = `AZ-${az}`;
  });
  el("line", { x1: POS.client.x, y1: POS.client.y, x2: POS.gateway.x, y2: POS.gateway.y, class: "link" }, links);
  el("line", { x1: POS.gateway.x, y1: POS.gateway.y, x2: POS["cho-vault"].x, y2: POS["cho-vault"].y, class: "vpn" }, links);
  const vpn = el("text", { x: 440, y: 32, "text-anchor": "middle", class: "sub" }, links);
  vpn.textContent = "site-to-site VPN";
  Object.entries(POS).forEach(([id, p]) => {
    if (p.shard) el("line", { x1: POS.gateway.x, y1: POS.gateway.y + 22, x2: p.x, y2: p.y - 22, class: "link", opacity: 0.55 }, links);
  });

  Object.entries(POS).forEach(([id, p]) => {
    const w = p.small ? 112 : NODE_W, h = p.small ? 34 : NODE_H;
    const g = el("g", { class: "node", id: `node-${id}`, transform: `translate(${p.x - w / 2},${p.y - h / 2})` });
    el("rect", { width: w, height: h, rx: 7 }, g);
    const t = el("text", { x: 10, y: p.small ? 15 : 18, "font-weight": 600 }, g);
    t.textContent = p.label;
    const st = el("text", { x: 10, y: p.small ? 27 : 33, class: "sub" }, g);
    st.textContent = p.sub;
    if (p.shard) {
      const b = el("g", { class: "lead", id: `lead-${id}`, visibility: "hidden" }, g);
      el("rect", { x: w - 24, y: 7, width: 16, height: 16, rx: 3, class: "badge" }, b);
      const bt = el("text", { x: w - 16, y: 19, "text-anchor": "middle", class: "badge-t" }, b);
      bt.textContent = "L";
    }
  });
  el("g", { id: "pulses" });
}

function flashNode(id, ms = 600) {
  const g = $(`node-${id}`);
  if (!g) return;
  g.classList.add("active");
  clearTimeout(g._t);
  g._t = setTimeout(() => g.classList.remove("active"), ms);
}

function pulse(from, to, ms) {
  const a = POS[from], b = POS[to];
  if (!a || !b || from === to) return;
  const layer = $("pulses");
  const c = document.createElementNS("http://www.w3.org/2000/svg", "circle");
  c.setAttribute("r", 5);
  c.setAttribute("class", "pulse");
  layer.appendChild(c);
  const t0 = performance.now();
  const step = (now) => {
    const k = Math.min(1, (now - t0) / ms);
    const e = k < 0.5 ? 2 * k * k : 1 - Math.pow(-2 * k + 2, 2) / 2;
    c.setAttribute("cx", a.x + (b.x - a.x) * e);
    c.setAttribute("cy", a.y + (b.y - a.y) * e);
    if (k < 1) requestAnimationFrame(step); else c.remove();
  };
  requestAnimationFrame(step);
}

function renderStatus() {
  const st = state.status;
  const chip = $("clusterChip");
  if (!st) {
    chip.className = "cluster-chip bad";
    $("clusterText").textContent = "gateway unreachable";
    return;
  }
  let up = 0, total = 0;
  Object.values(st.shards).forEach((sh) => {
    Object.entries(sh.replicas).forEach(([id, r]) => {
      total++; if (r.status === "UP") up++;
      $(`node-${id}`)?.classList.toggle("down", r.status !== "UP");
      $(`lead-${id}`)?.setAttribute("visibility", id === sh.leader ? "visible" : "hidden");
    });
  });
  Object.entries(st.services).forEach(([id, s]) => {
    total++; if (s.status === "UP") up++;
    $(`node-${id}`)?.classList.toggle("down", s.status !== "UP");
  });
  chip.className = "cluster-chip " + (up === total ? "ok" : "bad");
  $("clusterText").textContent = `${up}/${total} nodes up · N=3 W=2 R=2`;
}

async function pollStatus() {
  const r = await call("GET", "/cluster/status").catch(() => null);
  state.status = r && r.ok ? r.data : null;
  if (state.status) {
    Object.entries(state.status.shards).forEach(([s, sh]) =>
      sh.barangays.forEach((b) => (state.barangayShard[b] = s)));
  }
  renderStatus();
}

/* --------------------------------------------------------------------------------------
 * Map
 * ------------------------------------------------------------------------------------ */
let proj = null;

function setupProjection() {
  const pts = [...state.sites.values()];
  if (!pts.length) return;
  const lat = pts.map((p) => p.lat), lon = pts.map((p) => p.lon);
  const k = Math.cos((7.05 * Math.PI) / 180);
  proj = { minX: Math.min(...lon) * k, maxX: Math.max(...lon) * k, minY: Math.min(...lat), maxY: Math.max(...lat), k };
}

function drawMap() {
  const cv = $("map");
  const dpr = window.devicePixelRatio || 1;
  const size = cv.clientWidth;
  cv.width = size * dpr; cv.height = size * dpr;
  const ctx = cv.getContext("2d");
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, size, size);
  const anyVisible = SHARDS.some((s) => state.shardStage[s] !== "none");
  $("mapEmpty").style.display = anyVisible ? "none" : "grid";
  if (!proj || !anyVisible) { renderLegend(); return; }
  const pad = 14;
  const span = Math.max(proj.maxX - proj.minX, proj.maxY - proj.minY);
  const sc = (size - 2 * pad) / span;
  const ox = pad + ((size - 2 * pad) - (proj.maxX - proj.minX) * sc) / 2;
  const oy = pad + ((size - 2 * pad) - (proj.maxY - proj.minY) * sc) / 2;
  const xy = (p) => [ox + (p.lon * proj.k - proj.minX) * sc, oy + (proj.maxY - p.lat) * sc];
  const r = Math.max(2, size / 210);
  const colors = { coastal: css("--coastal"), north: css("--north"), upland: css("--upland"),
    high: css("--c-high"), mod: css("--c-mod"), low: css("--c-low"), not: css("--c-not"), gated: css("--gated") };

  for (const p of state.sites.values()) {
    const stage = state.shardStage[p.shard];
    if (stage === "none") continue;
    const [x, y] = xy(p);
    // auto: shard colours until a shard has classified its sites, then suitability classes
    const byShard = state.colorMode === "shard" ||
      (state.colorMode === "auto" && (stage === "raw" || stage === "gated"));
    const gatedVisible = stage !== "raw" && p.recommendation_eligible === false;
    ctx.beginPath();
    ctx.arc(x, y, gatedVisible ? r * 0.85 : r, 0, Math.PI * 2);
    if (gatedVisible && state.colorMode !== "shard") {
      ctx.strokeStyle = colors.gated; ctx.lineWidth = 1; ctx.stroke();
      continue;
    }
    ctx.fillStyle = byShard || !p.suitability_class ? colors[p.shard]
      : colors[CLASS_KEY[p.suitability_class]];
    ctx.fill();
  }
  // shard-local candidates during scatter-gather
  ctx.lineWidth = 2;
  for (const id of state.candidates) {
    const p = state.sites.get(id); if (!p) continue;
    const [x, y] = xy(p);
    ctx.strokeStyle = colors[p.shard];
    ctx.beginPath(); ctx.arc(x, y, r * 3, 0, Math.PI * 2); ctx.stroke();
  }
  // final ranking
  state.top.forEach((t, i) => {
    const p = state.sites.get(t.site_id); if (!p) return;
    const [x, y] = xy(p);
    ctx.strokeStyle = css("--ink"); ctx.lineWidth = 2.5;
    ctx.beginPath(); ctx.arc(x, y, r * 3.4, 0, Math.PI * 2); ctx.stroke();
    ctx.fillStyle = css("--ink");
    ctx.font = `600 ${Math.max(11, size / 42)}px system-ui, sans-serif`;
    ctx.fillText(String(i + 1), x + r * 3.8, y - r * 2.6);
  });
  renderLegend();
}

function renderLegend() {
  const stages = SHARDS.map((s) => state.shardStage[s]);
  const showClass = state.colorMode === "class" ||
    (state.colorMode === "auto" && stages.some((s) => s === "classified" || s === "published"));
  const items = [];
  if (!showClass || state.colorMode === "shard") {
    SHARDS.forEach((s) => items.push(`<span><i style="background:var(--${s})"></i>${s}</span>`));
  }
  if (showClass && state.colorMode !== "shard") {
    CLASSES.forEach((c) => items.push(`<span><i style="background:var(--c-${CLASS_KEY[c]})"></i>${c}</span>`));
  }
  if (stages.some((s) => s !== "raw" && s !== "none")) items.push(`<span><i class="hollow"></i>gated (shoreline / High hazard)</span>`);
  if (state.top.length) items.push(`<span><i class="ring"></i>recommended (rank)</span>`);
  $("legend").innerHTML = items.join("");
}

/* --------------------------------------------------------------------------------------
 * Narration: what each event means, in plain language
 * ------------------------------------------------------------------------------------ */
const PHASE_TEXT = {
  "1-feature-extraction": "Phase 1 · map: each shard leader derives road accessibility RA = 1/(1+d) for its own sites and reports only the min and max of each factor.",
  "2-composite-and-gates": "Phase 2 · map: with the district-wide bounds, each shard normalises the six factors, computes S = 0.40·BD′ + 0.25·RA′ + 0.15·FD′ − 0.08·FL′ − 0.07·LS′ − 0.05·SS′, and applies the hard gates (within 50 m of shore, or any High hazard).",
  "3-rescale-and-classify": "Phase 3 · map: each shard rescales S to 0–1 with the district-wide bounds and assigns a suitability class (τ = 0.25 · 0.40 · 0.60).",
  "4-levels-and-persist": "Phase 4 · map: sites in the upper half of their class get the higher recommendation level; gated sites get level 0. Each leader then writes the scores to a quorum of replicas.",
};

function narrate(ev) {
  const a = ev.attrs || {};
  const n = ev.node;
  switch (ev.event) {
    case "INGEST_PARTITIONED":
      return `The gateway splits ${a.total} sites by barangay: coastal ${a.per_shard.coastal}, north ${a.per_shard.north}, upland ${a.per_shard.upland}. A barangay always lives on one shard.`;
    case "SHARD_ROUTED":
      return `The ${a.shard} batch (${a.records} records) goes to that shard's leader, ${a.leader} (epoch ${a.epoch}).`;
    case "REPLICATION_SENT":
      return `${n} gives the write log position ${a.lsn} and ships ${a.records} records to ${a.followers.join(" and ")}.`;
    case "REPLICA_APPEND_STAGED":
      return `${n} stores the entry (lsn ${a.lsn}) and acknowledges it to its leader.`;
    case "WRITE_COMMITTED":
      return `Shard ${a.shard}: ${a.acks} of 3 replicas hold lsn ${a.lsn}, which meets the quorum of ${a.w_required}, so the write is committed.`;
    case "REPLICA_COMMIT_APPLIED":
      return `${n} applies lsn ${a.lsn}. This replica can now serve the new data.`;
    case "JOB_STARTED":
      return `Scoring job ${a.run_id} starts. Min-max scaling needs district-wide minimums and maximums, but the sites are spread over 3 shards. So each shard computes locally and only summary numbers travel to the gateway.`;
    case "JOB_PHASE_STARTED":
      return PHASE_TEXT[a.phase] || `Phase ${a.phase} started.`;
    case "FEATURE_EXTRACTION_COMPLETE":
      return `${n}: features for ${a.sites} ${a.shard} sites are ready. Only 6 (min, max) pairs leave the shard, not the ${a.sites} rows.`;
    case "GLOBAL_REDUCE":
      if (a.s_min !== undefined) return `Reduce: the gateway combines the shard results into district-wide score bounds S_min = ${fmt(a.s_min)} and S_max = ${fmt(a.s_max)}. Sites in the shoreline setback are left out of the bounds.`;
      if (a.class_counts) return `Reduce: the gateway merges each class's scores from all shards and finds each class's median cut. Classes: ${Object.entries(a.class_counts).map(([c, v]) => `${c} ${v}`).join(", ")}.`;
      return `Reduce: the gateway combines 3 × 6 local bounds into district-wide bounds for all ${a.sites} sites.`;
    case "COMPOSITE_SCORED":
      return `${n}: the composite score is computed for ${a.sites} ${a.shard} sites. ${a.gated} sites fail a hard gate and can never be recommended, whatever their score.`;
    case "CLASSIFIED":
      return `${n}: ${a.shard} sites classified: ${Object.entries(a.class_counts).map(([c, v]) => `${v} ${c}`).join(", ")}.`;
    case "SCORES_PUBLISHED":
      return `${n} publishes ${a.sites} scores (lsn ${a.lsn}, on ${a.acks}/3 replicas). The best eligible site in ${a.shard} is ${a.shard_best}.`;
    case "JOB_COMPLETED":
      return `Job done: ${a.sites} sites scored, ${a.eligible} eligible, ${a.gated} excluded by the hard gates.`;
    case "QUERY_PLANNED":
      return a.kind === "targeted"
        ? `Barangay ${a.barangay} lives on shard ${a.shards[0]}, so only that shard is asked (a targeted query).`
        : `Recommendation query: every shard is asked for its top ${a.top} eligible sites in parallel (scatter-gather, ${a.consistency} reads).`;
    case "LOCAL_TOPK":
      return `${n} sorts its ${a.candidates} eligible ${a.shard} sites${a.barangay ? ` in ${a.barangay}` : ""} by score and returns its best ${a.k}.`;
    case "SCATTER_GATHER_MERGED":
      return `The gateway merges the shard answers by score and keeps the overall top ${a.returned}. A gated site cannot appear, because the shards filter them out before sorting.`;
    case "DEGRADED_RESPONSE":
      return `Partial answer: shard(s) ${a.missing_shards.join(", ")} did not respond, so ${a.missing_barangays.join(", ")} are missing.`;
    case "VPN_TUNNEL_FORWARD":
      return `The gateway forwards the shortlist through the site-to-site VPN to the CHO private cloud.`;
    case "DOSSIER_STORED":
      return `cho-vault stores dossier ${a.dossier_id} (${a.classification}) and records it in the audit log. It is never written to public-cloud storage.`;
    case "STALE_READ_POSSIBLE":
      return `${a.served_by} is ${a.lag} log entries behind its leader, so this answer may be stale.`;
    case "REPLICA_FAILOVER":
      return `A replica failed (${a.failed.join(", ")}). ${a.served_by} answered instead.`;
    default:
      return null;
  }
}

/* --------------------------------------------------------------------------------------
 * Trace replay
 * ------------------------------------------------------------------------------------ */
const player = { items: [], i: 0, paused: false, skip: false, wake: null, t0: 0 };

function flatten(trace) {
  const items = [];
  const walk = (span, parentNode) => {
    const src = parentNode || (span.attrs && span.attrs.source !== "client" ? span.attrs.source : "client");
    items.push({ kind: "span", ts: span.ts, node: span.node, from: src, name: span.name,
                 status: span.status, dur: span.duration_ms });
    span.events.forEach((e) => items.push({ kind: "event", ts: e.ts, node: e.node, event: e.event, attrs: e.attrs, level: e.level }));
    span.children.forEach((c) => walk(c, span.node));
  };
  trace.roots.forEach((r) => walk(r, null));
  (trace.unattached_events || []).forEach((e) => items.push({ kind: "event", ts: e.ts, node: e.node, event: e.event, attrs: e.attrs, level: e.level }));
  items.sort((a, b) => a.ts - b.ts);
  return items;
}

function delay() { return BASE_DELAY / parseFloat($("speed").value); }

function logItem(it) {
  const li = document.createElement("li");
  const shard = POS[it.node]?.shard || "";
  const t = `${((it.ts - player.t0) * 1000).toFixed(1)}ms`;
  if (it.kind === "span") {
    li.className = "span";
    li.innerHTML = `<span class="t">${t}</span><span class="n ${shard}">${esc(it.node)}</span>` +
      `<span class="e">← ${esc(it.from)} · ${esc(it.name)} (${it.status}, ${fmt(it.dur, 1)} ms)</span>`;
  } else {
    li.className = `${it.level} new`;
    const attrs = Object.entries(it.attrs || {}).map(([k, v]) =>
      `${k}=${typeof v === "object" ? JSON.stringify(v) : typeof v === "number" && !Number.isInteger(v) ? fmt(v) : v}`).join(" ");
    li.innerHTML = `<span class="t">${t}</span><span class="n ${shard}">${esc(it.node)}</span>` +
      `<span class="e"><b>${esc(it.event)}</b> ${esc(attrs.length > 220 ? attrs.slice(0, 217) + "…" : attrs)}</span>`;
    setTimeout(() => li.classList.remove("new"), 900);
  }
  const list = $("events");
  list.appendChild(li);
  while (list.children.length > 400) list.firstChild.remove();
  list.scrollTop = list.scrollHeight;
}

function applyEffect(it) {
  if (it.kind !== "event") return;
  const a = it.attrs || {};
  switch (it.event) {
    case "SHARD_ROUTED":
      if (state.shardStage[a.shard] === "none") { state.shardStage[a.shard] = "raw"; drawMap(); }
      break;
    case "COMPOSITE_SCORED": state.shardStage[a.shard] = "gated"; drawMap(); break;
    case "CLASSIFIED": state.shardStage[a.shard] = "classified"; drawMap(); break;
    case "SCORES_PUBLISHED": state.shardStage[a.shard] = "published"; drawMap(); break;
    case "LOCAL_TOPK":
      (state.pendingLocal?.[a.shard] || []).forEach((id) => state.candidates.add(id));
      drawMap();
      break;
    case "SCATTER_GATHER_MERGED":
      state.top = state.pendingTop || [];
      state.candidates.clear();
      drawMap();
      break;
  }
}

function showItem(it) {
  logItem(it);
  if (it.kind === "span") {
    pulse(it.from, it.node, Math.min(delay() * 0.85, 700));
    flashNode(it.node, delay() * 1.4);
  } else {
    flashNode(it.node, delay() * 1.2);
    const text = narrate(it);
    if (text) $("narrMain").textContent = text;
    applyEffect(it);
  }
  $("counter").textContent = `${player.i} / ${player.items.length}`;
  $("progressBar").style.width = `${(100 * player.i) / Math.max(1, player.items.length)}%`;
}

async function replay(trace) {
  player.items = flatten(trace);
  player.i = 0; player.skip = false;
  player.t0 = trace.t0;
  $("traceId").textContent = `trace ${trace.trace_id} · ${trace.span_count} spans · ${trace.nodes.length} nodes`;
  const sep = document.createElement("li");
  sep.className = "span";
  sep.innerHTML = `<span class="t">—</span><span class="n">trace</span><span class="e">${esc(trace.trace_id)} (${esc(trace.roots[0]?.name || "")})</span>`;
  $("events").appendChild(sep);
  while (player.i < player.items.length) {
    const it = player.items[player.i++];
    showItem(it);
    if (player.skip) continue;
    if (player.paused) {
      await new Promise((r) => (player.wake = r));
    } else {
      await sleep(it.kind === "span" ? delay() * 0.7 : delay());
    }
  }
  $("counter").textContent = `${player.items.length} / ${player.items.length}`;
  $("progressBar").style.width = "100%";
}

/* --------------------------------------------------------------------------------------
 * Steps
 * ------------------------------------------------------------------------------------ */
const STEPS = [
  { id: "ingest", title: "Ingest candidate sites",
    desc: "Load 1,502 grid sites (250 m apart) with their extracted features. Each shard leader replicates its share to 2 followers.",
    run: stepIngest },
  { id: "score", title: "Score every site",
    desc: "A distributed job: shards compute locally, the gateway combines summary numbers, in 4 rounds.",
    run: stepScore, needs: "ingest" },
  { id: "recommend", title: "Recommend top sites",
    desc: "Each shard returns its best eligible sites; the gateway merges them into one ranking.",
    run: stepRecommend, needs: "score" },
  { id: "shortlist", title: "Save CHO shortlist",
    desc: "The planner's shortlist goes over the VPN to the CHO private cloud.",
    run: stepShortlist, needs: "recommend" },
];

function renderSteps(activeId) {
  $("stepList").innerHTML = STEPS.map((s, i) => {
    const blocked = s.needs && !state.done.has(s.needs);
    return `<li class="step ${state.done.has(s.id) ? "done" : ""} ${activeId === s.id ? "active" : ""}">
      <div class="step-top"><span class="step-num">${state.done.has(s.id) ? "✓" : i + 1}</span>
        <span class="step-title">${s.title}</span>
        <button class="btn ${activeId === s.id ? "" : "primary"}" data-step="${s.id}"
          ${state.busy || blocked ? "disabled" : ""}>${state.done.has(s.id) ? "Re-run" : "Run"}</button></div>
      <p>${s.desc}</p></li>`;
  }).join("");
  $("runAll").disabled = state.busy;
  document.querySelectorAll("[data-step]").forEach((b) =>
    b.addEventListener("click", () => runStep(b.dataset.step)));
}

async function runStep(id) {
  if (state.busy) return;
  const step = STEPS.find((s) => s.id === id);
  state.busy = true;
  renderSteps(id);
  $("narrStep").textContent = `Step ${STEPS.indexOf(step) + 1} · ${step.title}`;
  try {
    await step.run();
    state.done.add(id);
  } catch (err) {
    $("narrMain").textContent = `Step failed: ${err.message}`;
  } finally {
    state.busy = false;
    renderSteps(null);
  }
}

async function runAll() {
  for (const id of ["ingest", "score", "recommend"]) {
    await runStep(id);
    if (!state.done.has(id)) return;
    await sleep(600);
  }
}

async function stepIngest() {
  $("narrMain").textContent = "Reading the candidate-site dataset and sending it to the gateway…";
  const csv = await (await fetch("/ui/data/sites.csv")).text();
  const rows = csv.trim().split(/\r?\n/);
  const head = rows.shift().split(",");
  const records = rows.map((line) => {
    const v = line.split(","), o = {};
    head.forEach((h, i) => (o[h] = v[i]));
    return o;
  });
  state.sites.clear();
  records.forEach((r) => state.sites.set(r.site_id, {
    lat: +r.latitude, lon: +r.longitude, barangay: r.barangay, shard: state.barangayShard[r.barangay] }));
  setupProjection();
  SHARDS.forEach((s) => (state.shardStage[s] = "none"));
  state.top = []; state.candidates.clear(); state.done.clear();
  drawMap();
  const payload = records.map(({ baseline_score, ...rest }) => rest);
  const r = await call("POST", "/api/v1/ingest", { records: payload });
  if (!r.ok) throw new Error(`ingest returned HTTP ${r.status}`);
  renderIngestResults(r.data);
  await replay(await fetchTrace(r.trace));
  SHARDS.forEach((s) => (state.shardStage[s] = "raw"));
  drawMap();
  $("narrMain").textContent = "All 1,502 sites are stored, each on 3 replicas in 3 availability zones. Next: score them.";
}

async function stepScore() {
  $("narrMain").textContent = "Starting the distributed scoring job…";
  const r = await call("POST", "/api/v1/jobs/score");
  if (!r.ok) throw new Error(r.data?.detail?.error || `HTTP ${r.status}`);
  state.lastRun = r.data;
  const exp = await call("GET", "/api/v1/export?consistency=strong");
  exp.data.sites.forEach((s) => {
    const p = state.sites.get(s.site_id) || {};
    state.sites.set(s.site_id, { ...p, ...s, lat: s.latitude, lon: s.longitude,
      shard: state.barangayShard[s.barangay] });
  });
  setupProjection();
  SHARDS.forEach((s) => (state.shardStage[s] = "raw"));
  state.top = [];
  drawMap();
  renderScoreResults(r.data);
  await replay(await fetchTrace(r.trace));
  SHARDS.forEach((s) => (state.shardStage[s] = "published"));
  drawMap();
  $("narrMain").textContent = `${r.data.eligible} sites are eligible and ${r.data.gated} are excluded by the hard gates. Next: ask for the best sites.`;
}

async function stepRecommend() {
  const top = parseInt($("topN").value, 10);
  const barangay = $("barangay").value;
  const q = new URLSearchParams({ top, consistency: "eventual" });
  if (barangay) q.set("barangay", barangay);
  $("narrMain").textContent = "Sending the recommendation query…";
  const r = await call("GET", `/api/v1/recommend?${q}`);
  if (!r.ok) throw new Error(r.data?.detail?.error || `HTTP ${r.status}`);
  if (!state.lastRun) state.lastRun = (await call("GET", "/api/v1/jobs/last")).data;
  // shard-local top-k (what each shard sends back) for the replay
  state.pendingLocal = {};
  const shards = barangay ? [state.barangayShard[barangay]] : SHARDS;
  for (const s of shards) {
    const local = [...state.sites.values()]
      .filter((p) => p.shard === s && p.recommendation_eligible && (!barangay || p.barangay === barangay))
      .sort((a, b) => b.score - a.score).slice(0, top);
    state.pendingLocal[s] = local.map((p) => p.site_id);
  }
  state.pendingTop = r.data.top;
  state.top = []; state.candidates.clear(); drawMap();
  renderRecommendResults(r.data, barangay);
  await replay(await fetchTrace(r.trace));
  state.top = r.data.top; state.candidates.clear(); drawMap();
  $("narrMain").textContent = `Done. The ${r.data.top.length} recommended sites are ringed on the map. Open "Why this site?" on each one to see the full calculation.`;
}

async function stepShortlist() {
  const sites = state.top.map((t) => t.site_id);
  const dossier = { dossier_id: `CHO-${new Date().toISOString().slice(0, 10)}-${Math.floor(Math.random() * 900 + 100)}`,
    title: "Talomo new health facility shortlist", shortlisted_sites: sites,
    planner_notes: "For ocular inspection and lot-ownership check.", prepared_by: "CHO planning officer" };
  const r = await call("POST", "/api/v1/private/dossiers", dossier, CHO_HEADERS);
  if (!r.ok) throw new Error(r.data?.detail?.error || r.data?.detail || `HTTP ${r.status}`);
  $("resultsTitle").textContent = "Results · CHO shortlist";
  $("results").innerHTML = `<table class="kv"><tr><th>Dossier</th><td>${esc(r.data.dossier_id)}</td></tr>
    <tr><th>Stored in</th><td>${esc(r.data.location)}</td></tr>
    <tr><th>Sites</th><td>${sites.map(esc).join(", ")}</td></tr>
    <tr><th>Access</th><td>CHO planner role only. A call that skips the VPN gets HTTP 403 from the vault itself.</td></tr></table>`;
  await replay(await fetchTrace(r.trace));
}

/* --------------------------------------------------------------------------------------
 * Results panels
 * ------------------------------------------------------------------------------------ */
function renderIngestResults(d) {
  $("resultsTitle").textContent = "Results · ingest";
  const rows = Object.entries(d.shards).map(([s, r]) => `<tr><td style="color:var(--${s});font-weight:600">${s}</td>
    <td class="num">${r.records}</td><td class="num">${r.lsn}</td><td class="num">${r.acks}/3</td>
    <td>${(r.replicas || []).join(", ")}</td></tr>`).join("");
  $("results").innerHTML = `<table class="kv"><tr><th>Shard</th><th>Sites</th><th>LSN</th><th>Acks</th><th>Replicas holding it</th></tr>${rows}</table>
    <p class="muted">A write commits once 2 of 3 replicas hold it (W = 2). The barangay decides the shard,
    so a query for one barangay touches one shard only.</p>`;
}

function classBar(counts) {
  const total = Object.values(counts).reduce((a, b) => a + b, 0);
  return `<div class="classbar">${CLASSES.map((c) =>
    `<div style="width:${(100 * counts[c]) / total}%;background:var(--c-${CLASS_KEY[c]})" title="${c}: ${counts[c]}">${counts[c] > 60 ? counts[c] : ""}</div>`).join("")}</div>`;
}

function renderScoreResults(d) {
  $("resultsTitle").textContent = `Results · scoring ${d.run_id}`;
  const fb = d.feature_bounds || {};
  const label = Object.fromEntries(WEIGHTS.map(([k, l]) => [k, l]));
  const bounds = Object.entries(fb).map(([k, [lo, hi]]) =>
    `<tr><td>${label[k] || k}</td><td class="num">${fmt(lo)}</td><td class="num">${fmt(hi)}</td></tr>`).join("");
  const cuts = Object.entries(d.class_cuts || {}).map(([c, v]) =>
    `<tr><td><span class="pill ${CLASS_KEY[c]}">${c}</span></td><td class="num">${d.class_counts[c]}</td>
     <td class="num">${v == null ? "—" : fmt(v)}</td></tr>`).join("");
  $("results").innerHTML = `
    <table class="kv"><tr><th>Sites</th><td class="num">${d.sites}</td><th>Eligible</th><td class="num">${d.eligible}</td>
      <th>Gated</th><td class="num">${d.gated} (${((100 * d.gated) / d.sites).toFixed(1)}%)</td></tr></table>
    ${classBar(d.class_counts)}
    <table class="kv"><tr><th>Round 1 · district-wide factor bounds</th><th class="num">min</th><th class="num">max</th></tr>${bounds}</table>
    <table class="kv"><tr><th>Round 2 · composite S bounds</th><td class="num">${fmt(d.score_bounds_raw[0])}</td><td class="num">${fmt(d.score_bounds_raw[1])}</td></tr></table>
    <table class="kv"><tr><th>Round 3 · class</th><th class="num">sites</th><th class="num">median cut</th></tr>${cuts}</table>
    <table class="kv"><tr><th>Phase timings (ms)</th>${Object.entries(d.phase_ms).map(([k, v]) => `<td>${k} <b>${v}</b></td>`).join("")}</tr></table>`;
}

function whyHtml(site, rank) {
  const run = state.lastRun || {};
  const n = site.factors_normalized || {};
  let sum = 0;
  const bars = WEIGHTS.map(([k, lbl, w, sign]) => {
    const v = sign * w * (n[k] || 0);
    sum += v;
    const pct = (Math.abs(v) / 0.40) * 75;
    return `<div class="bar-row"><span>${sign > 0 ? "+" : "−"} ${w.toFixed(2)} × ${lbl} (${fmt(n[k] || 0, 3)})</span>
      <div class="bar-track"><div class="mid"></div><div class="bar ${v >= 0 ? "pos" : "neg"}" style="width:${pct}%"></div></div>
      <span class="v">${v >= 0 ? "+" : ""}${fmt(v)}</span></div>`;
  }).join("");
  const [smin, smax] = run.score_bounds_raw || [NaN, NaN];
  const cut = (run.class_cuts || {})[site.suitability_class];
  const upper = cut != null && site.score > cut;
  const hz = [["flood_susceptibility", "Flood"], ["landslide_susceptibility", "Landslide"], ["storm_surge_susceptibility", "Storm surge"]];
  return `<div class="why">
    <h4>1 · Hard gates (pass/fail, checked before any score is considered)</h4>
    <div class="gate"><span class="${site.shoreline_distance >= 50 ? "ok" : "no"}">${site.shoreline_distance >= 50 ? "✓" : "✗"}</span>
      ${fmt(site.shoreline_distance, 0)} m from the shoreline (must be at least 50 m)</div>
    <div class="gate"><span class="${site.high_hazard_excluded ? "no" : "ok"}">${site.high_hazard_excluded ? "✗" : "✓"}</span>
      Hazards: ${hz.map(([k, l]) => `${l} ${HAZARD_LABEL[site[k]]}`).join(" · ")} (no layer may be High)</div>
    <h4>2 · Weighted factors (normalised 0–1 across the district)</h4>
    ${bars}
    <div class="muted" style="margin-top:4px">Raw inputs: ${fmt(site.building_density, 0)} buildings within 300 m ·
      ${fmt(site.road_distance, 0)} m to the nearest road · ${fmt(site.facility_distance, 0)} m to the nearest existing facility</div>
    <h4>3 · Score and class</h4>
    <div class="formula">S = ${fmt(sum)} → S′ = (S − ${fmt(smin)}) / (${fmt(smax)} − ${fmt(smin)}) = <b>${fmt(site.score)}</b></div>
    <div style="margin-top:6px">${fmt(site.score, 3)} is ${site.score >= 0.6 ? "≥ 0.60" : site.score >= 0.4 ? "between 0.40 and 0.60" : site.score >= 0.25 ? "between 0.25 and 0.40" : "< 0.25"}
      → <span class="pill ${CLASS_KEY[site.suitability_class]}">${site.suitability_class}</span>.
      ${cut != null ? `It scores ${upper ? "above" : "at or below"} the class median cut (${fmt(cut)}), so it takes the
      ${upper ? "upper" : "lower"} level of that class: <b>Level ${site.recommendation_level} · ${site.recommendation_label}</b>.` : ""}</div>
    <h4>4 · Ranking</h4>
    <div>Rank ${rank} after the gateway merged each shard's best sites. Shard: <b style="color:var(--${state.barangayShard[site.barangay]})">${state.barangayShard[site.barangay]}</b> · run ${site.run_id} · record version ${site._v}</div>
  </div>`;
}

function renderRecommendResults(d, barangay) {
  $("resultsTitle").textContent = `Results · top ${d.top.length} ${barangay ? "in " + barangay : "in Talomo District"}`;
  const served = Object.entries(d.served_by).map(([s, n]) => `${s} → ${n}`).join(" · ");
  $("results").innerHTML = `<p class="muted">Served by ${served} (${d.consistency} reads${d.degraded ? ", <b>partial answer</b>" : ""}).</p>` +
    d.top.map((s, i) => `<div class="card"><div class="card-top"><span class="rank">#${i + 1}</span>
      <b>${esc(s.site_id)}</b><span>${esc(s.barangay)}</span>
      <span class="pill ${CLASS_KEY[s.suitability_class]}">${s.suitability_class}</span>
      <span class="score">${fmt(s.score, 3)}</span></div>
      <div class="muted">Level ${s.recommendation_level} · ${s.recommendation_label}</div>
      <details ${i === 0 ? "open" : ""}><summary>Why this site?</summary>${whyHtml(s, i + 1)}</details></div>`).join("");
}

/* --------------------------------------------------------------------------------------
 * Boot
 * ------------------------------------------------------------------------------------ */
async function boot() {
  buildTopology();
  await pollStatus();
  setInterval(pollStatus, 2000);
  const brgys = Object.keys(state.barangayShard).sort();
  $("barangay").innerHTML += brgys.map((b) => `<option>${esc(b)}</option>`).join("");

  // If the cluster already holds scored data (e.g. after simulate.py --keep), show it.
  const last = await call("GET", "/api/v1/jobs/last").catch(() => null);
  if (last?.ok && last.data.run_id) {
    state.lastRun = last.data;
    const exp = await call("GET", "/api/v1/export?consistency=strong");
    if (exp.ok) {
      exp.data.sites.forEach((s) => state.sites.set(s.site_id, { ...s, lat: s.latitude, lon: s.longitude,
        shard: state.barangayShard[s.barangay] }));
      setupProjection();
      SHARDS.forEach((s) => (state.shardStage[s] = "published"));
      state.done.add("ingest"); state.done.add("score");
      $("narrMain").textContent = `The cluster already holds scored data (${last.data.run_id}). Run step 3 to watch a recommendation, or start again from step 1.`;
    }
  }
  renderSteps(null);
  drawMap();

  $("runAll").addEventListener("click", runAll);
  $("playPause").addEventListener("click", () => {
    player.paused = !player.paused;
    $("playPause").textContent = player.paused ? "▶" : "❚❚";
    $("playPause").setAttribute("aria-label", player.paused ? "Resume" : "Pause");
    if (!player.paused && player.wake) { player.wake(); player.wake = null; }
  });
  $("stepOnce").addEventListener("click", () => { if (player.wake) { player.wake(); player.wake = null; } });
  $("skipEnd").addEventListener("click", () => { player.skip = true; if (player.wake) { player.wake(); player.wake = null; } });
  document.querySelectorAll("#colorMode button").forEach((b) => b.addEventListener("click", () => {
    document.querySelectorAll("#colorMode button").forEach((x) => x.classList.toggle("on", x === b));
    state.colorMode = b.dataset.mode; drawMap();
  }));
  window.addEventListener("resize", drawMap);
  window.matchMedia("(prefers-color-scheme: dark)").addEventListener?.("change", drawMap);
}

boot();
