"""API gateway + coordinator (control plane) for the POLARIS cloud cluster.

Data plane
  * directory-based geographic sharding: barangay -> shard
  * quorum writes (W=2 of N=3) through the shard leader
  * tunable reads: consistency=strong (R=2 quorum + read repair, CP)
                   consistency=eventual (any one replica, AP)
  * scatter-gather queries with graceful degradation
  * the distributed scoring job (map -> reduce -> broadcast rounds)
  * edge cache for the public export (CDN stand-in)
  * VPN path to the CHO private cloud with role-based access control

Control plane
  * heartbeat failure detector (UP -> SUSPECT -> DOWN -> RECOVERED)
  * leader election with epochs; refuses to elect without a majority
  * per-node circuit breakers, retries and time-outs (see common.net)
"""
from __future__ import annotations

import asyncio
import heapq
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse

from common import scoring as sc
from common import telemetry, topology
from common.models import ChaosRequest, Dossier, HazardUpdate, IngestRequest
from common.net import Network, NodeUnavailable

NODE_ID = "gateway"
HEARTBEAT_S = 0.5
DOWN_AFTER_MISSES = 2
CHO_API_KEY = "cho-demo-key"           # demo credential (would come from a secrets manager)
VPN_TUNNEL_ID = "vpn-tunnel-cho-01"

tel = telemetry.init(NODE_ID, topology.node("collector").url)
net = Network(NODE_ID)
app = FastAPI(title="POLARIS Cloud Gateway / Coordinator")
telemetry.install(app, tel)


# --------------------------------------------------------------------------
# Cluster state (in production: kept in a Raft-replicated config store, e.g. etcd)
# --------------------------------------------------------------------------
class Member:
    def __init__(self, spec: topology.NodeSpec):
        self.spec = spec
        self.status = "UNKNOWN"
        self.misses = 0
        self.health: Dict[str, Any] = {}


MEMBERS: Dict[str, Member] = {n.node_id: Member(n) for n in topology.NODES.values()
                              if n.node_id != NODE_ID}
SHARDS: Dict[str, Dict[str, Any]] = {
    s: {"epoch": 1, "leader": topology.initial_leader(s).node_id, "read_only": False}
    for s in topology.SHARD_MAP}
SITE_INDEX: Dict[str, str] = {}              # directory: site_id -> shard
EDGE_CACHE: Dict[str, Any] = {}
LAST_RUN: Dict[str, Any] = {}
RUN_COUNTER = {"n": 0}


def sys_trace() -> None:
    telemetry.trace_id_var.set("sys-" + telemetry.new_id(4))
    telemetry.span_id_var.set("-")


def up(node_id: str) -> bool:
    return MEMBERS[node_id].status in ("UP", "RECOVERED")


def leader_lsn(shard: str) -> int:
    return MEMBERS[SHARDS[shard]["leader"]].health.get("applied_lsn", 0)


# --------------------------------------------------------------------------
# Failure detector + leader election
# --------------------------------------------------------------------------
async def probe(m: Member) -> None:
    try:
        if m.spec.role == "data":
            sh = SHARDS[m.spec.shard]
            h = await net.call(m.spec.node_id, "POST", "/control/heartbeat",
                               json={"epoch": sh["epoch"], "leader_id": sh["leader"],
                                     "leader_lsn": leader_lsn(m.spec.shard)},
                               timeout=0.4, use_breaker=False, quiet=True)
        else:
            h = await net.call(m.spec.node_id, "GET", "/health", timeout=0.4,
                               use_breaker=False, quiet=True)
        m.health = h
        if m.status in ("DOWN", "SUSPECT"):
            telemetry.event("NODE_RECOVERED", node=m.spec.node_id, was=m.status,
                            zone=m.spec.zone, role=h.get("role"))
        elif m.status == "UNKNOWN":
            telemetry.event("NODE_JOINED", node=m.spec.node_id, zone=m.spec.zone,
                            subnet=m.spec.subnet, role=h.get("role"), shard=m.spec.shard)
        m.status, m.misses = "UP", 0
    except NodeUnavailable as exc:
        m.misses += 1
        if m.status == "UNKNOWN":
            return
        new = "DOWN" if m.misses >= DOWN_AFTER_MISSES else "SUSPECT"
        if new != m.status:
            telemetry.event(f"NODE_{new}", "WARN" if new == "SUSPECT" else "ERROR",
                            node=m.spec.node_id, missed_heartbeats=m.misses, reason=exc.reason)
            m.status = new


async def elect(shard: str) -> None:
    sh = SHARDS[shard]
    members = topology.shard_members(shard)
    alive = [n for n in members if up(n.node_id)]
    majority = len(members) // 2 + 1
    if len(alive) < majority:
        if not sh["read_only"]:
            sh["read_only"] = True
            telemetry.event("SHARD_READ_ONLY", "ERROR", shard=shard, alive=len(alive),
                            majority=majority,
                            reason="no majority: refusing to elect a leader (avoids split brain)")
        return
    best = max(alive, key=lambda n: (MEMBERS[n.node_id].health.get("last_lsn", 0),
                                     -ord(n.zone[-1])))
    new_epoch = sh["epoch"] + 1
    try:
        await net.call(best.node_id, "POST", "/control/promote",
                       json={"epoch": new_epoch, "leader_id": best.node_id}, timeout=1.0,
                       use_breaker=False)
    except NodeUnavailable:
        return
    telemetry.event("LEADER_ELECTED", "WARN", shard=shard, old_leader=sh["leader"],
                    new_leader=best.node_id, epoch=new_epoch, zone=best.zone,
                    rule="highest log position among a live majority")
    sh.update(epoch=new_epoch, leader=best.node_id, read_only=False)


async def control_loop() -> None:
    await asyncio.sleep(1.0)
    while True:
        sys_trace()
        await asyncio.gather(*(probe(m) for m in MEMBERS.values()))
        for shard, sh in SHARDS.items():
            if not up(sh["leader"]):
                if MEMBERS[sh["leader"]].status == "DOWN":
                    await elect(shard)
            elif sh["read_only"]:
                sh["read_only"] = False
                telemetry.event("SHARD_WRITABLE", shard=shard, leader=sh["leader"])
        await asyncio.sleep(HEARTBEAT_S)


@app.on_event("startup")
async def _start():
    asyncio.get_event_loop().create_task(control_loop())


# --------------------------------------------------------------------------
# Data-plane helpers
# --------------------------------------------------------------------------
def unavailable(detail: Any) -> HTTPException:
    return HTTPException(503, detail=detail)


async def shard_write(shard: str, op: str, records: List[Dict[str, Any]],
                      w: int = topology.WRITE_QUORUM) -> Dict[str, Any]:
    sh = SHARDS[shard]
    alive = [n.node_id for n in topology.shard_members(shard) if up(n.node_id)]
    if len(alive) < w:
        telemetry.event("QUORUM_PRECHECK_FAILED", "ERROR", shard=shard, alive=alive, w_required=w,
                        decision="CP: reject write rather than accept a write that may be lost")
        raise unavailable({"error": "write quorum unavailable", "shard": shard, "alive": alive,
                           "w": w, "cap": "consistency chosen over availability"})
    if not up(sh["leader"]):
        telemetry.event("LEADER_UNAVAILABLE", "WARN", shard=shard, leader=sh["leader"])
        raise unavailable({"error": "leader election in progress; retry", "shard": shard})
    key = f"{telemetry.trace_id_var.get()}-{shard}-{op}"
    for attempt in range(2):
        telemetry.event("SHARD_ROUTED", shard=shard, leader=sh["leader"], epoch=sh["epoch"], op=op,
                        records=len(records), w=w)
        try:
            resp = await net.call(sh["leader"], "POST", "/kv/write", timeout=5.0,
                                  json={"op": op, "records": records, "epoch": sh["epoch"],
                                        "w": w, "idempotency_key": key})
        except NodeUnavailable as exc:
            raise unavailable({"error": f"write failed on leader: {exc.reason}", "shard": shard})
        if resp.get("_status") == 409 and attempt == 0:
            telemetry.event("LEADER_REDIRECT", "WARN", shard=shard, detail=resp.get("detail"))
            await asyncio.sleep(HEARTBEAT_S * 2)
            continue
        if resp.get("_status"):
            raise HTTPException(resp["_status"], detail=resp.get("detail"))
        return resp
    raise unavailable({"error": "leader changed during write", "shard": shard})


def read_candidates(shard: str) -> List[str]:
    """Eventual reads: prefer followers (offload the leader), then lowest link latency."""
    leader = SHARDS[shard]["leader"]
    gz = topology.zone_of(NODE_ID)
    nodes = [n for n in topology.shard_members(shard) if MEMBERS[n.node_id].status != "DOWN"]
    nodes.sort(key=lambda n: (n.node_id == leader, topology.link_latency_ms(gz, n.zone),
                              n.node_id))
    return [n.node_id for n in nodes]


async def eventual_call(shard: str, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
    tried = []
    for nid in read_candidates(shard):
        try:
            resp = await net.call(nid, "GET", path, params=params, timeout=0.8)
            if tried:
                telemetry.event("REPLICA_FAILOVER", "WARN", shard=shard, failed=tried, served_by=nid)
            return resp
        except NodeUnavailable as exc:
            tried.append(f"{nid}({exc.reason})")
    raise unavailable({"error": "no replica reachable", "shard": shard, "tried": tried})


async def strong_call(shard: str, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
    """Strong single-shard query: answered by the current leader (epoch-fenced)."""
    sh = SHARDS[shard]
    alive = [n.node_id for n in topology.shard_members(shard) if up(n.node_id)]
    if len(alive) < topology.READ_QUORUM or not up(sh["leader"]):
        telemetry.event("STRONG_READ_REJECTED", "ERROR", shard=shard, alive=alive,
                        decision="CP: refuse rather than risk a stale answer")
        raise unavailable({"error": "strong read unavailable", "shard": shard, "alive": alive})
    try:
        return await net.call(sh["leader"], "GET", path, params=params, timeout=1.0, retries=1)
    except NodeUnavailable as exc:
        raise unavailable({"error": f"leader unreachable: {exc.reason}", "shard": shard})


async def locate(site_id: str) -> str:
    if site_id in SITE_INDEX:
        return SITE_INDEX[site_id]
    telemetry.event("DIRECTORY_MISS", "WARN", site_id=site_id, action="broadcast lookup")
    for shard in SHARDS:
        try:
            resp = await eventual_call(shard, f"/kv/site/{site_id}")
            if "record" in resp:
                SITE_INDEX[site_id] = shard
                return shard
        except HTTPException:
            continue
    raise HTTPException(404, detail=f"site {site_id} not found on any shard")


# --------------------------------------------------------------------------
# Cluster endpoints
# --------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "HEALTHY", "node_id": NODE_ID}


@app.get("/cluster/health")
async def cluster_health():
    return {"members": {nid: m.status for nid, m in MEMBERS.items()},
            "trace_id": telemetry.trace_id_var.get()}


@app.get("/cluster/status")
async def cluster_status():
    shards = {}
    for s, sh in SHARDS.items():
        llsn = leader_lsn(s)
        shards[s] = {
            "barangays": topology.SHARD_MAP[s], "leader": sh["leader"], "epoch": sh["epoch"],
            "read_only": sh["read_only"], "leader_lsn": llsn,
            "replicas": {n.node_id: {"zone": n.zone, "status": MEMBERS[n.node_id].status,
                                     "role": MEMBERS[n.node_id].health.get("role"),
                                     "applied_lsn": MEMBERS[n.node_id].health.get("applied_lsn"),
                                     "lag": llsn - MEMBERS[n.node_id].health.get("applied_lsn", 0),
                                     "keys": MEMBERS[n.node_id].health.get("stored_keys_count"),
                                     "chaos": MEMBERS[n.node_id].health.get("chaos")}
                         for n in topology.shard_members(s)}}
    return {"deployment_model": "hybrid cloud", "zones": topology.ZONES,
            "replication_factor": topology.REPLICATION_FACTOR, "write_quorum": topology.WRITE_QUORUM,
            "read_quorum": topology.READ_QUORUM, "shards": shards,
            "services": {nid: {"zone": m.spec.zone, "subnet": m.spec.subnet, "status": m.status}
                         for nid, m in MEMBERS.items() if m.spec.role != "data"},
            "circuit_breakers": {t: b.state for t, b in net.breakers.items() if b.state != "CLOSED"},
            "directory_entries": len(SITE_INDEX), "last_run": LAST_RUN.get("run_id")}


@app.get("/cluster/metrics")
async def cluster_metrics():
    async def one(nid):
        try:
            return nid, await net.call(nid, "GET", "/metrics", timeout=0.5, use_breaker=False,
                                       quiet=True)
        except NodeUnavailable as exc:
            return nid, {"unreachable": exc.reason}
    results = dict(await asyncio.gather(*(one(n) for n in MEMBERS)))
    results[NODE_ID] = {"node_id": NODE_ID, **tel.metrics.snapshot()}
    return results


@app.post("/admin/chaos/{node_id}")
async def admin_chaos(node_id: str, req: ChaosRequest):
    if node_id not in MEMBERS:
        raise HTTPException(404, detail="unknown node")
    telemetry.event("CHAOS_REQUESTED", "WARN", target=node_id, mode=req.mode)
    return await net.call(node_id, "POST", "/admin/chaos", json=req.model_dump(), timeout=1.0,
                          use_breaker=False)


# --------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------
@app.post("/api/v1/ingest")
async def ingest(req: IngestRequest):
    groups: Dict[str, List[Dict[str, Any]]] = {s: [] for s in SHARDS}
    for r in req.records:
        groups[topology.shard_for_barangay(r.barangay)].append(r.model_dump())
    telemetry.event("INGEST_PARTITIONED", source=req.source, total=len(req.records),
                    per_shard={s: len(v) for s, v in groups.items()})
    results = await asyncio.gather(*(shard_write(s, "upsert", recs) for s, recs in groups.items()
                                     if recs), return_exceptions=True)
    out = {}
    for (s, recs), res in zip([(s, v) for s, v in groups.items() if v], results):
        if isinstance(res, Exception):
            out[s] = {"committed": False, "error": getattr(res, "detail", str(res))}
        else:
            out[s] = {**res, "records": len(recs)}
            for r in recs:
                SITE_INDEX[r["site_id"]] = s
    EDGE_CACHE.clear()
    return {"shards": out, "trace_id": telemetry.trace_id_var.get()}


@app.post("/api/v1/jobs/score")
async def score_job():
    """Distributed, hazard-integrated scoring of every candidate site (F1-F22).

    Global min-max normalisation needs ALL sites, so the job requires every shard
    leader (CP for batch analytics: a partial run would silently shift everyone's score).
    """
    for attempt in range(2):
        try:
            return await _run_job()
        except NodeUnavailable as exc:
            telemetry.event("JOB_ATTEMPT_FAILED", "ERROR", attempt=attempt + 1, node=exc.node_id,
                            reason=exc.reason, action="wait for failover, restart job" if
                            attempt == 0 else "abort")
            if attempt == 0:
                await asyncio.sleep(HEARTBEAT_S * (DOWN_AFTER_MISSES + 3))
    raise unavailable({"error": "scoring job aborted: a shard is unavailable",
                       "cap": "batch job requires a complete, consistent dataset"})


async def _phase(name: str, path: str, body_for) -> Dict[str, Any]:
    t0 = time.time()
    telemetry.event("JOB_PHASE_STARTED", phase=name, shards=list(SHARDS))
    for s, sh in SHARDS.items():
        if not up(sh["leader"]):
            raise NodeUnavailable(sh["leader"], f"leader of shard '{s}' is down")

    async def call(s):
        return s, await net.call(SHARDS[s]["leader"], "POST", path, json=body_for(s), timeout=8.0)
    res = dict(await asyncio.gather(*(call(s) for s in SHARDS)))
    for s, r in res.items():
        if r.get("_status"):
            raise NodeUnavailable(SHARDS[s]["leader"], f"HTTP {r['_status']}: {r.get('detail')}")
    telemetry.event("JOB_PHASE_COMPLETED", phase=name, ms=round((time.time() - t0) * 1000, 1))
    return res


async def _run_job() -> Dict[str, Any]:
    RUN_COUNTER["n"] += 1
    run_id = f"run-{RUN_COUNTER['n']:03d}"
    t0 = time.time()
    telemetry.event("JOB_STARTED", run_id=run_id, shards=list(SHARDS),
                    pattern="map -> reduce -> broadcast (x3) -> persist")
    phases = {}

    p = time.time()
    r1 = await _phase("1-feature-extraction", "/compute/features", lambda s: {"run_id": run_id})
    fb = sc.merge_bounds(r["bounds"] for r in r1.values())
    telemetry.event("GLOBAL_REDUCE", step="feature bounds (F1)", features=len(fb),
                    sites=sum(r["sites"] for r in r1.values()))
    phases["features"] = round((time.time() - p) * 1000, 1)

    p = time.time()
    r2 = await _phase("2-composite-and-gates", "/compute/score",
                      lambda s: {"run_id": run_id, "feature_bounds": fb})
    parts = [r["raw_bounds"] for r in r2.values() if r["raw_bounds"]]
    sb = [min(x[0] for x in parts), max(x[1] for x in parts)]
    telemetry.event("GLOBAL_REDUCE", step="score bounds (F5, shoreline-retained sites)",
                    s_min=sb[0], s_max=sb[1])
    phases["composite"] = round((time.time() - p) * 1000, 1)

    p = time.time()
    r3 = await _phase("3-rescale-and-classify", "/compute/classify",
                      lambda s: {"run_id": run_id, "score_bounds": sb})
    cuts, counts = {}, {}
    for c in sc.CLASSES:
        merged = list(heapq.merge(*(r["class_scores"][c] for r in r3.values())))
        counts[c] = len(merged)
        cuts[c] = sc.f15_class_cut(merged)
    telemetry.event("GLOBAL_REDUCE", step="intra-class percentile cuts (F15)", class_counts=counts)
    phases["classify"] = round((time.time() - p) * 1000, 1)

    p = time.time()
    r4 = await _phase("4-levels-and-persist", "/compute/finalize",
                      lambda s: {"run_id": run_id, "class_cuts": cuts})
    phases["persist"] = round((time.time() - p) * 1000, 1)

    n_sites = sum(r["sites"] for r in r4.values())
    eligible = sum(r["eligible"] for r in r4.values())
    levels = {lv: sum(r["level_counts"][lv] for r in r4.values()) for lv in r4[next(iter(r4))]["level_counts"]}
    EDGE_CACHE.clear()
    LAST_RUN.clear()
    LAST_RUN.update(run_id=run_id, finished=time.time(), class_counts=counts)
    total = round((time.time() - t0) * 1000, 1)
    telemetry.event("JOB_COMPLETED", run_id=run_id, sites=n_sites, eligible=eligible,
                    gated=n_sites - eligible, class_counts=counts, total_ms=total)
    return {"run_id": run_id, "sites": n_sites, "eligible": eligible, "gated": n_sites - eligible,
            "class_counts": counts, "level_counts": levels, "score_bounds_raw": sb,
            "phase_ms": phases, "total_ms": total,
            "shard_writes": {s: r["write"] for s, r in r4.items()},
            "trace_id": telemetry.trace_id_var.get()}


@app.get("/api/v1/sites/{site_id}")
async def get_site(site_id: str, consistency: str = "eventual"):
    shard = await locate(site_id)
    if consistency == "strong":
        return await quorum_read(shard, site_id)
    resp = await eventual_call(shard, f"/kv/site/{site_id}")
    lag = leader_lsn(shard) - resp["applied_lsn"]
    if lag > 0:
        telemetry.event("STALE_READ_POSSIBLE", "WARN", site_id=site_id, served_by=resp["node_id"],
                        replica_lsn=resp["applied_lsn"], leader_lsn=leader_lsn(shard), lag=lag)
    return {"consistency": "eventual", "shard": shard, "served_by": resp["node_id"],
            "replica_lsn": resp["applied_lsn"], "replication_lag": max(lag, 0),
            "record": resp["record"]}


async def quorum_read(shard: str, site_id: str) -> Dict[str, Any]:
    nodes = [n.node_id for n in topology.shard_members(shard) if MEMBERS[n.node_id].status != "DOWN"]

    async def one(nid):
        try:
            return nid, await net.call(nid, "GET", f"/kv/site/{site_id}", timeout=0.8)
        except NodeUnavailable:
            return nid, None
    answers = {nid: r for nid, r in await asyncio.gather(*(one(n) for n in nodes))
               if r and "record" in r}
    if len(answers) < topology.READ_QUORUM:
        telemetry.event("STRONG_READ_REJECTED", "ERROR", shard=shard, responses=len(answers),
                        r_required=topology.READ_QUORUM,
                        decision="CP: refuse rather than risk a stale answer")
        raise unavailable({"error": "read quorum unavailable", "shard": shard,
                           "responses": list(answers), "r": topology.READ_QUORUM,
                           "cap": "consistency chosen over availability"})
    newest_id = max(answers, key=lambda n: answers[n]["record"]["_v"])
    newest = answers[newest_id]["record"]
    stale = [n for n, a in answers.items() if a["record"]["_v"] < newest["_v"]]
    telemetry.event("QUORUM_READ", site_id=site_id, responses=list(answers),
                    versions={n: a["record"]["_v"] for n, a in answers.items()},
                    chosen=newest_id, stale=stale)
    for n in stale:
        telemetry.event("READ_REPAIR_SENT", "WARN", site_id=site_id, replica=n,
                        to_version=newest["_v"])
        try:
            await net.call(n, "POST", "/replication/repair", json={"record": newest}, timeout=0.8)
        except NodeUnavailable:
            pass
    return {"consistency": "strong", "shard": shard, "quorum": list(answers),
            "served_by": newest_id, "read_repaired": stale, "record": newest}


@app.patch("/api/v1/sites/{site_id}/hazard")
async def update_hazard(site_id: str, upd: HazardUpdate):
    shard = await locate(site_id)
    patch = {"site_id": site_id, **{k: v for k, v in upd.model_dump().items()
                                    if k != "source" and v is not None}}
    telemetry.event("HAZARD_UPDATE_RECEIVED", site_id=site_id, source=upd.source, patch=patch)
    res = await shard_write(shard, "hazard_update", [patch])
    EDGE_CACHE.clear()
    return {"shard": shard, "write": res, "note": "hard gates re-evaluated on write; "
            "score refreshed on the next scoring job"}


@app.get("/api/v1/recommend")
async def recommend(top: int = 3, barangay: Optional[str] = None, consistency: str = "eventual"):
    shards = [topology.shard_for_barangay(barangay)] if barangay else list(SHARDS)
    telemetry.event("QUERY_PLANNED", kind="targeted" if barangay else "scatter-gather",
                    shards=shards, top=top, barangay=barangay, consistency=consistency)
    params = {"n": top, "eligible_only": True}
    if barangay:
        params["barangay"] = barangay
    call = strong_call if consistency == "strong" else eventual_call

    async def one(s):
        try:
            return s, await call(s, "/kv/top", params)
        except HTTPException as exc:
            return s, exc
    results = await asyncio.gather(*(one(s) for s in shards))
    ok = {s: r for s, r in results if not isinstance(r, Exception)}
    missing = [s for s, r in results if isinstance(r, Exception)]
    merged = heapq.nlargest(top, (site for r in ok.values() for site in r["sites"]),
                            key=lambda x: x["score"])
    if missing:
        telemetry.event("DEGRADED_RESPONSE", "WARN", missing_shards=missing,
                        missing_barangays=[b for s in missing for b in topology.SHARD_MAP[s]])
    telemetry.event("SCATTER_GATHER_MERGED", shards_answered=list(ok), returned=len(merged),
                    served_by={s: r["node_id"] for s, r in ok.items()})
    return {"top": [{k: x.get(k) for k in ("site_id", "barangay", "score", "suitability_class",
                                           "recommendation_level", "recommendation_label",
                                           "latitude", "longitude")} for x in merged],
            "degraded": bool(missing), "missing_shards": missing,
            "served_by": {s: r["node_id"] for s, r in ok.items()}, "consistency": consistency}


@app.get("/api/v1/export")
async def export(consistency: str = "eventual"):
    """candidate_sites.json equivalent for the web map.

    eventual (default): served through the edge cache, any replica per shard (AP).
    strong: bypasses the cache and reads every shard leader (used for audits).
    """
    version = "|".join(f"{s}:{leader_lsn(s)}" for s in SHARDS) + f"|{LAST_RUN.get('run_id')}"
    if consistency != "strong":
        hit = EDGE_CACHE.get("export")
        if hit and hit["version"] == version and time.time() - hit["at"] < 60:
            telemetry.event("EDGE_CACHE_HIT", version=version, sites=len(hit["body"]["sites"]))
            return JSONResponse(hit["body"], headers={"X-Cache": "HIT"})
        telemetry.event("EDGE_CACHE_MISS", version=version, action="scatter-gather all shards")
    call = strong_call if consistency == "strong" else eventual_call
    parts = await asyncio.gather(*(call(s, "/kv/all") for s in SHARDS), return_exceptions=True)
    sites = [x for p in parts if not isinstance(p, Exception) for x in p["sites"]]
    sites.sort(key=lambda x: x.get("score", 0), reverse=True)
    body = {"metadata": {"project": "POLARIS", "study_area": "Talomo District, Davao City",
                         "run_id": LAST_RUN.get("run_id"), "data_version": version,
                         "consistency": consistency,
                         "served_by": {s: (p["node_id"] if not isinstance(p, Exception) else None)
                                       for s, p in zip(SHARDS, parts)},
                         "complete": not any(isinstance(p, Exception) for p in parts)},
            "sites": sites}
    if consistency != "strong":
        EDGE_CACHE["export"] = {"version": version, "at": time.time(), "body": body}
    return JSONResponse(body, headers={"X-Cache": "BYPASS" if consistency == "strong" else "MISS"})


# --------------------------------------------------------------------------
# Private cloud (CHO) via site-to-site VPN
# --------------------------------------------------------------------------
def authorize(role: Optional[str], key: Optional[str]) -> None:
    if role != "cho-planner" or key != CHO_API_KEY:
        telemetry.event("AUTHZ_DENIED", "WARN", role=role, reason="CHO planner role and key required")
        raise HTTPException(401, detail="CHO planner credentials required")


async def vault_call(method: str, path: str, actor: str, json: Any = None) -> Dict[str, Any]:
    telemetry.event("VPN_TUNNEL_FORWARD", tunnel=VPN_TUNNEL_ID, target="cho-vault", path=path,
                    link="public region -> CHO private cloud")
    try:
        resp = await net.call("cho-vault", method, path, json=json, timeout=1.5, retries=1,
                              headers={"X-Network-Zone": "PRIVATE_VPC",
                                       "X-VPN-Tunnel": VPN_TUNNEL_ID, "X-Actor": actor})
    except NodeUnavailable as exc:
        telemetry.event("PRIVATE_CLOUD_UNAVAILABLE", "ERROR", reason=exc.reason,
                        decision="fail closed: confidential data is never written to public cloud")
        raise unavailable({"error": "CHO private cloud unreachable", "fallback": "none by design"})
    if resp.get("_status"):
        raise HTTPException(resp["_status"], detail=resp.get("detail"))
    return resp


@app.post("/api/v1/private/dossiers")
async def create_dossier(d: Dossier, x_role: Optional[str] = Header(default=None),
                         x_api_key: Optional[str] = Header(default=None)):
    authorize(x_role, x_api_key)
    return await vault_call("POST", "/dossiers", d.prepared_by, d.model_dump())


@app.get("/api/v1/private/dossiers/{dossier_id}")
async def read_dossier(dossier_id: str, x_role: Optional[str] = Header(default=None),
                       x_api_key: Optional[str] = Header(default=None)):
    authorize(x_role, x_api_key)
    return await vault_call("GET", f"/dossiers/{dossier_id}", x_role or "?")
