"""Data / compute node: one replica of one geographic shard.

Each shard is a replica group of 3 nodes spread over 3 availability zones,
with single-leader replication:

  * the LEADER accepts writes, assigns a log sequence number (LSN), ships the
    entry to both followers and commits once W = 2 replicas (itself included)
    hold it. Writes are serialised per shard by a lock.
  * FOLLOWERS stage the in-flight entry, apply it once the leader's commit
    index covers it, and serve eventually-consistent reads.
  * Every request from a leader carries its EPOCH (term). A node that sees a
    higher epoch steps down; a request with a lower epoch is rejected (409).
    This fencing stops a recovered old leader from causing split brain.
  * A follower that fell behind catches up by pulling the committed log from
    the leader (or a full snapshot if its log diverged).

The node also hosts the shard-local steps of the distributed scoring job
(data locality: compute runs where the data is, only statistics move).
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException

from common import chaos as chaos_mod
from common import scoring as sc
from common import telemetry, topology
from common.models import AppendRequest, Heartbeat, WriteRequest
from common.net import Network, NodeUnavailable

NODE_ID = os.getenv("NODE_ID", "coastal-a")
SPEC = topology.node(NODE_ID)
SHARD = SPEC.shard
PEERS = [n.node_id for n in topology.shard_members(SHARD) if n.node_id != NODE_ID]

tel = telemetry.init(NODE_ID, topology.node("collector").url)
chaos = chaos_mod.Chaos()
net = Network(NODE_ID, outbound_guard=chaos.outbound_guard)
app = FastAPI(title=f"POLARIS data node {NODE_ID}")
telemetry.install(app, tel)
chaos_mod.install(app, chaos)   # installed last = outermost: a crashed node logs nothing


class ReplicaState:
    def __init__(self) -> None:
        self.role = "leader" if SPEC.initial_leader else "follower"
        self.epoch = 1
        self.leader_id = topology.initial_leader(SHARD).node_id
        self.store: Dict[str, Dict[str, Any]] = {}     # site_id -> record (durable "disk")
        self.log: List[Dict[str, Any]] = []            # committed entries, log[i].lsn == i+1
        self.staged: Dict[int, Dict[str, Any]] = {}    # follower: received, not yet committed
        self.applied_lsn = 0
        self.write_lock = asyncio.Lock()
        self.idempotency: Dict[str, Dict[str, Any]] = {}
        self.jobs: Dict[str, Dict[str, Any]] = {}      # run_id -> scratch state of scoring job
        self.catching_up = False
        self.catchup_failures = 0

    @property
    def last_lsn(self) -> int:
        return max([self.applied_lsn] + list(self.staged))


S = ReplicaState()


# --------------------------------------------------------------------------
# Local state machine
# --------------------------------------------------------------------------
def apply_entry(entry: Dict[str, Any]) -> None:
    lsn = entry["lsn"]
    for rec in entry["records"]:
        sid = rec["site_id"]
        cur = S.store.get(sid)
        if cur is not None and cur.get("_v", 0) >= lsn:
            continue  # already newer (e.g. via read repair)
        if entry["op"] == "upsert" or cur is None:
            S.store[sid] = {**rec, "_v": lsn}
        else:
            cur.update(rec)
            cur["_v"] = lsn
    S.log.append(entry)
    S.applied_lsn = lsn


def apply_staged_up_to(commit: int) -> int:
    n = 0
    while S.applied_lsn < commit and (S.applied_lsn + 1) in S.staged:
        apply_entry(S.staged.pop(S.applied_lsn + 1))
        n += 1
    return n


def on_chaos(old: str, new: str) -> None:
    if new == "crash":
        # Volatile state is lost in a crash; the store and committed log are on disk.
        S.staged.clear()
        S.jobs.clear()
        S.idempotency.clear()
    if old == "crash" and new == "ok":
        S.role = "follower"  # a restarted process always rejoins as a follower
        telemetry.trace_id_var.set("sys-" + telemetry.new_id(4))
        telemetry.event("NODE_RESTARTED", shard=SHARD, applied_lsn=S.applied_lsn,
                        note="rejoining as follower; waiting for control-plane heartbeat")


chaos.listeners.append(on_chaos)


def adopt_epoch(epoch: int, leader_id: str) -> None:
    """Called only with epoch >= our epoch (lower epochs are fenced off)."""
    new_role = "leader" if leader_id == NODE_ID else "follower"
    if S.role == "leader" and new_role == "follower":
        telemetry.event("LEADER_STEP_DOWN", "WARN", shard=SHARD, old_epoch=S.epoch,
                        new_epoch=epoch, new_leader=leader_id,
                        reason="control plane / peer reports a newer leader (fenced)")
    S.role, S.epoch, S.leader_id = new_role, epoch, leader_id


def health() -> Dict[str, Any]:
    return {"status": "HEALTHY", "node_id": NODE_ID, "shard": SHARD, "zone": SPEC.zone,
            "role": S.role, "epoch": S.epoch, "leader_id": S.leader_id,
            "applied_lsn": S.applied_lsn, "last_lsn": S.last_lsn,
            "stored_keys_count": len(S.store), "chaos": chaos.mode}


# --------------------------------------------------------------------------
# Control plane
# --------------------------------------------------------------------------
@app.get("/health")
async def get_health():
    return health()


@app.post("/control/heartbeat")
async def heartbeat(hb: Heartbeat):
    if hb.epoch >= S.epoch:
        adopt_epoch(hb.epoch, hb.leader_id)
    if S.role == "follower" and hb.leader_lsn > S.applied_lsn and not S.catching_up:
        asyncio.get_event_loop().create_task(catch_up(hb.leader_id))
    return health()


@app.post("/control/promote")
async def promote(hb: Heartbeat):
    applied = apply_staged_up_to(S.last_lsn)
    S.staged.clear()
    S.epoch, S.leader_id, S.role = hb.epoch, NODE_ID, "leader"
    telemetry.event("LEADER_PROMOTED", "WARN", shard=SHARD, epoch=S.epoch,
                    applied_lsn=S.applied_lsn, staged_entries_committed=applied)
    return health()


async def catch_up(leader_id: str) -> None:
    """Anti-entropy: pull committed entries we missed while away."""
    S.catching_up = True
    start = S.applied_lsn
    telemetry.trace_id_var.set("sys-" + telemetry.new_id(4))
    try:
        resp = await net.call(leader_id, "GET", "/replication/log",
                              params={"since": S.applied_lsn + 1}, timeout=2.0, quiet=True)
        if resp.get("snapshot_required"):
            snap = await net.call(leader_id, "GET", "/replication/snapshot", timeout=3.0)
            S.store, S.log = snap["store"], snap["log"]
            S.applied_lsn, S.staged = snap["applied_lsn"], {}
            telemetry.event("SNAPSHOT_RESYNC", "WARN", shard=SHARD, from_node=leader_id,
                            applied_lsn=S.applied_lsn, keys=len(S.store))
            return
        for e in resp.get("entries", []):
            if e["lsn"] == S.applied_lsn + 1:
                S.staged.pop(e["lsn"], None)
                apply_entry(e)
        S.catchup_failures = 0
        if S.applied_lsn > start:
            telemetry.event("CATCHUP_COMPLETE", shard=SHARD, from_node=leader_id,
                            from_lsn=start, to_lsn=S.applied_lsn, entries=S.applied_lsn - start)
    except NodeUnavailable as exc:
        S.catchup_failures += 1
        if S.catchup_failures == 1:  # log the first failure of a streak, not every heartbeat
            telemetry.event("CATCHUP_DEFERRED", "WARN", shard=SHARD, leader=leader_id,
                            reason=exc.reason, lag=f"applied_lsn={S.applied_lsn}")
    finally:
        S.catching_up = False


# --------------------------------------------------------------------------
# Replication (leader -> follower)
# --------------------------------------------------------------------------
@app.post("/replication/append")
async def append(req: AppendRequest):
    if req.epoch < S.epoch:
        telemetry.event("FENCED_STALE_LEADER", "WARN", sender=req.leader_id,
                        sender_epoch=req.epoch, my_epoch=S.epoch)
        raise HTTPException(409, detail={"error": "stale epoch", "epoch": S.epoch})
    adopt_epoch(req.epoch, req.leader_id)
    first = req.entries[0].lsn
    if first > S.applied_lsn + 1:
        telemetry.event("REPLICA_GAP_DETECTED", "WARN", have_lsn=S.applied_lsn, got_lsn=first)
        return {"ack": False, "need_from": S.applied_lsn + 1}
    S.staged.clear()   # anything still staged is superseded by the leader's log
    for e in req.entries:
        if e.lsn > S.applied_lsn:
            S.staged[e.lsn] = e.model_dump()
    applied = apply_staged_up_to(req.leader_commit)
    telemetry.event("REPLICA_APPEND_STAGED", lsn=req.entries[-1].lsn, entries=len(req.entries),
                    records=sum(len(e.records) for e in req.entries), applied_now=applied)
    return {"ack": True, "lsn": req.entries[-1].lsn}


@app.post("/replication/commit")
async def commit(body: Dict[str, int]):
    if body.get("epoch", 0) >= S.epoch:
        n = apply_staged_up_to(body["lsn"])
        if n:
            telemetry.event("REPLICA_COMMIT_APPLIED", lsn=S.applied_lsn, entries=n)
    return {"applied_lsn": S.applied_lsn}


@app.get("/replication/log")
async def get_log(since: int):
    if since < 1 or since - 1 > len(S.log):
        return {"snapshot_required": True}
    return {"entries": S.log[since - 1:], "applied_lsn": S.applied_lsn}


@app.get("/replication/snapshot")
async def snapshot():
    telemetry.event("SNAPSHOT_SERVED", keys=len(S.store), applied_lsn=S.applied_lsn)
    return {"store": S.store, "log": S.log, "applied_lsn": S.applied_lsn}


@app.post("/replication/repair")
async def repair(body: Dict[str, Any]):
    """Read repair pushed by the gateway after a quorum read found this replica stale."""
    rec = body["record"]
    cur = S.store.get(rec["site_id"])
    if cur is None or cur.get("_v", 0) < rec["_v"]:
        S.store[rec["site_id"]] = dict(rec)
        telemetry.event("READ_REPAIR_APPLIED", "WARN", site_id=rec["site_id"],
                        old_version=cur.get("_v") if cur else None, new_version=rec["_v"])
        return {"repaired": True}
    return {"repaired": False}


async def replicate_to(peer: str, entry: Dict[str, Any]) -> bool:
    body = {"entries": [entry], "epoch": S.epoch, "leader_id": NODE_ID,
            "leader_commit": S.applied_lsn}
    try:
        resp = await net.call(peer, "POST", "/replication/append", json=body, timeout=0.8,
                              retries=1)
        if resp.get("_status") == 409:
            telemetry.event("LEADER_FENCED", "WARN", peer=peer, detail=resp.get("detail"))
            S.role = "follower"
            return False
        if resp.get("ack") is False:  # follower is behind: ship the missing committed suffix
            body["entries"] = S.log[resp["need_from"] - 1:] + [entry]
            telemetry.event("REPLICATION_BACKFILL", peer=peer, from_lsn=resp["need_from"],
                            entries=len(body["entries"]))
            resp = await net.call(peer, "POST", "/replication/append", json=body, timeout=2.0)
        return bool(resp.get("ack"))
    except NodeUnavailable:
        return False


# --------------------------------------------------------------------------
# Client-facing writes (leader only)
# --------------------------------------------------------------------------
def hazard_update_to_merge(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Read-modify-write on the leader: apply new hazard classes and re-evaluate the
    non-compensatory F22 gates at once (the gates need no global statistics)."""
    out = []
    for upd in records:
        cur = S.store.get(upd["site_id"])
        if cur is None:
            raise HTTPException(404, detail=f"site {upd['site_id']} not on shard {SHARD}")
        patch = {k: v for k, v in upd.items() if v is not None and k != "source"}
        merged = {**cur, **patch}
        shore, high, eligible = sc.f22_gates(merged["shoreline_distance"],
                                             {h: merged[h] for h in sc.HAZARD_FEATURES})
        patch.update({"high_hazard_excluded": high, "shoreline_excluded": shore,
                      "recommendation_eligible": eligible, "rescore_pending": True})
        if not eligible:
            patch.update({"recommendation_level": 0, "recommendation_label": sc.LEVEL_LABELS[0]})
        out.append(patch)
    return out


async def leader_write(op: str, records: List[Dict[str, Any]], w: int) -> Dict[str, Any]:
    async with S.write_lock:
        if op == "hazard_update":
            records, op = hazard_update_to_merge(records), "merge"
        entry = {"lsn": S.applied_lsn + 1, "epoch": S.epoch, "op": op, "records": records,
                 "ts": time.time()}
        t0 = time.time()
        telemetry.event("REPLICATION_SENT", shard=SHARD, lsn=entry["lsn"], op=op,
                        records=len(records), followers=PEERS, w_required=w)
        results = await asyncio.gather(*(replicate_to(p, entry) for p in PEERS))
        acks = 1 + sum(results)
        acked = [p for p, ok in zip(PEERS, results) if ok]
        if S.role != "leader":
            raise HTTPException(409, detail="deposed during write (fenced by higher epoch)")
        if acks < w:
            telemetry.event("WRITE_REJECTED", "ERROR", shard=SHARD, lsn=entry["lsn"], acks=acks,
                            w_required=w, reason="quorum not reached; entry discarded (CP)")
            raise HTTPException(503, detail={"error": "write quorum not reached", "acks": acks,
                                             "w": w})
        apply_entry(entry)
        for p in acked:  # commit notice; also piggy-backed on the next append / heartbeat
            asyncio.get_event_loop().create_task(
                net.call(p, "POST", "/replication/commit",
                         json={"lsn": entry["lsn"], "epoch": S.epoch}, quiet=True))
        telemetry.event("WRITE_COMMITTED", shard=SHARD, lsn=entry["lsn"], acks=acks,
                        w_required=w, replicas=[NODE_ID] + acked,
                        latency_ms=round((time.time() - t0) * 1000, 1))
        return {"committed": True, "lsn": entry["lsn"], "acks": acks,
                "replicas": [NODE_ID] + acked, "leader": NODE_ID, "epoch": S.epoch}


@app.post("/kv/write")
async def kv_write(req: WriteRequest):
    if S.role != "leader":
        raise HTTPException(409, detail={"error": "not leader", "leader_id": S.leader_id,
                                         "epoch": S.epoch})
    if req.epoch < S.epoch:
        raise HTTPException(409, detail={"error": "stale epoch from client", "epoch": S.epoch})
    if req.idempotency_key and req.idempotency_key in S.idempotency:
        telemetry.event("IDEMPOTENT_REPLAY", key=req.idempotency_key)
        return S.idempotency[req.idempotency_key]
    result = await leader_write(req.op, req.records, req.w)
    if req.idempotency_key:
        S.idempotency[req.idempotency_key] = result
    return result


# --------------------------------------------------------------------------
# Reads
# --------------------------------------------------------------------------
@app.get("/kv/site/{site_id}")
async def kv_site(site_id: str):
    rec = S.store.get(site_id)
    if rec is None:
        raise HTTPException(404, detail="not on this replica")
    return {"record": rec, "node_id": NODE_ID, "role": S.role, "applied_lsn": S.applied_lsn}


@app.get("/kv/top")
async def kv_top(n: int = 3, barangay: Optional[str] = None, eligible_only: bool = True):
    rows = [r for r in S.store.values() if "score" in r
            and (barangay is None or r["barangay"] == barangay)
            and (not eligible_only or r.get("recommendation_eligible"))]
    rows.sort(key=lambda r: r["score"], reverse=True)
    telemetry.event("LOCAL_TOPK", shard=SHARD, candidates=len(rows), k=n, barangay=barangay)
    return {"node_id": NODE_ID, "shard": SHARD, "applied_lsn": S.applied_lsn, "sites": rows[:n]}


@app.get("/kv/all")
async def kv_all():
    telemetry.event("SHARD_EXPORT", shard=SHARD, sites=len(S.store), applied_lsn=S.applied_lsn)
    return {"node_id": NODE_ID, "shard": SHARD, "applied_lsn": S.applied_lsn,
            "sites": list(S.store.values())}


# --------------------------------------------------------------------------
# Distributed scoring job: shard-local MAP steps (run on the shard leader)
# --------------------------------------------------------------------------
def _job(run_id: str) -> Dict[str, Any]:
    if run_id not in S.jobs:
        raise HTTPException(409, detail="unknown run_id on this node (restart job)")
    return S.jobs[run_id]


@app.post("/compute/features")
async def compute_features(body: Dict[str, Any]):
    """Phase 1 MAP: derive RA = 1/(1+d) (F2) and report local feature bounds."""
    t0 = time.time()
    rows = []
    for r in S.store.values():
        row = {k: r[k] for k in ("site_id", "building_density", "facility_distance",
                                 "shoreline_distance", *sc.HAZARD_FEATURES)}
        row["road_accessibility"] = sc.f2_road_accessibility(r["road_distance"])
        rows.append(row)
    await asyncio.sleep(0.0005 * len(rows))  # stand-in for the GIS work done at this stage
    S.jobs[body["run_id"]] = {"rows": rows}
    bounds = sc.local_bounds(rows, sc.ALL_FEATURES)
    telemetry.event("FEATURE_EXTRACTION_COMPLETE", shard=SHARD, run_id=body["run_id"],
                    sites=len(rows), ms=round((time.time() - t0) * 1000, 1))
    return {"shard": SHARD, "node_id": NODE_ID, "sites": len(rows), "bounds": bounds}


@app.post("/compute/score")
async def compute_score(body: Dict[str, Any]):
    """Phase 2 MAP: F1 normalise with GLOBAL bounds, F3 composite, F22 gates."""
    job = _job(body["run_id"])
    gb = body["feature_bounds"]
    retained = []
    for row in job["rows"]:
        norm = {f: sc.f1_minmax(row[f], *gb[f]) for f in sc.ALL_FEATURES}
        row["norm"] = norm
        row["raw"] = sc.f3_composite(norm)
        row["shore"], row["high"], row["eligible"] = sc.f22_gates(
            row["shoreline_distance"], {h: row[h] for h in sc.HAZARD_FEATURES})
        if not row["shore"]:
            retained.append(row["raw"])
    telemetry.event("COMPOSITE_SCORED", shard=SHARD, run_id=body["run_id"],
                    sites=len(job["rows"]), retained_for_bounds=len(retained),
                    gated=sum(1 for r in job["rows"] if not r["eligible"]))
    return {"shard": SHARD, "raw_bounds": [min(retained), max(retained)] if retained else None}


@app.post("/compute/classify")
async def compute_classify(body: Dict[str, Any]):
    """Phase 3 MAP: F5 rescale with GLOBAL S bounds, F14 classify; return per-class scores."""
    job = _job(body["run_id"])
    lo, hi = body["score_bounds"]
    per_class: Dict[str, List[float]] = {c: [] for c in sc.CLASSES}
    for row in job["rows"]:
        row["score"] = sc.f5_rescale(row["raw"], lo, hi)
        row["cls"] = sc.f14_classify(row["score"])
        per_class[row["cls"]].append(row["score"])
    telemetry.event("CLASSIFIED", shard=SHARD, run_id=body["run_id"],
                    class_counts={c: len(v) for c, v in per_class.items()})
    return {"shard": SHARD, "class_scores": {c: sorted(v) for c, v in per_class.items()}}


@app.post("/compute/finalize")
async def compute_finalize(body: Dict[str, Any]):
    """Phase 4 MAP: F15 levels from GLOBAL class cuts, then persist via a quorum write."""
    job = S.jobs.pop(body["run_id"], None)
    if job is None:
        raise HTTPException(409, detail="unknown run_id on this node (restart job)")
    cuts = body["class_cuts"]
    patches = []
    for row in job["rows"]:
        lo_l, hi_l = sc.CLASS_LEVEL_RANGE[row["cls"]]
        cut = cuts.get(row["cls"])
        level = hi_l if (cut is not None and row["score"] > cut) else lo_l
        if not row["eligible"]:
            level = 0
        patches.append({
            "site_id": row["site_id"], "run_id": body["run_id"],
            "road_accessibility": row["road_accessibility"],
            "factors_normalized": {k: round(v, 6) for k, v in row["norm"].items()},
            "raw_hazard_integrated": row["raw"], "score": row["score"],
            "suitability_class": row["cls"], "recommendation_level": level,
            "recommendation_label": sc.LEVEL_LABELS[level],
            "shoreline_excluded": row["shore"], "high_hazard_excluded": row["high"],
            "recommendation_eligible": row["eligible"], "rescore_pending": False})
    result = await leader_write("merge", patches, body.get("w", topology.WRITE_QUORUM))
    top = sorted((p for p in patches if p["recommendation_eligible"]),
                 key=lambda p: p["score"], reverse=True)[:1]
    telemetry.event("SCORES_PUBLISHED", shard=SHARD, run_id=body["run_id"], sites=len(patches),
                    lsn=result["lsn"], acks=result["acks"],
                    shard_best=top[0]["site_id"] if top else None)
    return {"shard": SHARD, "sites": len(patches), "write": result,
            "eligible": sum(1 for p in patches if p["recommendation_eligible"]),
            "level_counts": {str(lv): sum(1 for p in patches if p["recommendation_level"] == lv)
                             for lv in range(5)}}
