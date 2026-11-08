"""POLARIS Cloud - end-to-end simulation of the distributed architecture.

    python simulate.py                 # starts the cluster, runs every scenario, stops it
    python simulate.py --keep          # leave the cluster running afterwards
    python simulate.py --only 5 7      # run selected scenarios (1-2 always run first)

If a cluster is already running (python run_cluster.py in another terminal),
the simulation uses it and its node logs appear in that terminal. Otherwise
node logs go to logs/cluster_console.log. Every event of every process is also
in logs/<node>.jsonl and, cluster-wide, in logs/collector_all.jsonl.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import httpx

from common import topology
from run_cluster import LOG_DIR, start_cluster, stop_cluster, wait_ready
from tools.trace_view import render

try:
    sys.stdout.reconfigure(errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

ROOT = Path(__file__).resolve().parent
GW = topology.node("gateway").url
COLLECTOR = topology.node("collector").url
VAULT = topology.node("cho-vault").url
CHO = {"X-Role": "cho-planner", "X-Api-Key": "cho-demo-key"}
client = httpx.Client(timeout=30.0)
SUMMARY: Dict[str, Any] = {"scenarios": {}}


# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------
def banner(n: int, title: str, concept: str) -> None:
    print("\n" + "=" * 78)
    print(f" SCENARIO {n}: {title}")
    print(f" concept: {concept}")
    print("=" * 78)


def say(msg: str) -> None:
    print(f"  {msg}")


def api(method: str, path: str, base: str = GW, quiet: bool = False,
        **kw: Any) -> Tuple[int, Any, str]:
    r = client.request(method, base + path, **kw)
    tid = r.headers.get("X-Trace-ID", "-")
    try:
        body = r.json()
    except ValueError:
        body = r.text
    if not quiet:
        print(f"  -> {method} {path}  [{r.status_code}]  trace={tid}")
    return r.status_code, body, tid


def status() -> Dict[str, Any]:
    return client.get(GW + "/cluster/status").json()


def chaos(node_id: str, mode: str, **kw: Any) -> None:
    api("POST", f"/admin/chaos/{node_id}", json={"mode": mode, **kw}, quiet=True)
    say(f"[chaos] {node_id} -> {mode}")


def wait_until(pred: Callable[[Dict[str, Any]], bool], what: str, timeout: float = 10.0) -> float:
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred(status()):
            dt = time.time() - t0
            say(f"[control plane] {what} after {dt:.1f}s")
            return dt
        time.sleep(0.2)
    say(f"[control plane] TIMEOUT waiting for: {what}")
    return -1.0


def replica(st: Dict[str, Any], node_id: str) -> Dict[str, Any]:
    return st["shards"][topology.node(node_id).shard]["replicas"][node_id]


def node_status(node_id: str, want: str) -> Callable[[Dict[str, Any]], bool]:
    return lambda st: replica(st, node_id)["status"] == want


def caught_up(shard: str) -> Callable[[Dict[str, Any]], bool]:
    return lambda st: all(r["status"] == "UP" and r["lag"] == 0
                          for r in st["shards"][shard]["replicas"].values())


def show_trace(tid: str, title: str, max_lines: int = 60) -> None:
    time.sleep(0.9)  # telemetry is shipped asynchronously in 0.3 s batches
    try:
        r = client.get(f"{COLLECTOR}/traces/{tid}")
        r.raise_for_status()
        print(f"\n  --- distributed trace: {title} ---")
        for line in render(r.json(), max_lines=max_lines):
            print("  " + line)
    except Exception as exc:
        say(f"(trace {tid} not available: {exc})")


CONTROL_EVENTS = ("NODE_SUSPECT,NODE_DOWN,NODE_RECOVERED,NODE_RESTARTED,LEADER_ELECTED,"
                  "LEADER_PROMOTED,LEADER_STEP_DOWN,SHARD_READ_ONLY,SHARD_WRITABLE,"
                  "CATCHUP_COMPLETE,CATCHUP_DEFERRED,SNAPSHOT_RESYNC,CIRCUIT_OPEN,"
                  "CIRCUIT_HALF_OPEN,CIRCUIT_CLOSED,REPLICA_GAP_DETECTED,REPLICATION_BACKFILL")


def control_timeline(since: float, title: str = "control-plane events") -> None:
    """Background events (failure detector, elections, catch-up) run on sys-* traces."""
    time.sleep(0.9)
    evs = client.get(f"{COLLECTOR}/events", params={"since": since, "names": CONTROL_EVENTS}).json()
    print(f"\n  --- {title} ---")
    for e in evs["events"]:
        attrs = " ".join(f"{k}={v}" for k, v in e["attrs"].items())
        print(f"  {time.strftime('%H:%M:%S', time.localtime(e['ts']))}."
              f"{int(e['ts'] * 1000) % 1000:03d} [{e['node']:<10}] {e['level']:<5} "
              f"{e['event']} {attrs}"[:170])


def site_brief(rec: Dict[str, Any]) -> str:
    return (f"{rec['site_id']} [{rec['barangay']}] score={rec.get('score', 0):.4f} "
            f"flood={rec['flood_susceptibility']} eligible={rec.get('recommendation_eligible')} "
            f"level={rec.get('recommendation_level')} v={rec.get('_v')}")


def load_sites() -> List[Dict[str, Any]]:
    with open(ROOT / "data" / "talomo_site_features.csv", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def print_topology() -> None:
    st = status()
    say(f"deployment model : {st['deployment_model']}   N={st['replication_factor']} "
        f"W={st['write_quorum']} R={st['read_quorum']}")
    for nid, svc in st["services"].items():
        say(f"  {nid:<10} {svc['zone']:<11} {svc['subnet']:<14} {svc['status']}")
    for s, sh in st["shards"].items():
        reps = "  ".join(f"{n}({r['zone']},{r['role'][0].upper() if r['role'] else '?'},{r['status']})"
                         for n, r in sh["replicas"].items())
        say(f"  shard {s:<8} epoch={sh['epoch']} leader={sh['leader']:<10} {reps}")
        say(f"           barangays: {', '.join(sh['barangays'])}")


# ----------------------------------------------------------------------------
# scenarios
# ----------------------------------------------------------------------------
CTX: Dict[str, Any] = {}


def s1_ingest() -> None:
    banner(1, "Bootstrap + sharded, replicated ingest of 1,502 candidate sites",
           "network topology, directory-based geographic sharding, quorum replication")
    print_topology()
    rows = load_sites()
    CTX["reference"] = {r["site_id"]: float(r["baseline_score"]) for r in rows}
    records = [{k: v for k, v in r.items() if k != "baseline_score"} for r in rows]
    code, body, tid = api("POST", "/api/v1/ingest", json={"records": records})
    for s, r in body["shards"].items():
        say(f"shard {s:<8} records={r['records']:<4} lsn={r['lsn']} acks={r['acks']}/3 "
            f"replicas={r['replicas']}")
    SUMMARY["scenarios"]["1_ingest"] = {"status": code, "trace": tid, "shards": body["shards"]}
    show_trace(tid, "ingest (gateway -> 3 leaders -> 6 followers)", max_lines=40)


def s2_score() -> None:
    banner(2, "Distributed hazard-integrated scoring job (map -> reduce -> broadcast)",
           "parallel shard-local compute, global min-max via reduce, quorum publish")
    code, body, tid = api("POST", "/api/v1/jobs/score")
    say(f"run {body['run_id']}: {body['sites']} sites, eligible={body['eligible']}, "
        f"gated={body['gated']} ({100 * body['gated'] / body['sites']:.1f}%)")
    say(f"class counts : {body['class_counts']}")
    say(f"phase ms     : {body['phase_ms']}  total={body['total_ms']} ms")
    CTX["score_trace"] = tid
    # System-integrity check: distributed result == centralised notebook baseline S'
    _, exp, _ = api("GET", "/api/v1/export", params={"consistency": "strong"})
    got = {s["site_id"]: s.get("score") for s in exp["sites"]}
    missing = [k for k in CTX["reference"] if got.get(k) is None]
    worst = max((abs(got[k] - v) for k, v in CTX["reference"].items() if got.get(k) is not None),
                default=float("nan"))
    ok = not missing and worst < 1e-9
    say(f"integrity    : strong export from leaders {list(exp['metadata']['served_by'].values())}")
    say(f"               max |S'_distributed - S'_notebook| = {worst:.2e} over {len(got)} sites, "
        f"missing={len(missing)} -> {'PASS' if ok else 'FAIL'}")
    SUMMARY["scenarios"]["2_score"] = {**{k: body[k] for k in ("run_id", "sites", "eligible",
                                                                "gated", "class_counts",
                                                                "phase_ms", "total_ms")},
                                       "max_abs_diff_vs_notebook": worst, "trace": tid}
    show_trace(tid, "scoring job", max_lines=70)


def s3_queries() -> None:
    banner(3, "Read paths: scatter-gather, targeted shard query, consistency levels, edge cache",
           "query routing, tunable consistency, CDN-style caching")
    code, body, tid = api("GET", "/api/v1/recommend", params={"top": 5})
    say(f"district top-5 (scatter-gather over {list(body['served_by'])}):")
    for s in body["top"]:
        say(f"   {s['site_id']} {s['barangay']:<18} {s['score']:.4f} {s['suitability_class']:<20} "
            f"L{s['recommendation_level']} {s['recommendation_label']}")
    _, b2, _ = api("GET", "/api/v1/recommend", params={"top": 3, "barangay": "Talomo"})
    say(f"barangay Talomo top-3 (single shard, served by {b2['served_by']}): "
        f"{[s['site_id'] for s in b2['top']]}")
    # pick demo sites in the coastal shard (highest-scoring eligible ones)
    _, b3, _ = api("GET", "/api/v1/recommend", params={"top": 2, "barangay": "Matina Crossing"},
                   quiet=True)
    CTX["site_x"], CTX["site_y"] = b3["top"][0]["site_id"], b3["top"][1]["site_id"]
    _, b4, _ = api("GET", "/api/v1/recommend", params={"top": 1, "barangay": "Magtuod"}, quiet=True)
    CTX["site_north"] = b4["top"][0]["site_id"]
    _, b5, _ = api("GET", "/api/v1/recommend", params={"top": 1, "barangay": "Bago Gallera"},
                   quiet=True)
    CTX["site_upland"] = b5["top"][0]["site_id"]
    x = CTX["site_x"]
    _, ev, _ = api("GET", f"/api/v1/sites/{x}", params={"consistency": "eventual"})
    say(f"eventual read {x}: served_by={ev['served_by']} lag={ev['replication_lag']}")
    _, sg, _ = api("GET", f"/api/v1/sites/{x}", params={"consistency": "strong"})
    say(f"strong read   {x}: quorum={sg['quorum']} -> {site_brief(sg['record'])}")
    r1 = client.get(GW + "/api/v1/export")
    r2 = client.get(GW + "/api/v1/export")
    say(f"export x2 (web map dataset): X-Cache {r1.headers.get('X-Cache')} then "
        f"{r2.headers.get('X-Cache')}  ({len(r2.json()['sites'])} sites)")
    SUMMARY["scenarios"]["3_queries"] = {"top5": [s["site_id"] for s in body["top"]],
                                         "cache": [r1.headers.get("X-Cache"),
                                                   r2.headers.get("X-Cache")], "trace": tid}
    show_trace(tid, "district top-5 scatter-gather", max_lines=25)


def s4_replica_crash() -> None:
    t0 = time.time()
    banner(4, "Follower crash: writes keep working with W=2 of 3, follower catches up",
           "replication, fault tolerance, anti-entropy")
    chaos("coastal-c", "crash")
    wait_until(node_status("coastal-c", "DOWN"), "failure detector marked coastal-c DOWN")
    y = CTX["site_y"]
    code, body, tid = api("PATCH", f"/api/v1/sites/{y}/hazard",
                          json={"landslide_susceptibility": 1, "source": "NOAH advisory #1"})
    say(f"hazard write {y}: committed={body['write']['committed']} acks={body['write']['acks']} "
        f"replicas={body['write']['replicas']}")
    chaos("coastal-c", "ok")
    wait_until(caught_up("coastal"), "coastal-c rejoined and caught up (lag 0)")
    SUMMARY["scenarios"]["4_replica_crash"] = {"write_acks": body["write"]["acks"], "trace": tid}
    control_timeline(t0)


def s5_gray_failure() -> None:
    banner(5, "Asymmetric partition: stale eventual read vs. quorum read with read repair",
           "CAP trade-off per request, why gating decisions use strong reads")
    x = CTX["site_x"]
    chaos("coastal-b", "peer_partition")
    say("coastal-b can still talk to the gateway, but not to its peers (replication cut)")
    code, body, tid_w = api("PATCH", f"/api/v1/sites/{x}/hazard",
                            json={"flood_susceptibility": 3, "source": "NOAH advisory #2: "
                                  "5-year flood class raised to High"})
    say(f"hazard write {x} (flood -> High): acks={body['write']['acks']} "
        f"replicas={body['write']['replicas']}")
    time.sleep(0.6)
    _, ev, tid_e = api("GET", f"/api/v1/sites/{x}", params={"consistency": "eventual"})
    say(f"EVENTUAL read: served_by={ev['served_by']} lag={ev['replication_lag']} -> "
        f"{site_brief(ev['record'])}")
    stale = ev["record"].get("recommendation_eligible") is True
    if stale:
        say("   ^ STALE: this replica still says the site is ELIGIBLE although it is now High flood")
    _, sg, tid_s = api("GET", f"/api/v1/sites/{x}", params={"consistency": "strong"})
    say(f"STRONG read  : quorum={sg['quorum']} chosen={sg['served_by']} "
        f"read_repaired={sg['read_repaired']} -> {site_brief(sg['record'])}")
    _, ev2, _ = api("GET", f"/api/v1/sites/{x}", params={"consistency": "eventual"})
    say(f"EVENTUAL again after read repair: served_by={ev2['served_by']} -> "
        f"eligible={ev2['record'].get('recommendation_eligible')}")
    chaos("coastal-b", "ok")
    wait_until(caught_up("coastal"), "coastal-b caught up through the replication log")
    SUMMARY["scenarios"]["5_gray_failure"] = {
        "eventual_was_stale": stale, "strong_eligible": sg["record"]["recommendation_eligible"],
        "read_repaired": sg["read_repaired"], "traces": [tid_w, tid_e, tid_s]}
    show_trace(tid_w, "quorum write while coastal-b is partitioned", max_lines=30)
    show_trace(tid_s, "strong (quorum) read + read repair", max_lines=25)


def s6_majority_loss() -> None:
    t0 = time.time()
    banner(6, "Majority of a shard lost: CP for writes/strong reads, AP for eventual reads",
           "CAP theorem under partition")
    x = CTX["site_x"]
    chaos("coastal-b", "crash")
    chaos("coastal-c", "crash")
    wait_until(lambda st: node_status("coastal-b", "DOWN")(st) and node_status("coastal-c", "DOWN")(st),
               "coastal-b and coastal-c DOWN (1 of 3 replicas left)")
    c1, b1, _ = api("PATCH", f"/api/v1/sites/{x}/hazard", json={"storm_surge_susceptibility": 1})
    say(f"write        -> HTTP {c1}: {b1['detail']['error']} ({b1['detail'].get('cap')})")
    c2, b2, _ = api("GET", f"/api/v1/sites/{x}", params={"consistency": "strong"})
    say(f"strong read  -> HTTP {c2}: {b2['detail']['error']}")
    c3, b3, _ = api("GET", f"/api/v1/sites/{x}", params={"consistency": "eventual"})
    say(f"eventual read-> HTTP {c3}: served_by={b3['served_by']} (availability kept)")
    c4, b4, _ = api("GET", "/api/v1/recommend", params={"top": 3})
    say(f"district top-3 (eventual) -> HTTP {c4}, degraded={b4['degraded']}, "
        f"{[s['site_id'] for s in b4['top']]}")
    chaos("coastal-b", "ok")
    chaos("coastal-c", "ok")
    wait_until(caught_up("coastal"), "coastal shard back to 3 healthy replicas")
    SUMMARY["scenarios"]["6_majority_loss"] = {"write": c1, "strong_read": c2,
                                               "eventual_read": c3, "recommend": c4}
    control_timeline(t0)


def s7_leader_failover() -> None:
    t0 = time.time()
    banner(7, "Leader failure: election with epochs, fencing of the old leader, re-score",
           "high availability, split-brain prevention")
    st = status()
    old = st["shards"]["north"]["leader"]
    say(f"north leader is {old} (epoch {st['shards']['north']['epoch']})")
    chaos(old, "partition")
    wait_until(lambda s: s["shards"]["north"]["leader"] != old, "new north leader elected",
               timeout=12)
    st = status()
    new = st["shards"]["north"]["leader"]
    say(f"north leader is now {new} (epoch {st['shards']['north']['epoch']})")
    code, body, tid = api("PATCH", f"/api/v1/sites/{CTX['site_north']}/hazard",
                          json={"flood_susceptibility": 2, "source": "NOAH advisory #3"})
    say(f"write via new leader: leader={body['write']['leader']} epoch={body['write']['epoch']} "
        f"acks={body['write']['acks']}")
    chaos(old, "ok")
    say(f"{old} heals still believing it is leader of epoch 1 ...")
    wait_until(caught_up("north"), f"{old} fenced (stepped down to follower) and caught up")
    say(f"{old} role now: {replica(status(), old)['role']}")
    control_timeline(t0, "control-plane events: detection -> election -> fencing -> catch-up")
    code, body, tid_j = api("POST", "/api/v1/jobs/score")
    say(f"re-score {body['run_id']} with updated hazards: eligible={body['eligible']} "
        f"gated={body['gated']} classes={body['class_counts']}")
    _, sx, _ = api("GET", f"/api/v1/sites/{CTX['site_x']}", params={"consistency": "strong"},
                   quiet=True)
    say(f"site {CTX['site_x']} after re-score: {site_brief(sx['record'])}")
    SUMMARY["scenarios"]["7_leader_failover"] = {"old_leader": old, "new_leader": new,
                                                 "rescore": body["run_id"],
                                                 "rescore_gated": body["gated"]}


def s8_circuit_breaker() -> None:
    t0 = time.time()
    banner(8, "Crash before detection: replica failover and circuit breaker",
           "resilience patterns: time-outs, failover, circuit breaker")
    site = CTX["site_upland"]
    target = status()["shards"]["upland"]
    first = [n for n in target["replicas"] if n != target["leader"]]
    first.sort(key=lambda n: (topology.link_latency_ms("az-a", topology.zone_of(n)), n))
    victim = first[0]
    say(f"preferred replica for eventual reads of upland is {victim} (same AZ as gateway)")
    chaos(victim, "crash")
    for i in range(4):
        _, b, _ = api("GET", f"/api/v1/sites/{site}", params={"consistency": "eventual"})
        say(f"   read {i + 1}: served_by={b['served_by']}")
    st = status()
    say(f"gateway circuit breakers: {st['circuit_breakers']}")
    chaos(victim, "ok")
    wait_until(caught_up("upland"), f"{victim} back")
    time.sleep(3.2)  # breaker cool-down -> HALF_OPEN on next call -> CLOSED
    for _ in range(3):
        api("GET", f"/api/v1/sites/{site}", params={"consistency": "eventual"}, quiet=True)
    say(f"after cool-down: breakers={status()['circuit_breakers'] or 'all CLOSED'}")
    SUMMARY["scenarios"]["8_circuit_breaker"] = {"victim": victim,
                                                 "breakers_during": st["circuit_breakers"]}
    control_timeline(t0)


def s9_shard_outage() -> None:
    t0 = time.time()
    banner(9, "Whole-shard outage: graceful degradation vs. refusing the batch job",
           "partial availability, CP for analytics")
    for n in topology.shard_members("upland"):
        chaos(n.node_id, "partition")
    wait_until(lambda st: all(r["status"] == "DOWN"
                              for r in st["shards"]["upland"]["replicas"].values()),
               "all upland replicas DOWN; shard read-only / unavailable", timeout=12)
    c1, b1, _ = api("GET", "/api/v1/recommend", params={"top": 3})
    say(f"district top-3 -> HTTP {c1} degraded={b1['degraded']} missing={b1['missing_shards']} "
        f"results={[s['site_id'] for s in b1['top']]}")
    c2, b2, tid = api("POST", "/api/v1/jobs/score")
    say(f"scoring job   -> HTTP {c2}: {b2['detail']['error']}")
    for n in topology.shard_members("upland"):
        chaos(n.node_id, "ok")
    wait_until(caught_up("upland"), "upland shard healed")
    SUMMARY["scenarios"]["9_shard_outage"] = {"recommend": c1, "degraded": b1["degraded"],
                                              "job": c2}
    control_timeline(t0)


def s10_private_cloud() -> None:
    banner(10, "Hybrid cloud: CHO dossier in the private cloud over the VPN",
           "cloud deployment model, zero trust, data sovereignty (RA 10173)")
    _, top, _ = api("GET", "/api/v1/recommend", params={"top": 3}, quiet=True)
    sites = [s["site_id"] for s in top["top"]]
    dossier = {"dossier_id": "CHO-2026-011", "title": "Talomo new BHS shortlist",
               "shortlisted_sites": sites, "planner_notes": "For ocular inspection; check lot "
               "ownership with City Assessor.", "prepared_by": "CHO planning officer"}
    c1, b1, tid = api("POST", "/api/v1/private/dossiers", json=dossier, headers=CHO)
    say(f"store dossier (CHO role) -> HTTP {c1}: {b1.get('location')}")
    c2, b2, _ = api("GET", "/api/v1/private/dossiers/CHO-2026-011", headers=CHO)
    say(f"read dossier  (CHO role) -> HTTP {c2}: sites={b2.get('shortlisted_sites')}")
    c3, _, _ = api("GET", "/api/v1/private/dossiers/CHO-2026-011")
    say(f"read dossier  (no role)  -> HTTP {c3} (blocked at gateway)")
    c4, _, _ = api("GET", "/dossiers/CHO-2026-011", base=VAULT,
                   headers={"X-Source-Node": "upland-a"})
    say(f"direct call from a public-cloud node, bypassing the VPN -> HTTP {c4} (blocked at vault)")
    chaos("cho-vault", "crash")
    c5, b5, _ = api("POST", "/api/v1/private/dossiers", json={**dossier, "dossier_id": "CHO-2026-012"},
                    headers=CHO)
    say(f"vault down: store dossier -> HTTP {c5}: {b5['detail']['error']} "
        f"(fallback: {b5['detail']['fallback']})")
    chaos("cho-vault", "ok")
    SUMMARY["scenarios"]["10_private_cloud"] = {"store": c1, "read": c2, "no_role": c3,
                                                "bypass_vpn": c4, "vault_down": c5}
    show_trace(tid, "dossier write over the VPN", max_lines=15)


def s11_observability() -> None:
    banner(11, "Observability: metrics, event inventory, where the logs are",
           "metrics, tracing, structured logging")
    time.sleep(1.0)
    m = client.get(GW + "/cluster/metrics").json()
    say(f"{'node':<11} {'requests':>8} {'errors':>6} {'p95 ms (busiest data-plane route)':>36}")
    for nid in ["gateway"] + [n for n in m if n != "gateway"]:
        routes = m[nid].get("routes", {})
        req = sum(r["requests"] for r in routes.values())
        err = sum(r["errors"] for r in routes.values())
        data_routes = {k: v for k, v in routes.items() if not any(
            x in k for x in ("/health", "/heartbeat", "/metrics", "/telemetry", "/cluster/", "/admin"))}
        busiest = max(data_routes.items(), key=lambda kv: kv[1]["requests"], default=None)
        p95 = f"{busiest[1]['p95_ms']:.1f} ({busiest[0]})" if busiest else "-"
        say(f"{nid:<11} {req:>8} {err:>6}   {p95}")
    ev = client.get(COLLECTOR + "/events/summary").json()
    say(f"collector holds {sum(ev['event_counts'].values())} events of "
        f"{len(ev['event_counts'])} types:")
    items = list(ev["event_counts"].items())
    for i in range(0, len(items), 3):
        say("   " + "   ".join(f"{k}={v}" for k, v in items[i:i + 3]))
    SUMMARY["event_counts"] = ev["event_counts"]
    say(f"per-node JSONL logs : {LOG_DIR}/<node>.jsonl")
    say(f"cluster-wide log    : {LOG_DIR}/collector_all.jsonl")
    say("inspect any trace   : python -m tools.trace_view --list")


SCENARIOS = {1: s1_ingest, 2: s2_score, 3: s3_queries, 4: s4_replica_crash, 5: s5_gray_failure,
             6: s6_majority_loss, 7: s7_leader_failover, 8: s8_circuit_breaker,
             9: s9_shard_outage, 10: s10_private_cloud, 11: s11_observability}


def gateway_up() -> bool:
    try:
        return client.get(GW + "/health", timeout=1.0).status_code == 200
    except httpx.HTTPError:
        return False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true", help="leave the cluster running")
    ap.add_argument("--only", type=int, nargs="*", help="scenario numbers to run")
    args = ap.parse_args()

    procs, console = [], None
    if not gateway_up():
        LOG_DIR.mkdir(exist_ok=True)
        console = open(LOG_DIR / "cluster_console.log", "w", encoding="utf-8")
        print(f"[simulate] starting cluster (node console output -> {console.name})")
        procs = start_cluster(console)
    if not wait_ready():
        print("[simulate] cluster did not become ready; see logs/cluster_console.log")
        stop_cluster(procs)
        sys.exit(1)

    chosen = sorted(set([1, 2, 3] + (args.only or list(SCENARIOS))))
    t0 = time.time()
    try:
        for n in chosen:
            SCENARIOS[n]()
        SUMMARY["wall_clock_s"] = round(time.time() - t0, 1)
        (LOG_DIR / "simulation_summary.json").write_text(json.dumps(SUMMARY, indent=2),
                                                         encoding="utf-8")
        print(f"\n[simulate] done in {SUMMARY['wall_clock_s']} s - summary: "
              f"{LOG_DIR / 'simulation_summary.json'}")
    finally:
        if procs and not args.keep:
            time.sleep(0.5)
            stop_cluster(procs)
            print("[simulate] cluster stopped")
        elif procs:
            print(f"[simulate] cluster left running - gateway {GW}/docs")


if __name__ == "__main__":
    main()
