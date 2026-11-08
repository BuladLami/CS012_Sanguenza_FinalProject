"""Observability collector (an OpenTelemetry-collector / Jaeger stand-in).

Every node pushes its events and spans here in batches. The collector
  * appends them to logs/collector_all.jsonl (the cluster-wide log)
  * indexes them by trace id so a request can be followed across nodes
  * rebuilds the span tree:  GET /traces/{trace_id}
  * lists recent traces:     GET /traces
  * counts events by type:   GET /events/summary
  * recent events, filtered: GET /events?since=<epoch s>&names=A,B
"""
from __future__ import annotations

import json
from collections import Counter, OrderedDict, defaultdict, deque
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException

from common import telemetry
from common.telemetry import LOG_DIR

tel = telemetry.init("collector", None)
app = FastAPI(title="POLARIS observability collector")
telemetry.install(app, tel)

TRACES: "OrderedDict[str, List[Dict[str, Any]]]" = OrderedDict()
EVENT_COUNTS: Counter = Counter()
DROPPED: Dict[str, int] = defaultdict(int)
MAX_TRACES = 2000
RECENT: deque = deque(maxlen=20000)
_sink = open(LOG_DIR / "collector_all.jsonl", "a", encoding="utf-8")


@app.get("/health")
async def health():
    return {"status": "HEALTHY", "node_id": "collector", "traces": len(TRACES),
            "events": sum(EVENT_COUNTS.values())}


@app.post("/telemetry")
async def ingest(batch: Dict[str, Any]):
    DROPPED[batch.get("node", "?")] = batch.get("dropped", 0)
    for rec in batch.get("records", []):
        _sink.write(json.dumps(rec, default=str, ensure_ascii=False) + "\n")
        if rec["kind"] == "event":
            EVENT_COUNTS[rec["event"]] += 1
            RECENT.append(rec)
        tid = rec.get("trace_id", "-")
        if tid in ("-",):
            continue
        TRACES.setdefault(tid, []).append(rec)
        TRACES.move_to_end(tid)
        while len(TRACES) > MAX_TRACES:
            TRACES.popitem(last=False)
    _sink.flush()
    return {"accepted": len(batch.get("records", []))}


@app.get("/traces")
async def list_traces(limit: int = 20):
    out = []
    for tid, recs in list(TRACES.items())[-limit:]:
        spans = [r for r in recs if r["kind"] == "span"]
        root = min(spans, key=lambda s: s["ts"]) if spans else None
        out.append({"trace_id": tid, "root": root["name"] if root else None,
                    "nodes": sorted({r["node"] for r in recs}), "spans": len(spans),
                    "events": len(recs) - len(spans)})
    return {"traces": out}


@app.get("/traces/{trace_id}")
async def get_trace(trace_id: str):
    recs = TRACES.get(trace_id)
    if not recs:
        raise HTTPException(404, detail="trace not found (yet)")
    spans = {r["span_id"]: {**r, "children": [], "events": []}
             for r in recs if r["kind"] == "span"}
    roots = []
    for s in sorted(spans.values(), key=lambda s: s["ts"]):
        parent = spans.get(s["parent_span_id"])
        (parent["children"] if parent else roots).append(s)
    orphan_events = []
    for r in sorted((r for r in recs if r["kind"] == "event"), key=lambda r: r["ts"]):
        (spans[r["span_id"]]["events"] if r["span_id"] in spans else orphan_events).append(r)
    t0 = min(r["ts"] for r in recs)
    return {"trace_id": trace_id, "t0": t0, "nodes": sorted({r["node"] for r in recs}),
            "span_count": len(spans), "roots": roots, "unattached_events": orphan_events}


@app.get("/events/summary")
async def summary():
    return {"event_counts": dict(EVENT_COUNTS.most_common()), "dropped_by_node": dict(DROPPED)}


@app.get("/events")
async def recent_events(since: float = 0.0, names: Optional[str] = None, limit: int = 200):
    wanted = set(names.split(",")) if names else None
    out = [r for r in RECENT if r["ts"] >= since and (wanted is None or r["event"] in wanted)]
    out.sort(key=lambda r: r["ts"])
    return {"events": out[-limit:]}
