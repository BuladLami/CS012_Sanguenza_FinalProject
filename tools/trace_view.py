"""Render a distributed trace as a text waterfall.

    python -m tools.trace_view --list          # recent traces from the collector
    python -m tools.trace_view <trace_id>      # one trace as a span tree with events
"""
from __future__ import annotations

import json
import sys
from typing import Any, Dict, List

import httpx

from common import topology

COLLECTOR = topology.node("collector").url


def _attrs(a: Dict[str, Any], limit: int = 110) -> str:
    parts = []
    for k, v in a.items():
        s = json.dumps(v, ensure_ascii=False, default=str) if isinstance(v, (dict, list)) else str(v)
        if isinstance(v, float):
            s = f"{v:.4f}".rstrip("0").rstrip(".")
        parts.append(f"{k}={s}")
    out = " ".join(parts)
    return out if len(out) <= limit else out[:limit - 3] + "..."


def render(trace: Dict[str, Any], max_lines: int = 200) -> List[str]:
    t0 = trace["t0"]
    lines = [f"TRACE {trace['trace_id']}  spans={trace['span_count']}  "
             f"nodes={','.join(trace['nodes'])}"]

    def walk(span: Dict[str, Any], depth: int) -> None:
        pad = "  " * depth
        lines.append(f"{pad}|- [{(span['ts'] - t0) * 1000:7.1f}ms +{span['duration_ms']:7.1f}ms] "
                     f"{span['node']:<10} {span['name']}  ({span['status']})")
        items = [("e", e) for e in span["events"]] + [("s", c) for c in span["children"]]
        items.sort(key=lambda x: x[1]["ts"])
        for kind, item in items:
            if kind == "e":
                lines.append(f"{pad}   . {(item['ts'] - t0) * 1000:7.1f}ms {item['level']:<5} "
                             f"{item['event']} {_attrs(item['attrs'])}")
            else:
                walk(item, depth + 1)

    for root in trace["roots"]:
        walk(root, 0)
    for e in trace.get("unattached_events", []):
        lines.append(f"   . {(e['ts'] - t0) * 1000:7.1f}ms [{e['node']}] {e['event']} "
                     f"{_attrs(e['attrs'])}")
    if len(lines) > max_lines:
        cut = len(lines) - max_lines
        lines = lines[:max_lines] + [f"   ... {cut} more lines (python -m tools.trace_view "
                                     f"{trace['trace_id']})"]
    return lines


def fetch(trace_id: str) -> Dict[str, Any]:
    r = httpx.get(f"{COLLECTOR}/traces/{trace_id}", timeout=3.0)
    r.raise_for_status()
    return r.json()


def main(argv: List[str]) -> None:
    if not argv or argv[0] == "--list":
        for t in httpx.get(f"{COLLECTOR}/traces", params={"limit": 30}).json()["traces"]:
            print(f"{t['trace_id']:<12} spans={t['spans']:<4} events={t['events']:<4} "
                  f"{t['root']}  [{','.join(t['nodes'])}]")
        return
    print("\n".join(render(fetch(argv[0]), max_lines=10_000)))


if __name__ == "__main__":
    main(sys.argv[1:])
