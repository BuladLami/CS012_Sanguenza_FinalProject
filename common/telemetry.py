"""Observability: structured events, distributed tracing and metrics.

Every node uses this module, so every process emits the same three signals:

  * EVENTS  - one structured record per meaningful action ("WRITE_COMMITTED",
              "LEADER_ELECTED", ...). Printed to the console, appended to
              logs/<node>.jsonl, and shipped to the collector node.
  * SPANS   - one record per handled HTTP request, carrying trace_id, span_id
              and parent_span_id so the collector can rebuild the request tree
              across nodes (W3C-traceparent style propagation via headers).
  * METRICS - request counts, error counts and latency percentiles per route,
              exposed at GET /metrics on every node.

Telemetry is deliberately AP: shipping is asynchronous and best-effort. If the
collector is unreachable, the node keeps a bounded buffer, drops the oldest
records when it is full, and the local JSONL file stays complete.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
import uuid
from collections import defaultdict, deque
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

import httpx

ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = Path(os.getenv("POLARIS_LOG_DIR", ROOT / "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

TRACE_HEADER = "X-Trace-ID"
PARENT_HEADER = "X-Parent-Span-ID"
SOURCE_HEADER = "X-Source-Node"

trace_id_var: ContextVar[str] = ContextVar("trace_id", default="-")
span_id_var: ContextVar[str] = ContextVar("span_id", default="-")

# Health checks and telemetry traffic are not traced (standard sampling rule),
# otherwise heartbeats would drown the interesting events.
UNTRACED_PATHS = ("/health", "/control/heartbeat", "/telemetry", "/metrics", "/favicon.ico")

try:  # Windows consoles default to cp1252; never crash on a barangay name.
    sys.stdout.reconfigure(errors="replace")  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    pass


def new_id(n: int = 8) -> str:
    return uuid.uuid4().hex[:n]


def generate_trace_id() -> str:  # scaffold-compatible name
    return new_id(8)


def _fmt_value(v: Any) -> str:
    if isinstance(v, float):
        return (f"{v:.4f}".rstrip("0").rstrip(".") or "0") if abs(v) < 1000 else f"{v:.0f}"
    if isinstance(v, (dict, list)):
        s = json.dumps(v, default=str, ensure_ascii=False)
        return s if len(s) <= 120 else s[:117] + "..."
    return str(v)


class Metrics:
    def __init__(self) -> None:
        self.count: Dict[str, int] = defaultdict(int)
        self.errors: Dict[str, int] = defaultdict(int)
        self.latency: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=2000))
        self.counters: Dict[str, int] = defaultdict(int)

    def observe(self, route: str, ms: float, ok: bool) -> None:
        self.count[route] += 1
        if not ok:
            self.errors[route] += 1
        self.latency[route].append(ms)

    def incr(self, name: str, by: int = 1) -> None:
        self.counters[name] += by

    @staticmethod
    def _pct(sorted_vals: List[float], p: float) -> float:
        if not sorted_vals:
            return 0.0
        k = min(len(sorted_vals) - 1, max(0, int(round(p / 100 * (len(sorted_vals) - 1)))))
        return round(sorted_vals[k], 2)

    def snapshot(self) -> Dict[str, Any]:
        routes = {}
        for r, n in self.count.items():
            vals = sorted(self.latency[r])
            routes[r] = {"requests": n, "errors": self.errors[r],
                         "p50_ms": self._pct(vals, 50), "p95_ms": self._pct(vals, 95),
                         "p99_ms": self._pct(vals, 99)}
        return {"routes": routes, "counters": dict(self.counters)}


class Telemetry:
    def __init__(self, node_id: str, collector_url: Optional[str]) -> None:
        self.node_id = node_id
        self.collector_url = collector_url
        self.metrics = Metrics()
        self._buffer: Deque[Dict[str, Any]] = deque(maxlen=5000)
        self._dropped = 0
        self._file = open(LOG_DIR / f"{node_id}.jsonl", "a", encoding="utf-8")
        self._log = logging.getLogger(node_id)
        if not self._log.handlers:
            h = logging.StreamHandler(sys.stdout)
            h.setFormatter(logging.Formatter("%(asctime)s.%(msecs)03d %(message)s", "%H:%M:%S"))
            self._log.addHandler(h)
            self._log.setLevel(logging.INFO)
            self._log.propagate = False
        self._shipper: Optional[asyncio.Task] = None

    # -- records --------------------------------------------------------------
    def _write(self, rec: Dict[str, Any]) -> None:
        self._file.write(json.dumps(rec, default=str, ensure_ascii=False) + "\n")
        self._file.flush()
        if self.collector_url and self.node_id != "collector":
            if len(self._buffer) == self._buffer.maxlen:
                self._dropped += 1
            self._buffer.append(rec)

    def event(self, name: str, level: str = "INFO", **attrs: Any) -> None:
        rec = {"kind": "event", "ts": time.time(), "node": self.node_id,
               "trace_id": trace_id_var.get(), "span_id": span_id_var.get(),
               "level": level, "event": name, "attrs": attrs}
        self._write(rec)
        kv = " ".join(f"{k}={_fmt_value(v)}" for k, v in attrs.items())
        line = (f"{level:<5} [{self.node_id:<10}] [trace={rec['trace_id']} span={rec['span_id'][:6]}] "
                f"{name} {kv}")
        self._log.log(getattr(logging, level, logging.INFO), line)

    def span(self, name: str, start: float, duration_ms: float, parent: str,
             status: int, **attrs: Any) -> None:
        rec = {"kind": "span", "ts": start, "node": self.node_id,
               "trace_id": trace_id_var.get(), "span_id": span_id_var.get(),
               "parent_span_id": parent, "name": name, "duration_ms": round(duration_ms, 2),
               "status": status, "attrs": attrs}
        self._write(rec)

    # -- shipping (AP, best-effort) -------------------------------------------
    def start_shipper(self) -> None:
        if self.collector_url and self.node_id != "collector" and self._shipper is None:
            self._shipper = asyncio.get_event_loop().create_task(self._ship_loop())

    async def _ship_loop(self) -> None:
        async with httpx.AsyncClient(timeout=1.0) as client:
            while True:
                await asyncio.sleep(0.3)
                if not self._buffer:
                    continue
                batch = list(self._buffer)
                try:
                    await client.post(f"{self.collector_url}/telemetry",
                                      json={"node": self.node_id, "records": batch,
                                            "dropped": self._dropped})
                    for _ in range(len(batch)):
                        self._buffer.popleft()
                except Exception:
                    # Collector down: keep buffering; local JSONL is the source of truth.
                    self.metrics.incr("telemetry_ship_failures")


_TELEMETRY: Optional[Telemetry] = None


def init(node_id: str, collector_url: Optional[str]) -> Telemetry:
    global _TELEMETRY
    _TELEMETRY = Telemetry(node_id, collector_url)
    return _TELEMETRY


def get() -> Telemetry:
    assert _TELEMETRY is not None, "telemetry.init() not called"
    return _TELEMETRY


def event(name: str, level: str = "INFO", **attrs: Any) -> None:
    get().event(name, level, **attrs)


def install(app, tel: Telemetry) -> None:
    """FastAPI middleware: continue or start a trace and record a server span."""
    from fastapi import Request

    @app.middleware("http")
    async def _tracing(request: Request, call_next):
        path = request.url.path
        traced = not path.startswith(UNTRACED_PATHS)
        trace_id = request.headers.get(TRACE_HEADER) or new_id(8)
        parent = request.headers.get(PARENT_HEADER, "-")
        trace_id_var.set(trace_id)
        span_id_var.set(new_id(12))
        start = time.time()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers[TRACE_HEADER] = trace_id
            return response
        finally:
            ms = (time.time() - start) * 1000
            route = f"{request.method} {_route_template(request)}"
            tel.metrics.observe(route, ms, status < 500)
            if traced:
                tel.span(f"{request.method} {path}", start, ms, parent, status,
                         source=request.headers.get(SOURCE_HEADER, "client"))

    @app.on_event("startup")
    async def _start_shipper():
        tel.start_shipper()

    @app.get("/metrics")
    async def _metrics():
        return {"node_id": tel.node_id, **tel.metrics.snapshot()}


def _route_template(request) -> str:
    route = request.scope.get("route")
    return getattr(route, "path", request.url.path)
