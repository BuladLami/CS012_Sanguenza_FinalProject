"""Fault injection (chaos engineering) shared by every node.

Modes
  ok              normal operation
  crash           process "down": every request fails immediately (503)
  partition       node isolated from the whole network: requests hang until the
                  caller's time-out; the node cannot send either
  peer_partition  asymmetric / gray failure: the link to the other DATA nodes is
                  cut (replication fails) but the gateway can still reach it
  slow            every request is delayed by delay_ms
  flaky           each request fails with probability fail_rate

The /admin/* control plane is exempt so the simulator can heal the node.
"""
from __future__ import annotations

import asyncio
import random
from typing import Callable, List, Optional

from fastapi import Request
from fastapi.responses import JSONResponse

from common import telemetry, topology
from common.models import ChaosRequest
from common.telemetry import SOURCE_HEADER


class Chaos:
    def __init__(self) -> None:
        self.mode = "ok"
        self.delay_ms = 0
        self.fail_rate = 0.0
        self.listeners: List[Callable[[str, str], None]] = []

    def outbound_guard(self, target: str) -> Optional[str]:
        if self.mode in ("crash", "partition"):
            return f"local node is {self.mode}"
        if self.mode == "peer_partition" and topology.node(target).role == "data":
            return "link to peer data nodes is partitioned"
        return None


def install(app, chaos: Chaos) -> None:
    @app.middleware("http")
    async def _chaos(request: Request, call_next):
        path = request.url.path
        if path.startswith("/admin") or chaos.mode == "ok":
            return await call_next(request)
        if chaos.mode == "crash":
            return JSONResponse({"detail": "node is down (simulated crash)"}, status_code=503)
        if chaos.mode == "partition":
            await asyncio.sleep(3.0)  # the packet never arrives; caller times out first
            return JSONResponse({"detail": "partitioned"}, status_code=503)
        if chaos.mode == "peer_partition":
            src = request.headers.get(SOURCE_HEADER, "")
            if src in topology.NODES and topology.node(src).role == "data":
                return JSONResponse({"detail": "peer link partitioned"}, status_code=503)
        if chaos.mode == "slow":
            await asyncio.sleep(chaos.delay_ms / 1000)
        if chaos.mode == "flaky" and random.random() < chaos.fail_rate:
            return JSONResponse({"detail": "transient failure (flaky)"}, status_code=503)
        return await call_next(request)

    @app.post("/admin/chaos")
    async def _set_chaos(req: ChaosRequest):
        old = chaos.mode
        chaos.mode, chaos.delay_ms, chaos.fail_rate = req.mode, req.delay_ms, req.fail_rate
        telemetry.event("CHAOS_INJECTED" if req.mode != "ok" else "CHAOS_HEALED",
                        "WARN" if req.mode != "ok" else "INFO",
                        previous=old, mode=req.mode, delay_ms=req.delay_ms, fail_rate=req.fail_rate)
        for fn in chaos.listeners:
            fn(old, req.mode)
        return {"node_id": telemetry.get().node_id, "mode": chaos.mode}
