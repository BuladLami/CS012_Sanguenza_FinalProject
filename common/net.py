"""Inter-node networking: the only way nodes talk to each other.

Responsibilities
  * propagate trace context (trace id, parent span, source node) on every hop
  * inject simulated link latency from the topology (same AZ / cross AZ / VPN)
  * time-outs, bounded retries with exponential back-off + jitter
  * a per-destination circuit breaker (CLOSED -> OPEN -> HALF_OPEN)
  * honour the caller's own chaos state (a partitioned node cannot send)
"""
from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Callable, Dict, Optional

import httpx

from common import telemetry, topology
from common.telemetry import PARENT_HEADER, SOURCE_HEADER, TRACE_HEADER, span_id_var, trace_id_var


class NodeUnavailable(Exception):
    def __init__(self, node_id: str, reason: str, status: Optional[int] = None):
        super().__init__(f"{node_id}: {reason}")
        self.node_id = node_id
        self.reason = reason
        self.status = status


class CircuitBreaker:
    def __init__(self, target: str, threshold: int = 3, cooldown_s: float = 3.0):
        self.target = target
        self.threshold = threshold
        self.cooldown_s = cooldown_s
        self.failures = 0
        self.state = "CLOSED"
        self.opened_at = 0.0

    def allow(self) -> bool:
        if self.state == "OPEN" and time.time() - self.opened_at >= self.cooldown_s:
            self.state = "HALF_OPEN"
            telemetry.event("CIRCUIT_HALF_OPEN", target=self.target)
        return self.state != "OPEN"

    def success(self) -> None:
        if self.state != "CLOSED":
            telemetry.event("CIRCUIT_CLOSED", target=self.target)
        self.state, self.failures = "CLOSED", 0

    def failure(self) -> None:
        self.failures += 1
        if self.state == "HALF_OPEN" or (self.state == "CLOSED" and self.failures >= self.threshold):
            self.state, self.opened_at = "OPEN", time.time()
            telemetry.event("CIRCUIT_OPEN", "WARN", target=self.target,
                            consecutive_failures=self.failures, cooldown_s=self.cooldown_s)


class Network:
    """One instance per process."""

    def __init__(self, self_id: str, outbound_guard: Optional[Callable[[str], Optional[str]]] = None):
        self.self_id = self_id
        self.self_zone = topology.zone_of(self_id)
        self.breakers: Dict[str, CircuitBreaker] = {}
        self._client = httpx.AsyncClient(timeout=2.0)
        # returns a reason string if this node is not allowed to send to target
        self.outbound_guard = outbound_guard

    def breaker(self, target: str) -> CircuitBreaker:
        if target not in self.breakers:
            self.breakers[target] = CircuitBreaker(target)
        return self.breakers[target]

    def headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        h = {TRACE_HEADER: trace_id_var.get(), PARENT_HEADER: span_id_var.get(),
             SOURCE_HEADER: self.self_id}
        if extra:
            h.update(extra)
        return h

    async def call(self, target: str, method: str, path: str, *, json: Any = None,
                   params: Optional[Dict[str, Any]] = None, timeout: float = 1.0,
                   retries: int = 0, headers: Optional[Dict[str, str]] = None,
                   use_breaker: bool = True, quiet: bool = False) -> Any:
        """Call another node. Returns parsed JSON or raises NodeUnavailable.

        HTTP 4xx responses are returned as {"_status": code, **body} so callers can
        react to protocol answers (409 fencing, 404 not found) without retrying.
        """
        spec = topology.node(target)
        br = self.breaker(target)
        attempt = 0
        while True:
            if use_breaker and not br.allow():
                raise NodeUnavailable(target, "circuit open")
            try:
                if self.outbound_guard:
                    reason = self.outbound_guard(target)
                    if reason:
                        raise httpx.ConnectError(reason)
                await asyncio.sleep(topology.link_latency_ms(self.self_zone, spec.zone) / 1000)
                resp = await self._client.request(method, spec.url + path, json=json, params=params,
                                                  headers=self.headers(headers), timeout=timeout)
                if resp.status_code >= 500:
                    raise httpx.HTTPStatusError(f"HTTP {resp.status_code}", request=resp.request,
                                                response=resp)
                if use_breaker:
                    br.success()
                body = resp.json() if resp.content else {}
                if resp.status_code >= 400:
                    if isinstance(body, dict):
                        return {"_status": resp.status_code, **body}
                    return {"_status": resp.status_code, "detail": body}
                return body
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                if use_breaker:
                    br.failure()
                status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
                reason = type(exc).__name__ if status is None else f"HTTP {status}"
                if attempt >= retries:
                    if not quiet:
                        telemetry.event("CALL_FAILED", "WARN", target=target, path=path,
                                        reason=reason, attempts=attempt + 1)
                    raise NodeUnavailable(target, reason, status)
                backoff = 0.05 * (2 ** attempt) + random.uniform(0, 0.03)
                telemetry.event("RETRY", "WARN", target=target, path=path, attempt=attempt + 1,
                                backoff_ms=round(backoff * 1000), reason=reason)
                attempt += 1
                await asyncio.sleep(backoff)
