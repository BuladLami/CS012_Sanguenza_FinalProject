"""CHO private-cloud vault.

Holds the records that must not leave the City Health Office's own
infrastructure (Data Privacy Act of 2012, RA 10173): planner shortlists and
notes, Tier-4 expert validation checklists, UAT sign-offs.

Zero-trust rules enforced here, not only at the gateway:
  * only traffic that arrived through the site-to-site VPN tunnel
    (X-Network-Zone: PRIVATE_VPC plus the tunnel id) is accepted
  * every access, allowed or denied, is written to an append-only audit trail
  * single node, strongly consistent (CP): if the vault is down, writes fail;
    they are never redirected to the public cloud.
"""
from __future__ import annotations

import os
import time
from typing import Dict, List

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from common import chaos as chaos_mod
from common import telemetry, topology
from common.models import Dossier
from common.telemetry import SOURCE_HEADER

NODE_ID = os.getenv("NODE_ID", "cho-vault")
VPN_TUNNEL_ID = os.getenv("VPN_TUNNEL_ID", "vpn-tunnel-cho-01")

tel = telemetry.init(NODE_ID, topology.node("collector").url)
chaos = chaos_mod.Chaos()
app = FastAPI(title="POLARIS CHO private-cloud vault")
DOSSIERS: Dict[str, Dict] = {}
AUDIT: List[Dict] = []


@app.middleware("http")
async def vpc_guard(request: Request, call_next):
    if request.url.path.startswith("/dossiers"):
        zone = request.headers.get("X-Network-Zone")
        tunnel = request.headers.get("X-VPN-Tunnel")
        src = request.headers.get(SOURCE_HEADER, "unknown")
        if zone != "PRIVATE_VPC" or tunnel != VPN_TUNNEL_ID:
            AUDIT.append({"ts": time.time(), "action": "DENIED", "source": src,
                          "path": request.url.path})
            telemetry.event("VPC_ACCESS_DENIED", "WARN", source=src, path=request.url.path,
                            zone=zone, reason="request did not arrive through the CHO VPN tunnel")
            return JSONResponse({"detail": "Forbidden: CHO private cloud is reachable only via "
                                           "the site-to-site VPN"}, status_code=403)
    return await call_next(request)


telemetry.install(app, tel)
chaos_mod.install(app, chaos)


@app.get("/health")
async def health():
    return {"status": "HEALTHY", "node_id": NODE_ID, "zone": topology.PRIVATE,
            "role": "vault", "stored_keys_count": len(DOSSIERS), "audit_entries": len(AUDIT),
            "chaos": chaos.mode}


@app.post("/dossiers")
async def store(d: Dossier, request: Request):
    DOSSIERS[d.dossier_id] = {**d.model_dump(), "stored_at": time.time()}
    AUDIT.append({"ts": time.time(), "action": "WRITE", "dossier_id": d.dossier_id,
                  "actor": request.headers.get("X-Actor", "?")})
    telemetry.event("DOSSIER_STORED", dossier_id=d.dossier_id, sites=d.shortlisted_sites,
                    classification=d.classification, actor=request.headers.get("X-Actor"))
    return {"stored": True, "dossier_id": d.dossier_id, "node_id": NODE_ID,
            "location": topology.ZONES[topology.PRIVATE]}


@app.get("/dossiers/{dossier_id}")
async def read(dossier_id: str, request: Request):
    if dossier_id not in DOSSIERS:
        raise HTTPException(404, detail="no such dossier")
    AUDIT.append({"ts": time.time(), "action": "READ", "dossier_id": dossier_id,
                  "actor": request.headers.get("X-Actor", "?")})
    telemetry.event("DOSSIER_READ", dossier_id=dossier_id, actor=request.headers.get("X-Actor"))
    return DOSSIERS[dossier_id]


@app.get("/audit")
async def audit():
    return {"entries": AUDIT[-50:]}
