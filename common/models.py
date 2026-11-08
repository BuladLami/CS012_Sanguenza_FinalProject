"""Data contracts shared by the gateway, data nodes, vault and collector."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class SiteRecord(BaseModel):
    """Raw per-site features as produced by the feature-extraction stage."""
    site_id: str
    barangay: str
    latitude: float
    longitude: float
    building_density: float
    road_distance: float
    facility_distance: float
    shoreline_distance: float
    flood_susceptibility: int
    landslide_susceptibility: int
    storm_surge_susceptibility: int


class IngestRequest(BaseModel):
    records: List[SiteRecord]
    source: str = "OSM 2026-07-29 + Project NOAH"


class HazardUpdate(BaseModel):
    """A hazard re-classification (e.g. a new Project NOAH advisory)."""
    flood_susceptibility: Optional[int] = Field(default=None, ge=0, le=3)
    landslide_susceptibility: Optional[int] = Field(default=None, ge=0, le=3)
    storm_surge_susceptibility: Optional[int] = Field(default=None, ge=0, le=3)
    source: str = "Project NOAH advisory"


class WriteRequest(BaseModel):
    """Gateway -> shard leader."""
    op: str                       # upsert | merge | hazard_update
    records: List[Dict[str, Any]]
    epoch: int
    w: int = 2
    idempotency_key: Optional[str] = None


class ReplicationEntry(BaseModel):
    lsn: int
    epoch: int
    op: str
    records: List[Dict[str, Any]]


class AppendRequest(BaseModel):
    """Leader -> follower log shipping."""
    entries: List[ReplicationEntry]
    epoch: int
    leader_id: str
    leader_commit: int


class Heartbeat(BaseModel):
    """Gateway (control plane) -> data node."""
    epoch: int
    leader_id: str
    leader_lsn: int = 0


class ChaosRequest(BaseModel):
    mode: str = "ok"              # ok | crash | partition | peer_partition | slow | flaky
    delay_ms: int = 0
    fail_rate: float = 0.0


class Dossier(BaseModel):
    """CHO-internal planning record: kept only in the private cloud."""
    dossier_id: str
    title: str
    shortlisted_sites: List[str]
    planner_notes: str
    prepared_by: str
    classification: str = "CONFIDENTIAL - CHO internal"
