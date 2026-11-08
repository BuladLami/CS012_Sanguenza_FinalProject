"""Cluster topology: the single source of truth for every simulated node.

In a real deployment this information lives in a service-discovery / config
store (e.g. etcd, Consul, AWS Cloud Map). In the simulation every process
imports this module and looks itself up by NODE_ID.

Deployment model: HYBRID CLOUD
  * Public cloud region (3 availability zones) runs the API gateway, the
    observability collector and the 9 data/compute nodes.
  * The Davao City Health Office (CHO) private cloud runs the vault that keeps
    sensitive planning records. It is reachable only through a site-to-site
    VPN tunnel that terminates at the gateway.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List, Optional

HOST = os.getenv("POLARIS_HOST", "127.0.0.1")

# Network zones -------------------------------------------------------------
AZ_A, AZ_B, AZ_C = "az-a", "az-b", "az-c"
PRIVATE = "cho-private"
ZONES = {
    AZ_A: "Public cloud / asia-southeast region / AZ-a",
    AZ_B: "Public cloud / asia-southeast region / AZ-b",
    AZ_C: "Public cloud / asia-southeast region / AZ-c",
    PRIVATE: "CHO private cloud (Davao City data centre)",
}

# Simulated one-way link latency in milliseconds (applied by common.net).
LATENCY_MS = {
    "same_az": 1.0,
    "cross_az": 4.0,
    "vpn": 30.0,  # public region <-> CHO private cloud over site-to-site VPN
}


@dataclass(frozen=True)
class NodeSpec:
    node_id: str
    role: str            # gateway | data | vault | collector
    port: int
    zone: str
    subnet: str          # public-subnet | private-subnet | cho-vpc
    shard: Optional[str] = None
    initial_leader: bool = False

    @property
    def url(self) -> str:
        return f"http://{HOST}:{self.port}"


# Geographic (directory-based) sharding ------------------------------------
# The 14 Talomo barangays are grouped into three spatially contiguous shards of
# roughly equal size (443-534 candidate sites each). A spatial query for one
# barangay touches exactly one shard; a district-wide query is a scatter-gather.
SHARD_MAP: Dict[str, List[str]] = {
    "coastal": ["Bago Aplaya", "Bucana", "Matina Aplaya", "Talomo", "Dumoy",
                "Matina Crossing", "Catalunan Pequeño"],
    "north": ["Ma-a", "Langub", "Magtuod", "Matina Pangi"],
    "upland": ["Catalunan Grande", "Bago Gallera", "Baliok"],
}
BARANGAY_TO_SHARD = {b: s for s, bs in SHARD_MAP.items() for b in bs}

REPLICATION_FACTOR = 3
WRITE_QUORUM = 2   # W
READ_QUORUM = 2    # R   (W + R > N  ->  quorum reads see the latest committed write)

# Leaders are spread across AZs so one AZ outage never takes out every leader.
_INITIAL_LEADER_AZ = {"coastal": AZ_A, "north": AZ_B, "upland": AZ_C}


def _build() -> Dict[str, NodeSpec]:
    nodes = [
        NodeSpec("gateway", "gateway", 8000, AZ_A, "public-subnet"),
        NodeSpec("collector", "collector", 8300, AZ_B, "private-subnet"),
        NodeSpec("cho-vault", "vault", 8200, PRIVATE, "cho-vpc"),
    ]
    port = 8101
    for shard in SHARD_MAP:
        for az in (AZ_A, AZ_B, AZ_C):
            nodes.append(NodeSpec(
                node_id=f"{shard}-{az[-1]}", role="data", port=port, zone=az,
                subnet="private-subnet", shard=shard,
                initial_leader=(_INITIAL_LEADER_AZ[shard] == az)))
            port += 1
    return {n.node_id: n for n in nodes}


NODES: Dict[str, NodeSpec] = _build()


def node(node_id: str) -> NodeSpec:
    return NODES[node_id]


def data_nodes() -> List[NodeSpec]:
    return [n for n in NODES.values() if n.role == "data"]


def shard_members(shard: str) -> List[NodeSpec]:
    return [n for n in data_nodes() if n.shard == shard]


def initial_leader(shard: str) -> NodeSpec:
    return next(n for n in shard_members(shard) if n.initial_leader)


def shard_for_barangay(barangay: str) -> str:
    try:
        return BARANGAY_TO_SHARD[barangay]
    except KeyError:
        raise KeyError(f"Barangay '{barangay}' is not in the shard directory")


def zone_of(node_id: str) -> str:
    spec = NODES.get(node_id)
    return spec.zone if spec else AZ_A


def link_latency_ms(src_zone: str, dst_zone: str) -> float:
    if PRIVATE in (src_zone, dst_zone) and src_zone != dst_zone:
        return LATENCY_MS["vpn"]
    return LATENCY_MS["same_az"] if src_zone == dst_zone else LATENCY_MS["cross_az"]
