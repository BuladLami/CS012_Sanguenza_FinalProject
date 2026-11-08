"""Launch the simulated POLARIS hybrid-cloud cluster (13 processes) on localhost.

    python run_cluster.py            # foreground; all node logs stream to this console
    python simulate.py               # in a second terminal (or run it alone: it
                                     # starts and stops the cluster by itself)
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import IO, List, Optional

import httpx

from common import topology

ROOT = Path(__file__).resolve().parent
LOG_DIR = ROOT / "logs"

APP_FOR_ROLE = {"collector": "collector.main:app", "vault": "vault.main:app",
                "data": "node.main:app", "gateway": "coordinator.main:app"}
START_ORDER = ["collector", "vault", "data", "gateway"]


def reset_logs() -> None:
    LOG_DIR.mkdir(exist_ok=True)
    for f in LOG_DIR.glob("*.jsonl"):
        f.unlink()


def start_cluster(console: Optional[IO] = None) -> List[subprocess.Popen]:
    """Start every node. If `console` is given, node output goes there instead of stdout."""
    reset_logs()
    procs = []
    for role in START_ORDER:
        for spec in [n for n in topology.NODES.values() if n.role == role]:
            env = {**os.environ, "NODE_ID": spec.node_id, "PORT": str(spec.port),
                   "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}
            cmd = [sys.executable, "-m", "uvicorn", APP_FOR_ROLE[role], "--host", topology.HOST,
                   "--port", str(spec.port), "--log-level", "warning"]
            print(f"[cluster] start {spec.node_id:<10} {role:<9} {spec.zone:<11} "
                  f"{spec.subnet:<14} {spec.url}"
                  + (f"  shard={spec.shard}{' (leader)' if spec.initial_leader else ''}"
                     if spec.shard else ""))
            procs.append(subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=console or None,
                                          stderr=subprocess.STDOUT if console else None))
    return procs


def wait_ready(timeout_s: float = 40.0) -> bool:
    """Ready when the gateway's failure detector has seen every node join."""
    deadline = time.time() + timeout_s
    url = topology.node("gateway").url + "/cluster/health"
    while time.time() < deadline:
        try:
            members = httpx.get(url, timeout=1.0).json()["members"]
            if all(s == "UP" for s in members.values()):
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def stop_cluster(procs: List[subprocess.Popen]) -> None:
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def main() -> None:
    print("=" * 78)
    print(" POLARIS Cloud - simulated hybrid-cloud distributed cluster")
    print("=" * 78)
    procs = start_cluster()
    ok = wait_ready()
    print(f"\n[cluster] {'ONLINE' if ok else 'NOT READY (check output above)'}"
          f" - gateway {topology.node('gateway').url}  (Swagger UI: /docs)")
    print("[cluster] collector traces: "
          f"{topology.node('collector').url}/traces   logs: {LOG_DIR}")
    print("[cluster] Ctrl+C to stop.\n")
    try:
        while all(p.poll() is None for p in procs):
            time.sleep(1)
        print("[cluster] a node process exited; stopping.")
    except KeyboardInterrupt:
        print("\n[cluster] shutting down...")
    finally:
        stop_cluster(procs)
        print("[cluster] stopped.")


if __name__ == "__main__":
    main()
