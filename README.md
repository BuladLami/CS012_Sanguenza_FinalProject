# POLARIS Cloud: CS012 Final Project

A simulation of **POLARIS** (*Priority-Optimized Location and Risk-Adaptive Infrastructure Siting*)
redesigned as a **cloud-based distributed system**. POLARIS is a GIS decision-support system that ranks
candidate health-facility sites in Talomo District, Davao City, and treats hazard risk as part of every score.

The simulation runs 13 cooperating processes on localhost:

* an API gateway and coordinator
* nine data/compute nodes: three geographic shards, three replicas each, spread over three availability zones
* a CHO private-cloud vault, reached over a simulated VPN
* an observability collector

The simulation uses the thesis's real data: 1,502 candidate sites and their extracted features. Every
process emits structured events and spans, so each request can be followed across nodes in the logs.

**Design document: [`ARCHITECTURE.md`](ARCHITECTURE.md).** It covers network topology, the CAP theorem,
sharding and replication, cloud components and the deployment model, and observability, tracing and resilience.

Built on the CS012 scaffold ([ctrl-siege/CS012FinalScaffold](https://github.com/ctrl-siege/CS012FinalScaffold)).
The scaffold's layout (`coordinator/`, `node/`, `common/`, `run_cluster.py`) and its ideas are kept and
extended: trace-ID propagation, key sharding and a private-VPC node.

## Quick start

Requires Python 3.9 or newer.

```bash
python -m venv venv
venv\Scripts\activate            # Windows   (macOS/Linux: source venv/bin/activate)
pip install -r requirements.txt
```

**One command (recommended):** this starts the cluster, runs all 11 scenarios, prints traces, and stops the cluster.

```bash
python simulate.py
```

**Two terminals:** this way, every node's live event log streams in terminal 1.

```bash
python run_cluster.py            # terminal 1: cluster + live logs of all 13 processes
python simulate.py               # terminal 2: the scenarios
```

Useful options:

```bash
python simulate.py --only 5 7    # scenarios 1-3 (setup) always run first
python simulate.py --keep        # leave the cluster up, then explore http://127.0.0.1:8000/docs
python -m tools.trace_view --list
python -m tools.trace_view <trace_id>
python -m unittest discover -s tests -t .
```

A complete sample run is in [`docs/sample_simulation_output.txt`](docs/sample_simulation_output.txt).

## What the simulation shows

| # | Scenario | Distributed-systems concept |
|---|---|---|
| 1 | Bootstrap; 1,502 sites partitioned by barangay and written to 3 replicas per shard | topology, geographic sharding, quorum replication |
| 2 | Distributed scoring job (map, reduce, broadcast ×3), checked against the thesis notebook (\|Δ\| ≈ 1.7e-16, 275 gated sites) | parallel compute with data locality, global reduce |
| 3 | District top-N (scatter-gather), barangay query (one shard), eventual vs strong reads, edge-cache MISS→HIT | query routing, tunable consistency, CDN |
| 4 | A follower crashes; writes still commit with W = 2; the follower catches up from the log | fault tolerance, anti-entropy |
| 5 | Asymmetric partition: an **eventual read returns a site as eligible after it was re-classified to High flood**; a strong read returns the truth and repairs the replica | CAP per request, read repair |
| 6 | 2 of 3 replicas lost: writes and strong reads are refused (CP); eventual reads still served (AP) | CAP under partition |
| 7 | Leader partitioned: detection → majority election (epoch 2) → old leader fenced and caught up → re-score | HA, split-brain prevention |
| 8 | A crash before detection: replica failover, then the circuit breaker opens, half-opens and closes | resilience patterns |
| 9 | Whole shard down: district query degrades gracefully; batch job refuses to run | graceful degradation, CP analytics |
| 10 | CHO dossier stored in the private cloud over the VPN; 401 without role, 403 when bypassing the VPN, 503 fail-closed when the vault is down | hybrid cloud, zero trust, RA 10173 |
| 11 | Per-node metrics (p95), event inventory (about 290 events, 56 types), log locations | observability |

## Where the logs are

| Location | Contents |
|---|---|
| console of `run_cluster.py` (or `logs/cluster_console.log`) | one human-readable line per event, from every process |
| `logs/<node>.jsonl` | that node's events and spans as JSON lines |
| `logs/collector_all.jsonl` | cluster-wide stream gathered by the collector |
| `logs/simulation_summary.json` | scenario results |
| `http://127.0.0.1:8300/traces/{id}` | reconstructed span tree for one request |

Example log line:

```
05:22:41.989 WARN  [gateway   ] [trace=sys-3fa1 span=-] LEADER_ELECTED shard=north old_leader=north-b new_leader=north-a epoch=2 zone=az-a rule=highest log position among a live majority
```

## Repository layout

```
common/
  topology.py     nodes, zones, subnets, shard map, link-latency model
  telemetry.py    events, spans, metrics, async shipping to the collector
  net.py          traced inter-node client: latency, time-outs, retries, circuit breaker
  chaos.py        fault injection: crash / partition / peer_partition / slow / flaky
  scoring.py      thesis formula register F1-F22 (pure Python)
  models.py       request and record contracts
coordinator/      API gateway + control plane (failure detector, elections, quorum, jobs)
node/             shard replica: replication log, fencing, catch-up, shard-local scoring
vault/            CHO private-cloud vault with VPN check and audit trail
collector/        observability collector and trace reconstruction
tools/trace_view.py   text waterfall of a trace
data/talomo_site_features.csv   1,502 sites: extracted features + the notebook's baseline score
run_cluster.py    launches all 13 processes
simulate.py       scenario driver
tests/            unit tests for the formula library
```

## Main HTTP endpoints (gateway :8000)

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/v1/ingest` | sharded, replicated ingest |
| POST | `/api/v1/jobs/score` | distributed scoring job |
| GET | `/api/v1/recommend?top=&barangay=&consistency=` | top-N eligible sites |
| GET | `/api/v1/sites/{id}?consistency=eventual\|strong` | site lookup |
| PATCH | `/api/v1/sites/{id}/hazard` | hazard re-classification (quorum write, gates re-evaluated) |
| GET | `/api/v1/export?consistency=` | web-map dataset (edge cached) |
| POST/GET | `/api/v1/private/dossiers` | CHO-only, private cloud via VPN (`X-Role: cho-planner`, `X-Api-Key: cho-demo-key`) |
| GET | `/cluster/status`, `/cluster/metrics`, `/cluster/health` | topology, membership, lag, breakers, metrics |
| POST | `/admin/chaos/{node_id}` | fault injection |

## Data note

`data/talomo_site_features.csv` comes from the thesis pipeline (`POLARIS_model.ipynb`, OSM snapshot
2026-07-29, Project NOAH layers). The simulation scores sites with the deterministic baseline S′, which is
the Random Forest's training target. That is why it reports 40 Highly Suitable sites; the RF-published
figure is 37. See `ARCHITECTURE.md` §0.
