# GameSense

A distributed, real-time game telemetry platform. Simulated game servers in three regions stream player telemetry (movement, aim, shots, match results) through a stateless ingest tier into a replicated log. Horizontally scaled workers detect cheaters in real time, and an independent auditor checks the whole pipeline end to end: **zero lost events, duplicates accounted for, and detector precision and recall measured against ground truth.**

The traffic is realistic, not a flat load test. Each region has an evening peak in its own time zone, weekends run ~35% hotter, and a scripted week includes a global patch drop, a regional outage followed by a reconnect storm, and a tournament final.

```
 simulator (55 game servers, 3 regions, daily/weekly curve + incidents)
      │  gzip JSON batches, retries w/ backoff + jitter, honors Retry-After
      ▼
   nginx LB ──▶ ingest ×2  (stateless: validate → rate-limit → shed load → produce, ack after acks=all)
                   │   key = match_id  → all events of a match on one partition, in order
                   ▼
   Redpanda ×3  topic telemetry.raw (24 partitions, RF=3, Raft majority acks)
      │                                   │
      ▼                                   ▼
 detector workers ×N (consumer group)   auditor (own group)
  per-player state, aim-snap &           • acked-vs-seen per (server, seq) → lost events
  speed-hack rules → detections topic    • duplicates in the raw log
      │                                  • precision/recall vs simulator ground truth
      ▼
   topic detections (deterministic IDs → idempotent downstream)

 Prometheus scrapes everything ──▶ Grafana dashboard  •  Redpanda Console for topics & lag
```

**→ Step-by-step plan from first run to finished project: [docs/PLAN.md](docs/PLAN.md)**

## Start here: Phase 1 (ingest alone, no Docker needed)

```bash
make p1-ingest     # terminal 1: Go ingest API, discarding batches
make p1-sim L=4    # terminal 2: simulator at 4x load
make stats         # terminal 3: live throughput, latency, CPU, memory
```
On a 2-core VM this measured ~88K events/s through ingest with 0 rejections. See [docs/PLAN.md](docs/PLAN.md) for Phases 2–3 (add the queue, then workers).

## Quickstart (full pipeline)

Requirements: Go 1.24+, Docker with Compose v2, and about 6 GB of free RAM.

```bash
make deps      # fetch franz-go (Kafka client) and write go.sum — run once
make test      # unit + end-to-end tests with an in-memory broker (no Docker)
make up        # build and start the full stack
make check     # PASS/FAIL health report (lost events, throughput, latency, lag, DLQ)
```

Chaos game days: `make chaos-broker`, `chaos-workers`, `chaos-ingest`, `chaos-delay`, `chaos-poison`. Each runs before/during/after health checks and logs to `docs/gamedays/` (see `docs/postmortems/TEMPLATE.md`).

| What | Where |
|---|---|
| Grafana dashboard | http://localhost:3000 |
| Redpanda Console (topics, consumer lag) | http://localhost:8081 |
| Prometheus | http://localhost:9090 |
| Ingest endpoint | `POST http://localhost:8080/v1/batch` |
| Simulator stats (acked seq per server) | http://localhost:9101/stats and :9111/stats (2 shards) |

By default the simulator runs **one simulated day per real hour** (`SIM_SCALE=24`), so a full scripted week takes about 7 hours. To change load or speed:

```bash
make load L=5 S=24      # 5x players, 1 day/hour
make load L=1 S=1       # real time, for multi-day soak tests
docker compose -f deploy/docker-compose.yml up -d --scale worker=6   # scale workers live
```

## Billion-event runs

Total events = average rate (~17k events/s × `SIM_LOAD`) × real seconds. The traffic shape is identical in every mode; `SIM_SCALE` only changes how fast the simulated week passes, not the request rate.

| Command | What it simulates | Real duration | Avg / peak events/s | Total events | HTTP requests |
|---|---|---|---|---|---|
| `make billion` | 1 week at 3× load | ~7 h | ~51k / ~145k | **~1.3 billion** | ~10M |
| `make soak` | 1 week at 2× load | ~24 h | ~34k / ~97k | **~3 billion** | ~35M |
| `make soak-week` | 1 week, real time | 7 days | ~17k / ~48k | **~10 billion** | ~250M |

Request counts are approximate, because they depend on batch flush timing. Before a long run:

- **Disk.** Raw-topic retention defaults to 30 minutes (`RAW_RETENTION_MS`). At roughly 50 bytes per event after compression × 3 replicas, 30 minutes at 50k events/s is about 14 GB. The 50-byte figure is an estimate, so check broker disk usage in Console early in the run.
- **Auditor lag.** A single auditor decodes every event. It only checks a snapshot for loss once it has read past that snapshot on every partition (see `audit_watermark_lag_seconds`), so falling behind delays the verdict instead of producing false "lost" counts. The auditor must stay within the retention window, though, or unread events expire.
- **Finding the ceiling.** If the dashboard's *generated* and *ingested* lines separate, or p99 climbs, you've found your laptop's first bottleneck. Profile it, fix it, and write it up.

## Services

| Service | Role |
|---|---|
| `cmd/simulator` | Player population model → game servers → batched telemetry. Supports sharding (`-shard/-shards`) to run several load generators. |
| `cmd/ingest` | Stateless HTTP front door: size limits, validation, per-server token buckets, in-flight cap (429 + Retry-After), acks only after the broker confirms. Graceful drain on SIGTERM. |
| `cmd/worker` | Consumer-group worker. `-job detector` runs streaming anti-cheat rules over per-player state. |
| `cmd/auditor` | Independent verifier: lost events, duplicates, detector precision/recall. |

## Guarantees and design decisions

**No acknowledged event is ever lost.**
- Ingest returns `200` only after `ProduceSync` with `acks=all`. On Redpanda that means a Raft majority (2 of 3 brokers) has fsynced the write. Brokers deliberately run *without* `--mode dev-container`, because dev mode disables fsync.
- Clients never drop a batch. They retry with exponential backoff and full jitter, honoring `Retry-After`. Under pressure, game servers buffer locally and slow down (visible on the dashboard as *client-side buffered events*).
- The auditor proves it. Each game server numbers its events 1, 2, 3, … and sends batches sequentially, so the simulator's `acked_through` means *every* seq up to that number was acknowledged. The auditor reads the raw topic and reports any acknowledged seq it never saw. It checks a snapshot only after its own read watermark (the slowest partition) has passed it, so an auditor that falls behind can't report false losses.

**Processing is at-least-once, and the results come out exactly once.**
- Workers follow poll → process → **produce outputs → commit offsets**. A crash anywhere replays from the last commit, so data is reprocessed and never lost.
- Replays are harmless. The detector ignores events whose timestamp isn't newer than the player's last one, and detections carry deterministic IDs (`rule:match:player`), so downstream consumers can upsert.
- The consumer uses `BlockRebalanceOnPoll`, so a partition is never revoked between processing and commit, and the revoke hook never commits work that wasn't done.
- `internal/e2e` crashes a worker mid-stream and shows a fresh worker finishes with zero loss and no false positives.

**Partitioning by match ID** puts all of a match's events on one partition, in order. A single worker owns that match's state with no locks or coordination. Ingest also groups each batch into one record per match, which cuts per-record overhead by about 5×. The trade-off is skew: one partition gets hotter if a few matches dominate. Matches are small and numerous here, so the trade-off pays off.

**Overload is handled explicitly instead of failing slowly.**
- A full in-flight semaphore → `429 overloaded`.
- One noisy server over its token bucket → `429 rate_limited`, and other servers are unaffected (there's a test for this).
- Brokers unreachable → `503` with `Retry-After`, and clients retry safely.

**Ground truth for detection quality.** Whether a player cheats is a deterministic hash of `(seed, player_id)`, so the auditor computes precision and recall without the answer ever travelling inside the telemetry.

**Debugging story.** The first end-to-end run flagged an innocent player moving at 5,640 m/s. The detector was right: the simulator occasionally put the same player ID in a match twice, and two bodies sharing one ID look like teleporting. Fixed in `sim/world.go`.

## Observability

The Grafana dashboard is provisioned automatically. The key panels:

- **The daily wave:** concurrent players per region over simulated time
- **Throughput:** generated vs ingested vs processed. These lines should overlap; a gap means backpressure or lag.
- **Ingest p50/p99**, broker-ack p99, and **end-to-end p50/p99** (event generated → processed)
- **Consumer lag**, backpressure (429/503 by reason), and client-side buffering
- **Lost events (must be 0)**, ingest availability against a 99.9% SLO, and detection precision/recall

Metrics use a small dependency-free Prometheus exporter (`internal/metrics`), and logs are structured JSON.

## Baseline numbers

| Measurement | Result | How |
|---|---|---|
| Ingest CPU throughput (broker stubbed), 2 cores | ~590k events/s | `make bench` |
| **Phase 1, ingest only (discard), i5-9400F 6 cores, simulator on the same PC** | **325K events/s avg, 378K peak, 0 rejected, 19 MB heap, p99 ~235 ms at 16x** | `make p1-ingest` + `make p1-sim L=16` + `make stats` |
| E2E test: 120k events, worker killed mid-stream | 0 lost, 0 false positives | `go test ./internal/e2e` |
| Full stack on a laptop | *fill in:* events/s, p99, lost = 0 | `make up` and the Grafana dashboard |

## Roadmap

- [x] **M1: core pipeline.** Simulator with daily/weekly curve and incidents, ingest with backpressure, 3-broker Redpanda, detector workers, auditor, dashboards, e2e crash test
- [x] **Dead-letter topic:** poison records are parked in `telemetry.dlq` instead of crash-looping workers
- [x] **Health checker** (`make check`) and **chaos scripts** (`make chaos-*`)
- [ ] **M2: more workers.** Per-minute rollups to ClickHouse, MMR/rank updates from `match_end` (ordered and exactly once per player), session report cards (session windows)
- [ ] **M3: chaos game days.** Scripts that kill a broker, a worker or an ingest replica at peak, partition the network (`tc netem`), and slow the disk. Each run gets a written postmortem in `docs/postmortems/`
- [ ] **M4: performance.** Profile with pprof, switch the wire format to protobuf, measure before/after, and hit a published events/s target with flame graphs
- [ ] **M5: Kubernetes.** Helm charts, HPA on ingest CPU, KEDA autoscaling workers on consumer lag, PodDisruptionBudgets, and graceful rollouts
- [ ] **M6: production practices.** SLOs with burn-rate alerts, canary deploys with automatic rollback (Argo Rollouts), CI load-test gate that fails on p99 regression, OpenTelemetry tracing
- [ ] **M7: long soak.** A 72-hour real-time run, reporting slow leaks, retention and compaction behavior, and cost per million events

## Hosting on free plans

The pipeline is made of long-running processes (brokers, consumers, a load generator), so serverless and sleep-on-idle platforms can't run its core.

| Option | Fit |
|---|---|
| **Your own laptop/desktop** | Best and free. All throughput and billion-event numbers should come from here, or from a paid cloud box for a few hours. |
| **Oracle Cloud Always Free (Arm VM)** | Runs the whole Compose stack at reduced load as an always-on public demo. The allowance was halved in June 2026 to about 2 Arm cores and 12 GB RAM, which is enough for `SIM_LOAD=0.5`–`1`. |
| **Vercel (Hobby)** | Only a static landing page or project site. Functions are request-scoped with a max duration, so they can't host brokers or consumers. |
| **Render (free)** | Free web services spin down after 15 minutes idle, and background workers have no free tier. Not suitable. |
| **Aiven free Kafka** | Real managed Kafka, but capped at roughly 125–250 KB/s and 2 partitions per topic: thousands of events/s, not billions. |
| **Grafana Cloud free** | Optional: remote-write metrics from the demo box for a public dashboard. |

## Layout

```
cmd/{simulator,ingest,worker,auditor}   service entrypoints
internal/sim        traffic curve, game world, HTTP sender, per-server run loop
internal/ingest     HTTP ingest server
internal/detect     anti-cheat rules
internal/worker     consume → process → produce → commit loop
internal/jobs       handlers wiring domain logic into workers
internal/audit      loss/duplicate tracker, precision/recall scorer
internal/broker     broker interface, Kafka adapter (franz-go), in-memory log for tests
internal/metrics    tiny Prometheus exporter
internal/e2e        whole-pipeline test with a worker crash
internal/svc        logging, signals, metrics + pprof endpoints
internal/health     health checks behind `make check`
scripts/chaos/      chaos game-day scripts
deploy/             docker-compose, nginx, Prometheus, Grafana
docs/PLAN.md        phase-by-phase project plan
```
