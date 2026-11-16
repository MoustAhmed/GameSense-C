# GameSense: project plan

The path from unzipping the code to a finished, resume-ready project. Each phase has a **done when** checkpoint, and you shouldn't move on until it's true. Time estimates assume part-time work alongside school.

| Phase | What | Time | Status |
|---|---|---|---|
| 0 | Set up your machine and repo | 30 min | |
| 1 | **Ingest alone:** how fast is the Go API? | 1–2 h | |
| 2 | **Add the queue:** what does durability cost? | 1–2 h | |
| 3 | **Add workers:** the full pipeline | 1–2 h | |
| 4 | Baseline numbers and your first bottleneck | 1–3 days | |
| 5 | First billion-event run | 1 day (mostly waiting) | |
| 6 | More workers: rollups, MMR, dead-letter topic (M2) | 1–2 weeks | |
| 7 | Chaos game days and postmortems (M3) | 1 week | |
| 8 | Performance: profiling, protobuf (M4) | 1 week | |
| 9 | Kubernetes: Helm, HPA, KEDA (M5) | 1–2 weeks | |
| 10 | Production practices: CI/CD, SLOs, canaries (M6) | 1–2 weeks | |
| 11 | Long soak and a public demo box (M7) | 1 week + runtime | |
| 12 | Write-up: README results, demo video, resume | 2–3 days | |

Phases 1–3 bring the system up one layer at a time, measuring each layer, so you know what every component costs. You can stop after any phase from 5 on and still have a strong project. Each later phase adds a new interview story.

---

## Phase 0: Setup (30 min)

1. Install:
   - **Go 1.24+** (go.dev/dl)
   - **Docker Desktop**. In Settings → Resources, give it **at least 4 CPUs and 8 GB RAM**. On Windows, use **WSL2** and run everything inside the Linux shell.
   - **git** and **make**. make comes with macOS Xcode tools and Linux; on WSL, `sudo apt install make`.
2. Unzip the project and put it under version control:
   ```bash
   cd gamesense
   git init && git add -A && git commit -m "GameSense M1"
   ```
3. Create an empty GitHub repo named `gamesense` and push to it. Commit early and often from here on, because recruiters do look at commit history.

**Done when** `go version` shows 1.24 or later, `docker compose version` works, and the code is on GitHub.

---

## Phase 1: Ingest alone. How fast is the Go API? (1–2 h)

The simulator sends real batches to the Go ingest API, which decodes, validates, rate-limits and acknowledges them, then **discards** them (`-broker discard`). No queue yet, so this measures the ingest API by itself. It's the upper bound for everything after it. It needs no `make deps` and no Docker.

**Option A, no Docker (fastest).** Use 3 terminals in the project folder:
```bash
make p1-ingest          # terminal 1: ingest on :8080, discarding batches
make p1-sim             # terminal 2: simulator at 1x load (add L=4 for 4x)
make stats              # terminal 3: live stats table; Ctrl-C prints a summary
```

**Option B, Docker + Grafana dashboard:**
```bash
make phase1             # ingest + simulator + Prometheus + Grafana
make stats              # same terminal stats
make phase1-load L=4    # change load
make phase1-down        # stop
```
Then open http://localhost:3000: the *GameSense - Phase 1* dashboard loads automatically.

### What the stats columns mean
| Column | Meaning |
|---|---|
| `gen ev/s` | events the simulator generated per second |
| `ingest/s` | events ingest accepted per second; should match `gen ev/s` |
| `req/s` | HTTP requests per second (batches) |
| `rej/s`, `retry/s` | 429/503 rejections and client retries; 0 means ingest is keeping up |
| `p50`, `p99` | ingest request latency |
| `ing CPU` | ingest CPU; 100% = one full core |
| `ing heap` | ingest memory in use; should stay flat |
| `pending` | events buffered in the simulator, waiting to be sent |
| `up Mbps` | bandwidth used by the simulator, a preview of your home upload needs |

### The load ladder
Run each level for ~1 minute and write down the summary:
```bash
make p1-sim L=1    # then Ctrl-C and restart with:
make p1-sim L=2
make p1-sim L=4
make p1-sim L=8
make p1-sim L=16
```
The ceiling is the first level where **`ingest/s` falls below `gen ev/s`**, `rej/s` climbs, or p99 jumps. Note whether ingest or the simulator ran out of CPU first. They share your machine in this setup.

### Reference numbers (measured while building this, on a 2-core cloud VM)
| Load | Events/s | Req/s | Events/req | p50 / p99 | Ingest CPU | Ingest heap | Rejected |
|---|---|---|---|---|---|---|---|
| 1× | ~22K | ~305 | 72 | 0.7 / ~15 ms | 25% | 2–3 MB | 0 |
| 4× | ~88K | ~326 | 270 | 4 / ~90 ms | 76% | 2–4 MB | 0 |

Upload was ~31 Mbps at 88K events/s (about 45 bytes per event, gzipped JSON).

**First real run (i5-9400F, 6 cores, Windows 11, simulator on the same PC):**
| Load | Avg / peak events/s | Req/s | Events/req | p50 / p99 | Heap | Rejected |
|---|---|---|---|---|---|---|
| 1× | 21.7K / 22.5K | 326 | 67 | 1.3 / ~10 ms | 3 MB | 0 |
| 2× | 43.1K / 44.9K | 328 | 131 | 2.3 / ~23 ms | 3 MB | 0 |
| 4× | 86.2K / 89.4K | 317 | 272 | 5.5 / ~47 ms | 9 MB | 0 |
| 8× | 172K / 194K | 431 | 399 | 9.5 / 50–100 ms | 9 MB | 0 |
| 16× | **325K / 378K** | 707 | 459 | 11 / ~235 ms | 19 MB | 0 |

No ceiling found at 16×. Next: rerun with CPU numbers (fixed for Windows), then 32× and 64×.

### Third run: ceiling and overload
Same PC (i5-9400F, 6 cores, Windows 11), with the simulator and ingest sharing the machine. `-server-scale` adds game servers so the simulator isn't the limit.

- **32/8 (440 servers):** ~448K events/s avg, 497K peak, p99 ~2.4 s, ingest CPU ~342% (peak 382%), sim CPU ~176%, heap 93 MB, 49 transient rejections (retried). This is the practical ceiling with the simulator and ingest sharing 6 cores.
- **5-minute stability run, load 9 / server-scale 3 / time-scale 1 (165 servers):** 198K events/s avg (196–200K), p50 ~3 ms, p99 82–95 ms, ingest CPU ~200%, heap 4–14 MB with no growth, 0 rejected, ~71 Mbps.
- **64/16 and 128/32: ~3× and ~6× overload.** Ingest stayed busy (250–330% CPU) but couldn't accept connections fast enough. ESTABLISHED connections stalled well short of one per server: ~110–200 in some runs (about Windows' accept backlog) and ~570–590 in others, never the 880 the simulator wanted. Connections piled up in CLOSE_WAIT after client timeouts, and new connections were refused, including loadstat's and `/healthz`. Raising the client's connection pool limit (512 → 4096 per host) did not help (hypothesis disproved). No data loss: unacknowledged batches stay with the client and are retried.
- **"localhost" isn't the cause:** pointing the simulator at `127.0.0.1` (64/16) gave the same picture: 573 ESTABLISHED, ~170–180 CLOSE_WAIT, 1 TIME_WAIT, ~11 network retries/s, and `/healthz` refused or timed out. The simulator ran 830 OS threads at 20 s and 1,653 at 40 s (vs 800–940 with `localhost`), while ingest stayed at 15. The extra simulator threads come from somewhere other than name resolution (still unexplained).
- **Lesson:** in-app backpressure (429s) only works while the server has spare capacity to reply; far beyond capacity, overload shows up at the network layer.

### Try this too
- **Batch size:** `make p1-sim L=4 BATCH=250` vs `BATCH=1000`. Watch req/s, CPU and p99.
- **Small-machine test:** in Option B, uncomment the `deploy.resources` limits in `deploy/docker-compose.phase1.yml` (1 CPU, 512 MB, about a free-tier server), rerun the ladder, and confirm memory stays flat and overload produces 429s, not crashes.
- **Profile ingest at the ceiling:** `go tool pprof -http=:7000 "http://localhost:8080/debug/pprof/profile?seconds=20"`

**Done when** you have a load-ladder table and can say "the Go ingest API alone handles **X events/sec** on my machine at **Y%** CPU and **Z MB** of memory."

---

## Phase 2: Add the queue. What does durability cost? (1–2 h)

Same traffic, but ingest now waits until each batch is replicated to a majority of the 3 Redpanda brokers before acknowledging (acks=all), and the auditor verifies nothing is lost.

```bash
make deps          # once: downloads the Kafka client
make test
make up            # full stack
# For a fair comparison with Phase 1, use a single ingest replica and no workers:
docker compose -f deploy/docker-compose.yml up -d --scale ingest=1 --scale worker=0
docker compose -f deploy/docker-compose.yml restart lb
make stats SIM=http://localhost:9101/metrics,http://localhost:9111/metrics
```

Compare against Phase 1 at the same load levels:
- **Throughput and p99:** expect lower throughput and higher latency. Waiting for replicated, durable writes costs time.
- **`make check` / Grafana:** *Lost events* must be **0**.
- Look at `ingest_produce_duration_seconds` in Grafana: that's the time spent waiting for broker acknowledgments.

**Done when** you can say "durable replicated writes cost X% throughput and Y ms of p99, in exchange for zero data loss," a strong interview point.

### Phase 2 results (2026-11-16)
**Headline (NVMe run, below):** on the NVMe SSD, durable replicated writes (fsync on 2 of 3 brokers before each ack) cost **nothing in throughput at 1×** (21.7K ev/s, the same as Phase 1) and **+37 ms p50 / +171 ms p99**. At 9× they cost 29–47% of throughput (−29% with 3× the client concurrency), adding ~130–300 ms p50 and ~0.4–0.6 s p99, with the 4 CPUs ~90% busy. **Lost = 0** at every level and in all six replica-removal tests.

#### Run A: Docker disk on a hard disk (D:)
Setup: i5-9400F, Windows 11, Docker Desktop (WSL2) with 4 CPUs / 7.8 GB, Docker's disk image on **D: (a 7200 rpm SATA hard disk, WD10EZEX)**. 3 Redpanda v24.2.7 brokers, `telemetry.raw` with 24 partitions × RF 3, `RAW_RETENTION_MS=600000`, 1 ingest replica, 0 workers, 55 game servers in 2 simulator shards, `SIM_SCALE=1`. Each level: simulators recreated, 30 s warm-up, then `loadstat` for 120 s (`bin/p2-stats-<L>.txt`). Phase 1 Docker baseline: `bin/p1docker-stats-<L>.txt`, same 4 CPUs and 55 servers.

**First, the brokers weren't durable.** The Redpanda image ships `developer_mode: true`, so `rpk redpanda start` added `--unsafe-bypass-fsync=true` and enabled write caching (`write.caching=true`), despite the compose comment saying dev mode was avoided. Fixed in `deploy/docker-compose.yml`:
- each broker's entrypoint runs `rpk redpanda mode production` before starting (rpk's `--set redpanda.developer_mode=false` is applied too late in v24.2.7 and the bypass stayed on);
- init-topics sets `write_caching_default=false` and `write.caching=false` on every topic.

Verified: the broker start lines show no `--unsafe-bypass-fsync`; the cluster's `write_caching_default` is `false`; all three topics show `write.caching false`.

| Load | Run | Avg ev/s | Gen ev/s | Req/s | Ev/req | p50 | p99 | Broker-ack p99 | Ingest CPU (avg / peak) | Rejected |
|---|---|---|---|---|---|---|---|---|---|---|
| 1× | P1 Docker | 21,742 | 21,738 | 325 | 67 | 0.8 ms | 13 ms | n/a | 27% / 27% | 0 |
| 1× | P2 durable | 11,005 | 10,910 | 30 | 369 | ~1.75 s\* | ~2.5 s\* | 2.48 s | 10% / 11% | 0 |
| 4× | P1 Docker | 87,496 | 87,484 | 319 | 274 | 5.9 ms | 50 ms | n/a | 83% / 85% | 0 |
| 4× | P2 durable | 14,197 | 14,657 | 28 | 500 | ~1.75 s\* | ~2.5 s (max 4.7 s)\* | 4.24 s | 14% / 16% | 0 |
| 9× | P1 Docker | 196,739 | 196,686 | 475 | 414 | 12.1 ms | 96 ms | n/a | 174% / 181% | 0 |
| 9× | P2 durable | 15,291 | 15,457 | 31 | 500 | ~1.75 s\* | ~2.5 s (max 4.7 s)\* | 2.94 s | 14% / 17% | 0 |

p50/p99 are medians of the 10 s rows. Broker-ack p99 = `histogram_quantile(0.99, …ingest_produce_duration_seconds…[1m])` at the end of each run. \*Nearly every produce landed in the 1–2.5 s histogram bucket, so p50 and p99 are interpolated inside that bucket. They really mean "between 1 and 2.5 s".

**Cost of durability on this machine:** throughput −49% (1×), −84% (4×), −92% (9×); p50 +~1.7 s, p99 +~2.4 s. **Auditor: lost = 0 at all three levels** (it checked a snapshot taken after each run ended).

**Why, and why the comparison isn't fair yet:** the cluster is limited by fsync on the hard disk, not CPU.
- **fsync cost:** a 4 KB write + fsync on Docker's disk takes ~41 ms (100 in 4.1 s, against 0.09 s without fsync). Three brokers fsyncing replicas of 24 partitions share that one spindle.
- **Brokers agree:** their own request histogram shows the produces taking 0.5–2.1 s.
- **Fixed in-flight cap:** each simulated game server sends batches one at a time and waits for each ack, so at most 55 requests are in flight. At ~1.75 s per ack that caps the rig at ~30 req/s × ≤500 events ≈ 15K events/s, whatever the load setting. The simulators fell behind (`gen ev/s` far below target) and ingest never rejected anything. So at 4× and 9× the numbers measure the ack latency, not ingest or broker capacity.
- **Little CPU used:** total container CPU was about 45% (1×), 47% (4×) and 61% (9×) of the 400% available.

| Container CPU (docker stats) | 1× | 4× | 9× |
|---|---|---|---|
| ingest | 14.4% | 10.8% | 6.3% |
| redpanda-0 / 1 / 2 | 4.0 / 4.2 / 3.9% | 3.5 / 4.0 / 4.1% | 12.1 / 12.3 / 11.9% |
| simulator-0 / 1 | 1.3 / 5.8% | 7.8 / 6.1% | 1.3 / 1.3% |
| auditor | 9.3% | 9.9% | 14.9% |
| everything (incl. lb, prometheus, grafana, console) | ~45% of 400% | ~47% | ~61% |

Broker memory grew 428 → 613 → 800 MB (1 GB cap each). D: free space stayed flat (871.7 → 871.3 GB) with 10-minute retention.

**The earlier "lost = 153" (dev-mode run) didn't reproduce.**
- **Original:** in the first, non-durable attempt the auditor reported 153 lost events. The loss appeared in snapshots taken right after a compose scale-down from 2 ingest replicas to 1 plus an `lb` restart. At the same moment (14:03:37.7–39.6) the brokers logged idempotent-producer errors (`producer_state … errc:25`) on 13 `telemetry.raw` partitions for producer id=1, whose epoch was bumped from 2 to 8 within 2 s.
- **Retest:** with durable brokers at 4×, I stopped one of 2 replicas twice with `docker stop` (container kept). Both times lost = 0, and the brokers logged no `producer_state` errors.
- **Shutdown log both times:** `draining` → `stopped` in ~6.6 s.
- **Retries:** ~36 nginx connect timeouts per stop (504s to the simulator, retried); no rejects.
- **Not the same conditions:** the retest ran at ~30 req/s against ~300 req/s originally, and without the lb restart. The original run's missing-range detail wasn't available then; the auditor now logs it (`audit missing`).

#### Run B: Docker disk on the NVMe SSD (E:, Crucial P3)
Same setup and procedure, with Docker's disk image moved to `E:\DockerData` and a fresh cluster (`down -v`). Brokers start without `--unsafe-bypass-fsync`, and `telemetry.raw` has `write.caching=false`. Two code changes first:
- `metrics.LatencyBuckets` gained 0.075, 0.15, 0.35, 0.75, 1.5, 2, 3, 4 and 7.5 s;
- the simulators take `-server-scale ${SIM_SERVERS:-1}` in compose.

**fsync on E:** ~6–7 ms per 4 KB write + fsync, net of the no-fsync baseline (100 took 0.69–0.94 s against 0.11–0.24 s without fsync; the stack was running at the same time). On the D: hard disk it was ~41 ms. Files: `bin/p2nvme-stats-<L>[-s3].txt`.

| Load | Run | Avg ev/s | Gen ev/s | Req/s | Ev/req | p50 | p99 | Broker-ack p99 | Ingest CPU (avg / peak) | Rejected |
|---|---|---|---|---|---|---|---|---|---|---|
| 1× | P1 Docker | 21,742 | 21,738 | 325 | 67 | 0.8 ms | 13 ms | n/a | 27% / 27% | 0 |
| 1× | P2 HDD | 11,005 | 10,910 | 30 | 369 | ~1.75 s | ~2.5 s | 2.48 s | 10% / 11% | 0 |
| 1× | **P2 NVMe** | **21,729** | 21,742 | 337 | 65 | 38 ms | 184 ms | 169 ms | 39% / 44% | 0 |
| 4× | P1 Docker | 87,496 | 87,484 | 319 | 274 | 5.9 ms | 50 ms | n/a | 83% / 85% | 0 |
| 4× | P2 HDD | 14,197 | 14,657 | 28 | 500 | ~1.75 s | ~2.5 s | 4.24 s | 14% / 16% | 0 |
| 4× | **P2 NVMe** | **76,964** | 77,434 | 253 | 304 | 155 ms | 468 ms | 597 ms | 84% / 90% | 0 |
| 9× | P1 Docker | 196,739 | 196,686 | 475 | 414 | 12.1 ms | 96 ms | n/a | 174% / 181% | 0 |
| 9× | P2 HDD | 15,291 | 15,457 | 31 | 500 | ~1.75 s | ~2.5 s | 2.94 s | 14% / 17% | 0 |
| 9× | **P2 NVMe** | **104,927** | 104,861 | 267 | 394 | 139 ms | 461 ms | 558 ms | 100% / 106% | 0 |
| 9×, 3× servers | **P2 NVMe** | **140,111** | 140,157 | 377 | 372 | 306 ms | 707 ms | 641 ms | 127% / 135% | 0 |

**Cost of the queue on NVMe (against P1 Docker):**

| Load | Throughput | p50 | p99 |
|---|---|---|---|
| 1× | −0.1% | +37 ms | +171 ms |
| 4× | −12% | +149 ms | +418 ms |
| 9× | −47% | +126 ms | +365 ms |
| 9×, 3× servers | −29% | +294 ms | +610 ms |

**Auditor:** lost = 0 at every level.

| Container CPU (docker stats) | 1× | 4× | 9× | 9×, 3× servers |
|---|---|---|---|---|
| ingest | 48.6% | 96.4% | 104.4% | 114.8% |
| redpanda-0 / 1 / 2 | 38.4 / 36.1 / 40.1% | 39.0 / 38.6 / 43.1% | 36.8 / 37.6 / 36.2% | 31.7 / 31.7 / 29.9% |
| simulator-0 / 1 | 58.4 / 37.2% | 33.9 / 26.0% | 33.7 / 32.9% | 32.2 / 37.0% |
| auditor | 23.6% | 61.1% | 62.5% | 72.3% (582 MB) |
| lb | 10.2% | 17.1% | 10.2% | 11.9% |
| **total, all containers** | **~293% of 400%** | **~356%** | **~354%** | **~362%** |

**Reading it:**
- **1×:** durability costs only latency. The ~40 ms p50 is roughly one fsync on each of 2 replicas, plus Raft and batching.
- **4× and 9×:** the rig is closed-loop: each game server waits for its ack before sending the next batch. Higher ack latency therefore slows the simulators (`gen ev/s` falls below the target), and ingest never rejects anything. Tripling the servers raised 9× from 105K to 140K ev/s.
- **Why 9× is CPU-bound:** the whole box is ~90% busy (4 CPUs), with the auditor alone using 60–70%. The 9× numbers measure this 4-CPU box, not Redpanda's ceiling.
- **The auditor can't keep up above ~100K ev/s.** Its watermark lag grew past 2 minutes at 140K ev/s, so lost was only checked after pausing the simulators (see Run 3).

#### Run 3: reproducing the original 153-loss conditions
At 9×, 3× servers (~140K ev/s, 300–400 req/s): scale to 2 ingest replicas, `restart lb`, wait 60 s, check lost = 0. Then scale back to 1 (`up -d --scale ingest=1 --scale worker=0`) and `restart lb` within ~10 s. Wait 90 s, then `docker pause` the simulators so the lagging auditor can read to the end of the log, and check lost. That covers snapshots up to ~75 s after the event. The removed replica's log was streamed to a file. Evidence is in `gamesense2/evidence/run3/`.

| Attempt | Ingest code | lost | Missing ranges | Broker `producer_state` errors | nginx errors (new + old lb) | Sim retries | Ingest rejects |
|---|---|---|---|---|---|---|---|
| r3a | produce uses `r.Context()` | **0** | none | 0 | 12 conn refused | +622 network | 0 |
| r3b | `r.Context()` | **0** | none | 0 | 31 reset by peer, 7 premature close | +569 network, +38 × 502 | 0 |
| r3c | own context (change b) | **0** | none | 0 | 9 reset, 4 refused, 2 premature close, 1 broken pipe | +721 network, +12 × 502 | 0 |
| r3d | own context (change b) | **0** | none | 0 | 13 conn refused | +675 network | 0 |

- **Removed replica's shutdown log, every time:** `draining` → `stopped` in 5.2–5.6 s; the container was gone ~6 s after the scale-down started.
- **Duplicates:** they rose only right after each simulator unpause. Requests that were in flight at the pause timed out and were resent; workers dedupe them by `(server_id, seq)`.

**What caused the original 153?** It didn't reproduce in 6 attempts with durable brokers: 2 plain stops at 4× (HDD), and 4 scale-downs plus lb restarts at ~140K ev/s (NVMe), before and after change (b). None produced a single `producer_state` error. The one run that lost data differed in several ways:
- its brokers were in developer mode (fsync bypassed, write caching on);
- the cluster was 1–2 minutes old, and brokers were still logging `id_allocator` timeouts;
- `SIM_SCALE=24`;
- the 3 workers were removed at the same moment.

Its loss coincided with a 2-second storm of idempotent-producer errors (`errc:25`) across 13 partitions, with the producer epoch bumped from 2 to 8. The likeliest explanation is a broker/producer event in that non-durable, freshly started cluster, not ingest's shutdown path, nginx retries, or client cancellation, all of which were exercised here without loss. This is not proven, because missing ranges weren't logged then. The auditor now logs them (`audit missing`) if it happens again.

**Next:**
1. Make the auditor keep up (it decodes every raw record's full JSON in one goroutine, ~70% CPU): parallel decode per partition, or a cheaper path that reads only `server_id`/`seq`.
2. Phase 3 (workers), keeping the stack on the NVMe disk.
3. Optional: add the original conditions (dev mode, fresh cluster, `SIM_SCALE=24`, workers 3→0) as a chaos test, to try to pin down the 153.

---

## Phase 3: Add workers, the full pipeline (1–2 h)

Now everything runs: ingest, the 3-broker queue, anti-cheat workers, the auditor and dashboards. Compare against your Phase 1 and 2 numbers.

```bash
make deps     # downloads the Kafka client (franz-go), writes go.sum
make test     # unit + end-to-end tests, no Docker needed (~1 min)
make up       # builds the images, starts the full stack (~3–5 min the first time)
docker compose -f deploy/docker-compose.yml up -d --scale ingest=2 --scale worker=3   # if you scaled down in Phase 2
docker compose -f deploy/docker-compose.yml restart lb
make ps       # everything should be "running"; init-topics "exited (0)"
```

Open:
- **Grafana:** http://localhost:3000. The *GameSense* dashboard loads automatically.
- **Redpanda Console:** http://localhost:8081. Check that the topics `telemetry.raw` and `detections` exist and the consumer groups `detector` and `auditor-raw` are listed.

### Checklist (wait ~10 minutes after `make up`)

Run **`make check`** to get every item below as PASS/FAIL/WAIT in one report. `make check WATCH=10s` keeps it refreshing. Then confirm it visually in Grafana:

- [ ] *Concurrent players by region* shows lines moving. Each real hour is one simulated day, so after 10 minutes you'll see a slope.
- [ ] *Throughput*: the generated, ingested and processed lines sit on top of each other.
- [ ] *Lost events (must be 0)* shows **0**. It starts reporting after about 40 seconds.
- [ ] *Ingest availability* is ~100%.
- [ ] *End-to-end p99* is under 1 s.
- [ ] `make logs` shows `"msg":"audit"` lines with `"lost":0`.

### Troubleshooting
| Symptom | Likely cause and fix |
|---|---|
| `make deps` or `go build` errors in `internal/broker/kafka.go` | This file wasn't compiled when the project was written, because there was no network. Paste the error and fix the API call; it's usually a one-line change. |
| `init-topics` keeps restarting, or brokers are unhealthy | Docker has too little memory. Give it 8 GB, then `make clean && make up`. |
| Port already in use (3000, 8080, 9090…) | Stop whatever is using it, or change the left side of `ports:` in `deploy/docker-compose.yml`. |
| Grafana shows "No data" | Wait 1 minute. Check http://localhost:9090/targets: every target should be UP. |
| *Lost events* is above 0 | Read the auditor log. Confirm it isn't lagging (`audit_watermark_lag_seconds`). If the loss is real, this is your first incident; write it up. |

**Done when** every checklist item passes. Take a screenshot of the dashboard: it's your first artifact.

---

## Phase 4: Baseline and first bottleneck (1–3 days)

The goal is a measured throughput ceiling for your machine, and the reason for it.

1. Step up the load and let each level settle for 10 minutes:
   ```bash
   make load L=2
   make load L=4
   make load L=8
   make load L=16
   ```
2. For each level, record into a table in the README: events/s (generated vs ingested), ingest p99, end-to-end p99, consumer lag, CPU (`docker stats`), and lost events.
3. The **first level where something breaks** is your ceiling. Signs: generated pulls away from ingested, p99 jumps, 429/503 responses rise, or consumer lag climbs without coming back.
4. Find out why by profiling the component that's struggling:
   ```bash
   # simulator (9101), worker (9102: add a port mapping), auditor (9103)
   go tool pprof -http=:7000 "http://localhost:9101/debug/pprof/profile?seconds=30"
   ```
   Also check `docker stats` to see whether the bottleneck is a broker, a simulator, ingest or the auditor.
5. Fix the biggest item, measure again, and write 3–5 sentences: what the bottleneck was, how you found it, what you changed, and the before/after numbers.

> `/debug/pprof` is exposed on every service for convenience. In production it would be on an internal-only port.

**Done when** the README has a load table and one "bottleneck → fix → result" story.

---

## Phase 5: First billion-event run (1 day)

```bash
make clean                 # fresh volumes
make billion               # ~1.3B events: a simulated week in ~7 h at 3x load
```

If Phase 4 showed your machine can't sustain 3× load at peak, use the highest level that worked: `SIM_LOAD=2 SIM_SCALE=24 docker compose -f deploy/docker-compose.yml up -d`. A smaller, honest number beats a big false one.

During the run, check once or twice that disk usage is stable (Console → Brokers) and that lost events is still 0.

At the end, capture:
- [ ] Screenshot of the full week: the player waves, the patch spike, the outage dip and reconnect surge, the weekend bump
- [ ] Final auditor log line: `acked`, `seen`, `lost: 0`, raw duplicates, precision, recall
- [ ] Peak events/s, ingest p99 and end-to-end p99

**Done when** you can truthfully write "processed N billion events over a simulated week with 0 lost."

---

## Phase 6: More workers (M2, 1–2 weeks)

Each worker is a new consumer group on `telemetry.raw` and demonstrates a different concept.

1. ~~**Dead-letter topic.**~~ ✅ Done: undecodable records go to `telemetry.dlq` with partition, offset and error, and there's a dashboard panel and a check. Next step: add a small `dlq-replay` command that re-publishes fixed records. *Concept: poison messages.*
2. **Rollups → ClickHouse.** Per region, per minute: active players, events, shots, hit rate. Add ClickHouse to Compose and batch-insert. *Concepts: windowed aggregation, late events, idempotent sinks* (use a ReplacingMergeTree keyed by window).
3. **MMR / rank updates** from `match_end` events: winners gain rating, losers lose it, stored per player. A redelivered match must not be applied twice, so keep a processed-match ID set. *Concepts: exactly-once effects, ordering per key.*
4. **Session report cards:** when a player has no events for 60 s, emit a summary. *Concept: session windows.*

**Done when** each worker has tests, a metrics panel, and a short design note in the README.

---

## Phase 7: Chaos game days (M3, 1 week)

The scripts are already written. Each one runs a baseline `make check`, injects the failure, observes, recovers, and checks again, and logs everything to `docs/gamedays/`. Run them at simulated peak:

```bash
make chaos-broker              # kill redpanda-1 (TARGET=redpanda-2 to choose)
make chaos-workers N=2         # kill 2 of 3 workers
make chaos-ingest              # kill one ingest replica
make chaos-delay DELAY=300ms   # network latency on a broker
make chaos-poison N=5          # malformed records -> dead-letter topic
```

| Experiment | Command idea | Expected result |
|---|---|---|
| Kill a broker | `docker kill deploy-redpanda-1-1` | Brief p99 spike, no lost events; restart it and the cluster heals |
| Kill 2 of 3 workers | `docker kill` two workers | Rebalance, lag grows then drains, no lost events, duplicates deduped |
| Kill an ingest replica | `docker kill` one ingest | Clients retry, nginx routes around it, no lost events |
| Network delay | `tc netem` 200 ms on a broker container | p99 rises, throughput holds, no lost events |
| Disk pressure | very low retention plus a tiny volume | Ingest returns 503, clients buffer, no silent loss |
| Poison record | produce garbage to `telemetry.raw` | Goes to the DLQ (after Phase 6), pipeline keeps flowing |

For each one, write `docs/postmortems/NNN-title.md` with a timeline, impact, root cause, what went well, and action items.

**Done when** you have 3+ postmortems with graphs, and every experiment ends with lost = 0 (or a documented fix).

---

## Phase 8: Performance (M4, 1 week)

1. Profile ingest, worker and simulator at your ceiling load and save the flame graphs.
2. Switch the wire format from JSON to **protobuf**, or another binary format, and compare CPU, bytes on the wire and throughput before and after.
3. Look for allocation hot spots: reuse buffers with `sync.Pool`, skip re-encoding in ingest.
4. Record before/after in a table.

**Done when** you have one table showing a meaningful improvement (like 2×+ throughput or −50% CPU), backed by flame graphs.

---

## Phase 9: Kubernetes (M5, 1–2 weeks)

1. Install **k3d** (or kind) and **Helm**, then create a local cluster.
2. Install Redpanda with its Helm chart (3 brokers), and kube-prometheus-stack for Prometheus and Grafana.
3. Write a Helm chart in `deploy/k8s/gamesense/` with Deployments for ingest, worker, simulator and auditor; a Service for ingest; readiness probes on `/readyz` and liveness probes on `/healthz`; resource requests and limits; and `terminationGracePeriodSeconds: 30`.
4. **HPA** on ingest (CPU). Install **KEDA** and scale workers on consumer lag with its Kafka scaler.
5. Add **PodDisruptionBudgets** for ingest and workers.
6. Rerun the simulated week and capture the chart that matters: **players rising and falling, pod counts following them, p99 staying flat.**
7. Do a rolling deploy of ingest at peak and show zero errors.

**Done when** the autoscaling chart and a zero-error rolling deploy are in the README.

---

## Phase 10: Production practices (M6, 1–2 weeks)

1. **GitHub Actions CI:** `go vet`, `go test -race`, `golangci-lint`, Docker image builds, vulnerability scan (Trivy or govulncheck).
2. **Performance gate:** run the ingest benchmark in CI and fail if it regresses more than 10%.
3. **SLOs:** write them down (99.9% ingest availability, p99 < 100 ms, 0 lost events). Add Prometheus burn-rate alert rules and an error-budget panel.
4. **Canary deploys** with Argo Rollouts on Kubernetes: 10% → 50% → 100%, with automatic rollback if the error rate or p99 gets worse. Demo it by deploying a deliberately broken version.
5. **Runbooks** in `docs/runbooks/` for every alert.
6. Optional: **OpenTelemetry tracing** from simulator → ingest → worker.

**Done when** CI is green on every PR, and the broken-canary rollback is recorded as a GIF or video.

---

## Phase 11: Long soak and public demo (M7)

1. **Local soak:** `make soak` (~3B events in 24 h) or `make soak-week` (~10B over 7 days). Watch for problems that only appear over time: memory growth (`worker_tracked_players`, container memory), disk, lag drift.
2. **Public demo (free):** an Oracle Cloud Always Free Arm VM running the Compose stack at reduced load (`SIM_LOAD=0.5`), with a read-only Grafana link in your README. See the hosting section of the README. The Go images and Redpanda both run on Arm.

**Done when** you've recorded soak results (including anything that broke and how you fixed it), and there's optionally a live dashboard link.

---

## Phase 12: Write-up (2–3 days)

- [ ] README top section: one-paragraph pitch, architecture diagram, headline numbers, dashboard screenshot
- [ ] Results tables: load ladder, billion run, chaos experiments, before/after performance
- [ ] Design decisions section (already started; keep it current)
- [ ] A 2-minute demo video: dashboard during the patch spike, killing a broker live, lost = 0
- [ ] Optional blog post: "How my anti-cheat detector found a bug in my simulator" or "What broke at 100k events/s"
- [ ] Resume bullet with real numbers:
  > **GameSense** — distributed game-telemetry pipeline (Go, Redpanda/Kafka, Kubernetes, Prometheus). Processed **X B events** at up to **Y k events/s** under realistic daily traffic cycles and failures, with **zero data loss** verified by an independent auditor; KEDA autoscaling on consumer lag; real-time anti-cheat at **Z% precision**.

---

## Interview prep: questions this project will get

- Why acknowledge only after `acks=all`? What does that cost you in latency?
- At-least-once vs exactly-once: where do you get duplicates, and how are they handled?
- Why partition by match ID? What happens with a hot partition?
- What happens during a consumer-group rebalance? Why `BlockRebalanceOnPoll`?
- How does the auditor prove zero loss? What could make it wrong? (The watermark answers that.)
- What was your bottleneck, and how did you find it?
- Why scale workers on consumer lag instead of CPU?
- What would change if this were truly multi-region?
