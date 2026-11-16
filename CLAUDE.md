# GameSense

Distributed game-telemetry pipeline in Go: simulated game servers → ingest HTTP API (behind nginx) →
3-broker Redpanda (Kafka API, `telemetry.raw`, acks=all, RF 3) → detector workers → `detections`.
An independent auditor proves zero loss by comparing simulator-acked sequence numbers with the log.
The plan, by phase, is in `docs/PLAN.md`. Phase results are recorded there.

## Status
- Phase 1 (ingest alone) is done. Phase 2 (add the queue) is done (2026-11-16): see "Phase 2 results" in PLAN.md
  (Run A on the HDD, Run B on NVMe, Run 3 loss reproduction).
- Next: make the auditor keep up above ~100K ev/s, then Phase 3 (workers). The one-off "lost=153" from a
  dev-mode run is unexplained and didn't reproduce in 6 attempts; `audit missing` lines will show ranges if it recurs.

## Key results so far
- **Phase 1 native** (Windows, 6 cores): 22K / 86K / 325K events/s at 1× / 4× / 16×, p99 ~10 / ~47 / ~235 ms,
  0 rejected. Ceiling ~448K events/s at 32×/8 (CPU-bound, sim and ingest sharing the PC).
- **Phase 1 Docker** (4 CPUs, 55 servers, `bin/p1docker-stats-*.txt`): 21.7K / 87.5K / 196.7K events/s at
  1× / 4× / 9×; p50 0.8 / 5.9 / 12 ms; p99 13 / 50 / 96 ms; ingest CPU 27 / 83 / 174%; 0 rejected.
- **Phase 2, durable brokers, Docker disk on NVMe** (`bin/p2nvme-stats-*.txt`): 21.7K / 77.0K / 104.9K ev/s at
  1× / 4× / 9× (140.1K at 9× with `SIM_SERVERS=3`); p50 38 / 155 / 139 (306) ms; p99 184 / 468 / 461 (707) ms;
  broker-ack p99 169 / 597 / 558 (641) ms. Throughput cost −0% / −12% / −47% (−29% with 3× servers);
  the box is ~90% CPU-busy at 4× and up. **lost = 0** everywhere, 0 rejected.
- **Phase 2 on the hard disk** (`bin/p2-stats-*.txt`): ~11–15K ev/s, ~1.75 s acks. fsync ~41 ms on the HDD
  vs ~6–7 ms on NVMe. Combined with the closed-loop simulator (one batch in flight per server), latency set
  the throughput.

## Gotchas
- Redpanda's Docker image defaults to developer mode (fsync bypassed, write caching on). The compose file
  runs `rpk redpanda mode production` in the broker entrypoint and sets `write.caching=false`. After any
  change, confirm with `docker logs <broker>`: the start line must not show `--unsafe-bypass-fsync`.
- Never restart or recreate the auditor during a run: it would report false losses. A plain `up -d --build`
  rebuilds and recreates every Go service, so to ship an ingest change mid-run use
  `up -d --build --no-deps --scale ingest=1 ingest`. Recreating the simulators gives them new server IDs,
  which is fine.
- Always pass `--scale worker=0` on `up` while workers aren't wanted; compose otherwise starts 3.
- The auditor falls behind above ~100K ev/s (watermark lag grows). To get a lost verdict, `docker pause` the
  simulators (not stop: that keeps their server IDs), wait until `rpk group describe auditor-raw` shows
  TOTAL-LAG 0, then read the audit line. `rpk` inside a broker needs
  `-X brokers=redpanda-0:9092,redpanda-1:9092,redpanda-2:9092` (otherwise it dials the advertised localhost ports).
  Pausing causes harmless duplicates (timed-out requests get resent).
- nginx resolves ingest replicas at startup: run `docker compose -f deploy/docker-compose.yml restart lb`
  after scaling ingest. nginx doesn't use `/readyz`; a removed replica causes connect errors and resets (502/504s,
  which clients retry).
- Ingest produces with its own context (not `r.Context()`), so a client disconnect never cancels an in-flight
  produce. That can leave duplicates, which are harmless because `(server_id, seq)` is the idempotency key.
- The Dockerfile builds with `golang:1.26`, because go.mod requires go 1.26 since franz-go was added.
- `docker system df` used to fail because of an orphaned containerd snapshot; use `Get-PSDrive` for disk usage.

## Environment
- Windows 11, Intel i5-9400F (6 cores). Go 1.26.4.
- Docker Desktop (WSL2), 4 CPUs / ~8 GB, disk image at `E:\DockerData\DockerDesktopWSL\disk\docker_data.vhdx`.
  Drives: C: SATA SSD (WD Green 240 GB), D: 7200 rpm SATA HDD (WD10EZEX), E: NVMe SSD (Crucial P3 1 TB).
  Keep Docker on E:.
- Evidence logs from the Phase 2 runs are in `gamesense2\evidence\` (outside the repo, not committed).
- Don't touch the unrelated `caramel-*` containers or volumes.

## Stopping native processes on Windows
To stop native `.exe` processes cleanly (so `loadstat` prints its summary), send CTRL_BREAK: start the
process in its own process group with a small helper (`runfor <seconds> <outfile> <cmd> args…`: starts with
CREATE_NEW_PROCESS_GROUP, sends CTRL_BREAK via GenerateConsoleCtrlEvent after N seconds). Build it outside
the repo (e.g. in a scratch folder). Force-kill only if the process is still running 5 s after CTRL_BREAK.
