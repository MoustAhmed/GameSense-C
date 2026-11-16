# NNN: <title, e.g. "Broker loss at peak load">

**Date:** YYYY-MM-DD · **Experiment:** `scripts/chaos/<name>.sh <args>` · **Log:** `docs/gamedays/<file>.log`
**Load:** SIM_LOAD=<n>, ~<k> events/s at the time of injection

## Summary
One or two sentences: what failed, what the impact was, and whether the system behaved as designed.

## Hypothesis
What you expected to happen, and why (cite the design: acks=all, consumer groups, retries…).

## Timeline
| Time | Event |
|---|---|
| hh:mm:ss | Baseline healthy (all checks PASS) |
| hh:mm:ss | Failure injected |
| hh:mm:ss | First visible effect (e.g. p99 spike to N ms) |
| hh:mm:ss | Recovered (all checks PASS) |

## Impact
- Lost events: **0** (auditor)
- Peak ingest p99: N ms (baseline N ms)
- 429/503 responses: N · client retries: N
- Max consumer lag: N records · time to drain: N s

## What went well

## What went wrong or surprised us

## Root cause (if anything broke)

## Action items
- [ ] …

## Screenshots
<!-- Grafana panels during the incident -->
