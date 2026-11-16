#!/usr/bin/env bash
# Inject N malformed records straight into telemetry.raw.
#
# Expect: workers keep running (no crash loop), each bad record lands in
# telemetry.dlq with its partition/offset and error, the "Dead-letter queue"
# check FAILs (that's the alert working), and lost events stay 0.
source "$(dirname "$0")/lib.sh"
N="${1:-5}"

step "producing $N poison records"
for i in $(seq 1 "$N"); do printf 'poison-%s: definitely not json\n' "$i"; done \
  | $COMPOSE exec -T redpanda-0 rpk topic produce telemetry.raw -k poison
observe 30

step "dead-letter topic contents"
$COMPOSE exec -T redpanda-0 rpk topic consume telemetry.dlq --offset start --num "$N" || true
finish
