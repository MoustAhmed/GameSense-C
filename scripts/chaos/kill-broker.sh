#!/usr/bin/env bash
# Kill one Redpanda broker under load, then bring it back.
#
# Expect: a short p99 spike while partition leadership moves to the surviving
# brokers, possibly a few 503s that clients retry, and ZERO lost events:
# with RF=3 and acks=all, every acknowledged write is already on >= 2 brokers.
source "$(dirname "$0")/lib.sh"
BROKER="${1:-redpanda-1}"

step "killing $BROKER (SIGKILL — no graceful shutdown)"
docker kill "$(ids "$BROKER")"
observe 60

step "restarting $BROKER — it rejoins and catches up from the leaders"
docker start "$(ids "$BROKER")"
observe 90
finish
