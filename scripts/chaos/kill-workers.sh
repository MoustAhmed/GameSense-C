#!/usr/bin/env bash
# Kill N detector workers at once (default 2 of 3), then restore them.
#
# Expect: consumer-group rebalance; the survivor takes over all partitions;
# lag grows, then drains after recovery. Uncommitted records are redelivered,
# so zero lost events, and duplicate detections are absorbed by deterministic IDs.
source "$(dirname "$0")/lib.sh"
N="${1:-2}"
VICTIMS=$(ids worker | head -n "$N" || true)

step "killing $N worker(s)"
docker kill $VICTIMS
observe 60

step "restarting them — watch lag drain on the dashboard"
docker start $VICTIMS
observe 90
finish
