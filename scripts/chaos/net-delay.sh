#!/usr/bin/env bash
# Add network latency to one container (default: 200ms to redpanda-0 for 120s).
#   scripts/chaos/net-delay.sh [service] [delay] [seconds]
#
# Expect: higher produce/ack latency and ingest p99, maybe more 429s as
# in-flight slots fill up, clients buffering more — but no lost events.
# Uses a throwaway netshoot container that shares the target's network namespace.
source "$(dirname "$0")/lib.sh"
TARGET="${1:-redpanda-0}"; DELAY="${2:-200ms}"; SECS="${3:-120}"
CID=$(ids "$TARGET" | head -n 1 || true)
netem() { docker run --rm --network "container:$CID" --cap-add NET_ADMIN nicolaka/netshoot tc "$@"; }
trap 'netem qdisc del dev eth0 root >/dev/null 2>&1 || true' EXIT

step "adding ${DELAY} (+/-20ms jitter) latency to $TARGET for ${SECS}s"
netem qdisc add dev eth0 root netem delay "$DELAY" 20ms
observe "$SECS"

step "removing latency"
netem qdisc del dev eth0 root
trap - EXIT
observe 60
finish
