#!/usr/bin/env bash
# Kill one ingest replica abruptly (no graceful drain).
#
# Expect: in-flight requests on that replica fail; clients retry (backoff +
# jitter) and nginx routes to the healthy replica. Zero lost events, because
# nothing was acknowledged that wasn't already in the log.
source "$(dirname "$0")/lib.sh"
VICTIM=$(ids ingest | head -n 1 || true)

step "killing ingest replica ${VICTIM:0:12}"
docker kill "$VICTIM"
observe 60

step "restarting it"
docker start "$VICTIM"
# nginx resolves upstream IPs only at startup; if the container came back with
# a new IP nginx won't find it. Kubernetes Services fix this (see Phase 9: Kubernetes).
step "reloading nginx so it re-resolves ingest replicas"
$COMPOSE exec -T lb nginx -s reload
observe 60
finish
