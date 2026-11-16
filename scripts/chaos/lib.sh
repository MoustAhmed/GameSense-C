#!/usr/bin/env bash
# Shared helpers for chaos game days. Each script: baseline check -> inject
# failure -> observe -> recover -> observe. Everything is logged to
# docs/gamedays/ so you can write the postmortem afterwards.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
COMPOSE="docker compose -f deploy/docker-compose.yml"
NAME="$(basename "$0" .sh)"
mkdir -p "$ROOT/docs/gamedays" "$ROOT/bin"
LOG="$ROOT/docs/gamedays/$(date +%Y%m%d-%H%M%S)-$NAME.log"
exec > >(tee -a "$LOG") 2>&1
go build -o "$ROOT/bin/check" ./cmd/check

step()    { printf '\n\033[1;36m== [%s] %s\033[0m\n' "$(date +%T)" "$*"; }
check()   { "$ROOT/bin/check" || true; }          # never abort the game day on a FAIL
observe() { step "observing for ${1}s — watch Grafana: http://localhost:3000"; sleep "$1"; check; }
ids()     { $COMPOSE ps -aq "$1"; }                # all containers of a service, running or not
finish()  {
  step "done — log saved to ${LOG#$ROOT/}"
  echo "Write it up:  cp docs/postmortems/TEMPLATE.md docs/postmortems/NNN-$NAME.md"
}

step "baseline"
if ! "$ROOT/bin/check"; then
  echo -e "\033[33mWARNING: the system is not fully healthy before the experiment; results may be misleading.\033[0m"
fi
