COMPOSE := docker compose -f deploy/docker-compose.yml
PHASE1  := docker compose -f deploy/docker-compose.phase1.yml

.PHONY: p1-ingest p1-sim stats phase1 phase1-load phase1-down deps build test bench up down logs ps load clean billion soak soak-week check chaos-broker chaos-workers chaos-ingest chaos-delay chaos-poison

# --- Phase 1: ingest alone (no queue). Needs no `make deps`. ---------------
# Option A, no Docker (3 terminals):
p1-ingest:       ## terminal 1: ingest that accepts and discards batches
	go run -tags nokafka ./cmd/ingest -broker discard -drain 0s $(if $(INFLIGHT),-max-inflight $(INFLIGHT))

p1-sim:          ## terminal 2: simulator, e.g. make p1-sim L=4 (add SERVERS=4 for 4x game servers)
	go run -tags nokafka ./cmd/simulator -load $(or $(L),1) -server-scale $(or $(SERVERS),1) -time-scale $(or $(S),24) \
	  -batch $(or $(BATCH),500) -flush $(or $(FLUSH),100ms)

stats:           ## terminal 3: live stats table (Ctrl-C for a summary); SIM=url1,url2 for 2 simulators
	go run -tags nokafka ./cmd/loadstat $(if $(SIM),-sim $(SIM))

# Option B, Docker + Grafana:
phase1:          ## start Phase 1 stack; dashboard at http://localhost:3000
	$(PHASE1) up -d --build

phase1-load:     ## change load, e.g. make phase1-load L=4
	SIM_LOAD=$(or $(L),1) SIM_SCALE=$(or $(S),24) SIM_BATCH=$(or $(BATCH),500) SIM_FLUSH=$(or $(FLUSH),100ms) \
	  $(PHASE1) up -d --no-deps --force-recreate simulator

phase1-down:
	$(PHASE1) down

# --- Full pipeline -----------------------------------------------------------
deps:            ## fetch Go dependencies and write go.sum (run once)
	go get github.com/twmb/franz-go@latest
	go mod tidy

build: deps
	go build ./...

test:            ## unit + end-to-end tests (in-memory broker, no Docker needed)
	go test -tags nokafka -race ./...

bench:           ## ingest hot-path benchmark
	go test -tags nokafka -run xxx -bench . -benchtime 3s ./internal/ingest/

up: deps         ## build images and start the whole stack
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

clean:           ## stop and delete all data volumes
	$(COMPOSE) down -v

check:           ## PASS/FAIL health report; make check WATCH=10s to repeat
	go run ./cmd/check $(if $(WATCH),-watch $(WATCH))

logs:
	$(COMPOSE) logs -f --tail=50 ingest worker auditor simulator-0 simulator-1

ps:
	$(COMPOSE) ps

load:            ## restart the simulators at a different load/speed, e.g. make load L=5 S=24
	SIM_LOAD=$(or $(L),1) SIM_SCALE=$(or $(S),24) $(COMPOSE) up -d --no-deps --force-recreate simulator-0 simulator-1

# --- Billion-event runs (events = avg ~17k/s x load x real seconds) ----------

billion: deps    ## ~1.3B events: a simulated week in ~7 real hours at 3x load
	SIM_LOAD=3 SIM_SCALE=24 $(COMPOSE) up -d --build --scale worker=4
	@echo "Running ~7h. Watch http://localhost:3000 (Lost events must stay 0)."

soak: deps       ## ~3B events: a simulated week over 24 real hours at 2x load
	SIM_LOAD=2 SIM_SCALE=7 $(COMPOSE) up -d --build --scale worker=4
	@echo "Running ~24h. Watch http://localhost:3000"

soak-week: deps  ## ~10B events: a real-time 7-day soak at 1x load
	SIM_LOAD=1 SIM_SCALE=1 $(COMPOSE) up -d --build
	@echo "Running 7 days. Check disk space first; retention is $${RAW_RETENTION_MS:-1800000} ms."

# --- Chaos game days (logs go to docs/gamedays/) -----------------------------

chaos-broker:    ## kill a Redpanda broker under load, then restore it
	scripts/chaos/kill-broker.sh $(TARGET)

chaos-workers:   ## kill 2 of 3 workers (N=... to change)
	scripts/chaos/kill-workers.sh $(or $(N),2)

chaos-ingest:    ## kill one ingest replica abruptly
	scripts/chaos/kill-ingest.sh

chaos-delay:     ## add 200ms latency to a broker for 2 min
	scripts/chaos/net-delay.sh $(or $(TARGET),redpanda-0) $(or $(DELAY),200ms) $(or $(SECS),120)

chaos-poison:    ## inject malformed records -> dead-letter topic
	scripts/chaos/poison.sh $(or $(N),5)
