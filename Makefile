# FADE - Fraud and Anomaly Detection Engine
# All targets run locally via Docker Compose. Run `make` for help.

.DEFAULT_GOAL := help

COMPOSE := docker compose -f deploy/docker-compose.yml --env-file "$(CURDIR)/.env"
PROFILES := --profile infra --profile app
GO := go
UV := uv
PY := .venv/bin/python
DATA ?= small
TMP ?= /tmp/fade-parity
PARITY_REDIS ?= localhost:6380
PARITY_REDIS_DB ?= 15
RATE ?= 1000
DURATION ?= 10s
K6 ?= k6
BASE_URL ?= http://localhost:8080
TIME_BUDGET ?= 300
MODEL_DIR ?= ml/models/lgbm-2026.11.1

.PHONY: help env up up-infra down nuke logs ps migrate migrate-down topics \
        test test-integration lint fmt build psql redis-cli clean \
        gen-features worker parity-golden parity-small parity-run42 \
        load-ingest lag dlq redis-flush bench-feature \	        rules-validate rules-test rules-bench tune decide-demo dev-token \
	        openapi-gen openapi-lint load-decide load-batch pprof-eval \
	        bench-explain privscan audit-verify audit-checkpoint fade-tool

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

env: ## Create .env from .env.example if it does not exist
	@test -f "$(CURDIR)/.env" || (cp "$(CURDIR)/.env.example" "$(CURDIR)/.env" && echo "created .env from .env.example")

up: env ## Start the full stack (infra + app profiles)
	$(COMPOSE) $(PROFILES) up -d --build

up-infra: env ## Start infrastructure only (redpanda, redis, postgres, prometheus, grafana)
	$(COMPOSE) --profile infra up -d --build

down: ## Stop all services, keeping volumes
	$(COMPOSE) $(PROFILES) down

nuke: ## Stop all services and remove volumes (DESTROYS DATA)
	$(COMPOSE) $(PROFILES) down -v --remove-orphans

logs: ## Follow logs of all services
	$(COMPOSE) $(PROFILES) logs -f --tail=100

ps: ## Show service status
	$(COMPOSE) $(PROFILES) ps -a

migrate: env ## Apply database migrations (idempotent)
	$(COMPOSE) $(PROFILES) run --rm migrate

migrate-down: env ## Roll back the last migration batch
	$(COMPOSE) $(PROFILES) run --rm migrate 'exec /migrate -path /migrations -database "$$DATABASE_URL" down 1'

topics: ## List Redpanda topics (rpk topic list)
	$(COMPOSE) --profile infra exec -T redpanda rpk topic list

test: ## Run Go tests (-race) and pytest
	$(GO) test ./... -race
	$(UV) run pytest

ml-train: ## Train the Phase 4 model on DATA (default: run42)
	$(PY) -m ml.train build-table --data-dir data/$(DATA)
	$(PY) -m ml.train train --data-dir data/$(DATA) --time-budget $(TIME_BUDGET)

ml-serve: ## Run the ML scorer service locally
	MODEL_DIR=$(MODEL_DIR) $(UV) run uvicorn ml.serve.app:app --host 0.0.0.0 --port 8090

ml-test: ## Run ML-focused tests
	$(UV) run pytest tests/test_ml_serve.py tests/test_ml_explain.py -q

lint: ## Run go vet, staticcheck, ruff and eslint
	$(GO) vet ./...
	staticcheck ./...
	$(UV) run ruff check .
	cd dashboard && npm run lint

fmt: ## Format all code (gofmt, ruff format, prettier)
	gofmt -l -w .
	$(UV) run ruff format .
	cd dashboard && npx prettier --write .

build: ## Build all artifacts (Go binaries, dashboard)
	$(GO) build ./...
	cd dashboard && npm run build

psql: ## Open a psql shell in Postgres
	$(COMPOSE) --profile infra exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

redis-cli: ## Open a redis-cli shell in Redis
	$(COMPOSE) --profile infra exec redis redis-cli

gen-features: ## Compute reference features for DATA (default: small)
	$(PY) -m generator features --out data/$(DATA) --config config/features.yaml

worker: ## Run the Phase 2 feature worker locally
	$(GO) run ./cmd/feature-worker

rules-validate: ## Validate every rule document in rules/
	$(GO) run ./cmd/rules-validate -dir rules

rules-test: ## Run the rules engine, store and reload tests with the race detector
	$(GO) test ./internal/rules/... -race

rules-bench: ## Evaluate/compile benchmarks with allocation counts
	$(GO) test ./internal/rules -run '^$$' -bench . -benchmem -count=1

tune: ## Replay DATA through the active rules and write a tuning report
	$(GO) run ./cmd/replay-lite --set data/$(DATA) --rules rules/prod.json \
		--redis $${REPLAY_REDIS:-localhost:6390} --db $${REPLAY_DB:-15} \
		--out bench/results/replay-$(DATA)-$$(date +%Y%m%d)

decide-demo: ## Send one demo decision to a running API
	$(GO) run ./cmd/decide-demo -url $(BASE_URL)

dev-token: ## Print a local JWT (ROLE=service|analyst|admin)
	$(GO) run ./cmd/dev-token -role $${ROLE:-service}

openapi-gen: ## Regenerate the OpenAPI Go types
	$(GO) run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.4.1 \
		-config api/oapi-codegen.yaml api/openapi.yaml

openapi-lint: ## Validate the API contract and responses with kin-openapi
	$(GO) test ./internal/api -run 'TestOpenAPIContract' -count=1

load-decide: ## k6 constant-arrival load on /v1/decide
	BASE_URL=$(BASE_URL) TOKEN=$$($(GO) run ./cmd/dev-token) RATE=$(RATE) DURATION=$(DURATION) \
		$(K6) run bench/k6/decide.js

load-batch: ## k6 batch smoke load on /v1/decide/batch
	BASE_URL=$(BASE_URL) TOKEN=$$($(GO) run ./cmd/dev-token) \
		$(K6) run bench/k6/batch.js

fade-tool: ## Build the Phase 5 operator CLI (bin/fade)
	mkdir -p bin
	$(GO) build -trimpath -o bin/fade ./cmd/fade

bench-explain: ## Benchmark explanation rendering (off the decision path)
	$(GO) test ./internal/explain -run '^$$' -bench 'BenchmarkBuild' -benchmem -count=1

# Production surface only. Test fixtures deliberately contain fake identifiers
# to prove the scanner detects them, and model weight/text matrices are numeric
# artefacts, so both are excluded by path rather than by disabling rules.
PRIVSCAN_ALLOW := internal/redaction/testdata/*,internal/canonical/testdata/*,tests/golden/*,data/golden/*,*_test.go,scripts/jcs_crosscheck.py
PRIVSCAN_PATHS := internal cmd api config docs migrations scripts rules deploy config ml/models/registry.json

privscan: ## Fail if a raw personal identifier appears in production files
	mkdir -p bin
	$(GO) build -trimpath -o bin/privscan ./cmd/privscan
	./bin/privscan -allow '$(PRIVSCAN_ALLOW)' $(PRIVSCAN_PATHS)
	@echo "privscan: production surface clean"

privscan-all: ## Scan everything, including intentional negative-test fixtures
	mkdir -p bin
	$(GO) build -trimpath -o bin/privscan ./cmd/privscan
	./bin/privscan .

audit-verify: ## Verify the audit hash chain (requires FADE_DSN)
	./bin/fade audit verify -dsn "$(FADE_DSN)"

audit-checkpoint: ## Write a signed audit checkpoint (requires FADE_DSN, AUDIT_HMAC_KEY)
	./bin/fade audit checkpoint -dsn "$(FADE_DSN)"

pprof-eval: ## CPU/memory profile of the 200-rule evaluation benchmark
	@mkdir -p bench/results
	$(GO) test ./internal/rules -run '^$$' -bench BenchmarkEvaluate200Rules \
		-benchtime 2s -cpuprofile bench/results/rules-cpu.pprof \
		-memprofile bench/results/rules-mem.pprof -count=1 | tee bench/results/pprof-eval.txt
	$(GO) tool pprof -top -nodecount=15 bench/results/rules-cpu.pprof | tee -a bench/results/pprof-eval.txt

test-integration: ## Run Go tests including Redis/Redpanda testcontainers
	FADE_TEST_REDIS_ADDR=$${FADE_TEST_REDIS_ADDR:-} \
	FADE_TEST_KAFKA_BROKERS=$${FADE_TEST_KAFKA_BROKERS:-} \
	$(GO) test ./internal/... -race -cover

parity-golden: ## Python reference vs Go feature store on data/golden
	@mkdir -p $(TMP)/golden
	$(PY) -m generator features --out data/golden --config config/features.yaml \
		--events-jsonl $(TMP)/golden/events.jsonl
	$(GO) run ./cmd/feature-parity --events $(TMP)/golden/events.jsonl \
		--out $(TMP)/golden/go-features.jsonl --redis $(PARITY_REDIS) --db $(PARITY_REDIS_DB) --reset
	$(PY) -m generator.parity --reference data/golden/features.jsonl \
		--actual $(TMP)/golden/go-features.jsonl

parity-small: ## Python reference vs Go feature store on data/small
	@mkdir -p $(TMP)/small
	$(PY) -m generator features --out data/small --config config/features.yaml \
		--events-jsonl $(TMP)/small/events.jsonl
	$(GO) run ./cmd/feature-parity --events $(TMP)/small/events.jsonl \
		--out $(TMP)/small/go-features.jsonl --redis $(PARITY_REDIS) --db $(PARITY_REDIS_DB) --reset
	$(PY) -m generator.parity --reference data/small/features.jsonl \
		--actual $(TMP)/small/go-features.jsonl

parity-run42: ## Python reference vs Go feature store on data/run42
	@mkdir -p $(TMP)/run42
	$(PY) -m generator features --out data/run42 --config config/features.yaml \
		--events-jsonl $(TMP)/run42/events.jsonl
	$(GO) run ./cmd/feature-parity --events $(TMP)/run42/events.jsonl \
		--out $(TMP)/run42/go-features.jsonl --redis $(PARITY_REDIS) --db $(PARITY_REDIS_DB) --reset
	$(PY) -m generator.parity --reference data/run42/features.jsonl \
		--actual $(TMP)/run42/go-features.jsonl

load-ingest: ## Publish DATA to txn.raw at max speed (host external listener :19092)
	$(PY) -m generator publish --out data/$(DATA) --brokers localhost:19092 \
		--topic txn.raw --speed 0

lag: ## Show the feature-worker consumer group lag
	$(COMPOSE) --profile infra exec -T redpanda rpk group describe fade-feature-worker

dlq: ## Show up to 10 dead-letter records
	$(COMPOSE) --profile infra exec -T redpanda rpk topic consume txn.dead -n 10

bench-feature: ## Sustained feature-store load at RATE ev/s for DURATION
	$(GO) run ./cmd/feature-load --rate $(RATE) --duration $(DURATION) \
		--redis $(PARITY_REDIS) --config config/features.yaml

redis-flush: ## Flush Redis feature state (requires CONFIRM=yes)
	@test "$(CONFIRM)" = "yes" || (echo "refusing: run make redis-flush CONFIRM=yes"; exit 1)
	$(COMPOSE) --profile infra exec -T redis redis-cli FLUSHALL

clean: ## Remove local build artifacts
	go clean -cache=0 2>/dev/null || true
	rm -rf dashboard/.next dashboard/out coverage.out coverage.html
	find . -type d -name __pycache__ -not -path './.venv/*' -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache .ruff_cache .mypy_cache

# ---------------------------------------------------------------- Phase 6 replay

.PHONY: dataset-register replay replay-golden replay-regression replay-regression-update \
        gate tune replay-compare replay-worker replay-clean

dataset-register: ## Verify and register a dataset (DATASET=data/run42)
	$(GO) run ./cmd/dataset-register -dataset $(DATASET)

replay: ## Run a replay (DATASET, BASELINE, CANDIDATE=label=ref)
	$(GO) run ./cmd/fade replay run --dataset $(DATASET) \
		--baseline '$(BASELINE)' --candidate '$(CANDIDATE)' \
		--job $${JOB:-0} --out $${OUT:-} --seed $${SEED:-11}

replay-golden: ## Replay the frozen golden dataset
	$(GO) run ./cmd/fade replay run --dataset data/golden \
		--baseline file:rules/prod.json --candidate neutral=file:rules/prod.json \
		--job 9001 --out /tmp/fade-replay-golden --seed 11

replay-regression: ## CI: compare frozen replay expectations
	./scripts/replay-regression.sh

replay-regression-update: ## Regenerate expectations (REASON required)
	UPDATE=1 REASON='$(REASON)' ./scripts/replay-regression.sh

gate: ## Evaluate a gate result (REPORT, CANDIDATE)
	$(GO) run ./cmd/fade gate --report '$(REPORT)' --candidate '$(CANDIDATE)' \
		--out $${OUT:-gate.json}

tune: ## Tune thresholds from a stored report (REPORT, CANDIDATE)
	$(GO) run ./cmd/fade replay tune '$(JOB)' --candidate '$(CANDIDATE)' \
		--objective '$${OBJECTIVE:-net_benefit}'

replay-compare: ## Compare two replay jobs (A, B)
	$(GO) run ./cmd/fade replay compare $(A) $(B)

replay-worker: ## Run the replay worker
	$(GO) run ./cmd/replay-worker

replay-clean: ## Delete replay artifacts older than DAYS (default 30)
	find replay/jobs -mindepth 1 -maxdepth 1 -type d -mtime +$${DAYS:-30} -exec rm -rf {} +
