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

.PHONY: help env up up-infra down nuke logs ps migrate migrate-down topics \
        test test-integration lint fmt build psql redis-cli clean \
        gen-features worker parity-golden parity-small parity-run42 \
        load-ingest lag dlq redis-flush bench-feature

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
