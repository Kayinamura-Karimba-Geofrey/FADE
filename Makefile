# FADE - Fraud and Anomaly Detection Engine
# All targets run locally via Docker Compose. Run `make` for help.

.DEFAULT_GOAL := help

COMPOSE := docker compose -f deploy/docker-compose.yml --env-file "$(CURDIR)/.env"
PROFILES := --profile infra --profile app
GO := go
UV := uv

.PHONY: help env up up-infra down nuke logs ps migrate migrate-down topics \
        test lint fmt build psql redis-cli clean

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

clean: ## Remove local build artifacts
	go clean -cache=0 2>/dev/null || true
	rm -rf dashboard/.next dashboard/out coverage.out coverage.html
	find . -type d -name __pycache__ -not -path './.venv/*' -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache .ruff_cache .mypy_cache
