# Phase 0 - Foundations

Status: **IN PROGRESS** (exit checks recorded at the bottom once green)

Phase 0 builds the production-quality skeleton of FADE: repository layout,
service entrypoints, local infrastructure, database schema, developer
workflow, CI and documentation. No business logic (rules, features, ML
models) is implemented - only the foundations everything else plugs into.

## What was built

### Repository

- Exact tree from the Phase 0 spec: `cmd/`, `internal/` (10 packages),
  `ml/`, `generator/`, `dashboard/`, `migrations/`, `rules/`, `api/`,
  `deploy/`, `bench/`, `docs/`, `.github/workflows/`, `Makefile`,
  `.env.example`, `.gitignore`, `.editorconfig`, `README.md`.
- Empty directories kept in git via `.gitkeep`.

### Go service (`github.com/Kayinamura-Karimba-Geofrey/FADE`, Go 1.23.4)

- `cmd/decision-api`: chi HTTP server on `:8080` -
  `GET /healthz` (always `{"status":"ok"}`), `GET /readyz` (pings Postgres
  and Redis, 503 + failing check name otherwise), `GET /metrics`
  (Prometheus). Structured JSON logging via `log/slog`, config from env,
  graceful shutdown on SIGINT/SIGTERM with a 10 s timeout.
- `internal/obs`: JSON slog setup, per-service Prometheus registry with
  `fade_http_requests_total{method,path,status}`,
  `fade_http_request_duration_seconds{method,path}` and
  `fade_service_up`, plus request logging/metrics middleware (path label =
  matched chi route pattern, so cardinality stays bounded).
- `internal/store`: pgx pool and go-redis constructors, both bounded by a
  5 s context timeout; `RedisPinger` adapts Redis for readiness checks.
- `internal/api`: router wiring, handlers, readiness checks as an
  injectable `Pinger` interface (unit-tested with fakes).
- `cmd/feature-worker`, `cmd/case-worker`, `cmd/replay`: entrypoints that
  log name + version and exit 0 (no business logic in Phase 0).
- Unit tests: config defaults/env overrides/DSN, `/healthz` (200 + body),
  `/readyz` (all-pass 200, one-fail 503 naming the check, no-check 200),
  metrics exposition. All run with `-race`.
- Pinned deps: chi v5.1.0, prometheus client_golang v1.20.5, pgx v5.7.2,
  go-redis v9.7.0, franz-go v1.18.0; staticcheck 2024.1.1.

### Python (uv, Python 3.12)

- Root `pyproject.toml` with pinned dependency groups `generator`, `ml`,
  `dev` and a committed `uv.lock`.
- `ml/serve`: FastAPI app with `GET /healthz` -> `{"status":"ok"}` and
  `GET /metrics` (Prometheus exposition); pinned `requirements.txt` and a
  multi-stage `python:3.12.8-slim-bookworm` Dockerfile.
- `generator/`: Typer CLI with a single `version` command
  (`python -m generator version`); `click` pinned to 8.1.8 because typer
  0.15's help rendering predates click 8.2's `make_metavar(ctx)` change.
- Tests: one pytest per package (generator CLI, ML healthz+metrics) - 4
  tests, all passing. Ruff check + format clean.

### Dashboard (Next.js 15.5.28, TypeScript, Tailwind 4, ESLint 9)

- App Router scaffold; home page renders **FADE Analyst Dashboard**.
- `output: "standalone"` + multi-stage `node:22.12.0-alpine3.20`
  Dockerfile running as non-root.
- `npm run lint` and `npm run build` both pass (static prerender of `/`).

### Infrastructure (deploy/docker-compose.yml)

All images pinned, `restart: unless-stopped` (one-shots: `no`), healthchecks,
named volumes, shared network `fade`, profiles `infra` / `app`:

| Service | Image | Notes |
| --- | --- | --- |
| redpanda | redpandadata/redpanda:v24.3.4 | single node, `--mode` off, Kafka 9092 / admin 9644 |
| redpanda-init | (same) | one-shot: txn.raw(12p), txn.decisions(12p), txn.dead(3p), labels.feedback(3p), replication 1 |
| redis | redis:7.4.1-alpine3.20 | appendonly, maxmemory 1gb, noeviction, volume |
| postgres | postgres:16.6-alpine3.20 | db/user `fade`, volume `pgdata` |
| migrate | migrate/migrate:v4.18.1 | one-shot after postgres healthy, idempotent `up` |
| decision-api | built (golang:1.23.4-alpine3.20) | after postgres+redis healthy and migrate completed |
| ml-scorer | built (python:3.12.8-slim-bookworm) | :8000 |
| dashboard | built (node:22.12.0-alpine3.20) | :3000, standalone |
| prometheus | prom/prometheus:v3.1.0 | scrapes decision-api:8080 + ml-scorer:8000 every 5 s |
| grafana | grafana/grafana:11.5.2 | :3001, provisioned datasource + **FADE Overview** dashboard (HTTP request rate, p99 latency, service up, avg duration) |

- Environment: `.env.example` documents every variable consumed by
  compose interpolation and the decision-api process (see README table).

### Migrations

`migrations/0001_init.{up,down}.sql` create the 7 tables exactly per
spec (`rule_sets`, `decisions` range-partitioned with
`decisions_default` + 2 indexes, `cases`, `labels`, `audit_log`,
`list_entries`, `replay_jobs`); the down migration drops them in reverse
dependency order. Verified `up -> down -> up`.

### Developer workflow

- `Makefile` with `help` default and: up, up-infra, down, nuke, logs, ps,
  migrate, migrate-down, topics, test, lint, fmt, build, psql, redis-cli,
  clean.
- CI (`.github/workflows/ci.yml`): go (vet, staticcheck, race tests),
  python (ruff check/format, pytest via uv), dashboard (npm ci, lint,
  build), compose (`docker compose config -q`) - with Go/uv/npm caches.

## Exit checks

| # | Check | Expected | Result |
| --- | --- | --- | --- |
| 1 | `make up` | all services healthy within 60 s | _pending_ |
| 2 | `make migrate` | exit 0; `\dt` shows 7 tables | _pending_ |
| 3 | `make topics` | exactly txn.raw, txn.decisions, txn.dead, labels.feedback | _pending_ |
| 4 | `curl -s localhost:8080/healthz` | `{"status":"ok"}` | _pending_ |
| 5 | `curl -s localhost:8080/readyz` | 200 | _pending_ |
| 6 | `curl -s localhost:8000/healthz` | `{"status":"ok"}` | _pending_ |
| 7 | `curl -s localhost:9090/api/v1/targets` | decision-api and ml-scorer `up` | _pending_ |
| 8 | Grafana `localhost:3001` | FADE Overview dashboard with data | _pending_ |
| 9 | `make test` and `make lint` | exit 0 | _pending_ |
| 10 | `make down && make up` | data persists, migrations idempotent | _pending_ |
