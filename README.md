# FADE - Fraud and Anomaly Detection Engine

FADE detects fraud and anomalies in mobile-money transaction streams in near
real time by combining deterministic rules, machine-learning scores and
analyst feedback. It ingests events through Redpanda, evaluates them in a Go
decision pipeline, and stores decisions, cases and audit trails in PostgreSQL.
Everything runs locally through Docker Compose - no cloud dependencies.

## Architecture

```
                        FADE (Phase 0 in bold; later phases dashed)

  ┌───────────┐   txn.raw (12p)   ┌──────────────────────────────────────┐
  │ generator │ ────────────────▶ │              Redpanda               │
  │ (Python)  │                   │  txn.raw txn.decisions txn.dead     │
  └───────────┘                   │  labels.feedback                     │
                                  └───────────────┬──────────────────────┘
                                                  │
              ┌───────────────────────────────────┼────────────────────────┐
              │ (Phase 1+)                        │ (Phase 4+)             │
              ▼                                  ▼                        ▼
   ┌────────────────────┐            ┌────────────────────┐   ┌──────────────────┐
   │  feature-worker    │            │     rules engine   │   │    ml-scorer     │
   │  (Go, Phase 1)     │            │ (Go, Phase 3)      │   │ FastAPI :8000    │
   └─────────┬──────────┘            └─────────┬──────────┘   │ sklearn/LGBM/SHAP│
             │                                 │              └────────┬─────────┘
             └───────────────┬─────────────────┘                       │
                             ▼                                         │
             ┌───────────────────────────────────────────┐             │
             │           decision-api  (Go :8080)        │◀────────────┘
             │  /healthz  /readyz  /metrics  (chi+slog)  │
             │  internal/{events,features,rules,scoring, │
             │  combiner,explain,store,api,obs,pipeline} │
             └──────┬───────────────────────┬────────────┘
                    ▼                       ▼
          ┌──────────────────┐     ┌──────────────────┐
          │   PostgreSQL 16  │     │     Redis 7      │
          │ decisions/cases/ │     │ cache & state    │
          │ labels/audit ... │     └──────────────────┘
          └────────┬─────────┘
                   │
     ┌─────────────┴──────────────┐
     ▼                            ▼
┌───────────────┐          ┌──────────────────┐
│  Prometheus   │ ───────▶ │     Grafana      │  analyst dashboard
│    :9090      │          │  :3001 (FADE     │  (Next.js 15, :3000)
└───────────────┘          │  Overview board) │
                           └──────────────────┘
```

## Prerequisites

- Docker 24+ with Docker Compose v2 (`docker compose version`)
- GNU Make
- Go 1.23 (for `make test` / `make lint` outside Docker)
- [uv](https://docs.astral.sh/uv/) 0.9+ and Python 3.12 (for Python targets)
- Node.js 22+ and npm (for dashboard targets)
- `staticcheck` (`go install honnef.co/go/tools/cmd/staticcheck@2024.1.1`)

## Quick start

```bash
cp .env.example .env     # review/change the secrets
make up                  # build + start infra AND app profiles
make migrate             # apply database migrations (also runs in `make up`)
make ps                  # wait until everything is healthy
```

Then open:

- Dashboard: http://localhost:3000
- Decision API health: http://localhost:8080/healthz
- ML scorer health: http://localhost:8000/healthz
- Prometheus: http://localhost:9090
- Grafana (admin/admin by default): http://localhost:3001 - dashboard
  **FADE Overview** (folder: FADE)

Infra-only startup (no app containers):

```bash
make up-infra    # redpanda, redis, postgres, prometheus, grafana
```

## Environment variables

`.env.example` is copied to `.env` automatically by the `up` targets. Each
variable, its meaning and default:

| Variable | Meaning | Default |
| --- | --- | --- |
| `POSTGRES_HOST` | Postgres hostname as seen by the decision-api | `postgres` |
| `POSTGRES_PORT` | Postgres port | `5432` |
| `POSTGRES_DB` | Database name | `fade` |
| `POSTGRES_USER` | Database user | `fade` |
| `POSTGRES_PASSWORD` | Database password (change it) | `change-me` |
| `REDIS_ADDR` | Redis `host:port` | `redis:6379` |
| `KAFKA_BROKERS` | Redpanda Kafka bootstrap `host:port` | `redpanda:9092` |
| `HTTP_ADDR` | decision-api listen address | `:8080` |
| `LOG_LEVEL` | slog level: `debug`, `info`, `warn`, `error` | `info` |
| `ML_SCORER_URL` | Base URL of the ML scoring service | `http://ml-scorer:8000` |
| `JWT_SECRET` | HMAC secret for auth tokens (change it) | `change-me` |

Optional (compose-level): `GRAFANA_ADMIN_PASSWORD` (default `admin`).

## Ports

| Port | Service | Scope | Purpose |
| --- | --- | --- | --- |
| 8080 | decision-api | host | HTTP API (`/healthz`, `/readyz`, `/metrics`) |
| 8000 | ml-scorer | host | FastAPI ML service (`/healthz`, `/metrics`) |
| 3000 | dashboard | host | Next.js analyst dashboard |
| 3001 | grafana | host | Grafana UI (dashboards) |
| 9090 | prometheus | host | Prometheus UI / API |
| 5432 | postgres | host | PostgreSQL 16 |
| 6379 | redis | host | Redis 7 |
| 9092 | redpanda | container network | Kafka API (`redpanda:9092`) |
| 9644 | redpanda | container network | Redpanda admin API |

Redpanda's Kafka ports are intentionally bound to the compose network only;
reach brokers with `docker compose exec redpanda rpk ...`.

## Make targets

Run `make` (or `make help`) for the full list with descriptions:

| Target | Description |
| --- | --- |
| `make up` | Start the full stack (infra + app profiles), building images |
| `make up-infra` | Start infrastructure only (redpanda, redis, postgres, prometheus, grafana) |
| `make down` | Stop all services, **keep volumes** |
| `make nuke` | Stop all services and **remove volumes** (destroys data) |
| `make logs` | Tail logs of all services |
| `make ps` | Show service status (incl. healthchecks) |
| `make migrate` | Apply migrations (idempotent) |
| `make migrate-down` | Roll back the last migration batch |
| `make topics` | List Redpanda topics (`rpk topic list`) |
| `make test` | `go test ./... -race` + `pytest` |
| `make lint` | `go vet`, `staticcheck`, `ruff check`, `npm run lint` |
| `make fmt` | `gofmt`, `ruff format`, `prettier` |
| `make build` | Build Go binaries and the dashboard |
| `make psql` | Open a `psql` shell in Postgres |
| `make redis-cli` | Open a `redis-cli` shell in Redis |
| `make clean` | Remove local build artifacts |

## Repository layout

```
fade/
  cmd/                 entrypoints (decision-api, feature-worker, case-worker, replay)
  internal/            Go packages (events, features, rules, scoring, combiner,
                       explain, store, api, obs, pipeline)
  ml/                  Python ML service (train/, serve/, explain/, models/)
  generator/           synthetic transaction generator (Typer CLI)
  dashboard/           Next.js 15 analyst dashboard
  migrations/          golang-migrate SQL migrations
  rules/               rule-set definitions (JSON, Phase 3)
  api/                 API contracts / OpenAPI (Phase 1+)
  deploy/              docker-compose.yml, prometheus.yml, grafana provisioning
  bench/               k6 load tests (Phase 8)
  docs/                documentation index and phase notes
  .github/workflows/   CI (go, python, dashboard, compose)
  Makefile             one-command developer workflows
```

## Phase roadmap

- [ ] **Phase 0 - Foundations: IN PROGRESS** - repo skeleton, infra
      (Redpanda/Redis/Postgres/Prometheus/Grafana), migrations, CI, docs
- [ ] Phase 1 - Event ingestion: generator scenarios, event schema, `txn.raw`
- [ ] Phase 2 - Feature store: feature worker, Redis-backed features
- [ ] Phase 3 - Rules engine: rule sets, versions, audit logging
- [ ] Phase 4 - ML scoring: training pipeline, LightGBM + SHAP explanations
- [ ] Phase 5 - Decision pipeline: combiner, thresholds, `txn.decisions`
- [ ] Phase 6 - Case management: cases, labels, analyst dashboard workflows
- [ ] Phase 7 - Replay & feedback: shadow evaluation, replay jobs, label loop
- [ ] Phase 8 - Hardening: k6 load tests, tuning, runbooks, release

## Documentation

See [docs/README.md](docs/README.md) for the documentation index and
[docs/phase-0.md](docs/phase-0.md) for what Phase 0 delivered plus its exit
checks.
