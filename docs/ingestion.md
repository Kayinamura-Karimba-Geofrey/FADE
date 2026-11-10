# FADE Phase 2 ingestion and feature worker

Status: **implemented** (`feature_schema_version = "1.0.0"`).

This document describes the runtime around the normative feature contract in
[features.md](features.md): how events are consumed, validated, deduplicated,
turned into features, dead-lettered, and observed.

## Pipeline

```
txn.raw (Redpanda)
   │ Key = sender.account_id
   ▼
feature-worker
   │ 1. strict JSON parse + contract validation
   │ 2. Redis Lua: dedupe check → prior reads → update → dedupe ack
   │ 3. cycle detection (Redis 24h latest-edge graph)
   │ 4. optional feature vector publish
   ▼
txn.dead (invalid / late)              features topic (optional)
```

The worker is `cmd/feature-worker`. `cmd/feature-parity` replays a JSONL event
stream directly through the same Go store for oracle parity, and
`cmd/feature-load` drives a sustained load without Kafka for local benchmarks.

### Ordering and commits

- One Kafka record is one event. Events are sharded by Kafka key inside each
  partition, so all events for an account stay ordered while distinct accounts
  process concurrently.
- Offsets are committed only after every record in the fetched partition batch
  succeeds. A transient Redis/Kafka error aborts the batch, leaves it
  uncommitted, and retries with exponential backoff (25ms → 2s).
- The dedupe key is written by the same Lua script that applies state, after
  all state mutations. A crash between the script and the commit can replay the
  event; the replay is acknowledged as a duplicate and commits safely.
- Duplicates are skipped and committed. Late events are dead-lettered and
  committed. Neither updates state.

### Late events

The per-account watermark is the maximum event time applied for that account.
An event with `event_time < watermark - LATE_TOLERANCE_S` is published to the
DLQ with reason `LATE`. An event inside the tolerance is accepted and inserted
at its event-time position; the bounded history key makes scalar features
(device, SIM, geo, cash-out) correct for that backfill.

### Dead letters

`txn.dead` records are JSON envelopes:

```json
{
  "event_id": "...",
  "reason": "INVALID_JSON | INVALID_SCHEMA | INVALID_TIMESTAMP | EVENT_TOO_OLD | EVENT_FROM_FUTURE | LATE",
  "detail": "safe diagnostic",
  "source_topic": "txn.raw",
  "source_partition": 3,
  "source_offset": 91822,
  "failed_at": "RFC3339",
  "raw_base64": "..."
}
```

`raw_base64` preserves the original bytes verbatim, including invalid UTF-8, so
the DLQ is replayable after a parser fix.

## Configuration

All environment variables are validated at startup; the worker exits rather
than starting with a partial configuration.

| Variable | Default | Meaning |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated bootstrap brokers. |
| `KAFKA_TOPIC` | `txn.raw` | Raw event topic. |
| `KAFKA_GROUP_ID` | `fade-feature-worker` | Consumer group. |
| `KAFKA_DLQ_TOPIC` | `txn.dead` | Dead-letter topic. |
| `FEATURE_OUTPUT_TOPIC` | `""` | Optional feature-vector topic; disabled when empty. |
| `REDIS_ADDR` | `localhost:6379` | Redis primary `host:port` (single primary required). |
| `REDIS_DB` | `0` | Redis database. |
| `REDIS_PASSWORD` | `""` | Optional Redis password. |
| `WORKERS_PER_PARTITION` | `1` | Shards per partition, keyed by sender. |
| `MAX_INFLIGHT_PER_PARTITION` | `256` | Bound on queued records per partition. |
| `LATE_TOLERANCE_S` | `60` | Watermark tolerance. |
| `DEDUPE_TTL_S` | `86400` | Dedupe key TTL; must cover the 24h window. |
| `FEATURE_CONFIG` | `config/features.yaml` | Shared, versioned feature YAML. |
| `WORKER_HTTP_ADDR` | `:8083` | Health, readiness and metrics listener. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error`. |

`config/features.parity.yaml` is identical except `cycle.cache_ttl_s: 0`. The
Python oracle is uncached, so parity runs disable the Redis cycle cache to
compare like with like. The production file keeps the 60s cache.

## Health and metrics

- `GET /healthz` — process liveness, always `200 ok` once serving.
- `GET /readyz` — pings Redis and the Kafka client; `200` when both are
  available, otherwise `503` with the failing checks.
- `GET /metrics` — Prometheus text format.

Collectors:

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `fade_feature_events_total` | counter | `status` | `ok`, `duplicate`, `late`, `invalid`, `error`. |
| `fade_event_processing_seconds` | histogram | — | Full handler latency. |
| `fade_window_capped_total` | counter | — | Window entries dropped by caps. |
| `fade_consumer_lag` | gauge | — | Sum of partition lag. |
| `fade_events_dead_lettered_total` | counter | `reason` | DLQ publishes. |
| `fade_kafka_commits_total` | counter | `result` | Commit attempts. |
| `fade_feature_output_total` | counter | — | Vectors published. |
| `fade_redis_scripts_total` | counter | `result` | Lua executions. |
| `fade_redis_operation_seconds` | histogram | `operation` | Lua latency. |
| `fade_cycle_detection_seconds` | histogram | — | Cycle detection latency. |
| `fade_dlq_publish_seconds` | histogram | — | DLQ publish latency. |
| `fade_event_retries_total` | counter | — | Transient failures retried. |
| `fade_event_parse_failures_total` | counter | — | Strict parse failures. |

The Grafana dashboard **FADE Ingestion** (`deploy/grafana/dashboards/fade-ingestion.json`)
charts these collectors. Prometheus scrapes `feature-worker:8083`.

## Local workflows

```bash
make up-infra                 # redpanda, redis, postgres, prometheus, grafana
make up                       # also builds and starts feature-worker
make gen-small                # Phase 1 dataset (if not present)
make load-ingest DATA=small   # publish to txn.raw through localhost:19092
make lag                      # rpk group describe fade-feature-worker
make dlq                      # consume up to 10 dead letters
make redis-flush CONFIRM=yes  # DESTRUCTIVE: clear feature state
```

`make up` publishes Redis on host `6380` and the Redpanda external listener on
`19092`; containers use `redis:6379` and `redpanda:9092`.

### Parity and reference

`generator/reference_features.py` is an independent Python implementation of
the 25 features. It reads the dataset in arrival order and applies the same
event-time rules. Use:

```bash
make parity-golden
make parity-small
make parity-run42
```

Each target runs the Python reference, replays the same event stream through
`cmd/feature-parity` against Redis DB 15, and compares every field with a
`1e-9` float tolerance. The target flushes that Redis DB first (`--reset`).
The committed oracle fixture lives in `tests/golden/` and is exercised by
`pytest`.

### Tests and coverage

```bash
make test              # Go race tests + pytest
make test-integration  # Redis/Redpanda testcontainers, coverage
```

The Phase 2 coverage gate is 85% for `internal/events`,
`internal/features` and `internal/ingest`. Tests start pinned Redis 7.4.1 and
Redpanda 24.3.4 containers when `FADE_TEST_REDIS_ADDR` /
`FADE_TEST_KAFKA_BROKERS` are not supplied.

### Benchmarks

```bash
RATE=1000 DURATION=10s make bench-feature
FADE_TEST_REDIS_ADDR=localhost:6380 go test ./internal/features \
  -run '^$' -bench BenchmarkFeatureCompute -benchmem
```

The benchmark builds events before the timer so `allocs/op` is the store hot
path, not fixture construction. `cmd/feature-load` reports p50/p95/p99 and
actual throughput at a requested rate.

## Known constraints

- Redis Cluster is unsupported: the script constructs sender and recipient
  keys and must mutate both atomically on one primary.
- Feature output is at-most-once relative to committed state. If a publish
  fails after state was applied, the retry is a duplicate and emits no vector;
  Kafka producer retries cover transient broker failures, but there is no
  outbox. Use `FEATURE_OUTPUT_TOPIC=""` unless downstream can tolerate this.
- Liveness does not prove broker health; use `/readyz`.
- The local single-primary Lua path is CPU-bound in Redis. See
  [phase-2.md](phase-2.md) for measured throughput on the development host;
  the 5,000 events/s target is not reached there.
