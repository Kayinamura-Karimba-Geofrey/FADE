# Changelog

All notable changes to FADE are recorded here. The project follows
[Semantic Versioning](https://semver.org/) and keeps performance, chaos and
detection evidence under `bench/results/<date>/`.

## [1.0.0] - 2026-11-19

### Added

- End-to-end decision pipeline: Redpanda ingestion, Go feature worker, rules
  engine, Python ML scorer, combiner, explainability and PostgreSQL audit trail.
- Analyst dashboard (Next.js) with case queue, decision detail, replay and rule
  editing.
- Case worker with SLA tracking and idempotent case transitions.
- Replay engine and `fade replay run` for offline evaluation of rule/model
  candidates against a labelled dataset.
- `fade features backfill` to rebuild feature state after a Redis loss.
- Micro-batching for ML scoring (`internal/scoring/microbatch.go`).
- Benchmark harness (`bench/k6`, `scripts/bench_run.py`) and chaos harness
  (`bench/chaos/run.sh`) with committed JSON/Markdown evidence.
- Detection-quality evaluation script (`scripts/detection_report.py`).
- Release documentation: `docs/benchmarks.md`, `docs/failure-modes.md`,
  `docs/security.md`, `docs/known-issues.md`.

### Changed

- Redis client tuned with dial/read/write timeouts and a bounded pool so a Redis
  outage degrades to `STEP_UP` instead of hanging the request.
- ML scorer caps BLAS/OpenMP threads to one and serves `/score/batch` with a
  vectorised path.
- Decision API ML timeout is 50ms and the feature stage budget is 20ms; both are
  visible in `/metrics`.

### Fixed

- Redis DNS lookup could block a decision for ~12s during an outage.
- A stale idempotency record could keep the circuit breaker open after an ML
  scorer restart.
- Replay could not load the LightGBM model without `pyarrow`.
- `fade replay run` now reports the engine version and honours `--accounts-sample`.

### Known issues

See `docs/known-issues.md`. The S1 steady-state target (p99 < 50ms at 1000 rps)
is not met on the reference laptop: the median run recorded p99 90.08ms with
`FEATURE_TIMEOUT` degradation. This is reported as a measured limitation, not a
release blocker hidden behind a weaker threshold.

## [0.9.0] - 2026-11-12

- Phase 7 operations work: backup/restore runbook, rolling upgrade, alert rules,
  Grafana overview board.

## [0.8.0] - 2026-11-05

- Phase 6: case management, analyst feedback loop, SLA policy.

## [0.7.0] - 2026-10-29

- Phase 5: explanation payloads, SHAP attributions, tamper-evident audit chain.

## [0.6.0] - 2026-10-22

- Phase 4: ML scorer service, model registry, shadow scoring.

## [0.5.0] - 2026-10-15

- Phase 3: rules engine, rule compilation/validation, decision API.

## [0.4.0] - 2026-10-08

- Phase 2: feature worker and Redis feature state.

## [0.3.0] - 2026-10-01

- Phase 1: event ingestion and Redpanda topics.

## [0.2.0] - 2026-09-24

- Phase 0: local infrastructure, Compose profiles, observability skeleton.
