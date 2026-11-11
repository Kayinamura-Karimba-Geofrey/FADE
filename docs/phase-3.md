# Phase 3 - Rules engine and synchronous decision API

Phase 3 turns the Phase 2 feature store into a rules-only decision service. A
transaction is validated, scored by a hot-reloadable rule set, combined with
KYC/list overrides and returned in the Phase 1 contract shape while its decision
is persisted asynchronously. ML scoring is intentionally absent:
`ml_status` is always `NOT_CONFIGURED`.

## Delivered

| Area | Where |
| --- | --- |
| Rule model, validation, compilation, canonical hash | `internal/rules/{types,compile,hash,interpret,parse}.go` |
| Allocation-free evaluator, combiners, reason rendering | `internal/rules/{eval,combine,reason,fields}.go` |
| Hot reload, version history, rollback, audit chain | `internal/rules/{store,audit,postgres}.go` |
| Decision pipeline, idempotency, overrides, async persistence | `internal/pipeline/*.go` |
| HTTP API, JWT/RBAC, rate limits, CORS, gzip, request IDs | `internal/api/*.go` |
| OpenAPI contract and generated types | `api/openapi.yaml`, `internal/api/oapi/types.gen.go` |
| Decision API / demo client / token tool | `cmd/{decision-api,decide-demo,dev-token}` |
| Replay-lite tuner and dataset reader | `cmd/replay-lite`, `internal/dataset` |
| Starter rule pack | `rules/prod.json` |
| Migrations | `migrations/0002_phase3.{up,down}.sql` |
| Load and profiling assets | `bench/k6`, `bench/results`, `bench/results/pprof-eval.txt` |
| Alerts and dashboard | `deploy/alerts.yml`, `deploy/grafana/dashboards/fade-rules-api.json` |

## HTTP surface

| Method | Path | Scope | Notes |
| --- | --- | --- | --- |
| POST | `/v1/decide` | `decisions:write` | One decision; `Idempotency-Key` optional |
| POST | `/v1/decide/batch` | `decisions:write` | 1-500 events; partial success |
| GET | `/v1/decisions/{id}` | `decisions:read` | Stored decision |
| GET | `/v1/decisions` | `decisions:read` | Keyset pagination and filters |
| POST | `/v1/rules/reload` | `rules:write` | Compiles then atomically swaps |
| GET | `/v1/rules/active` | `rules:read` | Active document, hash and source |
| GET | `/v1/rules/versions` | `rules:read` | Last 20 activations |
| PUT | `/v1/rules/{id}` | `rules:write` | Create or edit one rule (new version) |
| POST | `/v1/rules/validate` | `rules:read` | Aggregated validation errors |
| GET/POST | `/v1/lists` | `lists:read` / `lists:write` | BLOCK/ALLOW administration |
| DELETE | `/v1/lists/{id}` | `lists:write` | Remove entry |
| GET | `/v1/metrics/rules` | `rules:read` | Fires, TP/FP labels and shadow hits |
| GET | `/healthz`, `/readyz`, `/version`, `/metrics` | none | Operational endpoints |

Tokens are HS256 JWTs (`sub`, `role`, `iss`, `exp`). Roles:

| Role | Scopes |
| --- | --- |
| `service` | decide, read decisions, read rules |
| `analyst` | read decisions, read rules, read lists |
| `admin` | everything, including rule and list writes |

Rate limits default to service 5,000 rps / burst 10,000, analyst 50/100 and
admin 20/40, configurable through `SERVICE_RPS`, `SERVICE_BURST`,
`ANALYST_RPS`, `ANALYST_BURST`, `ADMIN_RPS`, `ADMIN_BURST`. Errors are RFC 7807
`application/problem+json` with a stable `type` under
`https://fade.local/problems/` and the request id.

## Decision path

1. Validate the event (unknown fields rejected, ULID/time/fee/geo checks).
2. Reserve `idem:{key}` in Redis (24 h TTL). Concurrent duplicates poll for at
   most 50 ms; a different payload is `422 IDEMPOTENCY_CONFLICT`.
3. Compute Phase 2 features with a 20 ms cap inside a 40 ms request budget.
4. Evaluate the active rule set (zero heap allocations).
5. Combine enforce weights (capped at 100), take the maximum action, choose
   thresholds; shadow rules never affect the decision.
6. Apply overrides in order: BLOCK list > rule BLOCK > ALLOW list > thresholds;
   a tier-limit breach raises the decision to at least HOLD.
7. Return `decision_id = dec_<ULID>`; enqueue persistence and Kafka publish.

Feature/Redis failures use the rule set's fail mode (`open`, `step_up`,
`closed`); evaluator panics become a `RULE_PANIC` degraded decision. The
concurrency semaphore sheds with 503 when `MAX_CONCURRENCY` is exhausted.

Measured on this machine (`go test ./internal/pipeline -bench`): 33 allocs per
decision. Rule evaluation itself:

| Benchmark | Result |
| --- | --- |
| 200-rule evaluate | 4.7 µs/op, 0 B/op, 0 allocs/op |
| 200-rule evaluate (parallel) | 1.2 µs/op, 0 allocs/op |
| 1-rule evaluate | 28 ns/op, 0 allocs/op |
| 200-rule compile | 4.8 ms/op |

`bench/results/pprof-eval.txt` holds the CPU profile and top functions. The
hottest path is predicate dispatch (`compileCondition.func3` / `intOp`), not
allocation or reflection.

## Rule document

```json
{
  "rule_set": "prod",
  "version": 1,
  "author": "risk-team",
  "config": {
    "combiner": { "thresholds": {"step_up": 40, "hold": 65, "block": 85} },
    "fail_mode": {"P2P_SEND": "step_up", "default": "step_up"}
  },
  "rules": [
    {
      "id": "R-VEL-002", "name": "burst sends", "enabled": true,
      "mode": "enforce", "weight": 20, "action": "STEP_UP",
      "when": {"f": "velocity_1m", "op": ">=", "v": 3},
      "reason": "{velocity_1m} sends in 1 min",
      "tags": ["velocity"], "applies_to": ["P2P_SEND"]
    }
  ]
}
```

Identifiers are the 25 Phase 2 features plus `amount`, `fee`, `txn_type`,
`channel`, `kyc_tier` and `balance_before_ratio`. Operators are
`== != > >= < <= in not_in`; combinators are `all`, `any`, `not` to depth 6.
Validation aggregates every error, enforces the `^R-[A-Z]{2,6}-\d{3}$` id
pattern, weights 0-100, unique ids, supported actions and a maximum of 256
rules. The canonical SHA-256 covers every field and is stored with each version.

## Hot reload and rollback

`fsnotify` reloads after a 300 ms debounce; `POST /v1/rules/reload` uses the same
path. A candidate is fully compiled before `atomic.Pointer` swap, so in-flight
requests finish on the old set. Rejections keep the previous set and increment
`fade_rules_reload_total{result="rejected"}`.

The manager records evaluator failures in one-second buckets. If more than 1% of
at least 20 evaluations fail during the 30 seconds after an activation, it
restores the previous set once and increments
`fade_rules_rollback_total`. The fault-injection seam
`rules.SetEvalHook(func(rules.EvalContext))` drives this test.

Startup order is Postgres active version, newest valid file, then an allow-all
empty set (reported as `degraded` on `/readyz`).

## Persistence

Decisions are written off the hot path by a 20,000-item queue with 200 ms /
500-row batches. Postgres failures spool NDJSON to disk (1 GiB rotation) and are
drained on the next tick; inserts use `ON CONFLICT DO NOTHING` so replay is
idempotent. The decisions topic is published best effort.

Migration `0002_phase3` is additive and mirrors `EnsureSchema`: list provenance
columns, `decisions.shadow_hits`, and account/decision/event indexes. Existing
Phase 0/2 databases can start the API without a migration run.

## Starter pack and tuning

`rules/prod.json` has 19 rules (16 enforce, 3 shadow) and covers all seven
scenarios. Tuning evidence, per-rule precision and scenario recall are in
[rules.md](rules.md#starter-pack-tuning). Headline results:

- `data/small` (38,584 events, 164 fraud): 145 caught (88.41%), 1.46% FPR,
  104 blocked, 0 degraded.
- `data/run42` first 20,000 events (39 fraud): 25 caught (64.10%), 1.74% FPR,
  0 degraded. The prefix contains only STRUCTURING and VELOCITY labels; the full
  5.68M-event replay was not run (see deviations).

The only material false-positive source is the deterministic KYC tier ceiling
(495 of 559 FPs on `data/small`), which is an intended override rather than a
rule. CIRCULAR recall is capped by the frozen Phase 2 `cycle_len_2_4` feature:
4 of 8 labelled events expose a cycle.

## Load test

`bench/k6/decide.js` is a constant-arrival-rate test. Accounts and devices are
randomised per iteration so the test measures API capacity, not the cost of a
single hot account's growing 24 h windows.

| Run | Rate | Duration | Requests | Failed | p95 | p99 | Dropped |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `k6-decide-20261111-1000` | 1,000/s | 30 s | 29,990 | 0.00% | 33.8 ms | 61.9 ms | 11 |
| `k6-decide-20261111` | 500/s | 20 s | 10,001 | 0.00% | 9.4 ms | 38.6 ms | 0 |
| `k6-batch-20261111` | 5 VUs, 10 events | 10 s | 1,285 batches | 0.00% | 42.8 ms | 64.7 ms | 0 |

Raw logs and k6 summary JSON are in `bench/results/`. The 1,000/s run stays
inside the 50 ms p95 target on a single API process with a local Redis; the
`dropped_iterations` count is k6's fixed 30 s window ending, not rejected API
requests.

## Operational assets

- `deploy/alerts.yml`: API down, 5xx > 1%, decision p95 > 40 ms, shedding,
  rollback, rejected reload, persistence backlog and spool growth.
- `deploy/grafana/dashboards/fade-rules-api.json`: decisions by class, top rule
  hits, stage p95 vs budget, rule-set/persistence health, reloads/rollbacks,
  fail-policy decisions.
- `make pprof-eval` regenerates `bench/results/{pprof-eval.txt,rules-cpu.pprof,rules-mem.pprof}`.

## Exit checks

| # | Check | Result |
| --- | --- | --- |
| 1 | `go build ./...` | Pass |
| 2 | `go run ./cmd/rules-validate -dir rules` | `OK rules/prod.json rule_set=prod version=1 rules=19 hash=171024faa9af` |
| 3 | Rule unit/property/store tests | Pass (operator table, aggregate validation, combiners, golden reasons, panic recovery, canonical hash, 100k-case reference equivalence) |
| 4 | API auth/rate-limit/body-limit/idempotency tests | Pass |
| 5 | OpenAPI contract tests | Pass for decide request/response, version and 401 problem |
| 6 | Postgres testcontainers integration (decisions + lists) | Pass |
| 7 | Rule evaluation allocations | 0 allocs/op |
| 8 | Decision allocations | 33 allocs/decision (< 50) |
| 9 | Hot reload + rejection + auto-rollback + audit chain | Pass |
| 10 | Async persistence, spool and Kafka publisher | Pass |
| 11 | `rules/prod.json` replay | 88.41% recall, 1.46% FPR on `data/small` |
| 12 | Large-dataset replay | 20,000-event `data/run42` prefix: 64.10% recall, 1.74% FPR |
| 13 | k6 load | 1,000 req/s, 0% failures, p95 33.8 ms |
| 14 | Docs, migration, dashboard and alerts | Present |

## Deviations and limitations

- **run42 sample instead of full replay.** A full 5.68M-event replay at the
  observed ~370 events/s takes roughly four hours and was not run in this phase.
  The 20,000-event prefix is reported and labelled as such; full-dataset
  replay/tuning stays a Phase 6 deliverable.
- **replay-lite uses Redis DB 15 and `FLUSHDB`.** The frozen Phase 2 Lua script
  builds its own keys, so isolation is by database rather than key prefix.
  Never point it at a database shared with production state.
- **replay-lite uses a 365-day late tolerance.** The dataset is not strictly
  ordered, so this is required to avoid dropping out-of-order events.
- **ML scoring is not implemented.** `ml_status` is `NOT_CONFIGURED`,
  `ml_score` is null and `NoopScorer` is the only scorer.
- **k6 runs against one local API process with local Redis and no Postgres.**
  The load numbers validate the decision path, not a multi-node deployment or
  database write throughput.
