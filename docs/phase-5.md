# Phase 5 — Explainability & audit: delivery and exit checks

Phase 5 makes every decision explainable, reproducible from stored facts, and
governed by a tamper-evident audit log. This page records what shipped, the
measured exit checks, the defects found while verifying, and the deviations
that remain.

- Branch: `phase/5-explain-audit`
- Tool version: `0.6.0`
- Migration: `0003_explain_audit` (see deviation below)
- Full design: [explainability.md](explainability.md), [audit.md](audit.md)

## Delivered surface

| Area | Artefacts |
| --- | --- |
| Canonical JSON | `internal/canonical` — RFC 8785 JCS, 1,019 cross-checked vectors |
| Explanations | `internal/explain` + `templates/{en,fr,rw}.yaml`; API `GET /v1/decisions/{id}/explain`; `fade explain` |
| Provenance | snapshot id, rule-set hash, model version, combiner config, feature snapshot |
| Reproduction | `fade reproduce`, `cmd/fade` tests; `decision_reproductions` table |
| Audit | `internal/audit` — chain, append-only store, streaming verifier, signed checkpoints; API `GET /v1/audit`, `GET /v1/audit/verify`, `POST /v1/audit/checkpoint`; `fade audit` |
| Privacy | `internal/redaction`, `cmd/privscan`, API wire guard and withholding |
| Retention | `internal/retention`, `fade retention`, `ensure_decisions_partition()` |
| Observability | audit/explain metrics, 5 governance alerts, `fade-governance` dashboard |
| Docs/Make | this page, explainability/audit docs, README index, `make privscan audit-verify bench-explain` |

## Exit checks (measured 2026-11-16)

| # | Check | Result | Evidence |
| --- | --- | --- | --- |
| 1 | Specs and docs written | **PASS** | `docs/explainability.md`, `docs/audit.md`, this page |
| 2 | Migration applies cleanly | **PASS** | `0001`→`0003` applied to fresh PostgreSQL 16; `retention_policy` = 1 row; both triggers present |
| 3 | Every reason traces to a stored fact | **PASS** | `TestTemplateCoverage` covers all 19 prod rules in en/fr/rw; placeholder parity enforced |
| 4 | Explanation adds < 1 ms p99 to decision path | **PASS (by design)** | rendering is off-path; analyst build 91 µs/op, customer rw 85 µs/op; decision path adds only reason params |
| 5 | Reproduction deterministic | **PASS** | 10,000/10,000 reproduced in 0.64 s; tampered score detected |
| 6 | Audit tamper evidence | **PASS** | edit→`ROW_HASH`, middle delete→`SEQ_GAP`, tail rebuild→`CHECKPOINT`, single-bit rapid property always detected |
| 7 | API contract | **PASS** | `go test ./internal/api` incl. OpenAPI request/response validation |
| 8 | No raw PII | **PASS (qualified)** | `make privscan` production surface: 0 findings in 123 files; full-repo scan reports 27,007 hits only in deliberate negative fixtures and numeric model artefacts |
| 9 | Retention and partitioning | **PASS** | `Plan` refuses windows shorter than a month; unit tests pass; partition function created by migration |
| 10 | Observability | **PASS** | 5 new alerts, governance dashboard JSON valid, metrics registered |
| 11 | Test suite | **PARTIAL** | all unit packages pass except `internal/ingest` and `internal/features`, whose `TestMain` requires Docker-in-Docker (unavailable in this environment) |
| 12 | Make and docs | **PASS** | `fade-tool`, `bench-explain`, `privscan`, `audit-verify`, `audit-checkpoint` targets; indexes updated |
| 13 | 1M-row verify at ≥100k rows/s | **FAIL** | 15,863 rows/s against Postgres (500 rows, 20 writers), 24,360 rows/s in memory; 1M rows not run |
| 14 | Git tag v0.6.0 | **WITHHELD** | tag is conditional on all checks passing; #13 is unmet |

Because check 13 failed, `v0.6.0` is intentionally **not** tagged. The branch
contains the complete Phase 5 surface; the tag should be created when the
verifier meets the throughput target on production hardware.

## Explanation cards (real CLI output)

Decision: `dec_01JA1Z6Q2M8N4P7R9T2V5X8B3C`, HOLD, score 72 (rule 60, no ML),
rules `R-VEL-001` and `R-ATO-001`.

### Analyst (en)

> HOLD with risk score 72 (rule 60, ml n/a); driven by R-VEL-001, R-ATO-001,
> velocity_10m. Primary factor R-VEL-001 (35): 12 sends within 10 minutes
> totalling 70000 RWF. R-ATO-001 (25): device activity 8 minutes after a SIM
> change. Model contribution velocity_10m = 12 (SHAP 0.31).

### Regulator (en)

The regulator view is the analyst view plus `features[]`: `velocity_10m=12`,
`amount_sum_10m=70000`, `new_geo_km=42`, `sim_changed_72h=true`, plus
rule-set hash, model version, snapshot id, combiner thresholds and the decision
hash for reproduction.

### Customer (en)

> **This transaction is on hold while we review it.**
>
> - This transaction looked unusual for this account, so we need to confirm it
>   with you.
> - We noticed a recent security change on this account, so we need to confirm
>   this transaction with you.

### Customer (rw, machine-drafted)

> **Iyi butumwa irahagaritswe mu gihe tuyisuzuma.**
>
> - Iyi butumwa yasaga nk'idasanzwe kuri iyi konti, bityo dukeneye kuyemeza.
> - Twabonye impinduka z'umutekano ziheruka kuri iyi konti, bityo dukeneye
>   kwemeza iyi butumwa.

The `rw` response carries `"language_needs_review": true`. There are no rule
ids, feature names, scores, thresholds, model details or numbers in a customer
document.

## Tamper tests

| Attack | Detection | Test |
| --- | --- | --- |
| Change one payload value | `ROW_HASH` at the edited seq | `TestTamperEditDetected` |
| Delete a middle row | `SEQ_GAP` | `TestTamperDeleteMiddleDetected` |
| Delete tail, recompute forward | `CHECKPOINT` | `TestTamperRebuildForwardDetectedByCheckpoint` |
| Flip one bit anywhere in payload/actor/hash | always a break | `TestSingleBitMutationBreaksVerify` (rapid) |
| `UPDATE` / `DELETE` / `TRUNCATE` | trigger error | integration test + psql |
| Fork under concurrency | contiguous seq, one chain | `TestConcurrentWritersKeepChainValid`, Postgres integration |

## Privacy scan

```
$ ./bin/privscan -allow '…' internal cmd api config docs migrations scripts rules deploy ml/models/registry.json
privscan: scanned 123 files, skipped 28, findings 0
```

`make privscan` prints `privscan: production surface clean`. The full-repository
scan (`make privscan-all`) intentionally reports fake numbers inside
`internal/redaction/testdata`, `internal/canonical/testdata`,
`tests/golden`, `data/golden` and the negative-test Go files. Those exist to
prove the scanner detects identifiers, not to store real ones.

## Defects found and fixed during verification

1. **Customer view leaked score fields and the decision hash.** The first
   implementation reused `View` for all audiences; a customer document still
   contained `risk_score`, `rule_score` and `decision_hash`. Fixed with a
   wire-level `View.MarshalJSON` that emits a reduced struct, so future fields
   cannot leak by accident.
2. **API and CLI no longer compiled against the audit store.** `Store.List` and
   `Store.Verify`/`Checkpoint` changed shape during the rewrite. Fixed by
   wiring `Range`/`VerifyChain`/`CheckpointService` and adding HMAC config
   (`AUDIT_HMAC_KEY`, `AUDIT_CHECKPOINT_DIR`) to the API.
3. **Legacy NULL `seq` rows broke head lookup.** `ORDER BY seq DESC` puts NULLs
   first in Postgres, so a Phase 3 row could be scanned as the head. Fixed with
   `WHERE seq IS NOT NULL ORDER BY seq DESC` and `NULLS LAST` for reads.
4. **Duplicate audit row on checkpoint.** The API wrote a second
   `audit.checkpoint` row after the service had already appended one. Removed.
5. **Checkpoint could be written unsigned.** The service correctly returned
   `ErrNoHMACKey`; the API now maps it to `503 AUDIT_KEY_MISSING` instead of a
   generic failure.
6. **`rw` had no catalog.** Added a full machine-drafted Kinyarwanda catalog and
   flagged it `needs_review`.
7. **Decision allocation budget crept by one.** Structured reason params added
   one map allocation; formatting text and params in a single pass reduced the
   regression from +2 to +1, and the Phase 3 envelope was raised from 50 to 52
   with a comment. Time cost is unchanged.

## Deviations

- **Migration number.** The Phase 5 spec calls it `0002_explain_audit`; this
  repository already has `0002_phase3`. Phase 5 is `0003_explain_audit`. The
  schema content matches the spec's intent.
- **Throughput target.** Verification measures 15.9k rows/s against Postgres in
  this environment, not 100k rows/s. The operation is forensic/maintenance, not
  on the decision path; no attempt was made to hide the gap.
- **Languages.** `fr` and `rw` are machine-drafted and flagged. Native review
  is outstanding.
- **Counterfactuals** describe score movement against the stored thresholds,
  not real-world actions (see explainability §5).
- **Integration tests** that use testcontainers cannot run inside the Docker
  toolchain used here (no rootless Docker inside the container). The audit
  integration test was run separately with `--network host` against a real
  PostgreSQL 16 instance.
