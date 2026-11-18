# Replay and simulation

Phase 6 makes rule sets, model versions and combiner configurations testable
against labelled history before activation. The same decision code that serves
live traffic (`pipeline` + `rules` + `combiner`) runs a replay; there is no
second implementation of decisioning.

- Owner: risk platform / data science
- Status: `replay_report_version` **1**
- Related: [features.md](features.md), [rules.md](rules.md), [ml.md](ml.md),
  [explainability.md](explainability.md), [phase-6.md](phase-6.md)

## 1. Terms

| Term | Definition |
| --- | --- |
| **dataset** | Immutable tuple of `events` (arrival-ordered event stream), `labels` (event id → FRAUD/LEGIT + scenario), optional `arrivals`, `accounts` (segment metadata) and a `manifest.json` with byte sizes and SHA-256 hashes. Identified by its manifest hash. |
| **config** | A resolved bundle: rule set (`prod@14` or a draft file), model version (`model:lgbm-2026.12.1` or `none`), combiner configuration, fail policy and the snapshot of list entries and tier limits used. Identified by a config hash over the canonical JSON of everything resolved. |
| **run** | One config over one dataset (events in scope, mode, seed, state policy, warm-up). A job contains one baseline run and one to five candidate runs. |
| **job** | A queued unit of work: dataset + baseline config + candidates + options. Persisted in `replay_jobs`; artefacts under `replay/jobs/{job_id}/`. |
| **report** | Deterministic JSON (`report.json`) plus Markdown and parquet artefacts for one completed job. |

## 2. Config reference syntax

A config is a JSON object. Every field is optional except at least one of
`rules`, `model` or `combiner`.

```json
{
  "rules": "file:rules/draft.json",
  "model": "lgbm-2026.12.1",
  "combiner": {"thresholds": {"step_up": 45, "hold": 70, "block": 88}},
  "fail_policy": "rules_only"
}
```

| Reference | Meaning |
| --- | --- |
| `"prod@14"` | Active rule set named `prod`, version 14, from `rule_sets` in Postgres. The job stores the resolved content hash. |
| `"file:rules/draft.json"` | A not-yet-activated file, parsed and compiled with the same validator. Relative to the working directory; path traversal outside the repository is refused. |
| `"model:lgbm-2026.12.1"` | Model version pinned for the run. It must exist in the registry and be loadable. |
| `"model:none"` | No ML (rules-only combiner). |
| `"combiner:inline{...}"` | Combiner overrides applied on top of the rule set's combiner config. Keys follow `rules.Combiner`: weights, thresholds, floors. |

Reference resolution fails the job at validation time, before any event is
processed, if a rule set, model version or threshold is invalid or missing.
Paths are checked against the repository root and symlinks are resolved.

Rule sets are compiled exactly as activation compiles them. A draft file that
fails validation cannot be replayed; the error is reported verbatim.

## 3. Modes

### A. `full`

Recompute features from raw events through the Phase 2 feature path into an
isolated Redis namespace, then evaluate every config. Features are
config-independent: each event's features are computed **once** and shared by
all configs in the job. The ML score for a given model version is also computed
once per event and shared by every config that pins that version.

### B. `snapshot`

Reuse `features_snapshot` from decisions stored by Phase 5. This measures the
current rule/combiner configs over real production traffic and is much faster.
Events without a snapshot are counted as `skipped_no_snapshot` and excluded
from rates; the report shows the count. Snapshot mode does not recompute
features, so a changed feature implementation cannot be evaluated this way.

### C. `stored-ml`

Candidate configs that keep the baseline model reuse recorded ML outputs for
snapshot-mode events. New model versions are scored in batch, pinned to the
requested version. Mixed jobs are allowed: each event stores ML results per
model version, and each config selects its version.

## 4. State feedback policy

Replay normally updates feature state with every event, exactly like live
traffic (`state_policy: "ignore"`, the default). Decisions do not alter feature
state.

`state_policy: "apply-block"` simulates the effect of blocking: an event decided
`BLOCK` by the **baseline** config does not update windows or balances. This
creates a counterfactual, not a reconstruction of history; it is biased because
in production a blocked transaction may be retried, disputed or replaced. The
policy is printed in the report, and the field is part of the config hash.

The policy is baseline-only by design. Applying each candidate's decisions
would give each config a different feature stream, breaking comparability.

## 5. Clock, ordering and determinism

- **Event-time clock.** Feature computations use the event's timestamp, never
  wall-clock time. `--clock` is fixed at the dataset start for anything that
  needs a "now".
- **Ordering.** Events are processed in arrival order (`seq`). Per-account
  order is preserved. In `full` mode, feature state is updated in global
  arrival order; configs are evaluated after the feature for that event is
  computed.
- **Workers.** `--workers N` parallelises config evaluation and report
  accumulation across events while preserving feature order. Results are
  independent of worker count; the tests compare 1, 4 and 16 workers with a
  sequential oracle.
- **Late/duplicate handling** is Phase 2's: duplicates are counted once (first
  arrival), late events are counted as `not_scored_late`, invalid events as
  `not_scored_invalid`. They are excluded from rates and shown separately.
- **Seed** drives account sampling, bootstrap resampling and any tie-breaking.
  A job is reproducible from `dataset_hash + config hashes + seed + engine
  version`.

## 6. Labels and decision classes

Labels come from `labels.parquet` (`FRAUD`/`LEGIT`, `scenario`,
`scenario_run_id`). Only labelled events participate in rates; unlabelled events
are reported as `unlabelled`. Duplicate event ids use the first arrival.

| Class | Definition |
| --- | --- |
| alerted | decision severity ≥ STEP_UP |
| intervened | decision severity ≥ HOLD |
| blocked | decision == BLOCK |

Both event-level and account-level evaluation are defined. Every rate is
computed per event; confidence intervals resample **accounts**, because events
from one account are correlated.

## 7. Metrics

All rates carry 95% bootstrap CIs over accounts (1,000 resamples, seeded,
parallel). Counts are exact.

| Group | Metrics |
| --- | --- |
| Fraud detection | `fraud_caught_rate` (alerted recall), `fraud_intervened_rate` (≥ HOLD), `fraud_blocked_rate` |
| Customer impact | `false_positive_rate` (legit alerted), `legit_intervention_rate`, `customer_friction` (all events ≥ STEP_UP) |
| Precision | `precision_alerted`, `precision_intervened`, F2 at alerted |
| Amounts | `amount_saved` (FRAUD ≥ HOLD), `amount_missed` (FRAUD ALLOW/STEP_UP), `amount_friction` (LEGIT ≥ HOLD) |
| Workload | HOLD+BLOCK per day and per 1,000 events |
| Time to detect | per `scenario_run_id`: minutes from first fraud event to first ≥ HOLD event; loss before detection; share of runs detected |
| Scenario | recall (alerted/intervened), amount caught, time-to-detect p50/p90 |
| Segment | FPR and friction per `accounts.segment` (or `unknown`) |
| Daily | recall, FPR, workload per UTC day |
| Sweep | precision/recall over risk thresholds 10…95 step 5, from stored per-event risk |

### Cost model

`config/replay_cost.yaml` supplies:

```yaml
currency: RWF
analyst_cost_per_case: 1200
step_up_dropoff_probability: 0.08
average_fee: 250
fraud_recovery_factor: 0.35
```

`net_benefit = amount_saved * recovery_factor
             - interventions * analyst_cost_per_case
             - step_ups * dropoff_probability * average_fee`

The report prints every assumption and labels the result a modelling aid, not
an accounting figure.

### Per-rule statistics

For each config and rule: fires, share of scored events, TP, FP, precision,
unique catches (fraud caught only by that rule), overlap matrix for the top 10
rules, and mean weight contribution. Shadow rules report “would have fired”
counts and TP/FP, but never affect decisions.

### Ablation (`--ablate`)

Leave-one-rule-out and leave-ML-out deltas in recall (alerted/intervened) and
FPR for the candidate, computed from per-event fired-rule bitsets. This is
single-factor ablation, not a causal claim.

## 8. Delta analysis

For each candidate:

- 4×4 transition matrix of decision classes, counts and amounts;
- events newly intervened, newly allowed, newly blocked, with reasons from both
  configs (`deltas.parquet`);
- top 50 by amount for fraud and for legit separately;
- McNemar exact test on alerted vs not, for fraud and legit;
- paired bootstrap CI of recall and FPR differences (accounts resampled with the
  same index set for both configs).

With more than three candidates the report adds a multiple-comparison caveat;
p-values are not adjusted.

## 9. Report schema

`report.json` (version `"replay_report_version": "1"`) is deterministic:
running the same job twice yields byte-identical bytes except fields under
`manifest.json`. Wall times live only in the manifest.

Top-level keys:

```json
{
  "replay_report_version": "1",
  "job_id": 41,
  "dataset": {"name": "run42", "hash": "...", "from": "...", "to": "...",
              "warmup_days": 14, "events": 5675680, "scored": 5600000},
  "mode": "full", "state_policy": "ignore", "seed": 11,
  "engine_version": "0.7.0",
  "configs": [{"label": "baseline", "rules": "prod@14", "rules_hash": "...",
               "model": "none", "combiner": {...}, "config_hash": "..."}],
  "summary": {"baseline": {...}, "candidates": [{...}]},
  "scenarios": [...], "rules": [...], "daily": [...], "segments": [...],
  "sweep": [...], "time_to_detect": {...}, "transition": [...],
  "deltas": [...], "cost_model": {...}, "caveats": [...]
}
```

Per-decision parquet columns: `event_id, label, scenario, decision, risk,
rule_score, ml_score, fired_rule_ids, amount, event_time, account_segment` per
config plus a `config_label` column.

## 10. Limitations

- Synthetic or reconstructed labels are noisy; precision/recall inherit label
  error.
- Snapshot mode cannot evaluate feature-code changes and over-represents traffic
  that reached a decision.
- `apply-block` is a counterfactual with feedback effects it cannot model.
- Bootstrap CIs assume exchangeable accounts; a handful of accounts can dominate
  a scenario. Scenarios with fewer than 30 fraud events get a power warning and
  are excluded from gate scenario checks.
- Threshold tuning on the same period it is evaluated on overfits; use
  `--tune-on`/`--eval-on`.
- Cross-entity features are evaluated in arrival order; parallel config
  evaluation does not reorder feature state.
- The gate's statistical rule is deliberately conservative for regressions: a
  regression is declared only when the one-sided CI excludes the allowed
  slack. This trades power for a low false-alarm rate.
