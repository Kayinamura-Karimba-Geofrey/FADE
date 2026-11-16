# FADE Phase 4 ML specification

Status: **implemented** for `feature_schema_version = "1.0.0"`. This document is
normative for training, evaluation and serving. Results and measured values are
in `docs/phase-4.md` and `ml/reports/<version>/report.md`.

## 1. Model feature set

Models consume all 25 Phase 2 features from `docs/features.md` in table order,
plus two encoded event fields. Identifiers and metadata are excluded:
`event_id`, `feature_schema_version`, account/recipient/device/SIM hashes,
session and scenario identifiers. `scenario` is reporting-only and is never a
model input.

| Order | Name | Model dtype | Default | Encoding |
| ---: | --- | --- | --- | --- |
| 1 | `velocity_1m` | int32 | 1 | identity |
| 2 | `velocity_10m` | int32 | 1 | identity |
| 3 | `velocity_1h` | int32 | 1 | identity |
| 4 | `amount_sum_10m` | int64 | event amount | identity |
| 5 | `amount_sum_24h` | int64 | event amount | identity |
| 6 | `distinct_recip_24h` | int32 | 1 | identity |
| 7 | `amount_vs_avg_30d` | float32 | 1.0 | identity |
| 8 | `amount_zscore_30d` | float32 | 0.0 | identity |
| 9 | `near_limit_ratio` | float32 | 0.0 | identity |
| 10 | `small_txn_run_1h` | int32 | 0 | identity |
| 11 | `new_device` | int8 | 0 | bool to 0/1 |
| 12 | `new_geo_km` | float32 | 0.0 | identity |
| 13 | `sim_changed_72h` | int8 | 0 | bool to 0/1 |
| 14 | `time_since_sim_change_min` | int32 | -1 | identity |
| 15 | `acc_per_device_7d` | int32 | 1 | identity |
| 16 | `recip_new_for_sender` | int8 | 1 | bool to 0/1 |
| 17 | `recip_distinct_senders_24h` | int32 | 0 | identity |
| 18 | `recip_cashout_lag_min` | int32 | -1 | identity |
| 19 | `inbound_outbound_ratio_1h` | float32 | 1.0 | identity |
| 20 | `night_hour` | int8 | 0 | bool to 0/1 |
| 21 | `cycle_len_2_4` | int32 | 0 | identity |
| 22 | `agent_cico_ratio_24h` | float32 | 0.0 | identity |
| 23 | `agent_float_delta_pct` | float32 | 0.0 | identity |
| 24 | `account_age_days` | int32 | observed | identity |
| 25 | `kyc_tier` | int32 | observed | identity |
| 26 | `txn_type` | int32 | 0 | versioned vocabulary |
| 27 | `channel` | int32 | 0 | versioned vocabulary |

`config/ml_vocab.yaml` is the single source of category codes. Known values use
codes starting at 1; code **0 is reserved for unknown/unseen values** at serve
time. Changing an existing code or feature order requires a new model feature
schema version. `features.json` is written into every immutable artifact
directory and is the serving contract:

```json
{"schema_version": "1.0.0", "features": [
  {"name": "velocity_1m", "dtype": "int32", "default": 1, "vocab": null}
]}
```

Encoding happens once with the exact order in `features.json`. The Go client
generates its field order from the same committed file and the build fails on a
mismatch.

## 2. Target

`y = 1` for `labels.parquet:label == "FRAUD"`, `y = 0` for `LEGIT`. Labels are
joined by `event_id` and are never derived from features, scenario names,
account aggregates or transaction outcomes. `scenario` is kept for per-scenario
reporting only and is dropped before fitting.

## 3. Time splits and purge

The run42 dataset spans 90 Kigali days from `2026-01-01`. Boundaries are
midnight Africa/Kigali and configurable in `ml/config.py`:
`train_days=(1,56)`, `valid_days=(57,70)`, `test_days=(71,90)`.

A **24-hour purge gap** follows each labelled split. Rows in a purge gap carry
`split=purge` and are excluded from fitting, early stopping, calibration and
evaluation. No row is ever shuffled across a time boundary. The exact UTC
boundary timestamps are recorded in `metrics.json` and the training table
manifest.

## 4. Metrics

Computed on validation and test unless stated otherwise:

- PR-AUC (average precision) and ROC-AUC.
- Recall and precision at operating points where the false-positive rate over
  LEGIT events is **0.5%, 1% and 2%**. Thresholds are selected on the same
  labelled split and the achieved FPR is reported alongside them.
- Per-scenario recall at the 1% FPR threshold.
- Brier score, 10-bin expected calibration error (ECE), and reliability-curve
  points.
- Fraud/legit score percentiles (1, 5, 25, 50, 75, 95, 99).
- Amount-weighted recall: caught fraud amount / total fraud amount at 1% FPR.

Splits with no positive examples or no negatives produce an explicit error
rather than a silently dropped metric.

## 5. Scores

- `fraud_prob ∈ [0,1]` is the calibrated LightGBM probability.
- `anomaly_score ∈ [0,1]` is the percentile rank of the raw isolation-forest
  score against the empirical CDF of LEGIT training scores; 1 is most
  anomalous.
- `ml_score = round(100 * (w_fp * fraud_prob + w_an * anomaly_score))` with
  default weights `w_fp=0.7`, `w_an=0.3`. Weights live in the combiner config
  so replay can evaluate a candidate rule set.
- `risk = round(a * rule_score + b * ml_score)` with defaults
  `a=0.55, b=0.45` when ML is `OK`; when ML is degraded or absent
  `risk = rule_score`.
- `risk = max(risk, floor(rule_action))`; hard rule actions always win.
- Final decision uses `STEP_UP=40`, `HOLD=65`, `BLOCK=85` unless the active
  rule set overrides them.
- If `ml_status == OK` and `fraud_prob >= 0.98`, the decision is at least
  `HOLD` with reason `ML_VERY_HIGH_CONFIDENCE`.
- If `rule_score == 0` and `ml_score >= 85`, the decision is at most `HOLD`
  unless `allow_ml_only_block=true`.
- Override precedence is unchanged: BLOCK list > rule BLOCK > ALLOW list >
  thresholds, with the tier ceiling still applied.
- `ml_status` is one of `OK`, `DEGRADED`, `NOT_CONFIGURED`, `STALE_MODEL`.

## 6. Model versioning and registry

Versions are `name-YYYY.MM.N`, for example `lgbm-2026.11.1`. An artifact
directory is immutable after registration and contains:

```
model.txt                 LightGBM native model
model.onnx                optional ONNX export
calibrator.json           isotonic/Platt breakpoints
features.json             ordered serving contract
isoforest.joblib          sklearn isolation forest
isoforest_cdf.json        1001 LEGIT-train quantiles
shap_background.parquet   1000 seeded LEGIT train rows
golden_predictions.json   200 deterministic serving vectors
training_quantiles.json   PSI bins
metrics.json              machine-readable evaluation
manifest.json             sha256 of every artifact
model_card.md             intended use and limitations
```

`ml/models/registry.json` records version, status (`candidate`, `active`,
`retired`), artifact hash, creation time and promotion history.

Promotion gate (all must pass):

1. test PR-AUC ≥ active − 0.005;
2. test recall at 1% FPR ≥ active − 1 percentage point;
3. ECE ≤ 0.07;
4. no scenario recall regression > 5 points;
5. score PSI against the active model on a fixed reference slice < 0.25;
6. a human-readable report is attached;
7. for the first model, the same checks are made against the best rules-only
   and logistic-regression baselines.

Promotion is written to `registry.json` and, when Postgres is configured, to
the hash-chained `audit_log` with action `MODEL_PROMOTE`.

## 7. Serving contract

The scorer is a FastAPI/uvicorn service. Inputs are the 27 features above.
Missing features return 422 with the list; unknown extras are ignored and
counted; NaN/Inf are rejected. Assembly uses NumPy arrays in `features.json`
order, never pandas on the hot path.

`POST /score` returns model version, fraud probability, anomaly score, ML score,
latency and optional top-5 SHAP contributions. SHAP is computed only when
`explain=true`; the Go client requests it only when the provisional rule risk
is at least the step-up threshold or an analyst explicitly asks for an
explanation. SHAP has a 10 ms budget; if exceeded the response sets
`explain_skipped=true`.

Startup performs 100 warm-up predictions and verifies 200 golden vectors within
`1e-6`; it refuses to start on a mismatch. `/admin/reload` validates a candidate
artifact fully before atomically swapping it. `/readyz` requires a loaded model
and a successful warm-up.

## 8. Training-serving skew

The same 27-name list and order are used by the Python trainer, the FastAPI
service and the Go client. A committed parity fixture built from 50,000 run42
events compares reference and Go feature values before training. After
training, 5,000 test events are scored through both feature paths and must agree
within `1e-6`. Feature permutations must be order-independent, and the scorer's
golden-vector test covers the final assembly and calibration path.

## 9. Reproducibility

Every training command fixes Python, NumPy, scikit-learn, LightGBM and SHAP
versions (recorded in `metrics.json`), seeds, thread counts and deterministic
flags. Dataset file hashes and label/feature table hashes are recorded. Two
runs with the same inputs and seed must produce identical metrics and artifact
hashes, except for an explicitly proven LightGBM nondeterministic field.
