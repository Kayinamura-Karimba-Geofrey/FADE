# Replay 7 — golden

Mode `full`, state policy `ignore`, seed `11`, engine `0.7.0`.

Dataset `deadbeef` (12 events, 10 scored).

## Summary

| Config | Recall (alerted) | Recall (≥HOLD) | FPR | Friction | Amount saved | Amount missed | Workload/day |
|---|---|---|---|---|---|---|---|
| baseline | 50.00% (25.00–75.00) | 25.00% (0.00–50.00) | 16.67% (0.00–33.00) | 30.00% (20.00–40.00) | 500 | 1500 | 2.0 |
| cand | 50.00% (25.00–75.00) | 25.00% (0.00–50.00) | 16.67% (0.00–33.00) | 30.00% (20.00–40.00) | 600 | 1400 | 2.0 |

Intervals are 95% bootstrap CIs over accounts; the table prints the point estimate (CI in JSON).

## Per-scenario recall

| Config | Scenario | Fraud | Alerted | ≥HOLD | Caught amount |
|---|---|---|---|---|---|
| baseline | VELOCITY | 4 | 50.0% | 25.0% | 500 |
| cand | VELOCITY | 4 | 50.0% | 25.0% | 500 |

## Rule movers (top 10 by fires)

| Config | Rule | Fires | TP | FP | Precision | Unique catches |
|---|---|---|---|---|---|---|
| baseline | R-VEL-001 | 3 | 2 | 1 | 0.667 | 1 |
| cand | R-VEL-001 | 3 | 2 | 1 | 0.667 | 1 |

## Deltas

### cand

- McNemar (fraud): baseline-only 0, candidate-only 1, p=1
- McNemar (legit): baseline-only 0, candidate-only 0, p=1
- Recall difference +1.0000 (95% CI +1.0000, +1.0000)
- FPR difference +0.0000 (95% CI +0.0000, +0.0000)

Transition matrix:

| From | To | Count |
|---|---|---|
| ALLOW | STEP_UP | 1 |

## Time to detect

- baseline: 1/1 runs detected, p50 3.0 min, p90 3.0 min, loss before detection 0
- cand: 1/1 runs detected, p50 3.0 min, p90 3.0 min, loss before detection 0

## Cost model (modelling aid, not accounting)

Currency RWF; analyst 1200/case; STEP_UP drop-off 0% × fee 250; recovery 35%.
- cand net benefit: 100

## Caveats

- Labels are the dataset's ground truth; label noise propagates to every rate.
- Confidence intervals resample accounts, not events, to respect within-account correlation.
