# Detection quality (partial replay window)

Generated 2026-11-19T13:10:27.580659+00:00 from `/tmp/replay-eval-ignore/decisions.parquet`.

Window: 0.99 days, 68,800 events, 207 fraud events.

| config | events | recall | 95% CI | FPR | precision | recall @ ≤1% FPR |
| --- | --- | --- | --- | --- | --- | --- |
| baseline | 68,800 | 87.9% | 82.8%–91.7% | 1.34% | 16.5% | 70.0% |
| combined | 68,800 | 88.9% | 83.9%–92.5% | 1.34% | 16.7% | 70.5% |
| ml-only | 68,800 | 90.3% | 85.5%–93.7% | 1.34% | 16.9% | 72.9% |

## Per-scenario recall

| scenario | fraud events | baseline | combined | ml-only |
| --- | --- | --- | --- | --- |
| AGENT_FRAUD | 9 | 11.1% (1/9, CI 2.0%–43.5%) | 33.3% (3/9, CI 12.1%–64.6%) | 66.7% (6/9, CI 35.4%–87.9%) |
| ATO_SIMSWAP | 8 | 50.0% (4/8, CI 21.5%–78.5%) | 50.0% (4/8, CI 21.5%–78.5%) | 50.0% (4/8, CI 21.5%–78.5%) |
| STRUCTURING | 120 | 100.0% (120/120, CI 96.9%–100.0%) | 100.0% (120/120, CI 96.9%–100.0%) | 100.0% (120/120, CI 96.9%–100.0%) |
| VELOCITY | 70 | 81.4% (57/70, CI 70.8%–88.8%) | 81.4% (57/70, CI 70.8%–88.8%) | 81.4% (57/70, CI 70.8%–88.8%) |

Recall lift over rules-only: **+1.0 points**.
McNemar baseline vs combined: 4 newly caught, 0 lost, p=0.125.

Worst-segment FPR ratio: **2.39×**.

_Caveat: this is a partial window (the replay was stopped before the full 90 days). Confidence intervals are correspondingly wide; treat the numbers as evidence of the measurement pipeline, not as the final release recall._
