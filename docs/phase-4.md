# FADE Phase 4 results

Model version: `lgbm-2026.11.1` (active).

## Training data
- data/run42 (90 Kigali days), splits train/valid/test with 24h purge gaps.
- Train 3,187,768 / valid 924,288 / test 1,431,962 rows.

## Measured metrics (calibrated, test)
- PR-AUC: 0.8408
- ROC-AUC: 0.9983
- Recall @ 1% FPR: 0.9654
- ECE: 0.000084
- Per-scenario recall @ 1% FPR: AGENT_FRAUD 0.949, ATO_SIMSWAP 1.0, CIRCULAR 0.936, MULE 0.28, SOCIAL_ENG 0.986, STRUCTURING 0.996, VELOCITY 0.950.

## Targets
- LightGBM recall >= 80% at 1% FPR: met (96.5%).
- ECE < 0.05: met.
- MULE recall below 40% target: recorded, not forced (small sample, 25 test cases).