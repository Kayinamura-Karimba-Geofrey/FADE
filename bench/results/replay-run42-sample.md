# Replay-lite: data/run42

- Rule set: `prod` v1
- Events: 20000 (fraud 39, legit 19961)
- Caught (>= STEP_UP): 25 (64.10%)
- Blocked (>= HOLD): 7
- FPR: 1.74%
- Degraded: 0
- Elapsed: 50.4s

## Decisions

| Decision | Count |
| --- | ---: |
| ALLOW | 19627 |
| STEP_UP | 24 |
| HOLD | 349 |
| BLOCK | 0 |

## Per-scenario recall

| Scenario | Fraud | Caught | Blocked | Recall |
| --- | ---: | ---: | ---: | ---: |
| STRUCTURING | 6 | 6 | 6 | 100.0% |
| VELOCITY | 33 | 19 | 1 | 57.6% |

## Rule tuning table

| Rule | Mode | Fires | TP | FP | Precision |
| --- | --- | ---: | ---: | ---: | ---: |
| R-VEL-002 | enforce | 19 | 19 | 0 | 100.0% |
| R-STR-001 | enforce | 9 | 6 | 3 | 66.7% |
| R-AGT-001 | enforce | 6 | 0 | 6 | 0.0% |
| R-ATO-001 | enforce | 3 | 0 | 3 | 0.0% |
| R-RAW-001 | enforce | 2 | 2 | 0 | 100.0% |
| R-STR-002 | enforce | 2 | 2 | 0 | 100.0% |
| R-VEL-001 | enforce | 1 | 1 | 0 | 100.0% |

## Top false positives

- 01KDV7QSHGK061T1MM6XA958QB HOLD risk=65 amount=58350: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDV8GZ6RM57668TGTBTY8CSF HOLD risk=65 amount=115000: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDV8KCBGA0VT04R28W8T1EER HOLD risk=65 amount=73150: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDV8RS6RT3YXMVRZD89BZFD2 HOLD risk=65 amount=76850: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDV8Z0DR217BYKFV7QRA2W7E HOLD risk=65 amount=71900: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDV9JV68B7GSBZKQEB8G5W2M HOLD risk=65 amount=76400: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVA0TERCSY9H0CWNQDVHYP2 HOLD risk=65 amount=51000: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVA33PG4212825AWN6GQCV5 HOLD risk=65 amount=61000: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVA34NRDWV9CB7QC324QE6G HOLD risk=65 amount=617150: R-AGT-001: agent CICO ratio 617151.00 in 24h | TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVAPSJRMPWF0B5EY5W6SS0A HOLD risk=65 amount=51000: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVB704G3GPZGVVEP50ERWDV HOLD risk=65 amount=70000: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVB8580MA19Y8B1J9V58NA9 HOLD risk=65 amount=52700: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVBNTR0XJVPP01QBXX1RNPV HOLD risk=65 amount=142000: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVCMAAR7WD46N1DC3RKPRC0 HOLD risk=65 amount=64550: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVCR1F812WQ4D7NFF29CRVP HOLD risk=65 amount=206250: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVCRM102XSQ67NC9X8J1XWQ HOLD risk=65 amount=57000: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVD32ZRF7B86VBJ7H1DV5ZD HOLD risk=65 amount=54850: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVD4HVR281DVVYHA7QY9EEN HOLD risk=65 amount=92200: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVD78S07JPQXBMGYNB0M3EN HOLD risk=65 amount=60100: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVD9TT0H3RFNMJAVDD7VRZR HOLD risk=65 amount=64000: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
