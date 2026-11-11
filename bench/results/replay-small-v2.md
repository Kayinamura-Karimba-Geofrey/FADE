# Replay-lite: data/small

- Rule set: `prod` v1
- Events: 38584 (fraud 164, legit 38420)
- Caught (>= STEP_UP): 145 (88.41%)
- Blocked (>= HOLD): 104
- FPR: 1.46%
- Degraded: 0
- Elapsed: 105.2s

## Decisions

| Decision | Count |
| --- | ---: |
| ALLOW | 37879 |
| STEP_UP | 76 |
| HOLD | 626 |
| BLOCK | 3 |

## Per-scenario recall

| Scenario | Fraud | Caught | Blocked | Recall |
| --- | ---: | ---: | ---: | ---: |
| AGENT_FRAUD | 9 | 9 | 9 | 100.0% |
| ATO_SIMSWAP | 4 | 4 | 0 | 100.0% |
| CIRCULAR | 8 | 4 | 4 | 50.0% |
| MULE | 9 | 9 | 2 | 100.0% |
| SOCIAL_ENG | 9 | 9 | 9 | 100.0% |
| STRUCTURING | 50 | 50 | 50 | 100.0% |
| VELOCITY | 75 | 60 | 30 | 80.0% |

## Rule tuning table

| Rule | Mode | Fires | TP | FP | Precision |
| --- | --- | ---: | ---: | ---: | ---: |
| R-VEL-002 | enforce | 60 | 60 | 0 | 100.0% |
| R-STR-001 | enforce | 54 | 51 | 3 | 94.4% |
| R-STR-002 | enforce | 39 | 39 | 0 | 100.0% |
| R-ATO-001 | enforce | 32 | 12 | 20 | 37.5% |
| R-VEL-001 | enforce | 30 | 30 | 0 | 100.0% |
| R-SOC-001 | enforce | 19 | 9 | 10 | 47.4% |
| R-AGT-002 | enforce | 19 | 9 | 10 | 47.4% |
| R-SOC-002 | enforce | 15 | 10 | 5 | 66.7% |
| R-MUL-001 | enforce | 13 | 1 | 12 | 7.7% |
| R-AGT-001 | enforce | 12 | 9 | 3 | 75.0% |
| R-CIR-001 | enforce | 10 | 4 | 6 | 40.0% |
| R-MUL-003 | enforce | 8 | 8 | 0 | 100.0% |
| R-RAW-001 | enforce | 5 | 5 | 0 | 100.0% |

## Top false positives

- 01KE1329RGRNN5SQA0WP85JZH0 HOLD risk=65 amount=100650: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDW7V4HGTR7104Q300AH5QJC HOLD risk=65 amount=66200: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVVAVYG9PTZN9N6TKY5M9XW HOLD risk=65 amount=72000: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVW7WN876FVD9PN2RN3DPYX HOLD risk=65 amount=81300: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVY7FT8V51DA8V3610BH6VW HOLD risk=65 amount=104800: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVY8MXR0QPKQ0N91AYGFRTC HOLD risk=65 amount=74450: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDVZKG0RH1WM72VJNHD6EX3N HOLD risk=65 amount=57650: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDW0RWA0DAD8FWWNH4CGCGD2 HOLD risk=65 amount=200450: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDW15KH8MQBKXA86SBF4XYWW HOLD risk=65 amount=74000: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDW1DT7GZZJFPQG859NSPSR8 HOLD risk=65 amount=246150: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDW4T8Y0C120BRR1T0TD079X HOLD risk=65 amount=60400: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDW5H898Y35R93WS0QCDZWQN HOLD risk=65 amount=88000: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDW5TF6RP5EG25KE00B3M5JE HOLD risk=65 amount=66650: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KE12PBXRK4KVRRA7HRFH8NWR HOLD risk=65 amount=90000: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDW83PYR83H3G6TJVVKK73QV HOLD risk=65 amount=59000: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDW88W00BPFB0PKTZ74Z0PJC HOLD risk=65 amount=51150: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDWBA0CR88DPGDC142X5TEKS HOLD risk=65 amount=124750: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDWG6J58VJ6FW89QA5THD6SX HOLD risk=65 amount=85900: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDWK20XGJMMWW010KA64F6YA HOLD risk=65 amount=76400: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDWKVBF04T1BWCMSY7F9JHP3 HOLD risk=65 amount=97950: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
