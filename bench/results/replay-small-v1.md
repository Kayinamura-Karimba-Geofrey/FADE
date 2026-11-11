# Replay-lite: data/small

- Rule set: `prod` v1
- Events: 38584 (fraud 164, legit 38420)
- Caught (>= STEP_UP): 71 (43.29%)
- Blocked (>= HOLD): 55
- FPR: 17.08%
- Degraded: 0
- Elapsed: 75.9s

## Decisions

| Decision | Count |
| --- | ---: |
| ALLOW | 31951 |
| STEP_UP | 1427 |
| HOLD | 5206 |
| BLOCK | 0 |

## Per-scenario recall

| Scenario | Fraud | Caught | Blocked | Recall |
| --- | ---: | ---: | ---: | ---: |
| AGENT_FRAUD | 9 | 8 | 8 | 88.9% |
| ATO_SIMSWAP | 4 | 1 | 0 | 25.0% |
| CIRCULAR | 8 | 5 | 4 | 62.5% |
| MULE | 9 | 9 | 6 | 100.0% |
| SOCIAL_ENG | 9 | 4 | 4 | 44.4% |
| STRUCTURING | 50 | 44 | 33 | 88.0% |
| VELOCITY | 75 | 0 | 0 | 0.0% |

## Rule tuning table

| Rule | Mode | Fires | TP | FP | Precision |
| --- | --- | ---: | ---: | ---: | ---: |
| R-AGT-002 | enforce | 4872 | 9 | 4863 | 0.2% |
| R-AGT-001 | enforce | 3103 | 9 | 3094 | 0.3% |
| R-MUL-001 | enforce | 127 | 5 | 122 | 3.9% |
| R-STR-002 | enforce | 94 | 39 | 55 | 41.5% |
| R-SOC-002 | enforce | 50 | 5 | 45 | 10.0% |
| R-ATO-001 | enforce | 29 | 9 | 20 | 31.0% |
| R-CIR-001 | enforce | 10 | 4 | 6 | 40.0% |
| R-MUL-003 | enforce | 8 | 8 | 0 | 100.0% |
| R-SOC-001 | enforce | 7 | 4 | 3 | 57.1% |
| R-RAW-001 | enforce | 5 | 5 | 0 | 100.0% |

## Top false positives

- 01KDVVAVYG9PTZN9N6TKY5M9XW HOLD risk=65 amount=72000: R-AGT-001: agent CICO ratio 72001.00 in 24h | TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDWBA0CR88DPGDC142X5TEKS HOLD risk=65 amount=124750: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDV7F18R9C5C28S06W67XEW1 HOLD risk=65 amount=78550: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDW9BW3R1M8WECVEVK32W6XR HOLD risk=65 amount=6600: R-AGT-002: agent float moved 259.25% | R-AGT-001: agent CICO ratio 52.84 in 24h
- 01KDW9414G9CYDNW21B7YNF0X3 HOLD risk=65 amount=5000: R-AGT-002: agent float moved 208.90% | R-AGT-001: agent CICO ratio 208901.00 in 24h
- 01KDW92N68Y1N5EFN2VQFMWYSF HOLD risk=65 amount=19050: R-AGT-002: agent float moved 252.65% | R-AGT-001: agent CICO ratio 51.52 in 24h
- 01KDW88W00BPFB0PKTZ74Z0PJC HOLD risk=65 amount=51150: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDW83PYR83H3G6TJVVKK73QV HOLD risk=65 amount=59000: R-AGT-001: agent CICO ratio 121301.00 in 24h | TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDW7V4HGTR7104Q300AH5QJC HOLD risk=65 amount=66200: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDW6Z14807296AT19037GKWG HOLD risk=65 amount=22250: R-AGT-002: agent float moved 233.60% | R-AGT-001: agent CICO ratio 47.71 in 24h
- 01KDW606SRAT3HTFXWA42N9TDM HOLD risk=65 amount=191750: R-AGT-002: agent float moved 203.90% | R-AGT-001: agent CICO ratio 203901.00 in 24h
- 01KDW5TF6RP5EG25KE00B3M5JE HOLD risk=65 amount=66650: R-AGT-001: agent CICO ratio 0.05 in 24h | TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDW5H898Y35R93WS0QCDZWQN HOLD risk=65 amount=88000: R-AGT-001: agent CICO ratio 13.13 in 24h | TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDW590KRBPM1MBSRK003NZ4N HOLD risk=65 amount=20000: R-AGT-002: agent float moved 298.45% | R-AGT-001: agent CICO ratio 298451.00 in 24h
- 01KDW4T8Y0C120BRR1T0TD079X HOLD risk=65 amount=60400: TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDW4SCKRAA8ABY0ZPF7XPH7X HOLD risk=65 amount=4550: R-AGT-002: agent float moved 278.45% | R-AGT-001: agent CICO ratio 278451.00 in 24h
- 01KDW2XGK85CZDZCXP5C8J8VA2 HOLD risk=65 amount=3200: R-AGT-002: agent float moved 211.35% | R-AGT-001: agent CICO ratio 43.26 in 24h
- 01KDW1DT7GZZJFPQG859NSPSR8 HOLD risk=65 amount=246150: R-AGT-002: agent float moved 273.90% | R-AGT-001: agent CICO ratio 273901.00 in 24h | TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDW15KH8MQBKXA86SBF4XYWW HOLD risk=65 amount=74000: R-AGT-001: agent CICO ratio 9.43 in 24h | TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
- 01KDW0RWA0DAD8FWWNH4CGCGD2 HOLD risk=65 amount=200450: R-AGT-002: agent float moved 208.15% | R-AGT-001: agent CICO ratio 42.62 in 24h | TIER_LIMIT_EXCEEDED: amount above the daily KYC tier limit
