# Chaos results (2026-11-19)

Engine 1.0.0, commit `ea6e14edd5cc`. PASS 11 / FAIL 0 / NOT_RUN 3.

| ID | Result | Measured |
|---|---|---|
| C1 | PASS | outage_status=200 recovered_without_restart=yes backfill=fade features backfill |
| C2 | PASS | stalled_status=200 recovered=yes |
| C3 | PASS | state_recovered=yes status_before=200 status_after=200 |
| C4 | PASS | degraded_status=DEGRADED recovery_status=OK |
| C5 | PASS | decisions_unaffected=yes spool_drains=yes codes=200/200 |
| C6 | PASS | response_unaffected=yes status=200 (persister backpressure observed in logs) |
| C7 | PASS | sync_api_unaffected=yes codes=200/200 |
| C8 | NOT_RUN | iptables DROP needs CAP_NET_ADMIN on the host; not permitted in this environment |
| C9 | NOT_RUN | filling a spool volume needs a loopback mount (root); not permitted |
| C10 | PASS | compile failure rejected by /v1/rules/validate (HTTP 422) |
| C11 | NOT_RUN | changing the host clock needs root; event-time windows are tested in unit tests (features_test.go) |
| C12 | PASS | restarted=yes idempotent_replay_returns_stored_decision=yes |
| C13 | PASS | volumes_intact=yes migrations_idempotent=yes |
| C14 | PASS | recovered_within_ms=5 shed_with_retry_after=verified_by_k6_503 |
