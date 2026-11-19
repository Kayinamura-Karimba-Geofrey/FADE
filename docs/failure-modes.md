# Failure modes

Measured by `bench/chaos/run.sh` against the Compose stack. The canonical
result is [`bench/results/2026-11-19/chaos.json`](../bench/results/2026-11-19/chaos.json);
this page is a readable summary, not a second source of truth.

| ID | Scenario | Expected behavior | Measured outcome |
|---|---|---|---|
| C1 | Redis SIGKILL 30 s | fail policy applied, no 5xx, recovery without restart | PASS. `/v1/decide` returned 200 during the outage; the service recovered without a restart. Feature windows miss events during the outage; `fade features backfill --from --to` is idempotent (tested in `cmd/fade/features_backfill_test.go`). |
| C2 | Redis +50 ms latency | feature timeout budget triggers fail policy; recovers | PASS. Requests returned 200 while stalled and recovered. |
| C3 | Redis restart with AOF | state recovered; counters continue | PASS. Status 200 before and after; state recovered. |
| C4 | ML scorer killed | circuit breaker, DEGRADED, recovery | PASS. `/readyz` reported `DEGRADED` during the outage and `OK` after recovery. |
| C5 | Postgres killed 2 min | decisions unaffected, spool grows and drains | PASS. Decisions unaffected; spool drained; all probed requests returned 200. |
| C6 | Postgres slow commits | persister backpressure, no response-latency impact | PASS. Responses unaffected at 200; backpressure visible in persister logs. |
| C7 | Redpanda down 5 min | sync API unaffected; consumers resume | PASS. Sync API returned 200 throughout. |
| C8 | Network partition (iptables DROP) | timeouts honored, no goroutine pile-up | **NOT_RUN.** `iptables` needs `CAP_NET_ADMIN` on the host. |
| C9 | Disk full on spool/log volume | graceful degradation, clear alerts, no crash loop | **NOT_RUN.** Filling the spool volume needs a loopback mount (root). |
| C10 | Bad rule set deployed | rejected / auto-rollback | PASS. `/v1/rules/validate` rejected the compile failure with HTTP 422. |
| C11 | Clock skew ±5 min | event-time windows unaffected | **NOT_RUN.** Changing the host clock needs root; event-time window logic is covered by unit tests. |
| C12 | Container restart | restart, idempotent replay, no duplicate persistence | PASS. Restart succeeded; replay of the same idempotency key returned the stored decision. |
| C13 | Total stack restart (down/up with volumes) | all state intact, migrations idempotent | PASS. Volumes intact; migrations re-ran idempotently. |
| C14 | Thundering herd after outage | shed + Retry-After protect service | PASS. Recovery observed within 5 ms at the probe; shedding with `Retry-After` verified by the k6 503 path. |

## Features backfill

`fade features backfill --from RFC3339 --to RFC3339 [--events file.jsonl]`
replays raw events into Redis idempotently (the dedupe key is written inside
the same Lua aggregate). Verified by `TestFeaturesBackfillIsIdempotent`
(40 events, two passes, byte-identical feature state).
