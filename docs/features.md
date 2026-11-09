# FADE Phase 2 feature contract

Status: **frozen for Phase 2** (`feature_schema_version = "1.0.0"`).

This document is normative. The Go implementation (`internal/features`), the
Python reference (`generator/reference_features.py`) and
`config/features.yaml` must agree with it. Additive changes need a new minor
schema version; changes to an existing name, type, window or default need a
major version.

For every event `E`, all features are computed from `E` and state derived from
events with `event_time <= E.event_time`, never from an event with a later
`event_time`. The only exception is the dedupe key, which is not a feature.

## Definitions

| Term | Definition |
| --- | --- |
| `t` | `E.event_time` in Unix milliseconds (UTC). |
| Window `W` | An event `X` is in the window iff `t - W < X.t <= t`. The lower bound is **exclusive**. |
| Outbound | `P2P_SEND`, `CASH_OUT`, `AIRTIME`, `BILL_PAY`, `MERCHANT_PAY`, `BANK_TRANSFER`. `CASH_IN` is inbound for the customer. |
| Sender | `E.sender.account_id` (the wallet debited for outbound events, credited for `CASH_IN`). |
| Recipient | `E.recipient.account_id` (credited except for `CASH_OUT`, which debits an agent float). |
| Prior state | State after all events with `(event_time, event_id) < (E.t, E.event_id)`. The current event is added explicitly only where the table says "including current". |
| Kigali local time | `Africa/Kigali`, UTC+2, no DST. |
| Day key | UTC calendar day, `YYYYMMDD`. Day buckets are UTC-based even though simulation days are local; this is deliberate so the feature is replayable from `event_time` alone. |
| `count_30d` | Number of outbound events for the account in the 30 UTC calendar days ending on `E`'s UTC day, excluding `E`. |
| Agent event | `agent_id` is non-null, or the recipient has `is_agent = true`. The agent account is `agent_id` when set, otherwise `recipient.account_id`. |

### Late and duplicate events

- The per-account watermark is the maximum `event_time` processed for that
  account. An event with `t < watermark - LATE_TOLERANCE_S` is dead-lettered
  with reason `LATE` and does not update state.
- An accepted late event is inserted at its own `t`; all windows and scalar
  state are queried as of `(t, event_id)`. Scalar sender state is kept in a
  bounded event-time history (`w:acc:{id}:hist`, 200 snapshots, 31 days) so a
  late event cannot see a device, SIM, geo or cash-out change from a later
  event.
- Duplicate `event_id`s are checked before any state write. A duplicate is
  skipped, produces no feature vector, and its Kafka offset is committed. The
  dedupe key is written only after a successful state update, in the same Lua
  script, so a crash cannot acknowledge an event whose state was not applied.
- Ingest time is the Kafka record timestamp. Live brokers set it on append;
  the replay publisher sets it to the dataset `arrival_time`, so replaying a
  historical fixture does not trip `EVENT_TOO_OLD`/`EVENT_FROM_FUTURE`.
  `EVENT_TOO_OLD`: `t < ingest_time - 24h`. `EVENT_FROM_FUTURE`:
  `t > ingest_time + 5m`.

## Feature vector

Canonical JSON field order is exactly the table order below, preceded by
`event_id` and followed by `feature_schema_version`. Numbers are JSON numbers;
booleans are `true`/`false`; integer features are integers.

| # | Name | Type | Definition | No-history default |
| ---: | --- | --- | --- | --- |
| 1 | `velocity_1m` | int | Count of the sender's outbound events in `(t-60s, t]`, including `E`. | 1 (current is counted) |
| 2 | `velocity_10m` | int | Outbound events in `(t-600s, t]`, including `E`. | 1 |
| 3 | `velocity_1h` | int | Outbound events in `(t-3600s, t]`, including `E`. | 1 |
| 4 | `amount_sum_10m` | int | Sum of outbound `amount` in `(t-600s, t]`, including `E` (RWF). | `E.amount` |
| 5 | `amount_sum_24h` | int | Sum of outbound `amount` in `(t-86400s, t]`, including `E` (RWF). | `E.amount` |
| 6 | `distinct_recip_24h` | int | Distinct recipients of the sender's outbound events in `(t-86400s, t]`, including `E`. | 1 |
| 7 | `amount_vs_avg_30d` | float | `E.amount / mean(prior 30d outbound amounts)`. If `count_30d < 5` or mean is 0: `1.0`. | `1.0` |
| 8 | `amount_zscore_30d` | float | `(E.amount - mean) / population_stddev`, prior 30d. If `count_30d < 5`, stddev is 0 or mean is 0: `0.0`. | `0.0` |
| 9 | `near_limit_ratio` | float | `E.amount / kyc_daily_limit(E.sender.kyc_tier)`. Tiers 1/2/3 limits are `config/features.yaml` (default 50,000 / 500,000 / 5,000,000 RWF). Unknown tier: `0.0`. | `0.0` |
| 10 | `small_txn_run_1h` | int | Count of prior outbound events in `(t-3600s, t)` (current excluded) with `0.90 * threshold <= amount <= 1.00 * threshold`; threshold default 500,000 RWF. | 0 |
| 11 | `new_device` | bool | True iff the sender has prior state and `E.sender.device_id != prior.device_id`. The first event for an account is `false`. | `false` |
| 12 | `new_geo_km` | float | Haversine distance in km between `E.sender.geo` and the prior sender geo. First event: `0.0`. Earth radius `6371.0088 km`. | `0.0` |
| 13 | `sim_changed_72h` | bool | True iff the current event's SIM differs from prior SIM, or the prior SIM change was at most 72h before `t`. | `false` |
| 14 | `time_since_sim_change_min` | int | `0` if the current event changes the SIM; else `floor((t - last_change)/60000)` when a prior change exists within state; else `-1`. | `-1` |
| 15 | `acc_per_device_7d` | int | Distinct sender accounts using `E.sender.device_id` in `(t-7d, t]`, including `E`. | 1 |
| 16 | `recip_new_for_sender` | bool | True iff there is no prior outbound transaction sender -> recipient at `(t-30d, t]`. A recorded last transaction later than `t` does not count (the pair is new at `t`). | `true` |
| 17 | `recip_distinct_senders_24h` | int | Distinct senders that credited the recipient in `(t-86400s, t]`, including `E` when it credits the recipient. `CASH_OUT` debits the agent, so it does not credit. | 0 (or 1 if current credits) |
| 18 | `recip_cashout_lag_min` | int | Minutes from the recipient's most recent inbound credit at or before `t` to the recipient's most recent `CASH_OUT` at or before `t`, when the cash-out is not earlier than the inbound. Else `-1`. | `-1` |
| 19 | `inbound_outbound_ratio_1h` | float | `(in_1h + 1) / (out_1h + 1)` for the sender account in `(t-3600s, t]`, including `E` on the appropriate side (`CASH_IN` inbound, all other types outbound). Amounts are `amount`, not `amount+fee`. | `1.0` |
| 20 | `night_hour` | bool | True iff the Kigali local hour of `t` is in `[0, 5]`. | n/a |
| 21 | `cycle_len_2_4` | int | Shortest directed cycle through the current transfer edge sender -> recipient: 2, 3 or 4, else 0. Only `P2P_SEND` and `BANK_TRANSFER`; edges only if their most recent transfer is in `(t-24h, t]`; at most 50 neighbours per node, 200 visited, 3ms budget. | 0 |
| 22 | `agent_cico_ratio_24h` | float | For an agent event: `(cash_in_24h + 1) / (cash_out_24h + 1)` in `(t-24h, t]`, including `E`. `CASH_IN` and agent-credited `BANK_TRANSFER` are cash-in; `CASH_OUT` is cash-out. Non-agent events: `0.0`. | `0.0` |
| 23 | `agent_float_delta_pct` | float | For an agent event: `100 * net_cico / baseline`, where `net_cico` is the sum of cash-in minus cash-out amounts in the retained 31-day hourly buckets up to `E`'s hour, including `E`; baseline is `agent.float_baseline_rwf` (default 100,000). Non-agent events: `0.0`. | `0.0` |
| 24 | `account_age_days` | int | `E.sender.account_age_days` (observed, pass-through). | n/a |
| 25 | `kyc_tier` | int | `E.sender.kyc_tier` (observed, pass-through). | n/a |

`agent_float_delta_pct` is an observed-flow proxy because events do not carry an
agent's full float balance. It is documented, deterministic and identical in
both implementations.

## Redis key schema

All times are event-time milliseconds. Keys are refreshed with `EXPIRE` on
every successful state update. Windows are sorted sets (`ZSET`) capped at 5,000
entries by dropping the oldest score; the cap is observable through
`fade_window_capped_total`.

| Key | Type | Fields / members | TTL |
| --- | --- | --- | --- |
| `dedupe:{event_id}` | string | `"1"` | `DEDUPE_TTL_S` (86,400) |
| `s:acc:{id}` | hash | `first_seen_ms`, `last_seen_ms`, `watermark_ms`, `last_device`, `last_geo_lat`, `last_geo_lon`, `last_sim`, `last_sim_change_ms` | 31d |
| `w:acc:{id}:hist` | ZSET | score `t`, member `event_id|device|lat|lon|sim|sim_change_ms|last_cashout_ms` | 31d |
| `w:acc:{id}:send` | ZSET | score `t`, member `event_id:amount` (outbound) | 31d |
| `w:acc:{id}:in` | ZSET | score `t`, member `event_id:amount` (credits) | 31d |
| `w:acc:{id}:recip` | ZSET | score `t`, member `recipient_id:event_id` | 31d |
| `s:acc:{id}:daily` | hash | `YYYYMMDD -> "count,sum,sumsq"` (outbound) | 31d |
| `w:dev:{device}` | ZSET | score `t`, member `account_id:event_id` | 7d |
| `w:recip:{id}:senders` | ZSET | score `t`, member `sender_id:event_id` | 7d |
| `w:recip:{id}:in` | ZSET | score `t`, member `event_id:amount` (credits received) | 31d |
| `w:recip:{id}:send` | ZSET | score `t`, member `event_id:amount` (debits from recipient) | 31d |
| `g:edge:{from}:{to}` | hash | `count`, `total`, `last_ms`, `last_transfer_ms` | 30d |
| `g:out:{from}` | ZSET | score `last_ms`, member `to` (max 50) | 30d |
| `a:agent:{id}` | hash | `float_start`, `float_now`, `net_total`, `last_ms` | 31d |
| `a:agent:{id}:hr` | hash | `YYYYMMDDHH -> "in,out"` | 31d |
| `cyc:{from}:{to}` | string | shortest cycle length, 0 when absent | 60s |

`w:acc:{id}:hist` is an implementation key required to make scalar features
event-time correct for late events; it is not a business-facing feature.
`g:out` stores only `P2P_SEND`/`BANK_TRANSFER` edges and only the **most
recent** one per pair; an older transfer superseded by a younger one is not
traversed by cycle detection (documented latest-edge semantics). The
relationship clock in `g:edge:last_ms` still advances for every transaction so
`recip_new_for_sender` remains a true relationship feature.

The Lua update/read script constructs all key names from the event IDs inside
the script and touches the dedupe key and both accounts atomically. Phase 2
therefore requires a single Redis primary; Redis Cluster would reject keys not
declared in `KEYS`, and cross-slot atomicity is impossible for sender/recipient
pairs with different accounts. This is deliberate: the dedupe guarantee is
stronger than cluster fan-out.

## Freshness and metrics

- The script reads prior state, computes the raw aggregates, applies the
  update, and sets the dedupe key in that order. Feature values are therefore
  exactly the prior-state values plus the explicitly-included current event.
- `fade_event_processing_seconds` covers the complete handler: parse, the
  atomic Lua update, cycle detection and optional feature-output publish.
- `fade_window_capped_total` counts each time an insertion forces a ZSET cap.
- `fade_feature_events_total{status=...}` uses `ok`, `duplicate`, `late`,
  `invalid`, `error`.
- `fade_consumer_lag` is the sum of high-watermark minus fetched offsets per
  assigned partition, refreshed every poll.
- When `FEATURE_OUTPUT_TOPIC` is set, vectors are published synchronously with
  key `sender.account_id` and the headers `event_id` and
  `feature_schema_version`. Output is at-most-once relative to committed state;
  duplicate or late events emit no vector.

## Versioning

`config/features.yaml` carries `schema_version: "1.0.0"`. The worker refuses
to start if the embedded expectation, the file and the Python reference
disagree. Feature output (when `FEATURE_OUTPUT_TOPIC` is set) is the canonical
JSON object with `feature_schema_version` as its last field.
