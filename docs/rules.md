# FADE Phase 3 rule specification

Status: **frozen for Phase 3** (`rules_version = 1`, API `v1`).

This document is normative for the rule engine (`internal/rules`), the starter
pack (`rules/prod.json`) and the decision API. It defines the rule JSON format,
every addressable identifier, validation, evaluation order, scoring, action
precedence, shadow mode, versioning and hot-reload semantics.

Rules are data. A rule set is one JSON document stored in `rules/*.json` and
versioned in Postgres. The engine compiles a rule set once into typed closures;
the hot path never parses JSON, performs map lookups or uses reflection.

## Rule set document

```json
{
  "rule_set": "prod",
  "version": 14,
  "author": "rules@fade",
  "changelog": "tighten velocity burst",
  "config": {
    "combiner": {
      "thresholds": {"step_up": 40, "hold": 65, "block": 85},
      "floors": {"STEP_UP": 40, "HOLD": 65, "BLOCK": 85}
    },
    "fail_mode": {
      "P2P_SEND": "step_up",
      "CASH_OUT": "step_up",
      "AIRTIME": "open",
      "default": "step_up"
    }
  },
  "rules": [
    {
      "id": "R-VEL-001",
      "name": "Velocity burst",
      "enabled": true,
      "mode": "enforce",
      "weight": 35,
      "action": "HOLD",
      "when": {"all": [
        {"f": "velocity_10m", "op": ">", "v": 10},
        {"f": "amount_sum_10m", "op": ">", "v": 500000}
      ]},
      "reason": "{velocity_10m} sends in 10 min totalling {amount_sum_10m} RWF",
      "tags": ["velocity"],
      "applies_to": ["P2P_SEND", "CASH_OUT"]
    }
  ]
}
```

| Field | Required | Semantics |
| --- | --- | --- |
| `rule_set` | yes | Set name, `^[a-z][a-z0-9_]{1,31}$`. Versions are monotonic per name. |
| `version` | yes | Integer `>= 1`; must strictly increase over the last activated version of the same `rule_set`. |
| `author` | yes | Non-empty actor recorded in the audit log. |
| `changelog` | no | Free-form operator context, max 500 chars. |
| `config` | yes | Combiner and fail-policy configuration (below). |
| `rules` | yes | 0-256 rules. Zero rules is only allowed for the built-in empty set. |

### Rule fields

| Field | Required | Semantics |
| --- | --- | --- |
| `id` | yes | `^R-[A-Z]{2,6}-\d{3}$`, unique in the set. |
| `name` | yes | Human name, 1-80 characters, unique. |
| `enabled` | yes | Disabled rules are compiled out (they never fire) but still validated. |
| `mode` | yes | `enforce` or `shadow`. |
| `weight` | yes | Integer in `[0, 100]`. |
| `action` | yes | `ALLOW`, `STEP_UP`, `HOLD` or `BLOCK`. |
| `when` | yes | Non-empty condition tree, maximum depth 6. |
| `reason` | yes | Non-empty template, max 280 chars. `{identifier}` placeholders must name a known identifier. |
| `tags` | no | Up to 8 tags, each `^[a-z][a-z0-9_]{0,31}$`. |
| `applies_to` | no | Transaction types. Empty/omitted means every transaction type. Unknown types are rejected. |
| `stateless` | no | `true` means the rule is evaluated even when Redis/feature computation is unavailable. Only raw identifiers and list overrides may be referenced. |

`enabled: false` rules are rejected if any other field is invalid; validation is
never skipped because a rule is disabled.

## Condition tree

A node is exactly one of:

- comparison: `{"f": "<identifier>", "op": "<operator>", "v": <value>}` or
  `{"f": "<identifier>", "op": "in", "v": [<scalar>, ...]}`;
- `{"all": [<node>, ...]}` — true when every child is true (short-circuits);
- `{"any": [<node>, ...]}` — true when any child is true (short-circuits);
- `{"not": <node>}`.

Groups must have at least one child (`not` has exactly one). Nesting depth is
counted from 1 at the root and may not exceed **6**. A rule may contain at most
64 condition nodes.

Operators:

| Operator | Numeric | Bool | String | Notes |
| --- | --- | --- | --- | --- |
| `==`, `!=` | yes | yes | yes | |
| `>`, `>=`, `<`, `<=` | yes | no | no | |
| `in`, `not_in` | yes | yes | yes | `v` must be a non-empty list, max 32 values |

Comparison is feature-vs-constant only. There is no feature-to-feature
comparison in v1. Type mismatches (string value on numeric field, fractional
value on an `int` field, non-boolean on a bool field) are compile errors.

### Addressable identifiers

Feature identifiers come from the frozen Phase 2 contract
(`docs/features.md`, schema `1.0.0`) and are read from the same
`features.FeatureVector` returned by `ComputeFeatures`. Raw identifiers are
derived from the event by the pipeline, never reimplemented feature logic.

| Identifier | Type | Source |
| --- | --- | --- |
| `velocity_1m` | int | feature |
| `velocity_10m` | int | feature |
| `velocity_1h` | int | feature |
| `amount_sum_10m` | int | feature |
| `amount_sum_24h` | int | feature |
| `distinct_recip_24h` | int | feature |
| `amount_vs_avg_30d` | float | feature |
| `amount_zscore_30d` | float | feature |
| `near_limit_ratio` | float | feature |
| `small_txn_run_1h` | int | feature |
| `new_device` | bool | feature |
| `new_geo_km` | float | feature |
| `sim_changed_72h` | bool | feature |
| `time_since_sim_change_min` | int | feature |
| `acc_per_device_7d` | int | feature |
| `recip_new_for_sender` | bool | feature |
| `recip_distinct_senders_24h` | int | feature |
| `recip_cashout_lag_min` | int | feature |
| `inbound_outbound_ratio_1h` | float | feature |
| `night_hour` | bool | feature |
| `cycle_len_2_4` | int | feature |
| `agent_cico_ratio_24h` | float | feature |
| `agent_float_delta_pct` | float | feature |
| `account_age_days` | int | feature (pass-through) |
| `kyc_tier` | int | event sender `kyc_tier` (same value as the feature) |
| `amount` | int | event `amount` (RWF) |
| `fee` | int | event `fee` (RWF) |
| `txn_type` | string | event `txn_type` |
| `channel` | string | event `channel` |
| `balance_before_ratio` | float | `amount / sender.balance_before`, `0` when balance is `0` |

Reason placeholders may reference any identifier above. Rendering is lazy: text
is produced only when the final decision is `STEP_UP` or more severe, or when
the caller requests explanation. Values are rendered without PII: integers
without decimals, floats with two decimals, booleans as `true`/`false`.

## Compile-time validation

A rule set is accepted only if **all** of the following hold. Every violation
is returned in one response; the compiler never stops at the first error.

1. `rule_set` matches the name pattern; `author` non-empty; `version >= 1`.
2. `config.combiner.thresholds` satisfy `0 <= step_up < hold < block <= 100`.
3. `config.combiner.floors` (optional) are in `[0, 100]` and non-decreasing
   (`STEP_UP <= HOLD <= BLOCK`).
4. `config.fail_mode` keys are transaction types or `default`; values are
   `open`, `closed` or `step_up`.
5. Rule count is `0..256` (only the built-in empty set may have zero rules).
6. Rule `id`, `name`, `mode`, `weight`, `action`, `when`, `reason` are valid;
   ids and names are unique; id format matches `^R-[A-Z]{2,6}-\d{3}$`.
7. `applies_to` contains only the seven frozen transaction types.
8. Every condition identifier is known; every operator is valid for the
   identifier's type; every value has the right type; `in`/`not_in` get a
   non-empty list of at most 32 values.
9. The tree uses exactly one form per node, groups are non-empty, depth `<= 6`
   and node count `<= 64` per rule.
10. Every `{placeholder}` in `reason` names a known identifier.
11. A `stateless` rule references only raw identifiers (`amount`, `fee`,
    `txn_type`, `channel`, `kyc_tier`, `balance_before_ratio`) or `kyc_tier`.
12. `applies_to` is not empty when present; duplicate entries are rejected.

## Evaluation order and result

Evaluation is per transaction type over a precomputed index: generic rules plus
rules whose `applies_to` includes the type. Rules are ordered at compile time by
estimated cost (bool `==` first, then integer comparisons, then float and
string comparisons), with author order as the tie-breaker. Results are
order-independent: score is a capped sum and action is `max` by severity, so
reordering cannot change the decision. Reason lists follow evaluation order.

Compiled rules are immutable. `Evaluate(fv, ctx, out)` fills a caller-provided
`Result`; `Result` holds a fixed array of at most 256 fired rules and never
allocates. A rule that fires contributes:

- its `weight` to `rule_score` **only in `enforce` mode**;
- its `action` to `rule_action` **only in `enforce` mode**;
- its id to `shadow_hits` **only in `shadow` mode** (shadow rules are evaluated
  and logged but never affect score or action).

`rule_score = min(100, sum(weight of fired enforce rules))`.
`rule_action = most severe action among fired enforce rules`, severity
`BLOCK > HOLD > STEP_UP > ALLOW`; `ALLOW` when no enforce rule fires.

`applies_to` filtering happens through precomputed slices, not maps, on the hot
path. A request evaluates only the rules for its transaction type.

## Combiner (rules only)

```
risk = rule_score
risk = max(risk, floor[rule_action])   // floors default to thresholds
decision = threshold_decision(risk)    // 0-39 ALLOW, 40-64 STEP_UP, 65-84 HOLD, 85-100 BLOCK
decision = max_severity(decision, rule_action)
```

The second `max` makes the rule action authoritative even if an operator
configures an inconsistent floor. Thresholds are per rule set and default to
`40/65/85`.

The ML hook is defined now so Phase 4 can plug in without an API change:

```go
type Scorer interface {
    Score(ctx context.Context, fv *features.FeatureVector) (MLResult, error)
}
type MLResult struct {
    Score        *int
    Status       string // "NOT_CONFIGURED", "OK", "ERROR", "TIMEOUT"
    ModelVersion string
}
```

`NoopScorer` returns `Status: "NOT_CONFIGURED"` and no score. Phase 3 therefore
always reports `ml_score: null`, `ml_status: "NOT_CONFIGURED"`,
`model_version: null`, and `risk_score == rule_score` (except for explicit
overrides and fail-policy decisions, which are documented below). The combiner
adds an ML score only when it is non-nil, clamped to `[0, 100]`.

## Overrides, lists and tier limits

Precedence, highest first:

1. **BLOCK list** hit (account, device or MSISDN, unexpired): decision
   `BLOCK`, reason code `LIST_BLOCK`.
2. **Rule `BLOCK`**: a fired enforce rule whose action is `BLOCK`.
3. **ALLOW list** hit: decision `ALLOW` unless (1) or (2) applied. A non-BLOCK
   rule action (`STEP_UP`/`HOLD`) does not defeat an allow-list entry.
4. **Combiner thresholds**, raised by the KYC tier ceiling: if
   `amount > tier_daily_limit(kyc_tier)` the decision is at least `HOLD` with
   reason `TIER_LIMIT_EXCEEDED`. Limits are the Phase 2 defaults: tier 1
   `50,000`, tier 2 `500,000`, tier 3 `5,000,000` RWF.

Lists are cached in memory and refreshed every 10 seconds and immediately
after an administrative change. An entry may carry `expires_at`; expired
entries are ignored. `risk_score` is still reported for allow-list decisions so
analysts can see the rule evidence.

## Fail policy

`config.fail_mode` maps transaction type to one of:

| Mode | Decision when the decision path degrades |
| --- | --- |
| `open` | `ALLOW` |
| `closed` | `BLOCK` |
| `step_up` | `STEP_UP` |

`default` applies when the transaction type has no entry. Triggers:
`REDIS_UNAVAILABLE`, `FEATURE_TIMEOUT` (20 ms feature budget), `RULE_PANIC`,
`NO_RULE_SET`. Degraded decisions carry `degraded: true` and a reason with
`source: "fail_policy"` and id `FAIL_POLICY_<CAUSE>`. In Redis-down mode the
pipeline still applies stateless checks: block lists, allow lists, tier limits
and enforce rules marked `"stateless": true`. Other rules are skipped.

A caller may opt into the configured fail policy with `X-Fail-Policy: apply`.
Without it, a shed request gets `503`; pipeline degradation still returns a
decision, because a fraud decision is safer than a transport failure.

## Versioning and activation

- Each rule set has a monotonic `version` per `rule_set` name. Activation
  rejects a version that is not strictly greater than the last activated
  version for that name.
- Activation persists `{content, content_hash, config, author, active}` to
  Postgres in one transaction, marks the previous version inactive, and appends
  an `audit_log` row. `content_hash` is the SHA-256 of canonical JSON (sorted
  object keys, no insignificant whitespace).
- The audit row is hash-chained:
  `hash = sha256(prev_hash || canonical_json(row))`, where `row` excludes
  `hash` itself. `prev_hash` is the hash of the newest audit row.
- When Postgres is unavailable, the in-memory active set is still updated and
  the database write is queued to the disk spool; it is replayed in order on
  recovery. A response never waits on Postgres.

## Hot reload

Two triggers, same code path:

- `fsnotify` watch on `rules/`, debounced 300 ms (writes, creates, renames);
- `POST /v1/rules/reload`.

Reload compiles the whole new set first. Only after validation succeeds is the
`atomic.Pointer` swapped, so in-flight requests finish on the old set. The
logger records old version/hash and new version/hash.
`fade_rules_reload_total{result="rejected"}` is incremented on failure and every
validation error is returned; the previous set stays active.

Startup order: active set from Postgres, else the newest valid file, else the
built-in empty rule set (allow-all plus tier limits and lists). In the last case
`/readyz` reports `degraded`.

### Auto-rollback

The manager counts evaluator errors and panics in one-second buckets. If, in the
30 seconds after an activation, at least 20 evaluations ran and more than 1%
failed, it swaps back to the previous set exactly once and increments
`fade_rules_rollback_total`. The evaluator also recovers per request: a panic
becomes a `RULE_PANIC` failure handled by the fail policy, never a process
crash.

## Starter pack tuning

The starter pack is `rules/prod.json`, version 1. Thresholds and weights are
tuned with `cmd/replay-lite` on `data/small`; the full replay belongs to Phase 6.
The tuning table (rule, fires, TP, FP, precision) is appended below after the
final run. Placeholders in this section are replaced by measured values, never
guessed.

Replay command (isolated Redis DB 15, then `FLUSHDB`):

```sh
go run ./cmd/replay-lite --set data/small --rules rules/prod.json \
  --redis localhost:6390 --db 15 --out bench/results/replay-small-v2
```

Dataset `data/small`: 38,584 events, 164 fraud, 38,420 legit. Result: caught 145
(88.41%), blocked 104, false positives 559, FPR 1.46%, degraded 0. The dominant
false-positive source is the KYC tier ceiling (495 legitimate events above the
daily limit), which is a deterministic Phase 2 override rather than a rule.

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

Per-scenario recall on the same run:

| Scenario | Fraud | Caught | Blocked | Recall |
| --- | ---: | ---: | ---: | ---: |
| AGENT_FRAUD | 9 | 9 | 9 | 100.0% |
| ATO_SIMSWAP | 4 | 4 | 0 | 100.0% |
| CIRCULAR | 8 | 4 | 4 | 50.0% |
| MULE | 9 | 9 | 2 | 100.0% |
| SOCIAL_ENG | 9 | 9 | 9 | 100.0% |
| STRUCTURING | 50 | 50 | 50 | 100.0% |
| VELOCITY | 75 | 60 | 30 | 80.0% |

Thresholds that moved during tuning (and why):

- `R-AGT-001` CICO ratio `>5 || <0.2` fired on 3,103 legitimate agent events;
the synthetic fraud population sits at ~3.3-3.8M, so the enforce threshold is
now `>500000` (3 legitimate fires) and the `BANK_TRANSFER` leg is covered.
- `R-AGT-002` float delta `>200` fired on 4,863 legitimate events; fraud is
`>3,500%`, so the threshold is `>3400` (10 legitimate fires).
- `R-MUL-001/002` fan-in `>=8` had 122 legitimate fires; `>=12` keeps every
mule that `R-MUL-003` misses at 12 false positives.
- `R-SOC-002` now requires a new recipient, `amount_vs_avg_30d > 2` and 24h
turnover above 250,000 RWF: 9/9 social-engineering fraud, 5 false positives.
- `R-STR-001` keeps the small-run branch and adds a near-limit sweep branch;
`R-STR-002` raises the tier-1 turnover gate to 500,000 RWF, removing 55 false
positives without losing a structuring event.
- `R-VEL-001` uses `velocity_10m >= 6` with 10m turnover above 150,000 RWF
(30 fraud, 0 false positives); `R-VEL-002` triggers at 3 sends/minute (60
fraud, 0 false positives). These replace the original `>10` / `>5` gates, which
never fired on the dataset.
- `R-ATO-001` also accepts a SIM change within the last hour, which catches the
three ATO events whose device was not new.

CIRCULAR recall (4/8) is limited by the frozen Phase 2 feature: only 4 of the 8
labelled circular events report `cycle_len_2_4 >= 2`. `R-CIR-001` fires on every
event where the graph closes a cycle; recovering the rest needs a Phase 2
feature change, not a rule threshold.

## Security notes

Rule documents are untrusted input to the admin API: bodies are limited to
1 MiB, decoded strictly (unknown fields rejected), and compiled before
activation. Reasons never interpolate strings outside the identifier table, so
a rule cannot read PII into a response. Dashboard CORS is restricted to
`CORS_ORIGIN`.
