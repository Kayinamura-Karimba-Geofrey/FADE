# FADE Phase 1 dataset contract

Status: **frozen for Phase 1** (v0.2.0). Phase 2 ingestion and the Phase 4
training pipeline consume these files; changing a column, dtype, ordering
rule or label semantic is a breaking change and requires updating this
document and every test under `tests/generator/`.

## Files

| File | Format | Rows | Order |
| --- | --- | --- | --- |
| `manifest.json` | JSON | 1 | n/a |
| `events.jsonl.zst` | zstd-compressed JSON Lines, one event per line | one per emitted event, **including duplicates** | arrival order |
| `events.parquet` | Parquet (zstd) | one per emitted event, including duplicates | same order as JSONL |
| `labels.parquet` | Parquet (zstd) | one per **unique** event | generation (event_time) order |
| `arrivals.parquet` | Parquet (zstd) | one per emitted event, including duplicates | `seq` ascending |
| `accounts.parquet` | Parquet (zstd) | one per population wallet | account index order |
| `report.md` | Markdown | - | optional, not part of the contract |

`manifest.json` embeds the full effective config, sha256 and byte size of
every data file, counts, per-scenario counts, run ids, the special
(non-customer) wallets and their opening balances, and the skipped-event
reasons. `fade generator verify` recomputes every hash and row count.

## Event JSON

One object per line, keys in this fixed order:

```json
{
  "event_id": "01KDV6X6YR768RM6TMTHZS5X82",
  "event_time": "2026-01-01T09:06:31.575Z",
  "txn_type": "P2P_SEND",
  "amount": 5000,
  "currency": "RWF",
  "fee": 100,
  "sender": {
    "account_id": "acct_001160",
    "msisdn_hash": "h_72f288c3b5d3bf73876cf9477decd11b",
    "device_id": "dev_001160_0",
    "sim_serial_hash": "sim_33118b02606d997bc86e39011659742c",
    "geo": {"lat": -1.51, "lon": 29.653999999999996, "cell_id": "MSZ-04"},
    "account_age_days": 648,
    "kyc_tier": 2,
    "balance_before": 298700
  },
  "recipient": {
    "account_id": "spec_telecom_000",
    "msisdn_hash": "h_1349fa8dbd76f209b7d64891e86ff70c",
    "is_agent": false,
    "first_seen_by_sender": true
  },
  "channel": "USSD",
  "agent_id": null,
  "session_id": "top-VEN9RZED"
}
```

Rules enforced by `generator.schema.EventModel` (strict pydantic; JSON Schema
generated from it is the normative schema):

- No extra fields. Only `agent_id` is nullable.
- `amount` is a positive integer RWF value; `fee` is a non-negative integer
  from the fee schedule below. `currency` is `"RWF"`.
- `txn_type` is one of `P2P_SEND, CASH_IN, CASH_OUT, AIRTIME, BILL_PAY,
  MERCHANT_PAY, BANK_TRANSFER`; `channel` is one of `USSD, APP, AGENT, API`.
- `CASH_IN`/`CASH_OUT` always use `channel = "AGENT"`, a non-null
  `agent_id`, `agent_id == recipient.account_id` and `recipient.is_agent =
  true`. Every other type has `agent_id = null`. `recipient.is_agent` is
  false except for `BANK_TRANSFER` credits to an agent's float (the
  documented AGENT_FRAUD float jump).
- `event_time` is UTC ISO-8601 with exactly three fractional digits and `Z`
  (`YYYY-MM-DDTHH:MM:SS.mmmZ`). Simulation days are **local** days
  (Africa/Kigali, UTC+2, no DST), so the first UTC timestamp of a run can be
  on the previous calendar day at 22:00Z.
- `event_id` is a 26-character Crockford-base32 ULID. Its 48-bit timestamp
  component equals `event_time` to the millisecond. IDs are time-sortable.
- `sender.msisdn_hash` is `h_` + 32 lowercase hex chars (first 128 bits of
  sha256); `sender.sim_serial_hash` is `sim_` + 32 hex chars. Salts include
  the run seed, so hashes differ between datasets and cannot be reversed.
- `sender.geo` is inside the Rwanda bounding box `lat [-2.90, -1.00]`,
  `lon [28.80, 30.60]`; `cell_id` is one of the 60 documented cells
  (`KGL-01..20`, `NYG-01..10`, `MSZ-01..10`, `HYE-01..08`, `RBV-01..06`,
  `RSZ-01..06`).
- `sender.kyc_tier` is 1, 2 or 3; `account_age_days >= 0`.
- `sender.balance_before` is the sender wallet balance immediately before
  the event (integer RWF, never negative).
- `session_id` is `<prefix>-<8 Crockford chars>` where the prefix is
  `p2p, agt, top, bil, pos, bnk` for P2P_SEND, CASH_IN/OUT, AIRTIME,
  BILL_PAY, MERCHANT_PAY and BANK_TRANSFER respectively.

### Fee schedule (charged to the sender)

| amount (RWF) | fee |
| --- | ---: |
| `< 1,000` | 0 |
| `1,000 .. 4,999` | 50 |
| `5,000 .. 49,999` | 100 |
| `50,000 .. 499,999` | 500 |
| `>= 500,000` | 1,000 |

`CASH_IN` deducts the fee from the deposited amount (`wallet += amount -
fee`); all other sender-debit types deduct `amount + fee`.

## Ledger semantics

Balances change only through events, in `(event_time, ULID)` order:

- `CASH_IN`: sender wallet `+= amount - fee`; agent float `+= amount`.
- `CASH_OUT`: sender wallet `-= amount + fee`; agent float `-= amount`.
- every other type: sender `-= amount + fee`; recipient `+= amount`.

Special wallets (`spec_telecom_000`, `spec_biller_000..007`,
`spec_bank_000..002`) live in the same ledger after the population accounts
and appear as sender/recipient when they are the counterparty.

## Parquet columns

Machine-readable form (the schema audit parses this block and compares it to
the actual Parquet files):

<!-- contract-json:start -->
```json
{
  "events.parquet": [
    {"name": "event_id", "dtype": "string", "nullable": false},
    {"name": "event_time", "dtype": "string", "nullable": false},
    {"name": "txn_type", "dtype": "string", "nullable": false},
    {"name": "amount", "dtype": "int64", "nullable": false},
    {"name": "currency", "dtype": "string", "nullable": false},
    {"name": "fee", "dtype": "int64", "nullable": false},
    {"name": "sender.account_id", "dtype": "string", "nullable": false},
    {"name": "sender.msisdn_hash", "dtype": "string", "nullable": false},
    {"name": "sender.device_id", "dtype": "string", "nullable": false},
    {"name": "sender.sim_serial_hash", "dtype": "string", "nullable": false},
    {"name": "sender.geo.lat", "dtype": "float64", "nullable": false},
    {"name": "sender.geo.lon", "dtype": "float64", "nullable": false},
    {"name": "sender.geo.cell_id", "dtype": "string", "nullable": false},
    {"name": "sender.account_age_days", "dtype": "int32", "nullable": false},
    {"name": "sender.kyc_tier", "dtype": "int32", "nullable": false},
    {"name": "sender.balance_before", "dtype": "int64", "nullable": false},
    {"name": "recipient.account_id", "dtype": "string", "nullable": false},
    {"name": "recipient.msisdn_hash", "dtype": "string", "nullable": false},
    {"name": "recipient.is_agent", "dtype": "bool", "nullable": false},
    {"name": "recipient.first_seen_by_sender", "dtype": "bool", "nullable": false},
    {"name": "channel", "dtype": "string", "nullable": false},
    {"name": "agent_id", "dtype": "string", "nullable": true},
    {"name": "session_id", "dtype": "string", "nullable": false}
  ],
  "labels.parquet": [
    {"name": "event_id", "dtype": "string", "nullable": false},
    {"name": "label", "dtype": "string", "nullable": false},
    {"name": "scenario", "dtype": "string", "nullable": true},
    {"name": "scenario_run_id", "dtype": "string", "nullable": true},
    {"name": "source", "dtype": "string", "nullable": false}
  ],
  "arrivals.parquet": [
    {"name": "event_id", "dtype": "string", "nullable": false},
    {"name": "event_time", "dtype": "string", "nullable": false},
    {"name": "arrival_time", "dtype": "string", "nullable": false},
    {"name": "seq", "dtype": "int64", "nullable": false}
  ],
  "accounts.parquet": [
    {"name": "account_id", "dtype": "string", "nullable": false},
    {"name": "msisdn_hash", "dtype": "string", "nullable": false},
    {"name": "segment", "dtype": "string", "nullable": false},
    {"name": "kyc_tier", "dtype": "int32", "nullable": false},
    {"name": "created_day", "dtype": "int32", "nullable": false},
    {"name": "home_cell", "dtype": "string", "nullable": false},
    {"name": "devices", "dtype": "list<string>", "nullable": false},
    {"name": "sim_serial_hash", "dtype": "string", "nullable": false},
    {"name": "is_agent", "dtype": "bool", "nullable": false},
    {"name": "opening_balance", "dtype": "int64", "nullable": false}
  ]
}
```
<!-- contract-json:end -->

Column meanings not already covered by the event table:

- `events.parquet` uses the dotted flattening of the JSON event; values are
  identical to the JSONL stream and row *i* is the same event.
- `labels.parquet`: `label` is `FRAUD` or `LEGIT`; `scenario` is one of the
  seven scenario names for fraud (or `LABEL_NOISE` when label noise is
  enabled) and null for legit; `scenario_run_id` groups the events of one
  injected run and is null unless `scenario` is set. `source` is
  `GENERATOR`.
- `arrivals.parquet`: `arrival_time` is the delivery timestamp in UTC
  ISO-8601; `seq` is the global arrival sequence (0-based, gapless).
- `accounts.parquet`: `segment` is one of the five customer segments or
  `agent`; `created_day` is the simulation-day offset (negative =
  pre-existing); `home_cell` is the cell id; `devices` lists every device id
  the wallet has ever used; `opening_balance` is the wallet balance before
  the first event.

## Ordering guarantees

1. `events.jsonl.zst` and `events.parquet` are in **arrival order**.
2. `arrivals.seq` is strictly increasing with one row per emitted event,
   including duplicates; it is the join key back to the event stream order.
3. For a given `sender.account_id`, arrival order is preserved by the
   publisher's Kafka key, and the per-account reordering depth caused by
   delivery delay is at most 300 seconds (late range is `(60, 300] s`).
4. The ledger is replayed in `(event_time, event_id)` order. ULID timestamps
   equal `event_time`, so that is time order; events touching the same
   account at the same millisecond are ordered by ULID.

## Delivery noise

Noise touches `arrival_time` only; `event_time`, `event_id` and every event
field are immutable.

| Category | Config default | Semantics |
| --- | ---: | --- |
| duplicate | 0.5% | a second byte-identical copy of the event (same `event_id`), arriving 1-2000 ms after the original |
| delayed | 2% | `arrival_time = event_time + U[1, 45] s` |
| late | 0.1% | `arrival_time = event_time + U[61, 300] s` |
| on time | remainder | `arrival_time = event_time` |

Category assignment is stratified per 1,000-event block so realised rates
match the configured rates exactly (within one block of rounding) rather
than drifting with binomial noise.

## Labels and joining

`labels.parquet` is the ground truth for training and evaluation. Join it to
events on `event_id`; never join on row position, because labels are written
in generation order while events are written in arrival order. Duplicates
share an `event_id`, so a left join would attach the same label to both
copies - deduplicate by `event_id` first when training.

Fraud labels are `FRAUD` with a `scenario` and `scenario_run_id`; all other
events are `LEGIT`. `label_noise` (default 0) flips a label in
`labels.parquet` only, adding `scenario = "LABEL_NOISE"` for flipped legit
events; events are never modified.

## Regeneration

```bash
uv run python -m generator run --out data/small --config configs/small.yaml
uv run python -m generator verify --out data/small
uv run python -m generator run --out data/run42 --config configs/run42.yaml
uv run python -m generator run --out data/golden --config configs/golden.yaml
```
