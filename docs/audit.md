# FADE tamper-evident audit log

Every governance change — rule activation, weight edit, list change, override,
retention purge, auth failure burst, checkpoint — is written to one
append-only, hash-chained log per database. A verifier can prove that no row
was edited, reordered or deleted, and signed daily checkpoints survive even a
complete rewrite of Postgres.

- Package: `internal/audit`
- Migration: `migrations/0003_explain_audit.up.sql`
- Endpoints: `GET /v1/audit`, `GET /v1/audit/verify`, `POST /v1/audit/checkpoint`
- CLI: `fade audit verify`, `fade audit checkpoint`
- Checkpoints: `audit/checkpoints/YYYY-MM-DD.json`

## 1. Threat model

| Threat | Defence |
| --- | --- |
| Edit a rule-change row after the fact | Row hash includes every field; edit breaks the link |
| Delete a middle row | `seq` gap and broken `prev_hash` |
| Delete the tail and recompute forward | Signed checkpoint at the old head |
| Rewrite the whole chain | Checkpoint file held outside Postgres |
| Application bug issues `UPDATE`/`DELETE` | Trigger raises; app role has no privilege |
| Operator with `psql` access truncates | Trigger + revoked `TRUNCATE`; checkpoint detects |
| Reordering rows | Monotonic `seq` enforced under an advisory lock |

Threats explicitly **not** covered: an attacker who can rewrite both Postgres
and the checkpoint directory *and* knows the HMAC key. Phase 5 assumes the
checkpoint directory is append-only storage with separate access control.

## 2. Row contract

The hash pre-image is exactly this object, canonicalised with RFC 8785 JCS:

```json
{
  "actor": "admin@fade",
  "action": "rules.activate",
  "object_type": "rule_set",
  "object_id": "prod/7",
  "before": {"version": 6},
  "after": {"version": 7},
  "at": "2026-11-16T09:00:00.000000Z",
  "request_id": "req_…",
  "source_ip_hash": "sha256(salt||ip)"
}
```

- `prev_hash` is stored beside the row but is **not** part of this object; it is
  the ASCII prefix in the hash input.
- `at` is normalized to UTC microseconds with a fixed layout. This matters:
  Postgres would otherwise render `+00` or a different precision after a
  round-trip, changing the canonical bytes and breaking otherwise valid rows.
- `before`/`after` are redacted against a per-`object_type` allowlist before the
  hash is computed. Raw personal identifiers are dropped rather than hashed
  into the log; free-text fields that are allowlisted pass through
  `redaction.Redact`.
- `source_ip_hash` is `SHA-256(salt || ip)`. A raw IP is rejected by the writer.
- Adding or removing a field is a contract change: bump `audit.SchemaVersion`,
  document a verifier upgrade, and migrate.

## 3. Hash chain

```
genesis = SHA-256("FADE-AUDIT-GENESIS-v1")
hash_n  = SHA-256(prev_hash_n || canonical_json(row_n))
prev_hash_1 = genesis
prev_hash_n = hash_(n-1)
```

- One chain per database (`chain = "fade-audit"`), not per object.
- `seq` starts at 1 and is monotonic with no gaps. It is *not* in the pre-image;
  the chain protects content and order via `prev_hash`, while `seq` makes gaps
  and reordering visible.
- The first Phase 3/4 rows may have an empty `prev_hash` and a NULL `seq`. Such
  a chain is accepted once at the start of verification and reported as
  `legacy_prelude: true`; every row after the prelude must link to the genesis
  hash or to its predecessor.

## 4. Append protocol

`PostgresStore.Append` runs in a single transaction:

1. `pg_advisory_xact_lock(ChainLockKey)` serialises writers per database.
2. Read the head (`WHERE seq IS NOT NULL ORDER BY seq DESC LIMIT 1`).
3. Compute `prev` and `seq` (`genesis`/1 when empty).
4. Canonicalise and hash the row.
5. `INSERT`, commit.

Concurrent writers therefore never fork the chain. `TestConcurrentWritersKeepChainValid`
runs 50 goroutines × 40 writes and asserts contiguous sequences plus a valid
chain.

## 5. Verification

`audit.VerifyChain(ctx, store, opts)` streams rows in ascending order and checks,
for each entry:

- the recomputed `hash` equals the stored hash;
- `prev_hash` equals the previous entry's `hash` (or the genesis/legacy rule);
- `seq` increments by exactly one (NULL seq is only tolerated in the legacy
  prelude);
- `at` parses and is normalized;
- every checkpoint file in `opts.CheckpointDir`, when the range covers it, has a
  valid HMAC and the same hash/row count as the chain at that sequence.

It records the first break with a kind:

| Kind | Meaning |
| --- | --- |
| `ROW_HASH` | canonical row no longer hashes to the stored hash |
| `LINK` | `prev_hash` does not match the predecessor |
| `SEQ_GAP` | missing or duplicate sequence |
| `AT_FORMAT` | timestamp is not in the normalized form |
| `CHECKPOINT` | checkpoint signature/hash/row-count mismatch |
| `GENESIS` | first row does not chain from genesis (after legacy prelude) |
| `WRITE_ROLE` | application role can mutate the log |

`GET /v1/audit/verify` returns `200` when the chain is intact and `409` with the
first break otherwise. The API never returns partial rows on a break; it
returns the break plus a small context window for investigation.

### Measured verification

| Check | Result |
| --- | --- |
| In-memory chain, 20,000 rows | 24,360 rows/s (1.72 s), `ok` |
| Postgres chain, 500 rows (20 concurrent writers) | 15,863 rows/s, `ok` |
| 10,000 decision reproductions | 10,000/10,000 matching, 0.64 s |
| Edit a row payload | detected as `ROW_HASH` at seq 6 |
| Delete a middle row | detected as `SEQ_GAP` |
| Delete tail + rebuild forward | detected as `CHECKPOINT` |
| Single-bit mutation of payload/actor/hash (rapid property test) | always detected |
| 2,000 concurrent appends (in-memory) | contiguous seq, valid chain |
| `UPDATE`/`DELETE`/`TRUNCATE` on `audit_log` | rejected by trigger (`insufficient_privilege`) |

Rows/s is measured on the development host and includes canonicalisation and
context capture. The 100k rows/s target in the Phase 5 spec was not reached in
this environment; treat 24k rows/s as the measured floor and re-measure against
the production Postgres host.

## 6. Checkpoints

`audit.CheckpointService.Run`:

1. Reads the head and row count.
2. Builds `{schema_version, chain, seq, hash, row_count, created_at}`.
3. Signs it with HMAC-SHA256 over its canonical JSON (`digest`).
4. Appends an `audit.checkpoint` row to the chain (so the checkpoint itself is
   audited).
5. Writes `audit/checkpoints/YYYY-MM-DD.json` atomically (temp file + rename).

An unsigned checkpoint is impossible: without `AUDIT_HMAC_KEY` the service
returns `ErrNoHMACKey` and the API returns `503 AUDIT_KEY_MISSING`. Checkpoint
files are append-only; rewriting one is exactly the event they exist to reveal.
`LoadCheckpointFiles` + `VerifyCheckpointFiles` verify a directory offline,
without Postgres, given the key.

Daily job:

```bash
AUDIT_HMAC_KEY=… fade audit checkpoint -dsn "$FADE_DSN" -dir audit/checkpoints
```

## 7. Retention and partitioning

- `decisions` is range-partitioned by `decided_at` (monthly). Migration 0003
  adds `ensure_decisions_partition(ts)`; the maintenance job creates the current
  month and three months ahead.
- `retention_policy` holds the documented window (90 days, monthly). A purge
  drops whole partitions older than the cutoff; it never runs a row-by-row
  `DELETE` against the audit log.
- Agent action is itself audited (`retention.purge`).
- Letting Postgres auto-drop through the default partition is not permitted by
  the policy: an out-of-window write must create the partition, not land in
  `decisions_default`, or retention cannot drop it.

`fade retention plan|apply` prints and applies the exact SQL. `Plan` refuses a
window shorter than the partition interval, so it can never plan to drop live
data.

## 8. Roles and privileges

- Application role: `SELECT`, `INSERT` on `audit_log` and
  `audit_checkpoints`; `USAGE, SELECT` on their sequences. No `UPDATE`,
  `DELETE`, `TRUNCATE`.
- `audit_log_no_update` and `audit_log_no_truncate` triggers raise
  `insufficient_privilege` for any mutation, including one attempted by a
  superuser session that forgot to disable the trigger.
- `PostgresStore.Writable` probes `has_table_privilege`; readiness should treat
  `true` as a misconfiguration.

## 9. Deviation from the Phase 5 spec

The specification says the Phase 5 migration is `0002_explain_audit`. This
repository already has `0002_phase3` (shadow hits and indexes), so the Phase 5
migration is numbered **0003_explain_audit**. Nothing else about the ordering or
content changes; the spec's intent (a numbered, documented, tested migration) is
met.

## 10. Audit catalog

Actions accepted by the writer (`audit.Catalog`):

`auth.login`, `auth.failure`, `auth.failure_burst`, `user.create`,
`user.role_change`, `user.delete`, `config.change`,
`rules.create`, `rules.update`, `rules.activate`, `rules.rollback`,
`lists.add`, `lists.remove`, `case.open`, `case.assign`, `case.resolve`,
`decision.override`, `decision.reproduce`, `retention.purge`,
`audit.verify`, `audit.checkpoint`.

Each `object_type` has an explicit payload allowlist in
`internal/audit/payload.go`. Unknown actions and object types are rejected, and
disallowed fields are dropped before hashing. `TestWriterRedactsDisallowedFields`
asserts a raw MSISDN and an internal note cannot reach the chain.

## 11. Runbook

```bash
# Verify the whole chain (stops at the first break)
fade audit verify -dsn "$FADE_DSN" -dir audit/checkpoints

# Verify a window and include checkpoint comparison
AUDIT_HMAC_KEY=… fade audit verify -dsn "$FADE_DSN" -from 1000 -to 2000

# Write today's signed checkpoint
AUDIT_HMAC_KEY=… fade audit checkpoint -dsn "$FADE_DSN"

# List recent entries as an auditor
curl -H "Authorization: Bearer $TOKEN" \
  'https://fade.example/v1/audit?from=0&limit=100'
```

On a break: preserve the database and checkpoint directory, do not run the
retention job, and treat the reported `seq` as the start of the investigation.
A `CHECKPOINT` break with a valid chain means the database history was rewritten
or the checkpoint file was replaced; both are incidents.

## 12. Limitations

- The verifier reads the database through the application connection. A
  compromised database host can lie consistently; the checkpoint files are the
  cross-check, which is why their storage must be separate.
- Throughput is 24k rows/s in the measured environment, not the 100k rows/s
  target. Verification is a maintenance/forensic operation and is not on the
  decision path.
- The legacy prelude is accepted by design so Phase 3/4 history remains
  readable. It is reported, not hidden.
