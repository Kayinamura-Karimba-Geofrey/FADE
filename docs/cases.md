# Case management (Phase 7)

Analysts review every HOLD and BLOCK, a sample of ALLOW, and cases they open
manually. A resolution is a **label**: it is published to `labels.feedback` and
feeds model and rule evaluation. The implementation lives in `internal/cases`
(policy and persistence), `internal/api/cases.go` (HTTP), `cmd/case-worker`
(triage and the SLA clock) and `config/cases.yaml` (policy).

- Contract: [api/openapi.yaml](../api/openapi.yaml) (version 0.7.0)
- Dashboard: [dashboard.md](dashboard.md)
- Audit: [audit.md](audit.md)

## Lifecycle

Analyst transitions, enforced by `ValidateTransition`:

| From | To | Action |
| --- | --- | --- |
| OPEN | IN_REVIEW | `claim` (self) or `assign` (lead/admin) |
| IN_REVIEW | OPEN | `release` |
| IN_REVIEW | ESCALATED | `escalate` with a reason |
| IN_REVIEW | RESOLVED | `resolve` with an outcome |
| ESCALATED | IN_REVIEW | `claim` |
| ESCALATED | RESOLVED | `resolve` |
| RESOLVED | REOPENED | `reopen` (lead/admin only, within 7 days, with a reason) |
| REOPENED | IN_REVIEW | `claim` |

Two transitions are system-only:

- `case-worker` opens cases (`opened`).
- The SLA sweep escalates an overdue OPEN, IN_REVIEW or ESCALATED case
  (`Store.Breach`). Analysts cannot perform OPEN → ESCALATED themselves; the
  policy must not depend on someone having claimed the case first.

Reopening a case appends a `reopened` event, clears the outcome, supersedes the
case's labels in `label_history` (`superseded_at`) and restarts the SLA clock
from the reopen time. The 7-day window keeps a rejection from being reversed
once the label has matured.

## Outcomes

| Outcome | Operational meaning | Label |
| --- | --- | --- |
| `FRAUD` | Confirmed fraud: keep the block/hold, flag the account | `FRAUD` |
| `LEGIT` | False positive: release the hold | `LEGIT` |
| `INCONCLUSIVE` | Needs customer contact; `follow_up_at` is mandatory | none |

`INCONCLUSIVE` deliberately writes no label: an unresolved case must not teach
the model either way. It does keep the hold and stays in the queue until it is
resolved again.

## Case creation

- One case per HOLD or BLOCK decision, opened by `case-worker` with source
  `DECISION`.
- STEP_UP opens nothing up front. If the challenge fails, the decisioning path
  calls `POST /v1/decisions/{id}/step-up-result {"passed": false}` and the case
  is opened with source `STEP_UP_FAILED` (15-minute SLA).
- ALLOW decisions are QA-sampled at `qa_sample_rate` (default 0.5%) with source
  `QA_SAMPLE`. Sampling is a deterministic hash of the decision id, so a retried
  sweep selects the same decisions. `0` disables it.
- Analysts open manual cases with source `MANUAL`; `account_id` is hashed before
  it is stored and never returned.

The worker reads the decisions table rather than the `txn.decisions` topic:
the pipeline's topic payload omits `account_id` and `amount`, which cases and
SLAs need. The watermark is stored in `case_config` and only advances past rows
that were handled, so a failure retries rather than skips.

## Grouping

A decision joins the newest open case for the **same account** inside
`grouping.window_minutes` (default 60) as a linked decision, instead of opening
a duplicate. Linking adds the amount to `amount_total`, recomputes priority and
records a `linked` event. Decisions carry no scenario-run id, so run and reason
overlap are not grouping keys.

## Priority

```
priority = risk_score
         + min(20, amount_total / 100000)
         + (10 if the account had another case in the last 30 days)
         + (15 if the decision class is BLOCK)
         + min(20, 5 * linked decision count)
```

clamped to 0–100. Weights live under `priority` in `config/cases.yaml`.

## SLA

`SLADueAt` is set at creation: BLOCK 30 minutes, HOLD 15, STEP_UP_FAILED 15,
QA_SAMPLE 24 hours.

- The sweep runs every `CASE_SLA_SWEEP_INTERVAL` (default 30s) and processes at
  most 200 cases per pass.
- `breach_policy: escalate` (default) escalates an overdue case once
  (`escalated = true`); later sweeps skip it. `notify` and `none` leave the
  case untouched and only affect what the sweep reports.
- A resolution waiting for a second reviewer pauses the clock
  (`sla_paused_at`); approval clears it.
- Reopening restarts the clock.
- There is no automatic release: a hold is only released by an analyst verdict.

## Four eyes

`four_eyes.enabled` requires a second reviewer when:

- a BLOCK is overturned to `LEGIT`
  (`block_overturn_requires_approval`), or
- `amount_total` exceeds `amount_threshold` (5,000,000 RWF).

The first verdict is stored with `pending_approval = true` and **no label**. A
different lead or admin calls `approve`; the resolver cannot approve their own
verdict (`SELF_APPROVAL`). The approver's action writes the label, so an
unapproved verdict can never reach the feedback loop. The analyst's
`propagate_linked` choice is persisted and applied at approval time.

## Labels

`label_history` is append-only: a new verdict supersedes the previous one for
the same event (`superseded_by`) but never deletes it. `labels` holds the
current label per event for training and evaluation.

- Analyst verdicts are written with source `ANALYST`.
- With `propagate_linked: true`, each linked decision of the case is labelled
  with source `PROPAGATED`. Propagation never overwrites an existing label: a
  human verdict on that decision wins.
- Every label is published to the `labels.feedback` outbox topic with the
  event id, label, source, case id and amount. The relay in `case-worker`
  publishes through Kafka.
- `label_mature_hours` (168) records when a label may be used for evaluation,
  so a fresh verdict is not treated as ground truth immediately.
- Analysts are pseudonymous throughout (`usr_...`), as are accounts
  (`acct_...`) and decisions (`dec_...`).

## Label quality

A lead can submit a blind second opinion with
`POST /v1/cases/{id}/second-review`. `GET /v1/cases/metrics/labels` reports the
number of samples, observed agreement and Cohen's κ against the case label.
Automatic assignment of second reviews is not implemented; sampling is an
operational process rather than a worker task.

## Notes

Notes are limited to 2,000 characters and scanned before storage: any Rwanda
mobile number (`07XXXXXXXX`), email address or 16-digit sequence is rejected
with `REDACTED_CONTENT`. Analysts reference accounts by pseudonym, so a note
should never need to contain one.

## Idempotency and concurrency

Every mutating case endpoint requires an `Idempotency-Key` header (1–128
printable characters). The first response is stored in `case_idempotency` and
replayed with `Idempotent-Replay: true`; a second writer keeps the first
response. Mutations also carry an optional version (`If-Match` or the `version`
body field) and fail with `VERSION_CONFLICT` rather than overwrite another
analyst's work. Adding a note bumps the version, so clients re-read the case
before the next action.

## Feedback-loop risks

- **Confirmation bias**: analysts see the model's score. QA-sampled ALLOW cases
  are the unbiased sample; use `source = 'QA_SAMPLE'` for evaluation and as
  importance-weighted negatives.
- **Label shift**: rules and models change; `label_history` plus the decision
  snapshot allow a re-scoring rather than re-labelling.
- **Missing negatives**: only HOLD, BLOCK and sampled ALLOW produce labels, so
  the labelled set is not a random sample of traffic.
- **Propagated labels** are weaker evidence than human ones; keep the source
  column when training so they can be down-weighted or excluded.
