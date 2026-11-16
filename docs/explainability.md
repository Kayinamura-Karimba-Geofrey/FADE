# FADE explainability

Every FADE decision carries a human-readable **and** structured explanation. An
explanation is a view over stored facts, not a re-run of the model and not a
free-form generation: each sentence maps to a stored rule id, feature value,
model contribution or override, and the same decision always renders the same
sentences.

- Package: `internal/explain`
- Templates: `internal/explain/templates/{en,fr,rw}.yaml`
- Endpoint: `GET /v1/decisions/{id}/explain?audience=analyst|customer|regulator&lang=en|fr|rw`
- CLI: `fade explain -decision <id> -audience customer -lang rw`
- Schema: `explain_schema_version: 1` (breaking changes require an OpenAPI bump)

## 1. Facts, not prose

The decision path stores:

| Fact | Stored on | Example |
| --- | --- | --- |
| Fired rule id, weight, mode | `decisions.reasons[]` | `R-VEL-001`, 35, `ENFORCE` |
| Structured reason parameters | `reasons[].params` | `{"velocity_10m":"12","amount_sum_10m":"70000"}` |
| ML contributions | `reasons[]` with `source: model` | `velocity_10m`, SHAP 0.31 |
| List / tier / fail-policy overrides | `reasons[]` | `LIST_BLOCK`, `subject: ACCOUNT` |
| Rule set version + hash | `decisions.rule_set_version`, `ruleset_hash` | `7`, `sha256:…` |
| Model version | `decisions.model_version` | `lgbm-2026.11.1` |
| Snapshot id | `decisions.snapshot_id` | `snap_…` |
| Feature vector | `decisions.features` | `{"velocity_10m":12,…}` |
| Combiner thresholds | `decisions.provenance.combiner` | `{"hold_threshold":65,…}` |

`pipeline.Reason.Params` is populated when the rule fires, from the compiled
reason template's placeholders (`rules.ReasonTemplate.Params`). Rendering never
parses the English sentence to recover a value. A template that needs a
parameter which is absent **fails closed**: the renderer returns `ok=false` and
the analyst view falls back to the stored, already-structured decision text. It
never emits a half-filled sentence.

`TestTemplateCoverage` (in `internal/explain/explain_test.go`) loads
`rules/prod.json` and fails if any production rule lacks a sentence in `en`,
`fr` or `rw`, or if a language uses a different placeholder set.

## 2. Rendering pipeline

1. Load the stored decision and verify it exists.
2. Validate audience and language. Unknown audiences are a `400`; unknown
   languages fall back to `en` and the response reports the language used.
3. Build reason entries. Rule reasons re-render from `params` through the
   catalog; model reasons render `feature.<name>` with the observed value.
4. Sort factors by absolute contribution (rule weight or SHAP value);
   `primary_factor` is the first entry.
5. Counterfactuals are computed from the risk score and the combiner thresholds.
6. Audience projection (below).
7. Privacy guard: `redaction.CheckJSON` scans the encoded view for raw account,
   MSISDN, card and national-id patterns. A hit withholds the response with
   `500 PRIVACY_GUARD` instead of leaking it.
8. Emit `Cache-Control: no-store`.

Factors are capped at 8; customer reasons at 3. Ordering is deterministic:
absolute contribution descending, then name ascending.

## 3. Audiences

### Analyst (default)
Full facts: decision, scores, rule ids, weights, modes, features, SHAP
contributions, combiner config, provenance, decision hash, counterfactuals.

### Regulator
Everything the analyst sees plus the complete non-zero feature vector. Intended
for supervisory review and reproduction (`docs/audit.md`). No customer
identifiers are present in the feature vector or reason set.

### Customer
A deliberately reduced document. The wire format is enforced by
`View.MarshalJSON`, not by convention, so a field added to `View` later cannot
leak by accident. It contains only:

- `decision_id` (support reference), `decision`, `summary`
- localized `reasons[]` drawn from fixed customer-safe categories
- `language`, `language_needs_review`, `explain_schema_version`, `generated_at`

It never contains rule ids, feature names, weights, thresholds, scores, model
names/versions, SHAP values, event ids, the decision hash or provenance.
Customer sentences contain no numbers at all; `TestCustomerViewLeaksNothing`
asserts this across all four decisions.

Category selection (`customerCategory`):

| Internal source | Customer category |
| --- | --- |
| `LIST_BLOCK` | blocked |
| `LIST_ALLOW` | allowed |
| `TIER_LIMIT_EXCEEDED` | limit |
| `R-ATO-*` | account_security |
| `R-MUL-*`, `R-AGT-*` | recipient_pattern |
| `R-SOC-*` | new_recipient |
| other `R-*` | unusual_activity |
| `ML_VERY_HIGH_CONFIDENCE`, `FAIL_POLICY_*` | review |

Shadow rules (`R-EXP-*`) and model contributions are never disclosed to a
customer, and shadow hits are excluded from the customer document entirely.

## 4. Languages

`en` is authoritative and reviewed. `fr` and `rw` are machine-drafted; both
catalogs set `needs_review: true`, and every API response for those languages
includes `"language_needs_review": true`. Do **not** present machine-drafted
copy as officially approved.

Machine-drafted catalogs pending native review:

- `internal/explain/templates/fr.yaml` — all rule, system, feature and customer
  strings.
- `internal/explain/templates/rw.yaml` — all rule, system, feature and customer
  strings.

Both files carry the flag at the top and the flag is asserted by
`TestTemplateCoverage`. A reviewer should edit the YAML, set
`needs_review: false` only after sign-off, and bump `version`.

Rule ids used by the production rule set are all covered:
`R-VEL-001/002`, `R-STR-001/002`, `R-ATO-001/002/003`, `R-MUL-001/002/003`,
`R-AGT-001/002`, `R-CIR-001`, `R-SOC-001/002`, `R-RAW-001`, `R-EXP-001/002/003`.

## 5. Counterfactuals

Counterfactuals are expressed in **score points against the configured
thresholds**, a quantity FADE can verify exactly. It does not claim that a
specific real-world action (for example "remove one transfer") would produce a
given score, because that requires a counterfactual feature replay that Phase 5
does not ship.

Examples (thresholds 40/65/85):

| Decision | Score | Suggested change | Result |
| --- | --- | --- | --- |
| BLOCK | 95 | −11 points | HOLD |
| HOLD | 72 | −8 points | STEP_UP |
| STEP_UP | 45 | −6 points | ALLOW |
| ALLOW | 10 | +30 points | STEP_UP |

Thresholds come from `decisions.provenance.combiner`, so a historical decision
is explained against the policy that was actually in force.

## 6. Decision hash and reproduction

`explain.DecisionHash` is the SHA-256 of the canonical JSON of the decision
facts: ids, decision, scores, ML status, rule-set version/hash, snapshot id,
reasons (including params) and shadow hits. It intentionally excludes latency
and wall-clock timestamps, so a reproduction on another machine agrees.

`fade reproduce` re-runs the stored rule set over the stored feature vector and
compares the recomputed decision facts with the stored hash. See
`docs/audit.md` for the reproducibility check and its measured coverage.

## 7. Latency

Explanation rendering is **off the decision path**. The synchronous path only
allocates the small `params` map when a rule fires (a handful of strings); it
never loads templates, formats sentences or canonicalises a document.

Measured on the development host (Go 1.23, i7-1355U, `go test -bench`):

| Operation | Time per op | Allocations |
| --- | --- | --- |
| Analyst view (full) | 91 µs | 759 |
| Customer view, `rw` | 85 µs | 745 |

Both are ~11× under the 1 ms p99 budget for the explain endpoint, and the
decision path adds no rendering work at all. The measurement is a microbenchmark
of `Builder.Build`; it is not an HTTP round-trip. Reproduce with:

```bash
make bench-explain
```

## 8. Extending the catalog

1. Add the sentence to `en.yaml` with `{param}` placeholders.
2. Add the same placeholder set to `fr.yaml` and `rw.yaml`; mark drafts
   `needs_review: true` in the file header.
3. If the sentence belongs to a production rule, extend the rule's `reason` in
   `rules/prod.json` so the same parameters are stored at decision time.
4. Run `go test ./internal/explain/...`; coverage and placeholder parity are
   enforced.
5. If the change alters the response shape, bump `explain_schema_version` and
   `api/openapi.yaml` together.

## 9. Limitations

- `fr` and `rw` have not been reviewed by native speakers; the `needs_review`
  flag is the honest status, not a formality.
- Counterfactuals describe score movement, not real-world actions (see §5).
- The privacy guard is pattern-based. It reliably catches raw MSISDNs, account
  ids, card-like numbers and national ids, but it is not a substitute for never
  putting identifiers into features or reasons in the first place.
- Analyst summaries are English regardless of `lang`; only reasons, customer
  summaries and feature sentences are localized.
