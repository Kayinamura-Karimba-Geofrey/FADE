# FADE Documentation Index

FADE documentation lives here. The top-level [README](../README.md) covers
the quick start, ports, make targets and the phase roadmap.

| Document | Contents |
| --- | --- |
| [phase-0.md](phase-0.md) | Phase 0 (Foundations): what was built and the exit-check results |
| phase-1.md | Event ingestion and schemas _(planned)_ |
| phase-2.md | Feature store and feature worker _(planned)_ |
| [phase-3.md](phase-3.md) | Phase 3 (Rules engine and synchronous decision API): delivered surface, evidence, exit checks |
| phase-4.md | ML scoring, training and explainability _(planned)_ |
| [explainability.md](explainability.md) | Explanation facts, audiences, languages, counterfactuals, latency |
| [audit.md](audit.md) | Hash chain, genesis, checkpoints, tamper tests, retention, runbook |
| phase-5.md | Phase 5 delivery and exit-check results |
| [replay.md](replay.md) | Phase 6 (replay, shadow evaluation, regression gate) |
| [replay-regression-changelog.md](replay-regression-changelog.md) | Replay gate decisions and regression history |
| [cases.md](cases.md) | Phase 7 case lifecycle, priority, SLA, four-eyes and labels |
| [dashboard.md](dashboard.md) | Phase 7 analyst dashboard: BFF auth, queue, case detail, metrics |
| phase-8.md | Hardening, load testing and runbooks _(planned)_ |

Conventions:

- One document per phase; a phase is done when its exit checks pass.
- Every pinned version (images, packages, actions) is listed in the phase
  document that introduced it.
