# Contributing to FADE

## Development loop

```bash
make up            # infra + app containers
make migrate       # database migrations
make test          # Go tests and pytest
make lint          # go vet, staticcheck, ruff, eslint
make dash-build    # dashboard production build
```

Every change should keep `make test`, `make lint` and `make dash-build` green. The release branch
also runs `python3 scripts/readme_tables.py --check` so the README headline
numbers cannot drift from the committed evidence in `bench/results/`.

## Design rules

- Decisions must stay deterministic for a given event, rule set and model
  version. Replay and live scoring share the same evaluator.
- New failure handling must degrade, not block: a unavailable dependency should
  produce an explicit degraded decision with a metric.
- Do not weaken a benchmark target to make a run pass. Record the measured
  number and explain the bottleneck.
- Generated evidence belongs in `bench/results/<date>/` and must name the git
  sha, engine version, dataset hash, seeds and exact command.

## Commits

Use short, imperative subjects, for example `fix: bound redis dial timeout`.
Keep refactors separate from behaviour changes. Reference the affected phase or
issue in the body when useful.

## Security

Please do not open a public issue for a vulnerability. Follow `SECURITY.md`.
