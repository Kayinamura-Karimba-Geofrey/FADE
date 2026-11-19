# Security

## Scan results (Phase 8, 2026-11-19)

| Tool | Scope | Findings | Disposition |
|---|---|---|---|
| govulncheck | Go modules | 55 reachable stdlib advisory entries on Go 1.23.4 plus 7 module entries | Tracked in `docs/known-issues.md`; toolchain bump to 1.23.5+ not applied. |
| staticcheck | Go | 8 findings (unused `poolPinger`, SA4000 ×2, S1021, S1011 ×2, unused `lastActor`) | Fixed; `staticcheck ./...` is clean. |
| gosec | Go | 72 issues, mostly G104 unchecked errors | Triaged as low risk; not all rewritten. |
| pip-audit / bandit | Python | Not run (not installed). Dependencies are pinned in `pyproject.toml`/`uv.lock`. | Gap, not a pass. |
| npm audit | dashboard | Not run (not invoked in this window). | Gap, not a pass. |
| trivy / hadolint / checkov / kics / gitleaks / syft / cosign | images, Compose, secrets, SBOM, signing | Not installed. | Gap; no clean result claimed. |

## Threat model (STRIDE summary)

Components: decision-api, feature-worker, case-worker, replay-worker, ml-scorer,
dashboard (BFF), Redis, Postgres, Redpanda, Prometheus/Grafana.

Trust boundaries: client→dashboard (JWT), dashboard→decision-api (BFF, httpOnly
cookie), decision-api→Redis/Postgres/Redpanda/ml-scorer, operator→admin
endpoints (JWT + role).

Controls present in the code:

- Rule activation is audited and hash-chained; old versions remain available.
- Idempotency keys prevent duplicate decision persistence on retries.
- Analyst actions are role-checked and written to the audit trail.
- Model artifacts are verified against a sha256 manifest at load.
- Request body size and rate limits exist on the decision and admin endpoints.
- File-backed list imports validate paths.

Controls **not** implemented in the default Compose profile (checklist for a
production deployment):

- non-root containers, read-only root filesystem, dropped capabilities and
  `no-new-privileges`;
- TLS/mTLS between services;
- Redis `requirepass`/ACLs and separate Postgres roles (`fade_app`,
  `fade_audit_ro`, `fade_migrate`);
- Redpanda SASL/SCRAM and topic ACLs;
- Grafana/Prometheus authentication;
- secrets mounted as files instead of environment variables.

The default profile is for local/offline evaluation. Do not expose it directly
to an untrusted network.

## Supply chain

- Go modules are pinned in `go.sum`.
- Python dependencies are pinned in `pyproject.toml` and `uv.lock`.
- Dashboard dependencies are locked by `package-lock.json`.
- Third-party images in `deploy/docker-compose.yml` are pinned by tag **and**
  digest. Locally built FADE images are pinned by the `1.0.0` tag; publishing
  them by digest requires a registry push and was not done in this environment.
