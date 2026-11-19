# Security policy

## Supported versions

| Version | Supported |
| --- | --- |
| 1.0.x | yes |
| < 1.0 | no |

## Reporting a vulnerability

Do not open a public issue. Report privately to the maintainers with:

- affected version or commit,
- a minimal reproduction,
- the impact you observed,
- any suggested fix.

We aim to acknowledge within three working days and to ship a fix or mitigation
within 30 days for high-severity issues.

## Deployment assumptions

FADE is a local/offline reference deployment. Before exposing it to a network:

- replace every default secret in `.env` (`POSTGRES_PASSWORD`, `JWT_SECRET`,
  `GRAFANA_ADMIN_PASSWORD`);
- terminate TLS in front of the decision API and dashboard;
- restrict `/metrics` and Grafana to an internal network;
- keep the ML scorer and Redis on a private network;
- rotate the JWT secret and revoke outstanding tokens after a suspected leak.

## Supply chain

- Go modules are pinned in `go.sum`; Python dependencies are pinned in
  `ml/serve/requirements.txt`; dashboard dependencies are locked by
  `package-lock.json`.
- Container images are pinned by tag and digest in `deploy/docker-compose.yml`
  for the release.
- `govulncheck`, `gosec`, `npm audit`, `pip-audit` and image scanning are the
  expected checks. Gaps in the tooling available in a given environment are
  recorded in `docs/security.md` rather than silently skipped.
