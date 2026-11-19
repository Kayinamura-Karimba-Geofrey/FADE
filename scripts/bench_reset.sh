#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Wipe volumes
docker compose -f deploy/docker-compose.yml --env-file .env --profile infra --profile app down -v --remove-orphans || true
# Start infra
docker compose -f deploy/docker-compose.yml --env-file .env --profile infra up -d
# Wait for health
sleep 20
# Migrate
docker compose -f deploy/docker-compose.yml --env-file .env --profile infra --profile app run --rm migrate
# Load rules/model happens via migrate/registry; warm-up placeholder
echo "bench-reset: clean state prepared"