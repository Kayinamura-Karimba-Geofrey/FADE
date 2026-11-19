#!/usr/bin/env bash
# Create a backup bundle: PostgreSQL custom-format dump + Redis RDB snapshot.
#
# Usage: bash scripts/backup.sh [output-dir]
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f deploy/docker-compose.yml --env-file .env --profile infra)
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${1:-backups/$STAMP}"
mkdir -p "$OUT"

echo "backup: postgres -> $OUT/fade.dump"
"${COMPOSE[@]}" exec -T postgres sh -c \
  'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc -Z9' > "$OUT/fade.dump"

echo "backup: redis snapshot"
"${COMPOSE[@]}" exec -T redis redis-cli SAVE >/dev/null
REDIS_ID="$("${COMPOSE[@]}" ps -q redis)"
docker cp "$REDIS_ID:/data/dump.rdb" "$OUT/redis.rdb"

( cd "$OUT" && sha256sum fade.dump redis.rdb > SHA256SUMS )
echo "backup: wrote $OUT"
ls -lh "$OUT"
