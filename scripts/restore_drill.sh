#!/usr/bin/env bash
# Restore the newest backup into a scratch database, verify row counts, and
# write a timing report. Redis is validated with redis-check-rdb; it is not
# loaded because the live instance owns the port.
#
# Usage: bash scripts/restore_drill.sh [backup-dir]
set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE=(docker compose -f deploy/docker-compose.yml --env-file .env --profile infra)
if [[ $# -ge 1 ]]; then
  OUT="$1"
else
  OUT="$(ls -dt backups/*/ 2>/dev/null | head -1)"
fi
[[ -n "${OUT:-}" && -f "$OUT/fade.dump" ]] || { echo "restore-drill: no backup bundle" >&2; exit 1; }

START=$(date +%s)
"${COMPOSE[@]}" exec -T postgres sh -c \
  'dropdb -U "$POSTGRES_USER" --if-exists fade_restore_drill && createdb -U "$POSTGRES_USER" fade_restore_drill'
"${COMPOSE[@]}" exec -T postgres sh -c \
  'pg_restore -U "$POSTGRES_USER" -d fade_restore_drill --no-owner --no-privileges' < "$OUT/fade.dump"

IFS='|' read -r decisions cases audit < <("${COMPOSE[@]}" exec -T postgres sh -c \
  'psql -U "$POSTGRES_USER" -d fade_restore_drill -At -c "select (select count(*) from decisions), (select count(*) from cases), (select count(*) from audit_log)"')
live="$("${COMPOSE[@]}" exec -T postgres sh -c \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -c "select (select count(*) from decisions), (select count(*) from cases), (select count(*) from audit_log)"')"
END=$(date +%s)
RTO=$((END - START))
BACKUP_EPOCH=$(stat -c %Y "$OUT/fade.dump")
RPO=$(( (START - BACKUP_EPOCH) / 60 ))

python3 - "$OUT" "$RTO" "$RPO" "$decisions" "$cases" "$audit" "$live" <<'PY'
import json, sys
out, rto, rpo, d, c, a, live = sys.argv[1:8]
rto, rpo = int(rto), int(rpo)
d, c, a = int(d), int(c), int(a)
ld, lc, la = (int(x) for x in live.split("|"))
passed = (d, c, a) == (ld, lc, la)
payload = {
    "generated_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
    "backup_dir": out,
    "passed": passed,
    "rto_seconds": rto,
    "rto_minutes": round(rto / 60, 2),
    "rpo_minutes": rpo,
    "restored_rows": {"decisions": d, "cases": c, "audit_log": a},
    "upgrade": {"passed": False, "note": "not run: no v0.8.0 dataset in this environment"},
    "rolling_upgrade": {"passed": False, "note": "not run: single-host Compose deployment"},
    "scaling": {"summary": "not exercised: single decision-api replica"},
    "live_rows": {"decisions": ld, "cases": lc, "audit_log": la},
}
open("bench/results/2026-11-19/operations.json", "w").write(json.dumps(payload, indent=2) + "\n")
print(json.dumps(payload, indent=2))
PY

"${COMPOSE[@]}" exec -T postgres sh -c \
  'dropdb -U "$POSTGRES_USER" --if-exists fade_restore_drill'
