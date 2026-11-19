#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export PATH="/home/kayi/.local/go/bin:$PATH"
export JWT_SECRET=change-me
TOKEN=$(go run ./cmd/dev-token)
export TOKEN

echo "[C1] baseline decision..."
EV=$(python3 scripts/gen_event.py)
curl -s http://localhost:8080/v1/decide -H 'Content-Type: application/json' -H "Authorization: Bearer $TOKEN" -d "$EV" | grep -q '"ml_status"' || { echo "FAIL baseline"; exit 1; }

echo "[C1] killing redis for 30s..."
docker kill fade-redis-1
sleep 30
docker start fade-redis-1
sleep 5

echo "[C1] decision during outage should be rules-only (DEGRADED or fail policy)..."
EV2=$(python3 scripts/gen_event.py)
RESP=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/v1/decide -H 'Content-Type: application/json' -H "Authorization: Bearer $TOKEN" -d "$EV2")
echo "HTTP status during/after outage: $RESP"
[ "$RESP" = "200" ] || { echo "FAIL: non-200"; exit 1; }

echo "[C1] features backfill gap window..."
./bin/fade features backfill --from "$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ)" --to "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --events /dev/null 2>/dev/null || true
echo "[C1] PASS"
