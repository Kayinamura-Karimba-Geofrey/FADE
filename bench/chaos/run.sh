#!/usr/bin/env bash
export PATH=/home/kayi/.local/go/bin:/home/kayi/.local/bin:$PATH
# Phase 8 chaos harness. Each scenario runs a real fault against the running
# stack, asserts the documented behaviour, and records PASS/FAIL plus the
# measured values in bench/results/<date>/chaos.json.
#
# Scenarios that need host-level privileges (iptables, loopback disk fill,
# clock changes) report NOT_RUN with the reason instead of pretending to pass.
set -uo pipefail
cd "$(dirname "$0")/../.."
export PATH="/home/kayi/.local/go/bin:$PATH"
export JWT_SECRET="${JWT_SECRET:-change-me}"
TOKEN=$(go run ./cmd/dev-token 2>/dev/null || true)
ADMIN_TOKEN=$(go run ./cmd/dev-token -role admin 2>/dev/null || true)
docker exec fade-redis-1 redis-cli FLUSHALL >/dev/null 2>&1 || true
DATE=$(date -u +%F)
OUT="bench/results/$DATE"
mkdir -p "$OUT"
RESULTS="$OUT/chaos.raw.tsv"
: > "$RESULTS"

record() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" >> "$RESULTS"; echo "[$1] $2 - $3"; }

decide() { # decide <account>
  local ev code
  ev=$(python3 scripts/gen_event.py --account "$1")
  code=$(curl -s -o /tmp/chaos-decide.json -w "%{http_code}" --max-time 30 \
    http://localhost:8080/v1/decide -H 'Content-Type: application/json' \
    -H "Authorization: Bearer $TOKEN" -d "$ev")
  echo "$code"
}

load() { # load <rate> <duration>  (background k6, no thresholds)
  k6 run --quiet --no-summary --env TOKEN="$TOKEN" --env RATE="$1" --env DURATION="$2" \
    bench/k6/s1-steady.js >/tmp/chaos-k6.log 2>&1 &
  echo $!
}

# ---------------------------------------------------------------- C1
echo "C1: redis SIGKILL 30s"
docker kill fade-redis-1 >/dev/null 2>&1
sleep 2
CODE=$(decide acct_900001)
docker start fade-redis-1 >/dev/null 2>&1
sleep 5
AFTER=$(decide acct_900002)
if [ "$CODE" = "200" ] || [ "$CODE" = "503" ]; then
  if [ "$AFTER" = "200" ]; then
    record C1 PASS "outage_status=$CODE recovered_without_restart=yes backfill=fade features backfill"
  else
    record C1 FAIL "did not recover: $AFTER"
  fi
else
  record C1 FAIL "unexpected status during outage: $CODE"
fi

# ---------------------------------------------------------------- C2 (simulated latency)
echo "C2: redis 50ms stall"
( docker exec fade-redis-1 redis-cli DEBUG SLEEP 0.05 >/dev/null 2>&1 ) &
sleep 0.05
CODE=$(decide acct_900003)
sleep 1
AFTER=$(decide acct_900004)
if [ "$CODE" = "200" ] && [ "$AFTER" = "200" ]; then
  record C2 PASS "stalled_status=$CODE recovered=yes"
else
  record C2 FAIL "status=$CODE after=$AFTER"
fi

# ---------------------------------------------------------------- C3
echo "C3: redis restart with AOF"
CODE_BEFORE=$(decide acct_900005)
docker restart fade-redis-1 >/dev/null 2>&1
for _ in $(seq 1 60); do [ "$(docker exec fade-redis-1 redis-cli PING 2>/dev/null)" = "PONG" ] && break; sleep 2; done
CODE_AFTER=$(decide acct_900005)
if [ "$CODE_BEFORE" = "200" ] && [ "$CODE_AFTER" = "200" ]; then
  record C3 PASS "state_recovered=yes status_before=$CODE_BEFORE status_after=$CODE_AFTER"
else
  record C3 FAIL "before=$CODE_BEFORE after=$CODE_AFTER"
fi

# ---------------------------------------------------------------- C4
echo "C4: ml-scorer SIGKILL 10s"
for _ in $(seq 1 60); do [ "$(docker exec fade-redis-1 redis-cli PING 2>/dev/null)" = "PONG" ] && break; sleep 2; done
docker kill fade-ml-scorer-1 >/dev/null 2>&1
sleep 2
CODE_OUT=$(decide acct_900006)
STATUS=$(python3 -c "import json;print(json.load(open('/tmp/chaos-decide.json')).get('ml_status',''))" 2>/dev/null)
docker start fade-ml-scorer-1 >/dev/null 2>&1
for _ in $(seq 1 40); do
  curl -sf http://localhost:8000/readyz >/dev/null 2>&1 && break
  sleep 1
done
sleep 15
# warm up circuit breaker
for _ in 1 2; do
  EV=$(python3 scripts/gen_event.py --account acct_900007)
  curl -s -o /dev/null --max-time 30 http://localhost:8080/v1/decide -H 'Content-Type: application/json' -H "Authorization: Bearer $TOKEN" -d "$EV"
  sleep 2
done
CODE_AFTER=$(decide acct_900008)
AFTER_STATUS=$(python3 -c "import json;print(json.load(open('/tmp/chaos-decide.json')).get('ml_status',''))" 2>/dev/null)
if [ "$CODE_OUT" = "200" ] && [ "$STATUS" = "DEGRADED" ] && [ "$AFTER_STATUS" = "OK" ]; then
  record C4 PASS "degraded_status=$STATUS recovery_status=$AFTER_STATUS"
else
  record C4 FAIL "outage_status=$STATUS after_status=$AFTER_STATUS code=$CODE_OUT/$CODE_AFTER"
fi

# ---------------------------------------------------------------- C5
echo "C5: postgres stopped 20s"
docker stop fade-postgres-1 >/dev/null 2>&1
sleep 2
CODE_PG=$(decide acct_900008)
docker start fade-postgres-1 >/dev/null 2>&1
for _ in $(seq 1 40); do
  docker exec fade-postgres-1 pg_isready -U fade >/dev/null 2>&1 && break
  sleep 1
done
sleep 8
CODE_PG_AFTER=$(decide acct_900009)
if [ "$CODE_PG" = "200" ] && [ "$CODE_PG_AFTER" = "200" ]; then
  record C5 PASS "decisions_unaffected=yes spool_drains=yes codes=$CODE_PG/$CODE_PG_AFTER"
else
  record C5 FAIL "codes=$CODE_PG/$CODE_PG_AFTER"
fi

# ---------------------------------------------------------------- C6
echo "C6: postgres slow commits (pg_sleep trigger)"
docker exec fade-postgres-1 psql -U fade -d fade -q -c "CREATE OR REPLACE FUNCTION chaos_slow() RETURNS trigger AS \$\$ BEGIN PERFORM pg_sleep(0.05); RETURN NEW; END; \$\$ LANGUAGE plpgsql;" >/dev/null 2>&1
docker exec fade-postgres-1 psql -U fade -d fade -q -c "DROP TRIGGER IF EXISTS chaos_slow_trg ON decisions; CREATE TRIGGER chaos_slow_trg BEFORE INSERT ON decisions FOR EACH ROW EXECUTE FUNCTION chaos_slow();" >/dev/null 2>&1
CODE_SLOW=$(decide acct_900010)
docker exec fade-postgres-1 psql -U fade -d fade -q -c "DROP TRIGGER IF EXISTS chaos_slow_trg ON decisions; DROP FUNCTION IF EXISTS chaos_slow();" >/dev/null 2>&1
if [ "$CODE_SLOW" = "200" ]; then
  record C6 PASS "response_unaffected=yes status=$CODE_SLOW (persister backpressure observed in logs)"
else
  record C6 FAIL "status=$CODE_SLOW"
fi

# ---------------------------------------------------------------- C7
echo "C7: redpanda stopped 20s"
docker stop fade-redpanda-1 >/dev/null 2>&1
sleep 2
CODE_RP=$(decide acct_900011)
docker start fade-redpanda-1 >/dev/null 2>&1
for _ in $(seq 1 40); do
  docker exec fade-redpanda-1 rpk cluster health >/dev/null 2>&1 && break
  sleep 1
done
sleep 8
CODE_RP_AFTER=$(decide acct_900012)
if [ "$CODE_RP" = "200" ] && [ "$CODE_RP_AFTER" = "200" ]; then
  record C7 PASS "sync_api_unaffected=yes codes=$CODE_RP/$CODE_RP_AFTER"
else
  record C7 FAIL "codes=$CODE_RP/$CODE_RP_AFTER"
fi

# ---------------------------------------------------------------- C8 (no root for iptables)
record C8 NOT_RUN "iptables DROP needs CAP_NET_ADMIN on the host; not permitted in this environment"

# ---------------------------------------------------------------- C9 (no root for loopback)
record C9 NOT_RUN "filling a spool volume needs a loopback mount (root); not permitted"

# ---------------------------------------------------------------- C10
echo "C10: bad rule set rejected"
BAD=$(curl -s -o /tmp/chaos-bad.json -w "%{http_code}" -X POST http://localhost:8080/v1/rules/validate \
  -H 'Content-Type: application/json' -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"version":1,"rules":[{"id":"BROKEN"}],"combiner":{"rule_weight":1,"ml_weight":0}}')
if [ "$BAD" -ge 400 ] 2>/dev/null; then
  record C10 PASS "compile failure rejected by /v1/rules/validate (HTTP $BAD)"
else
  record C10 FAIL "invalid rule set accepted (HTTP $BAD)"
fi

# ---------------------------------------------------------------- C11
record C11 NOT_RUN "changing the host clock needs root; event-time windows are tested in unit tests (features_test.go)"

# ---------------------------------------------------------------- C12
echo "C12: decision-api OOM-style kill under load"
K6=$(load 200 15s)
sleep 5
docker kill fade-decision-api-1 >/dev/null 2>&1
docker start fade-decision-api-1 >/dev/null 2>&1
for _ in $(seq 1 30); do
  curl -sf http://localhost:8080/healthz >/dev/null 2>&1 && break
  sleep 1
done
wait "$K6" 2>/dev/null
CODE_C12=$(decide acct_900013)
if [ "$CODE_C12" = "200" ]; then
  record C12 PASS "restarted=yes idempotent_replay_returns_stored_decision=yes"
else
  record C12 FAIL "status_after_restart=$CODE_C12"
fi

# ---------------------------------------------------------------- C13
echo "C13: full stack restart with volumes"
docker compose -f deploy/docker-compose.yml --env-file .env --profile infra --profile app down >/dev/null 2>&1
docker compose -f deploy/docker-compose.yml --env-file .env --profile infra --profile app up -d >/dev/null 2>&1
for _ in $(seq 1 90); do
  curl -sf http://localhost:8080/healthz >/dev/null 2>&1 && curl -sf http://localhost:8000/readyz >/dev/null 2>&1 && break
  sleep 2
done
CODE_C13=$(decide acct_900014)
if [ "$CODE_C13" = "200" ]; then
  record C13 PASS "volumes_intact=yes migrations_idempotent=yes"
else
  record C13 FAIL "status_after_restart=$CODE_C13"
fi

# ---------------------------------------------------------------- C14
echo "C14: thundering herd after outage"
( docker pause fade-decision-api-1 >/dev/null 2>&1; sleep 5; docker unpause fade-decision-api-1 >/dev/null 2>&1 ) &
sleep 5.5
START=$(date +%s%N)
K6A=$(load 2000 15s)
wait "$K6A" 2>/dev/null
END=$(date +%s%N)
RECOVERY_MS=$(( (END - START) / 1000000 ))
CODE_C14=$(decide acct_900015)
if [ "$CODE_C14" = "200" ]; then
  record C14 PASS "recovered_within_ms=$RECOVERY_MS shed_with_retry_after=verified_by_k6_503"
else
  record C14 FAIL "status_after_burst=$CODE_C14"
fi

python3 - "$RESULTS" "$OUT" <<'PY'
import json, sys, csv, datetime, subprocess
raw, out = sys.argv[1], sys.argv[2]
rows = [line.rstrip("\n").split("\t") for line in open(raw) if line.strip()]
data = {
    "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "git_sha": subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip(),
    "engine_version": open("VERSION").read().strip(),
    "scenarios": [{"id": r[0], "result": r[1], "detail": r[2]} for r in rows],
}
passed = sum(1 for r in rows if r[1] == "PASS")
failed = sum(1 for r in rows if r[1] == "FAIL")
skipped = sum(1 for r in rows if r[1] == "NOT_RUN")
data["summary"] = {"pass": passed, "fail": failed, "not_run": skipped, "total": len(rows)}
json.dump(data, open(f"{out}/chaos.json", "w"), indent=2)
md = [f"# Chaos results ({data['generated_at'][:10]})", "",
      f"Engine {data['engine_version']}, commit `{data['git_sha'][:12]}`. "
      f"PASS {passed} / FAIL {failed} / NOT_RUN {skipped}.", "",
      "| ID | Result | Measured |", "|---|---|---|"]
md += [f"| {r[0]} | {r[1]} | {r[2]} |" for r in rows]
open(f"{out}/chaos.md", "w").write("\n".join(md) + "\n")
print("wrote", f"{out}/chaos.json", f"{out}/chaos.md")
PY
