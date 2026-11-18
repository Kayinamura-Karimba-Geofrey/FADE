#!/usr/bin/env bash
# Phase 6 replay regression: runs the frozen golden dataset and compares the
# per-decision digest and the deterministic report bytes.
set -euo pipefail
cd "$(dirname "$0")/.."

UPDATE=${UPDATE:-0}
REASON=${REASON:-}
OUT=${OUT:-/tmp/fade-replay-regression}
JOB=${JOB:-9001}
REDIS_ADDR=${REDIS_ADDR:-localhost:6379}

if [[ "$UPDATE" == "1" ]]; then
  if [[ -z "$REASON" ]]; then
    echo "replay-regression: UPDATE=1 requires REASON='...'" >&2
    exit 2
  fi
  mkdir -p tests/golden-replay
fi

go build -o bin/fade ./cmd/fade
rm -rf "$OUT"
REDIS_ADDR="$REDIS_ADDR" ./bin/fade replay run \
  --dataset data/golden \
  --baseline file:rules/prod.json \
  --candidate neutral=file:rules/prod.json \
  --job "$JOB" --out "$OUT" --seed 11 >/dev/null

if [[ "$UPDATE" == "1" ]]; then
  cp "$OUT/report.json" tests/golden-replay/expected_report.json
  cp "$OUT/decisions.sha256" tests/golden-replay/expected_decisions.sha256
  printf -- '- %s: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$REASON" >> docs/replay-regression-changelog.md
  echo "replay-regression: expectations updated (reason recorded)"
  exit 0
fi

diff -u tests/golden-replay/expected_decisions.sha256 "$OUT/decisions.sha256"
diff -u tests/golden-replay/expected_report.json "$OUT/report.json"
echo "replay-regression: decisions and report match expectations"
