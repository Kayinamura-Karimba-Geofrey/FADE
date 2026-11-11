// k6 load test for POST /v1/decide.
//
//   BASE_URL=http://localhost:8080 TOKEN=$(go run ./cmd/dev-token) \
//   RATE=1000 DURATION=30s k6 run bench/k6/decide.js
//
// RATE is decisions/second (constant arrival rate); DURATION is the test
// window. The thresholds encode the Phase 3 exit criteria: p95 < 50 ms,
// p99 < 120 ms and < 1% transport/HTTP failures.
import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const TOKEN = __ENV.TOKEN || '';
const RATE = Number(__ENV.RATE || 1000);
const DURATION = __ENV.DURATION || '30s';

const decisionLatency = new Trend('decision_latency_ms', true);
const businessErrors = new Counter('business_errors_total');

export const options = {
  scenarios: {
    decide: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.max(20, Math.ceil(RATE / 20)),
      maxVUs: Math.max(100, RATE * 2),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<50', 'p(99)<120'],
    checks: ['rate>0.99'],
  },
};

const ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

function ulid(ms) {
  let ts = '';
  let v = ms;
  for (let i = 0; i < 10; i++) {
    ts = ALPHABET[v % 32] + ts;
    v = Math.floor(v / 32);
  }
  let rest = '';
  for (let i = 0; i < 16; i++) {
    rest += ALPHABET[Math.floor(Math.random() * 32)];
  }
  return ts + rest;
}

function hex(n) {
  let out = '';
  for (let i = 0; i < n; i++) {
    out += '0123456789abcdef'[Math.floor(Math.random() * 16)];
  }
  return out;
}

// Vary the sender per iteration: a hot account would make the 24h feature
// windows grow linearly and measure the Lua aggregate, not API capacity.
function account() {
  return 'acct_' + String(Math.floor(Math.random() * 1000000)).padStart(6, '0');
}

function eventBody() {
  const now = Date.now();
  return JSON.stringify({
    event_id: ulid(now),
    event_time: new Date(now).toISOString(),
    txn_type: 'P2P_SEND',
    amount: 70000,
    currency: 'RWF',
    fee: 500,
    sender: {
      account_id: account(),
      msisdn_hash: 'h_' + hex(32),
      device_id: 'dev_' + String(Math.floor(Math.random() * 1000000)).padStart(6, '0') + '_1',
      sim_serial_hash: 'sim_' + hex(32),
      geo: { lat: -1.95, lon: 30.06, cell_id: 'KGL-01' },
      account_age_days: 120,
      kyc_tier: 2,
      balance_before: 100000,
    },
    recipient: {
      account_id: account(),
      msisdn_hash: 'h_' + hex(32),
      is_agent: false,
      first_seen_by_sender: false,
    },
    channel: 'APP',
    session_id: 'p2p-' + hex(8).toUpperCase(),
  });
}

export default function () {
  const res = http.post(`${BASE_URL}/v1/decide`, eventBody(), {
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${TOKEN}` },
    tags: { endpoint: 'decide' },
  });
  const ok = check(res, {
    'status is 200': (r) => r.status === 200,
    'has decision': (r) => {
      try {
        return ['ALLOW', 'STEP_UP', 'HOLD', 'BLOCK'].includes(r.json('decision'));
      } catch (e) {
        return false;
      }
    },
  });
  if (!ok) {
    businessErrors.add(1);
  }
  if (res.status === 200) {
    decisionLatency.add(res.timings.duration);
  }
}
