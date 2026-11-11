// Batch smoke test for POST /v1/decide/batch.
// BATCH_SIZE events per request; defaults to a short 10 VU / 15 s run.
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const TOKEN = __ENV.TOKEN || '';
const BATCH_SIZE = Number(__ENV.BATCH_SIZE || 10);
const ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

export const options = {
  vus: Number(__ENV.VUS || 10),
  duration: __ENV.DURATION || '15s',
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
};

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

function account() {
  return 'acct_' + String(Math.floor(Math.random() * 1000000)).padStart(6, '0');
}

function one() {
  const now = Date.now();
  return {
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
  };
}

export default function () {
  const events = [];
  for (let i = 0; i < BATCH_SIZE; i++) {
    events.push(one());
  }
  const res = http.post(`${BASE_URL}/v1/decide/batch`, JSON.stringify({ events }), {
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${TOKEN}` },
  });
  check(res, {
    'status is 200': (r) => r.status === 200,
    'all succeeded': (r) => {
      try {
        return r.json('succeeded') === BATCH_SIZE;
      } catch (e) {
        return false;
      }
    },
  });
}
