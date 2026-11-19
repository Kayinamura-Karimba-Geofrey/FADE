#!/usr/bin/env python3
"""Mint one schema-valid FADE event as JSON on stdout.

Used by the chaos harness, the demo script and docs tests so every probe
satisfies the frozen event contract (ULID timestamp == event_time, fee
schedule, account/device/session formats).
"""
from __future__ import annotations

import argparse
import json
import secrets
from datetime import UTC, datetime

ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def ulid(ms: int) -> str:
    out = []
    v = ms
    for _ in range(10):
        out.append(ALPHABET[v & 31])
        v >>= 5
    out.reverse()
    return "".join(out) + "".join(secrets.choice(ALPHABET) for _ in range(16))


def fee_for(amount: int) -> int:
    if amount < 1_000:
        return 0
    if amount < 5_000:
        return 50
    if amount < 50_000:
        return 100
    if amount < 500_000:
        return 500
    return 1_000


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", default="acct_000001")
    ap.add_argument("--recipient", default="acct_000002")
    ap.add_argument("--amount", type=int, default=70_000)
    ap.add_argument("--txn-type", default="P2P_SEND")
    ap.add_argument("--event-id", default="")
    ap.add_argument("--device", default="dev_000001_1")
    args = ap.parse_args()

    now = datetime.now(UTC).replace(microsecond=0)
    ms = int(now.timestamp() * 1000)
    event = {
        "event_id": args.event_id or ulid(ms),
        "event_time": now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "txn_type": args.txn_type,
        "amount": args.amount,
        "currency": "RWF",
        "fee": fee_for(args.amount),
        "sender": {
            "account_id": args.account,
            "msisdn_hash": "h_" + secrets.token_hex(16),
            "device_id": args.device,
            "sim_serial_hash": "sim_" + secrets.token_hex(16),
            "geo": {"lat": -1.95, "lon": 30.06, "cell_id": "KGL-01"},
            "account_age_days": 120,
            "kyc_tier": 2,
            "balance_before": 200_000,
        },
        "recipient": {
            "account_id": args.recipient,
            "msisdn_hash": "h_" + secrets.token_hex(16),
            "is_agent": False,
            "first_seen_by_sender": False,
        },
        "channel": "APP",
        "session_id": "p2p-" + secrets.token_hex(4).upper(),
    }
    print(json.dumps(event))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
