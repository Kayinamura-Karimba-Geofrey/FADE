"""Tests for the Phase 2 Python feature reference and golden oracle."""

from __future__ import annotations

import json
from pathlib import Path

from generator.reference_features import (
    FEATURE_FIELDS,
    FeatureComputer,
    compare_features,
    load_config,
    read_feature_jsonl,
    run_reference_jsonl,
)
from tests.golden.oracle import check

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "config" / "features.yaml"


def test_golden_oracle_matches_fixture() -> None:
    assert check() == []


def test_reference_vector_has_every_contract_field() -> None:
    vectors = list(run_reference_jsonl(ROOT / "tests" / "golden" / "events.jsonl", CONFIG))
    assert vectors
    assert list(vectors[0]) == list(FEATURE_FIELDS)
    assert all(vector["feature_schema_version"] == "1.0.0" for vector in vectors)
    assert {vector["cycle_len_2_4"] for vector in vectors} >= {0, 2, 3, 4}


def test_reference_is_deterministic() -> None:
    first = list(run_reference_jsonl(ROOT / "tests" / "golden" / "events.jsonl", CONFIG))
    second = list(run_reference_jsonl(ROOT / "tests" / "golden" / "events.jsonl", CONFIG))
    assert compare_features(first, second) == []


def _event(
    event_id: str, event_time: str, sender: str, recipient: str, txn: str, amount: int
) -> dict:
    return {
        "event_id": event_id,
        "event_time": event_time,
        "txn_type": txn,
        "amount": amount,
        "currency": "RWF",
        "fee": 0,
        "channel": "USSD",
        "agent_id": None,
        "session_id": "top-VEN9RZED",
        "sender": {
            "account_id": sender,
            "msisdn_hash": "h_" + "0" * 32,
            "device_id": "dev_000001_0",
            "sim_serial_hash": "sim_" + "0" * 32,
            "geo": {"lat": -1.9, "lon": 30.0, "cell_id": "KGL-01"},
            "account_age_days": 100,
            "kyc_tier": 2,
            "balance_before": 100000,
        },
        "recipient": {
            "account_id": recipient,
            "msisdn_hash": "h_" + "1" * 32,
            "is_agent": False,
            "first_seen_by_sender": True,
        },
    }


def test_reference_duplicate_and_late_rules() -> None:
    from generator.reference_features import flatten_event

    computer = FeatureComputer(load_config(CONFIG), late_tolerance_s=60)
    first = _event(
        "01M6ZW06807TFTK1P7CPF6Q3AT",
        "2026-11-09T10:00:00.000Z",
        "acct_000001",
        "acct_000002",
        "P2P_SEND",
        5000,
    )
    assert computer.compute(flatten_event(first)) is not None
    assert computer.compute(flatten_event(first)) is None
    assert computer.status == "duplicate"
    late = _event(
        "01M6ZW9B70CRE2PH59XBCW76TN",
        "2026-11-09T10:05:00.000Z",
        "acct_000001",
        "acct_000003",
        "P2P_SEND",
        900,
    )
    assert computer.compute(flatten_event(late)) is not None
    older = _event(
        "01M6ZWJG607JTAB7X0D2AHXGPW",
        "2026-11-09T09:00:00.000Z",
        "acct_000001",
        "acct_000004",
        "P2P_SEND",
        900,
    )
    assert computer.compute(flatten_event(older)) is None
    assert computer.status == "late"


def test_compare_features_reports_mismatch() -> None:
    left = read_feature_jsonl(ROOT / "tests" / "golden" / "expected_features.jsonl")[:1]
    right = [json.loads(json.dumps(left[0]))]
    right[0]["velocity_1m"] += 1
    problems = compare_features(left, right)
    assert problems and "velocity_1m" in problems[0]
