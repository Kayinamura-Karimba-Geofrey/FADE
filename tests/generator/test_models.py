"""Model, ULID and identifier-hash tests."""

from __future__ import annotations

import json
import re

from numpy.random import default_rng

from generator.models import (
    CHANNELS_BY_TYPE,
    SENDER_DEBIT_TYPES,
    Channel,
    Event,
    Geo,
    LabelRecord,
    LabelValue,
    Recipient,
    Sender,
    TxnType,
    crockford_b32,
    format_event_time,
    hash_identifier,
    make_ulid,
)

ULID_RE = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")


def _event(ts_ms: int = 1_767_225_600_000) -> Event:
    sender = Sender(
        account_id="acct_000001",
        msisdn_hash="h_abc",
        device_id="dev_1_0",
        sim_serial_hash="sim_abc",
        geo=Geo(lat=-1.94, lon=30.06, cell_id="KGL-C01"),
        account_age_days=100,
        kyc_tier=2,
        balance_before=50_000,
    )
    recip = Recipient(
        account_id="acct_000002",
        msisdn_hash="h_def",
        is_agent=False,
        first_seen_by_sender=True,
    )
    return Event(
        ts_ms=ts_ms,
        txn_type=TxnType.P2P_SEND,
        amount=5_000,
        currency="RWF",
        fee=100,
        sender=sender,
        recipient=recip,
        channel=Channel.APP,
        agent_id=None,
        session_id="ses_000001_0000",
        event_id="01JR0000000000000000000000",
        event_time=format_event_time(ts_ms),
    )


def test_event_json_shape() -> None:
    event = _event()
    payload = json.loads(json.dumps(event.to_dict(), separators=(",", ":")))
    assert set(payload) == {
        "event_id",
        "event_time",
        "txn_type",
        "amount",
        "currency",
        "fee",
        "sender",
        "recipient",
        "channel",
        "agent_id",
        "session_id",
    }
    assert set(payload["sender"]) == {
        "account_id",
        "msisdn_hash",
        "device_id",
        "sim_serial_hash",
        "geo",
        "account_age_days",
        "kyc_tier",
        "balance_before",
    }
    assert set(payload["sender"]["geo"]) == {"lat", "lon", "cell_id"}
    assert set(payload["recipient"]) == {
        "account_id",
        "msisdn_hash",
        "is_agent",
        "first_seen_by_sender",
    }
    assert payload["txn_type"] == "P2P_SEND"
    assert payload["channel"] == "APP"
    assert payload["amount"] == 5_000 and isinstance(payload["amount"], int)
    assert payload["fee"] == 100
    assert payload["agent_id"] is None


def test_label_never_in_event_dict() -> None:
    text = json.dumps(_event().to_dict()).lower()
    assert "label" not in text
    assert "scenario" not in text
    assert "fraud" not in text


def test_event_time_format() -> None:
    ts = 1_767_225_600_123  # 2026-01-01T00:00:00.123Z
    assert format_event_time(ts) == "2026-01-01T00:00:00.123Z"
    assert format_event_time(0) == "1970-01-01T00:00:00.000Z"


def test_ulid_deterministic_and_time_sortable() -> None:
    ts = 1_767_225_600_000
    a = make_ulid(ts, default_rng(42))
    b = make_ulid(ts, default_rng(42))
    assert a == b
    assert ULID_RE.match(a)
    early = make_ulid(ts, default_rng(1))
    late = make_ulid(ts + 60_000, default_rng(2))
    assert early < late  # lexicographic order == time order


def test_ulid_differs_within_same_millisecond() -> None:
    ts = 1_767_225_600_000
    ids = {make_ulid(ts, default_rng(s)) for s in range(50)}
    assert len(ids) == 50


def test_hash_identifier_prefixes_and_saliency() -> None:
    h1 = hash_identifier(42, "msisdn", "+250788000001")
    h2 = hash_identifier(42, "msisdn", "+250788000002")
    h3 = hash_identifier(43, "msisdn", "+250788000001")
    assert h1.startswith("h_") and len(h1) == 2 + 32
    assert h2.startswith("h_")
    assert h1 != h2 != h3
    assert h1 != h3
    sim = hash_identifier(42, "sim", "8959000000000000000")
    assert sim.startswith("sim_") and len(sim) == 4 + 32
    assert sim != hash_identifier(43, "sim", "8959000000000000000")


def test_crockford_b32() -> None:
    assert crockford_b32(0, 4) == "0000"
    assert crockford_b32(31, 4) == "000Z"
    assert len(crockford_b32(2**80 - 1)) == 26


def test_enums_and_channel_table() -> None:
    assert [t.value for t in TxnType] == [
        "P2P_SEND",
        "CASH_IN",
        "CASH_OUT",
        "AIRTIME",
        "BILL_PAY",
        "MERCHANT_PAY",
        "BANK_TRANSFER",
    ]
    assert [c.value for c in Channel] == ["USSD", "APP", "AGENT", "API"]
    assert set(CHANNELS_BY_TYPE) == set(TxnType)
    assert set(CHANNELS_BY_TYPE[TxnType.CASH_IN]) == {Channel.AGENT}
    assert set(CHANNELS_BY_TYPE[TxnType.CASH_OUT]) == {Channel.AGENT}
    assert TxnType.CASH_IN not in SENDER_DEBIT_TYPES
    assert TxnType.P2P_SEND in SENDER_DEBIT_TYPES


def test_label_record_row() -> None:
    legit = LabelRecord("01X", LabelValue.LEGIT, None, None)
    assert legit.to_row() == {
        "event_id": "01X",
        "label": "LEGIT",
        "scenario": None,
        "scenario_run_id": None,
        "source": "GENERATOR",
    }
    fraud = LabelRecord("01Y", LabelValue.FRAUD, "velocity", "velocity-000001")
    row = fraud.to_row()
    assert row["label"] == "FRAUD"
    assert row["scenario"] == "velocity"
    assert row["scenario_run_id"] == "velocity-000001"
    assert row["source"] == "GENERATOR"
