"""Event and label models for the FADE synthetic dataset.

The on-disk JSON shape is fixed by the Phase 1 spec::

    {
      "event_id": "<26-char ULID>", "event_time": "2026-01-01T00:00:00.000Z",
      "txn_type": "P2P_SEND", "amount": 5000, "currency": "RWF", "fee": 0,
      "sender":   {"account_id", "msisdn_hash", "device_id", "sim_serial_hash",
                   "geo": {"lat", "lon", "cell_id"},
                   "account_age_days", "kyc_tier", "balance_before"},
      "recipient": {"account_id", "msisdn_hash", "is_agent", "first_seen_by_sender"},
      "channel": "USSD", "agent_id": null, "session_id": "..."
    }

Rules enforced here:

* amounts are integer RWF,
* ``event_time`` is UTC ISO-8601 with milliseconds and a ``Z`` suffix,
* ``event_id`` is a ULID built from the event timestamp plus *seeded*
  randomness, so IDs are deterministic and time-sortable,
* identifiers (msisdn, SIM serial) are sha256 of a seed-salted value with
  an ``h_`` / ``sim_`` prefix,
* labels never live on the event - a separate :class:`LabelRecord` carries
  them, and no field of ``Event`` may contain the words label/scenario.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, timedelta
from enum import StrEnum
from typing import Any

from numpy.random import Generator

HASH_PREFIX_MSISDN = "h_"
HASH_PREFIX_SIM = "sim_"
HASH_LENGTH = 32  # first 128 bits of sha256 keeps files small, collisions impossible here


class TxnType(StrEnum):
    """Transaction type enum - exactly the spec values."""

    P2P_SEND = "P2P_SEND"
    CASH_IN = "CASH_IN"
    CASH_OUT = "CASH_OUT"
    AIRTIME = "AIRTIME"
    BILL_PAY = "BILL_PAY"
    MERCHANT_PAY = "MERCHANT_PAY"
    BANK_TRANSFER = "BANK_TRANSFER"


class Channel(StrEnum):
    """Channel enum - exactly the spec values."""

    USSD = "USSD"
    APP = "APP"
    AGENT = "AGENT"
    API = "API"


class LabelValue(StrEnum):
    """Ground-truth label values (labels.parquet only)."""

    FRAUD = "FRAUD"
    LEGIT = "LEGIT"


#: Transaction types whose *sender* is debited amount + fee.
#: ``CASH_IN`` is excluded: the customer deposits cash and is credited.
#: (A salary ``BANK_TRANSFER`` debits the external sender account, which
#: holds a ledger balance like everybody else.)
SENDER_DEBIT_TYPES: frozenset[TxnType] = frozenset(
    {
        TxnType.P2P_SEND,
        TxnType.CASH_OUT,
        TxnType.AIRTIME,
        TxnType.BILL_PAY,
        TxnType.MERCHANT_PAY,
        TxnType.BANK_TRANSFER,
    }
)

#: Allowed channels per transaction type.  Agent-mediated types must use
#: AGENT (with a non-null ``agent_id``).
CHANNELS_BY_TYPE: dict[TxnType, tuple[Channel, ...]] = {
    TxnType.P2P_SEND: (Channel.USSD, Channel.APP),
    TxnType.CASH_IN: (Channel.AGENT,),
    TxnType.CASH_OUT: (Channel.AGENT,),
    TxnType.AIRTIME: (Channel.USSD, Channel.APP),
    TxnType.BILL_PAY: (Channel.APP, Channel.USSD),
    TxnType.MERCHANT_PAY: (Channel.APP, Channel.USSD, Channel.API),
    TxnType.BANK_TRANSFER: (Channel.APP, Channel.API),
}

#: Types where the recipient is an agent wallet (float), not a normal credit.
AGENT_CREDIT_TYPES: frozenset[TxnType] = frozenset({TxnType.CASH_IN})
AGENT_DEBIT_TYPES: frozenset[TxnType] = frozenset({TxnType.CASH_OUT})


@dataclass(slots=True)
class Geo:
    """Sender position at event time."""

    lat: float
    lon: float
    cell_id: str

    def to_dict(self) -> dict[str, Any]:
        return {"lat": self.lat, "lon": self.lon, "cell_id": self.cell_id}


@dataclass(slots=True)
class Sender:
    """Sender block of an event.  ``balance_before`` is filled at apply time."""

    account_id: str
    msisdn_hash: str
    device_id: str
    sim_serial_hash: str
    geo: Geo
    account_age_days: int
    kyc_tier: int
    balance_before: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "msisdn_hash": self.msisdn_hash,
            "device_id": self.device_id,
            "sim_serial_hash": self.sim_serial_hash,
            "geo": self.geo.to_dict(),
            "account_age_days": self.account_age_days,
            "kyc_tier": self.kyc_tier,
            "balance_before": self.balance_before,
        }


@dataclass(slots=True)
class Recipient:
    """Recipient block of an event."""

    account_id: str
    msisdn_hash: str
    is_agent: bool
    first_seen_by_sender: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "msisdn_hash": self.msisdn_hash,
            "is_agent": self.is_agent,
            "first_seen_by_sender": self.first_seen_by_sender,
        }


@dataclass(slots=True)
class Event:
    """One transaction event.  Mutable: ``sender.balance_before`` and
    ``event_id``/``event_time`` are finalised when the event is applied."""

    ts_ms: int
    txn_type: TxnType
    amount: int
    currency: str
    fee: int
    sender: Sender
    recipient: Recipient
    channel: Channel
    agent_id: str | None
    session_id: str
    event_id: str = ""
    event_time: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Nested dict in exactly the spec key order."""
        return {
            "event_id": self.event_id,
            "event_time": self.event_time,
            "txn_type": self.txn_type.value,
            "amount": self.amount,
            "currency": self.currency,
            "fee": self.fee,
            "sender": self.sender.to_dict(),
            "recipient": self.recipient.to_dict(),
            "channel": self.channel.value,
            "agent_id": self.agent_id,
            "session_id": self.session_id,
        }


@dataclass(slots=True)
class LabelRecord:
    """Ground truth for one event.  Written only to labels.parquet."""

    event_id: str
    label: LabelValue
    scenario: str | None
    scenario_run_id: str | None
    source: str = "GENERATOR"

    def to_row(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "label": self.label.value,
            "scenario": self.scenario,
            "scenario_run_id": self.scenario_run_id,
            "source": self.source,
        }


# --------------------------------------------------------------------------- #
# Deterministic identifiers
# --------------------------------------------------------------------------- #

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


#: Cache of ISO date prefixes by days-since-epoch (populated once per day).
_DATE_PREFIX: dict[int, str] = {}
_EPOCH_DATE = date(1970, 1, 1)


def format_event_time(ts_ms: int) -> str:
    """UTC ISO-8601 with milliseconds and ``Z`` (``...T00:00:00.000Z``).

    Integer arithmetic instead of ``datetime.strftime``: the generator emits
    millions of timestamps, and the strftime path cost ~40% of emission time
    in the Audit 9 profile.  A per-day prefix cache keeps it allocation-light.
    """
    seconds, millis = divmod(int(ts_ms), 1000)
    days, second_of_day = divmod(seconds, 86_400)
    prefix = _DATE_PREFIX.get(days)
    if prefix is None:
        prefix = (_EPOCH_DATE + timedelta(days=days)).isoformat()
        _DATE_PREFIX[days] = prefix
    hour, rem = divmod(second_of_day, 3_600)
    minute, second = divmod(rem, 60)
    return f"{prefix}T{hour:02d}:{minute:02d}:{second:02d}.{millis:03d}Z"


#: Two-character Crockford chunks for 10-bit groups (ULID fast path).
_ULID_PAIRS: tuple[str, ...] = tuple(
    _CROCKFORD[(v >> 5) & 31] + _CROCKFORD[v & 31] for v in range(1024)
)


def encode_ulid(value: int) -> str:
    """Crockford base32-encode a 128-bit ULID value (26 chars, big-endian).

    Encodes 13 ten-bit chunks through a lookup table instead of shifting one
    bit at a time; the slow version cost ~40 microseconds per event in the
    Audit 9 profile.
    """
    out = [""] * 13
    for i in range(12, -1, -1):
        out[i] = _ULID_PAIRS[value & 1023]
        value >>= 10
    return "".join(out)


def make_ulid(ts_ms: int, rng: Generator) -> str:
    """A ULID from the event timestamp (48 bits) plus 80 seeded random bits.

    Time-sortable by construction: the timestamp occupies the most
    significant 48 bits, so ULID order == event_time order (event_time is
    millisecond-quantised, matching the ULID timestamp field).
    """
    raw = rng.bit_generator.random_raw(2)
    rand = ((int(raw[0]) << 64) | int(raw[1])) >> 48
    payload = (int(ts_ms) & 0xFFFFFFFFFFFF) << 80 | rand
    return encode_ulid(payload)


def hash_identifier(seed: int, kind: str, value: str) -> str:
    """sha256 of a seed-salted value, prefixed for readability."""
    digest = hashlib.sha256(f"{seed}|{kind}|{value}".encode()).hexdigest()[:HASH_LENGTH]
    if kind == "msisdn":
        return HASH_PREFIX_MSISDN + digest
    if kind == "sim":
        return HASH_PREFIX_SIM + digest
    raise ValueError(f"unknown hash kind {kind!r}")


def crockford_b32(value: int, length: int = 26) -> str:
    """Crockford base32-encode an integer (used for session identifiers)."""
    if length == 8:  # session suffixes: four 10-bit chunks, table lookup
        return (
            _ULID_PAIRS[(value >> 30) & 1023]
            + _ULID_PAIRS[(value >> 20) & 1023]
            + _ULID_PAIRS[(value >> 10) & 1023]
            + _ULID_PAIRS[value & 1023]
        )
    out = []
    for i in range(length):
        out.append(_CROCKFORD[(value >> (5 * i)) & 31])
    return "".join(reversed(out))
