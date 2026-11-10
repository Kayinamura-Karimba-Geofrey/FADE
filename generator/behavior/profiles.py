"""Per-segment behaviour profiles: hourly shapes, event rates, amount params.

These tables drive ``behavior.engine``.  Everything here is static segment
metadata - per-account jitter (hourly weights, amount ``mu``) is drawn once
at population build time from these bases, so a run stays deterministic.

Event *sources* (keys of ``rates``) and their meaning:

=================  =========================================================
key                event produced
=================  =========================================================
p2p_out            P2P_SEND to a family/peer contact
merchant_out       MERCHANT_PAY to a merchant contact
merchant_in        MERCHANT_PAY *received* by a trader (payer is a peer)
airtime            AIRTIME top-up (telecom counterparty)
bill               BILL_PAY to a biller contact
cash_out           CASH_OUT at an attached agent
cash_in            CASH_IN at an attached agent
bank_out           BANK_TRANSFER to an external bank (supplier/account)
salary_in          BANK_TRANSFER salary (gated: days 25-28 of a month)
school_fee         BILL_PAY school fee (gated: first 10 days of a term)
restock            night BANK_TRANSFER restocking (gated: 01:00-04:00)
first_large        first-time large P2P to a fresh contact (hard negative)
=================  =========================================================

``salary_in``/``school_fee``/``restock``/``first_large`` are gated by
calendar rules in the engine; their ``rates`` are per-day probabilities
*inside* the gate window.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from generator.models import TxnType

#: Mean amount params are lognormal (mu = ln of median-ish mean, sigma).
#: Keys are event sources; the engine maps each source to a TxnType.
AmountParams = Mapping[str, tuple[float, float]]


@dataclass(frozen=True)
class SegmentProfile:
    """Static behaviour profile of one population segment."""

    name: str
    #: local-time hourly shape (24 weights, order 0..23)
    hourly: tuple[float, ...]
    #: optional override used on weekends (Sat/Sun), else ``hourly``
    weekend_hourly: tuple[float, ...] | None
    #: mean events per account per day (before activity/calendar factors)
    rates: Mapping[str, float]
    #: lognormal (mu, sigma) per event source
    amount_params: AmountParams
    #: sources whose counts are scaled by the festive/market/holiday factors
    #: only for specific weekdays: map weekday (0=Mon) -> multiplier
    weekday_multipliers: Mapping[int, float]


_SALARIED = SegmentProfile(
    name="salaried",
    hourly=(
        0.10,
        0.05,
        0.05,
        0.05,
        0.05,
        0.10,
        0.50,
        1.20,
        1.00,
        0.70,
        0.70,
        0.80,
        0.90,
        0.70,
        0.60,
        0.60,
        0.80,
        1.20,
        1.30,
        1.10,
        0.80,
        0.50,
        0.30,
        0.20,
    ),
    weekend_hourly=None,
    rates={
        "p2p_out": 0.30,
        "merchant_out": 0.20,
        "airtime": 0.16,
        "bill": 0.15,
        "cash_out": 0.05,
        "bank_out": 0.015,  # savings transfer; keeps wallet roughly neutral
        "salary_in": 0.95 / 4.0,  # ~1 salary per month over days 25-28
        "first_large": 0.002,
    },
    amount_params={
        "p2p_out": (9.68, 0.70),  # ~16k
        "merchant_out": (8.70, 0.80),  # ~6k
        "airtime": (8.00, 0.60),  # ~3k
        "bill": (9.60, 0.70),  # ~15k
        "cash_out": (10.00, 0.70),  # ~22k
        "bank_out": (12.00, 0.80),  # ~160k
        "salary_in": (13.00, 0.35),  # ~440k
        "first_large": (11.00, 0.50),  # base for 5-15x multiplier
    },
    weekday_multipliers={},
)

_SMALL_TRADER = SegmentProfile(
    name="small_trader",
    hourly=(
        0.02,
        0.01,
        0.01,
        0.01,
        0.02,
        0.10,
        0.60,
        1.30,
        1.50,
        1.40,
        1.30,
        1.20,
        1.30,
        1.20,
        1.10,
        1.10,
        1.00,
        0.90,
        0.70,
        0.40,
        0.20,
        0.10,
        0.05,
        0.03,
    ),
    weekend_hourly=None,
    rates={
        # inflows: customer sales (merchant_in) + cash deposits at an agent
        "merchant_in": 1.10,
        "cash_out": 0.10,  # occasional cash withdrawal for the till
        "cash_in": 0.12,
        "p2p_out": 0.10,
        "airtime": 0.10,
        "merchant_out": 0.08,
        "bill": 0.06,
        "bank_out": 0.01,  # supplier payments
        "restock": 0.010,  # night restocking (gated 01:00-04:00 local)
        "first_large": 0.001,
    },
    amount_params={
        "merchant_in": (8.50, 0.90),  # ~5k
        "cash_out": (11.00, 0.90),  # ~60k
        "cash_in": (10.60, 0.90),  # ~40k
        "p2p_out": (9.20, 0.80),  # ~10k
        "airtime": (7.80, 0.60),  # ~2.5k
        "merchant_out": (9.00, 0.80),  # ~8k
        "bill": (9.40, 0.70),  # ~12k
        "bank_out": (12.20, 0.80),  # ~200k
        "restock": (12.60, 0.70),  # ~400k
        "first_large": (10.00, 0.50),
    },
    # market days: Tuesday(1), Friday(4)
    weekday_multipliers={1: 1.35, 4: 1.35},
)

_STUDENT_PARENT = SegmentProfile(
    name="student_parent",
    hourly=(
        0.10,
        0.05,
        0.05,
        0.05,
        0.10,
        0.30,
        0.90,
        1.10,
        0.80,
        0.70,
        0.70,
        0.80,
        0.90,
        0.70,
        0.60,
        0.70,
        0.90,
        1.10,
        1.00,
        0.80,
        0.60,
        0.40,
        0.30,
        0.20,
    ),
    weekend_hourly=None,
    rates={
        "airtime": 0.25,
        "p2p_out": 0.12,
        "merchant_out": 0.12,
        "bill": 0.08,
        "cash_in": 0.15,  # parents deposit cash before paying for things
        "school_fee": 0.05,  # gated: first 10 days of term (Jan/May/Sep)
        "first_large": 0.001,
    },
    amount_params={
        "airtime": (7.30, 0.60),  # ~1.5k
        "p2p_out": (8.00, 0.70),  # ~3k
        "merchant_out": (8.30, 0.80),  # ~4k
        "bill": (9.20, 0.60),  # ~10k
        "cash_in": (9.70, 0.70),  # ~16k deposit
        "school_fee": (9.60, 0.50),  # ~15k base, 3-10x => 45-150k
        "first_large": (9.00, 0.50),
    },
    weekday_multipliers={5: 1.10, 6: 1.10},  # weekend bump
)

_RURAL_SAVER = SegmentProfile(
    name="rural_saver",
    hourly=(
        0.05,
        0.03,
        0.03,
        0.03,
        0.06,
        0.30,
        1.00,
        1.30,
        1.20,
        1.00,
        0.90,
        0.70,
        0.80,
        0.90,
        0.90,
        0.80,
        0.60,
        0.50,
        0.40,
        0.30,
        0.20,
        0.10,
        0.07,
        0.05,
    ),
    weekend_hourly=None,
    rates={
        "cash_out": 0.20,
        "cash_in": 0.35,  # regular deposits: the "saver" habit
        "airtime": 0.10,
        "p2p_out": 0.03,
        "bill": 0.04,
        "first_large": 0.001,
    },
    amount_params={
        "cash_out": (9.60, 0.80),  # ~15k
        "cash_in": (9.20, 0.80),  # ~10k
        "airtime": (7.60, 0.60),  # ~2k
        "p2p_out": (8.50, 0.70),  # ~5k
        "bill": (9.00, 0.70),  # ~8k
        "first_large": (9.50, 0.50),
    },
    weekday_multipliers={},
)

_URBAN_YOUTH = SegmentProfile(
    name="urban_youth",
    hourly=(
        0.40,
        0.25,
        0.15,
        0.10,
        0.10,
        0.15,
        0.40,
        0.70,
        0.70,
        0.60,
        0.60,
        0.70,
        0.80,
        0.70,
        0.60,
        0.70,
        0.90,
        1.20,
        1.50,
        1.60,
        1.60,
        1.40,
        1.00,
        0.60,
    ),
    weekend_hourly=(
        0.80,
        0.60,
        0.40,
        0.25,
        0.15,
        0.15,
        0.20,
        0.40,
        0.60,
        0.90,
        1.10,
        1.20,
        1.20,
        1.10,
        1.00,
        1.00,
        1.10,
        1.30,
        1.50,
        1.60,
        1.60,
        1.50,
        1.30,
        1.00,
    ),
    rates={
        "p2p_out": 0.45,
        "airtime": 0.32,
        "merchant_out": 0.22,
        "bill": 0.06,
        "cash_in": 0.25,  # deposits from side gigs / allowances
        "first_large": 0.002,
    },
    amount_params={
        "p2p_out": (7.80, 0.80),  # ~2.5k
        "airtime": (7.30, 0.70),  # ~1.5k
        "merchant_out": (8.15, 0.80),  # ~3.5k
        "bill": (8.70, 0.60),  # ~6k
        "cash_in": (9.30, 0.70),  # ~11k deposit
        "first_large": (9.00, 0.50),
    },
    weekday_multipliers={5: 1.15, 6: 1.15},  # weekend evening peaks
)

#: All segment profiles, keyed by segment name.
SEGMENT_PROFILES: dict[str, SegmentProfile] = {
    p.name: p for p in (_SALARIED, _SMALL_TRADER, _STUDENT_PARENT, _RURAL_SAVER, _URBAN_YOUTH)
}

#: Event source -> transaction type mapping used by the engine.
SOURCE_TXN_TYPES: dict[str, TxnType] = {
    "p2p_out": TxnType.P2P_SEND,
    "merchant_out": TxnType.MERCHANT_PAY,
    "merchant_in": TxnType.MERCHANT_PAY,
    "airtime": TxnType.AIRTIME,
    "bill": TxnType.BILL_PAY,
    "cash_out": TxnType.CASH_OUT,
    "cash_in": TxnType.CASH_IN,
    "bank_out": TxnType.BANK_TRANSFER,
    "salary_in": TxnType.BANK_TRANSFER,
    "school_fee": TxnType.BILL_PAY,
    "restock": TxnType.BANK_TRANSFER,
    "first_large": TxnType.P2P_SEND,
}

#: Sources that credit the account (inbound flows).
INBOUND_SOURCES: frozenset[str] = frozenset({"merchant_in", "salary_in"})


def profile_for(segment: str) -> SegmentProfile:
    """Look up a segment profile with a helpful error."""
    try:
        return SEGMENT_PROFILES[segment]
    except KeyError:
        raise KeyError(
            f"no behaviour profile for segment {segment!r}; " f"known: {sorted(SEGMENT_PROFILES)}"
        ) from None
