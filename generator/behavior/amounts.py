"""Amount sampling and the tiered fee schedule.

Fee schedule (RWF, by transaction amount) - documented here as required by
the spec and encoded in :func:`fee_for`::

    amount < 1,000            -> 0
    1,000 <= amount < 5,000   -> 50
    5,000 <= amount < 50,000  -> 100
    50,000 <= amount < 500,000 -> 500
    amount >= 500,000         -> 1,000

The fee is charged to the sender on top of the amount (for ``CASH_IN`` it
is deducted from the deposited amount, i.e. the wallet is credited
``amount - fee``).
"""

from __future__ import annotations

from numpy.random import Generator

#: Smallest amount the generator will emit (RWF).
MIN_AMOUNT = 100

#: Largest raw draw cap before balance/limit clamping (RWF).
MAX_AMOUNT = 50_000_000


def fee_for(amount: int) -> int:
    """Tiered fee for an amount in RWF (see module docstring)."""
    if amount < 1_000:
        return 0
    if amount < 5_000:
        return 50
    if amount < 50_000:
        return 100
    if amount < 500_000:
        return 500
    return 1_000


def sample_amount(rng: Generator, mu: float, sigma: float, cap: int) -> int:
    """Draw a lognormal amount clamped to ``[MIN_AMOUNT, cap]``.

    Amounts are rounded down to a multiple of 50 RWF (except when the cap
    is tighter than that granularity).  ``cap`` is the effective upper
    bound after balance / tier-limit / agent-float checks.
    """
    if cap < MIN_AMOUNT:
        return 0  # caller must skip: nothing affordable
    raw = rng.lognormal(mean=mu, sigma=sigma)
    value = int(raw)
    if value >= MIN_AMOUNT:
        value -= value % 50
    value = max(MIN_AMOUNT, min(value, MAX_AMOUNT))
    if value > cap:
        value = cap
    return value


def clamp_amount(amount: int, available: int) -> int:
    """Clamp an amount to what is actually available (already fee-adjusted).

    Returns 0 when not even ``MIN_AMOUNT`` fits - the caller then skips
    the event rather than emitting a degenerate one.
    """
    if available < MIN_AMOUNT:
        return 0
    return min(amount, available)


def split_evenly(total: int, parts: int) -> list[int]:
    """Split ``total`` into ``parts`` positive integers with a stable remainder.

    Used by fraud injectors to divide a drain across several transfers while
    keeping every part >= ``MIN_AMOUNT`` (callers guarantee the total).
    """
    if parts <= 1:
        return [total]
    base = total // parts
    out = [base] * parts
    out[-1] += total - base * parts
    return out
