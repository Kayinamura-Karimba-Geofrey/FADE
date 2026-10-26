"""Account creation and the double-entry-consistent ledger.

* :class:`Cell`     - ~60 mobile-money cells across six Rwandan districts
* :class:`Account`  - one customer or agent wallet with all per-account
                      parameters (segment, tier, devices, hourly shape,
                      lognormal amount params, opening balance)
* :class:`Ledger`   - balances + daily volume + seen contact pairs for every
                      ledger index (population accounts and specials); the
                      single place balances change, so ``balance_before`` is
                      always consistent and never negative
"""

from __future__ import annotations

from dataclasses import dataclass, field
from math import asin, cos, radians, sin, sqrt

import numpy as np
from numpy.random import Generator

from generator.behavior.profiles import profile_for
from generator.config import GeneratorConfig
from generator.models import Event, TxnType, hash_identifier
from generator.population.segments import (
    AGENT_KYC_TIER,
    assign_kyc_tiers,
    assign_segments,
)

# --------------------------------------------------------------------------- #
# Cells
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class Cell:
    """A mobile-money coverage cell with approximate coordinates."""

    cell_id: str
    region: str
    lat: float
    lon: float


def _cells() -> tuple[Cell, ...]:
    """Sixty cells: Kigali(20), Nyagatare(10), Musanze(10), Huye(8),
    Rubavu(6), Rusizi(6) with real-ish coordinates."""
    specs: list[tuple[str, str, float, float, int]] = [
        # (prefix, region, lat0, lon0, count)
        ("KGL", "Kigali", -1.940, 30.060, 20),
        ("NYG", "Nyagatare", -1.290, 30.330, 10),
        ("MSZ", "Musanze", -1.500, 29.630, 10),
        ("HYE", "Huye", -2.370, 29.750, 8),
        ("RBV", "Rubavu", -1.680, 29.260, 6),
        ("RSZ", "Rusizi", -2.480, 29.340, 6),
    ]
    cells: list[Cell] = []
    for prefix, region, lat0, lon0, count in specs:
        for i in range(count):
            lat = lat0 + 0.010 * ((i * 7) % 5) - 0.02
            lon = lon0 + 0.012 * ((i * 3) % 5) - 0.024
            cells.append(Cell(cell_id=f"{prefix}-{i + 1:02d}", region=region, lat=lat, lon=lon))
    return tuple(cells)


#: All cells, indexed by position (used as ``Account.home_cell``).
CELLS: tuple[Cell, ...] = _cells()

#: Relative population weight per region (sums to 1).
REGION_WEIGHTS: dict[str, float] = {
    "Kigali": 0.45,
    "Nyagatare": 0.15,
    "Musanze": 0.14,
    "Huye": 0.12,
    "Rubavu": 0.08,
    "Rusizi": 0.06,
}

_REGIONS: tuple[str, ...] = tuple(REGION_WEIGHTS)
_REGION_PROBS = np.array([REGION_WEIGHTS[r] for r in _REGIONS], dtype=np.float64)
_REGION_CELLS: dict[str, np.ndarray] = {
    r: np.array([i for i, c in enumerate(CELLS) if c.region == r], dtype=np.int64) for r in _REGIONS
}


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres."""
    p1, p2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dlmb = radians(lon2 - lon1)
    a = sin(dphi / 2) ** 2 + cos(p1) * cos(p2) * sin(dlmb / 2) ** 2
    return 2 * 6371.0 * asin(sqrt(a))


def pick_home_cell(rng: Generator) -> int:
    """Sample a home cell with region population weights."""
    region = str(rng.choice(_REGIONS, p=_REGION_PROBS))
    return int(rng.choice(_REGION_CELLS[region]))


# --------------------------------------------------------------------------- #
# Accounts
# --------------------------------------------------------------------------- #

#: Lognormal (mu, sigma) opening balances per KYC tier, capped by tier cap.
OPENING_BY_TIER: dict[int, tuple[float, float]] = {
    1: (10.8, 0.90),  # median ~49k RWF
    2: (12.6, 0.90),  # median ~300k
    3: (13.8, 1.00),  # median ~1M
}

#: Agent float range in RWF (spec: 2M - 20M).
AGENT_FLOAT_RANGE: tuple[int, int] = (2_000_000, 20_000_000)

MIN_OPENING = 1_000

#: Probability an account owns a second device (used 10% of the time).
SECOND_DEVICE_PROB = 0.40
SECOND_DEVICE_USAGE = 0.10


@dataclass(slots=True)
class Account:
    """One wallet in the simulated population."""

    idx: int
    account_id: str
    msisdn_hash: str
    segment: str
    kyc_tier: int
    created_day: int  # day offset from start_date; negative = pre-existing
    home_cell: int
    devices: tuple[str, ...]
    sim_serial_hash: str
    is_agent: bool
    hourly: np.ndarray  # 24 normalised local-hour weights (weekdays)
    hourly_weekend: np.ndarray  # 24 normalised local-hour weights (Sat/Sun)
    amount_mu: dict[str, float]  # per event source
    amount_sigma: dict[str, float]  # per event source
    opening_balance: int
    # mutable runtime state - updated on apply and by fraud scenarios
    cur_device: str
    cur_cell: int
    cur_sim: str

    @property
    def region(self) -> str:
        return CELLS[self.home_cell].region

    def age_days(self, day: int) -> int:
        return day - self.created_day


def _jittered_hourly(base: tuple[float, ...], rng: Generator) -> np.ndarray:
    weights = np.asarray(base, dtype=np.float64) * rng.lognormal(0.0, 0.15, 24)
    total = weights.sum()
    if total <= 0:  # pragma: no cover - defensive, bases are positive
        weights = np.full(24, 1 / 24)
    else:
        weights = weights / total
    return weights


def create_accounts(config: GeneratorConfig, rng: Generator) -> list[Account]:
    """Create the full population (customers first, then agents).

    Segment shares come from the config, KYC tiers are segment-weighted,
    ``created_day`` mixes pre-existing (70%) and in-run (30%) accounts.
    """
    segments = assign_segments(rng, config)
    tiers = assign_kyc_tiers(rng, segments)

    accounts: list[Account] = []
    n_total = config.accounts
    n_customers = config.n_customers

    for idx in range(n_total):
        is_agent = idx >= n_customers
        if is_agent:
            segment = "agent"
            tier = AGENT_KYC_TIER
            created_day = -365
            profile = profile_for("small_trader")
        else:
            local = idx
            segment = segments[local]
            tier = tiers[local]
            created_day = (
                -int(rng.integers(1, 731))
                if rng.random() < 0.70
                else int(rng.integers(0, config.days))
            )
            profile = profile_for(segment)

        home_cell = pick_home_cell(rng)

        n_devices = 2 if rng.random() < SECOND_DEVICE_PROB else 1
        devices = tuple(f"dev_{idx:06d}_{d}" for d in range(n_devices))
        sim_hash = hash_identifier(config.seed, "sim", f"{idx}")
        msisdn = hash_identifier(config.seed, "msisdn", f"+2507{880000000 + idx}")

        hourly = _jittered_hourly(profile.hourly, rng)
        weekend_base = profile.weekend_hourly or profile.hourly
        hourly_weekend = _jittered_hourly(weekend_base, rng)

        amount_mu: dict[str, float] = {}
        amount_sigma: dict[str, float] = {}
        for source, (mu, sigma) in profile.amount_params.items():
            amount_mu[source] = mu + float(rng.normal(0.0, 0.25))
            amount_sigma[source] = sigma

        if is_agent:
            lo, hi = AGENT_FLOAT_RANGE
            opening = int(rng.integers(lo, hi + 1))
        else:
            mu, sigma = OPENING_BY_TIER[tier]
            opening = int(rng.lognormal(mu, sigma))
            opening = max(MIN_OPENING, min(opening, config.tier_limits(tier).balance_cap))
            opening -= opening % 50

        accounts.append(
            Account(
                idx=idx,
                account_id=f"acct_{idx:06d}",
                msisdn_hash=msisdn,
                segment=segment,
                kyc_tier=tier,
                created_day=created_day,
                home_cell=home_cell,
                devices=devices,
                sim_serial_hash=sim_hash,
                is_agent=is_agent,
                hourly=hourly,
                hourly_weekend=hourly_weekend,
                amount_mu=amount_mu,
                amount_sigma=amount_sigma,
                opening_balance=opening,
                cur_device=devices[0],
                cur_cell=home_cell,
                cur_sim=sim_hash,
            )
        )

    assert len(accounts) == n_total  # noqa: S101 - population integrity check
    return accounts


# --------------------------------------------------------------------------- #
# Ledger
# --------------------------------------------------------------------------- #


class LedgerViolation(RuntimeError):
    """Raised when an event would break a ledger invariant."""


class Ledger:
    """Balances and daily volume for every ledger index.

    Index layout: ``0..n_accounts-1`` are population accounts, followed by
    the special accounts (merchants, billers, telecom, banks).  All balance
    mutations go through :meth:`apply`; the caller is responsible for
    clamping amounts first (the ledger double-checks and refuses anything
    that would go negative).
    """

    def __init__(self, opening_balances: list[int]) -> None:
        self.balances: list[int] = list(opening_balances)
        self.daily_spent: dict[int, int] = {}
        self._seen: dict[int, set[int]] = {}

    def balance(self, idx: int) -> int:
        return self.balances[idx]

    def spent_today(self, idx: int) -> int:
        return self.daily_spent.get(idx, 0)

    def reset_daily_volume(self) -> None:
        """Clear per-day volume counters (called at each day boundary)."""
        self.daily_spent.clear()

    def has_seen(self, sender: int, recipient: int) -> bool:
        known = self._seen.get(sender)
        return known is not None and recipient in known

    def mark_seen(self, sender: int, recipient: int) -> None:
        self._seen.setdefault(sender, set()).add(recipient)

    def apply(
        self,
        event: Event,
        sender: int,
        recipient: int,
        *,
        day: int,
    ) -> None:
        """Apply one event's balance changes.

        Semantics (documented in docs/phase-1.md):

        * every type except ``CASH_IN`` debits the sender by
          ``amount + fee`` and credits the recipient by ``amount``;
        * ``CASH_IN`` credits the sender wallet with ``amount - fee`` and
          credits the agent's float with ``amount``;
        * ``CASH_OUT`` debits the sender by ``amount + fee`` and debits the
          agent's float by ``amount`` (the agent hands out cash).
        """
        bal = self.balances
        amount = event.amount
        fee = event.fee
        txn = event.txn_type

        sender_before = bal[sender]
        if txn is TxnType.CASH_IN:
            bal[sender] = sender_before + amount - fee
            bal[recipient] += amount
        else:
            cost = amount + fee
            if sender_before < cost:
                raise LedgerViolation(
                    f"{txn} from idx {sender}: balance {sender_before} < amount+fee {cost}"
                )
            bal[sender] = sender_before - cost
            if txn is TxnType.CASH_OUT:
                if bal[recipient] < amount:
                    raise LedgerViolation(f"agent float {bal[recipient]} < cash-out {amount}")
                bal[recipient] -= amount
            else:
                bal[recipient] += amount

        if bal[sender] < 0 or bal[recipient] < 0:
            raise LedgerViolation(
                f"negative balance after {txn}: sender={bal[sender]} recipient={bal[recipient]}"
            )

        # daily volume (sender side, amount + fee) - checked against tier limits
        self.daily_spent[sender] = self.daily_spent.get(sender, 0) + amount + fee

        if not event.recipient.first_seen_by_sender:
            self.mark_seen(sender, recipient)

    def total(self) -> int:
        return sum(self.balances)


@dataclass(slots=True)
class World:
    """Everything the simulation needs about the account universe."""

    accounts: list[Account]
    specials: list[SpecialAccount]
    cells: tuple[Cell, ...] = CELLS
    index_of: dict[str, int] = field(default_factory=dict)
    region_members: dict[str, list[int]] = field(default_factory=dict)
    merchant_payers: dict[int, list[int]] = field(default_factory=dict)
    customer_agents: dict[int, list[int]] = field(default_factory=dict)
    agent_customers: dict[int, list[int]] = field(default_factory=dict)

    @property
    def n_accounts(self) -> int:
        return len(self.accounts)

    @property
    def n_total(self) -> int:
        return len(self.accounts) + len(self.specials)

    def account(self, idx: int) -> Account:
        return self.accounts[idx]

    def is_special(self, idx: int) -> bool:
        return idx >= len(self.accounts)

    def is_agent(self, idx: int) -> bool:
        return idx < len(self.accounts) and self.accounts[idx].is_agent

    def display_id(self, idx: int) -> str:
        """account_id for any ledger index (population or special)."""
        if idx < len(self.accounts):
            return self.accounts[idx].account_id
        return self.specials[idx - len(self.accounts)].account_id


# SpecialAccount is defined in population.graph; import it here at the end to
# avoid a circular import at module load (graph imports helpers from here).
from generator.population.graph import SpecialAccount  # noqa: E402
