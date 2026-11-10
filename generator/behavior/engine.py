"""Day-by-day behaviour engine: planning and emission of legitimate events.

The planner (``plan_day``) is pure: it draws *what* an account does on a
day and *when*, without touching balances, so it can be run twice - once as
a cheap counting pass that sizes the fraud budget, once for real.  All
amount and balance decisions happen at apply time, where the ledger state
is known.

Ordering: events are processed in ``(event_time, stream)`` order - legit
plan items first, then fraud triggers, then fraud-emitted events, with a
sequence number as the final tie-break - so the emission order is fully
deterministic and identical across runs and environments.

Geo model: every legitimate event draws a small drift from the sender's
home cell (exponential 3 km, capped at 60 km; 2% of events draw a long
trip of 70-400 km).  Fraud injectors may pin a specific far cell.
"""

from __future__ import annotations

import heapq
from bisect import bisect_right
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta

import numpy as np
from numpy.random import Generator

from generator.behavior.amounts import MIN_AMOUNT, fee_for, sample_amount
from generator.behavior.calendar import (
    day_multiplier,
    is_salary_window,
    is_term_window,
)
from generator.behavior.profiles import INBOUND_SOURCES, SOURCE_TXN_TYPES, profile_for
from generator.config import GeneratorConfig
from generator.models import (
    CHANNELS_BY_TYPE,
    SENDER_DEBIT_TYPES,
    Event,
    Geo,
    LabelRecord,
    LabelValue,
    Recipient,
    Sender,
    TxnType,
    crockford_b32,
    encode_ulid,
    format_event_time,
)
from generator.population.accounts import CELLS, Account, Ledger, World
from generator.population.graph import Contacts

#: Event sources an agent wallet performs (agents never send P2P).
#: bank_out slowly sweeps surplus float to the agent's bank account; the
#: rate keeps the 2-20M float positive across a 90-day run.
AGENT_RATES: dict[str, float] = {
    "bank_out": 0.03,
    "bill": 0.06,
    "airtime": 0.02,
}

#: Session-id prefix per transaction type (identical for fraud and legit).
SESSION_PREFIX: dict[TxnType, str] = {
    TxnType.P2P_SEND: "p2p",
    TxnType.CASH_IN: "agt",
    TxnType.CASH_OUT: "agt",
    TxnType.AIRTIME: "top",
    TxnType.BILL_PAY: "bil",
    TxnType.MERCHANT_PAY: "pos",
    TxnType.BANK_TRANSFER: "bnk",
}

#: Share of planned legitimate items that survive amount/balance/daily-limit
#: caps.  Measured at 0.894 on both the small and run42 configurations; the
#: fraud budget is sized on this figure so the realised fraud rate matches
#: the configured rate instead of running ~12% high.
APPLIED_LEGIT_RATIO = 0.894

#: Probability a legitimate sender takes a long trip (70-400 km).
LONG_TRIP_RATE = 0.02
#: Probability a legit sender switches device (when it owns a second one).
DEVICE_SWITCH_RATE = 0.005

_DAY_MS = 86_400_000
_HOUR_MS = 3_600_000
_MIN_MS = 60_000
_LOCAL_UTC_OFFSET_MS = 2 * _HOUR_MS  # Africa/Kigali = UTC+2, no DST

#: Precomputed cell centre coordinates for fast nearest-cell lookup.
CELL_LAT = np.array([c.lat for c in CELLS], dtype=np.float64)
CELL_LON = np.array([c.lon for c in CELLS], dtype=np.float64)

#: Cumulative cell-choice distribution per home cell, built once from the
#: documented geo model (exponential 3 km, capped 60 km; 2% long trips).
#: Sampling directly from it avoids a 60-cell nearest-point scan per event.
_CELL_CUMS: tuple[np.ndarray, ...] | None = None


def _cell_cum_table() -> tuple[np.ndarray, ...]:
    """Build the per-home-cell cumulative cell distribution (Monte Carlo)."""
    global _CELL_CUMS
    if _CELL_CUMS is not None:
        return _CELL_CUMS
    rng = np.random.default_rng(20_260_101)  # fixed: independent of run seed
    n = 20_000
    rows: list[np.ndarray] = []
    for home in CELLS:
        long_trip = rng.random(n) < LONG_TRIP_RATE
        km = np.where(
            long_trip,
            np.minimum(rng.lognormal(4.6, 0.55, n), 400.0),
            np.minimum(rng.exponential(3.0, n), 60.0),
        )
        heading = rng.uniform(0.0, 2.0 * np.pi, n)
        lat = home.lat + (km * np.cos(heading)) / 111.0
        lon = home.lon + (km * np.sin(heading)) / (111.0 * np.cos(np.radians(home.lat)))
        d2 = ((CELL_LAT[None, :] - lat[:, None]) * 111.0) ** 2 + (
            (CELL_LON[None, :] - lon[:, None]) * 88.0
        ) ** 2
        counts = np.bincount(d2.argmin(axis=1), minlength=len(CELLS)).astype(np.float64)
        rows.append(np.cumsum(counts / counts.sum()))
    _CELL_CUMS = tuple(rows)
    return _CELL_CUMS


@dataclass(slots=True)
class PlanItem:
    """One legitimate event decided by the planner (amount unknown yet)."""

    ts_ms: int
    account: int  # subject ledger index (whose rate drew it)
    cp: int  # counterparty ledger index
    source: str  # behaviour source key (drives txn type / amount params)
    link: str = ""  # restock companion link id ("" = none)


def local_day_start_ms(config: GeneratorConfig, day: int) -> int:
    """Epoch ms of local midnight (00:00 EAT) for simulation day ``day``."""
    base = config.start_date + timedelta(days=day)
    import datetime as _dt

    utc_midnight = _dt.datetime(base.year, base.month, base.day, tzinfo=_dt.UTC)
    return int(utc_midnight.timestamp() * 1000) - _LOCAL_UTC_OFFSET_MS


def _round50(value: float) -> int:
    return max(MIN_AMOUNT, int(value) - int(value) % 50)


def _as_number(value: object) -> float:
    """Narrow a mode parameter to a float (fraud params are numbers)."""
    if isinstance(value, int | float):
        return float(value)
    raise TypeError(f"expected numeric fraud parameter, got {type(value).__name__}")


def _as_pair(value: object) -> tuple[float, float]:
    """Narrow a mode parameter to a (lo, hi) pair."""
    if isinstance(value, tuple) and len(value) == 2:
        return _as_number(value[0]), _as_number(value[1])
    raise TypeError(f"expected a (lo, hi) fraud parameter, got {value!r}")


def _pick_hour(cum: np.ndarray, rng: Generator) -> int:
    # ``bisect`` is ~3x faster than ``np.searchsorted`` for a 24-entry array.
    return bisect_right(cum, rng.random())


HourlyCums = tuple[tuple[np.ndarray, ...], tuple[np.ndarray, ...]]

#: Canonical order of behaviour sources.  Fixed so RNG consumption and the
#: resulting output are reproducible regardless of dict ordering.
PLAN_SOURCES: tuple[str, ...] = (
    "p2p_out",
    "merchant_out",
    "merchant_in",
    "airtime",
    "bill",
    "cash_out",
    "cash_in",
    "bank_out",
    "salary_in",
    "school_fee",
    "restock",
    "first_large",
)

#: Profile ids used by the vectorised planner; 5 is ``agent``.
PROFILE_ORDER: tuple[str, ...] = (
    "salaried",
    "small_trader",
    "student_parent",
    "rural_saver",
    "urban_youth",
    "agent",
)


def _profile_id(segment: str, is_agent: bool) -> int:
    return 5 if is_agent else PROFILE_ORDER.index(segment)


@dataclass(slots=True)
class PlanTables:
    """Per-account rate/gate arrays shared by both planning passes.

    The first planning implementation called ``rng.poisson`` once per account
    per source per day (45M Python calls on the 50k x 90d run).  Folding that
    into per-source vector calls is what brings generation inside the
    Audit 9 budget.
    """

    rates: np.ndarray  # (n_accounts, n_sources) base events/day
    gates: np.ndarray  # (n_accounts, n_sources) bool availability
    activity: np.ndarray  # (n_accounts,) lognormal activity multiplier
    profile_ids: np.ndarray  # (n_accounts,) index into PROFILE_ORDER
    created_day: np.ndarray  # (n_accounts,) int32, negative = pre-existing
    source_index: dict[str, int]


def estimate_legit_events(config: GeneratorConfig, world: World, tables: PlanTables) -> int:
    """Expected number of legitimate events without running a planning pass.

    The fraud budget only needs the *size* of the legitimate stream from
    ``fraud_rate / (1 - fraud_rate)``; drawing every plan item twice just to
    count them cost ~8% of total generation time.  This computes the same
    expectation analytically from the rate tables, then applies
    :data:`APPLIED_LEGIT_RATIO` for the events that are skipped because the
    wallet cannot afford them or has hit its KYC daily limit.  Both the
    small and run42 datasets apply ~89.4% of planned items, so sizing the
    fraud budget on planned events alone made the realised rate ~12% high.
    """
    total = 0.0
    n_sources = len(PLAN_SOURCES)
    salaried_id = PROFILE_ORDER.index("salaried")
    trader_id = PROFILE_ORDER.index("small_trader")
    for day in range(config.days):
        date = config.start_date + timedelta(days=day)
        mult_lookup = np.array(
            [
                day_multiplier(date, profile_for("small_trader" if name == "agent" else name))
                for name in PROFILE_ORDER
            ]
        )
        active = tables.activity * mult_lookup[tables.profile_ids]
        exists = tables.created_day <= day
        salary_ok = is_salary_window(date)
        term_ok = is_term_window(date)
        for si in range(n_sources):
            source = PLAN_SOURCES[si]
            if source == "salary_in" and not salary_ok:
                continue
            if source == "school_fee" and not term_ok:
                continue
            total += float(np.sum(tables.rates[:, si] * active * tables.gates[:, si] * exists))
        if salary_ok:
            n_salaried = int(np.sum((tables.profile_ids == salaried_id) & exists))
            total += 0.30 * 9.0 * n_salaried
        if date.weekday() in (1, 4):
            n_traders = int(np.sum((tables.profile_ids == trader_id) & exists))
            total += 0.15 * 10.0 * n_traders
    return int(round(total * APPLIED_LEGIT_RATIO))


def build_plan_tables(world: World) -> PlanTables:
    """Build the per-account rate matrices once per run."""
    accounts = world.accounts
    n = len(accounts)
    n_sources = len(PLAN_SOURCES)
    rates = np.zeros((n, n_sources), dtype=np.float64)
    gates = np.ones((n, n_sources), dtype=bool)
    activity = np.empty(n, dtype=np.float64)
    profile_ids = np.empty(n, dtype=np.int64)
    created_day = np.empty(n, dtype=np.int32)
    source_index = {name: i for i, name in enumerate(PLAN_SOURCES)}

    for a in accounts:
        i = a.idx
        if a.is_agent:
            sources: Mapping[str, float] = AGENT_RATES
        else:
            sources = profile_for(a.segment).rates
        for source, rate in sources.items():
            rates[i, source_index[source]] = rate
        contacts = world.contacts.get(i) or Contacts()
        gates[i, source_index["merchant_in"]] = bool(world.merchant_payers.get(i))
        gates[i, source_index["cash_in"]] = bool(world.customer_agents.get(i))
        gates[i, source_index["cash_out"]] = bool(world.customer_agents.get(i))
        gates[i, source_index["p2p_out"]] = bool(contacts.peers)
        gates[i, source_index["first_large"]] = bool(contacts.peers)
        gates[i, source_index["bill"]] = bool(contacts.billers)
        activity[i] = a.activity
        profile_ids[i] = _profile_id(a.segment, a.is_agent)
        created_day[i] = a.created_day
    return PlanTables(rates, gates, activity, profile_ids, created_day, source_index)


def plan_day(
    config: GeneratorConfig,
    world: World,
    day: int,
    rng: Generator,
    hourly_cums: HourlyCums,
    tables: PlanTables,
) -> list[PlanItem]:
    """Draw every legitimate event of one simulation day (pure, no state).

    ``hourly_cums`` is ``(weekday_cums, weekend_cums)`` - per-account
    cumulative hourly weights precomputed once per pass.  Counts are drawn
    per source with one vector call; only the events actually produced enter
    a Python loop.
    """
    date = config.start_date + timedelta(days=day)
    weekend = date.weekday() >= 5
    day_ms = local_day_start_ms(config, day)
    accounts = world.accounts
    items: list[PlanItem] = []

    mult_lookup = np.array(
        [
            day_multiplier(date, profile_for("small_trader" if name == "agent" else name))
            for name in PROFILE_ORDER
        ]
    )
    active = tables.activity * mult_lookup[tables.profile_ids]
    exists = tables.created_day <= day
    cums = hourly_cums[1] if weekend else hourly_cums[0]

    salary_ok = is_salary_window(date)
    term_ok = is_term_window(date)

    for source in PLAN_SOURCES:
        if source == "restock":
            continue  # handled below (needs a companion cash-in)
        if source == "salary_in" and not salary_ok:
            continue
        if source == "school_fee" and not term_ok:
            continue
        si = tables.source_index[source]
        lam = tables.rates[:, si] * active * tables.gates[:, si] * exists
        counts = rng.poisson(lam)
        for ai in np.flatnonzero(counts):
            idx = int(ai)
            acc = accounts[idx]
            contacts = world.contacts.get(idx) or Contacts()
            payers = world.merchant_payers.get(idx)
            cum = cums[idx]
            for _ in range(int(counts[ai])):
                hour = _pick_hour(cum, rng)
                ts = day_ms + hour * _HOUR_MS
                ts += int(rng.integers(0, 60)) * 60_000
                ts += int(rng.integers(0, 60)) * 1000
                cp = _counterparties(source, idx, contacts, payers, world, rng)
                items.append(PlanItem(ts, idx, cp, source))

    # night restocking: a cash-in shortly before banks the same money
    si = tables.source_index["restock"]
    lam = tables.rates[:, si] * active * exists
    counts = rng.poisson(lam)
    for ai in np.flatnonzero(counts):
        idx = int(ai)
        acc = accounts[idx]
        contacts = world.contacts.get(idx) or Contacts()
        attached = world.customer_agents.get(idx) or []
        for k in range(int(counts[ai])):
            hour = int(rng.integers(1, 4))  # 01:00 - 03:59
            ts = day_ms + hour * _HOUR_MS
            ts += int(rng.integers(0, 60)) * 60_000
            ts += int(rng.integers(0, 60)) * 1000
            gap = int(rng.integers(5, 21)) * _MIN_MS
            link = f"{idx}:{day}:{k}"
            if attached:
                items.append(
                    PlanItem(
                        ts - gap,
                        idx,
                        attached[int(rng.integers(0, len(attached)))],
                        "cash_in",
                        link=link,
                    )
                )
            items.append(PlanItem(ts, idx, contacts.bank, "restock", link=link))

    # salary-day family split burst (legit hard negative H1)
    if salary_ok:
        for acc in accounts:
            if acc.is_agent or acc.segment != "salaried" or acc.created_day > day:
                continue
            if rng.random() < 0.30:
                contacts = world.contacts.get(acc.idx) or Contacts()
                n = int(rng.integers(6, 13))
                t0 = day_ms + int(rng.integers(7, 20)) * _HOUR_MS
                t0 += int(rng.integers(0, 60)) * _MIN_MS
                span = int(rng.integers(5, 11)) * _MIN_MS
                base = contacts.peers + contacts.merchants
                if base:
                    for k in range(n):
                        ts = t0 + (span * k) // n
                        cp = base[k % len(base)]
                        items.append(PlanItem(ts, acc.idx, cp, "p2p_out"))

    # trader market-day supplier burst (legit hard negative, lookalike)
    if date.weekday() in (1, 4):
        for acc in accounts:
            if acc.is_agent or acc.segment != "small_trader" or acc.created_day > day:
                continue
            if rng.random() < 0.15:
                contacts = world.contacts.get(acc.idx) or Contacts()
                n = int(rng.integers(6, 15))
                t0 = day_ms + int(rng.integers(7, 11)) * _HOUR_MS
                t0 += int(rng.integers(0, 60)) * _MIN_MS
                span = int(rng.integers(6, 13)) * _MIN_MS
                base = contacts.merchants + contacts.peers
                if base:
                    for k in range(n):
                        ts = t0 + (span * k) // n
                        cp = base[k % len(base)]
                        items.append(PlanItem(ts, acc.idx, cp, "merchant_out"))

    return items


def _counterparties(
    source: str,
    acc_idx: int,
    contacts: Contacts,
    payers: list[int] | None,
    world: World,
    rng: Generator,
) -> int:
    """Pick the counterparty ledger index for one planned event."""
    if source == "p2p_out":
        return contacts.peers[int(rng.integers(0, len(contacts.peers)))]
    if source == "first_large":
        # fresh contact: not one of the account's usual peers/merchants and
        # already created by the event day
        known = set(contacts.peers) | set(contacts.merchants)
        for _ in range(20):
            cand = int(rng.integers(0, len(world.accounts)))
            target = world.accounts[cand]
            if (
                cand != acc_idx
                and cand not in known
                and target.created_day < 0
                and not target.is_agent
            ):
                return cand
        return contacts.peers[int(rng.integers(0, len(contacts.peers)))]
    if source == "merchant_out":
        return contacts.merchants[int(rng.integers(0, len(contacts.merchants)))]
    if source == "merchant_in":
        assert payers is not None  # noqa: S101 - gated by the caller
        return payers[int(rng.integers(0, len(payers)))]
    if source == "airtime":
        return world.index_of["spec_telecom_000"]
    if source in ("bill", "school_fee"):
        if source == "school_fee":
            return next(s.idx for s in world.specials if s.name == "school")
        return contacts.billers[int(rng.integers(0, len(contacts.billers)))]
    if source in ("cash_in", "cash_out"):
        agents = world.customer_agents.get(acc_idx) or []
        return agents[int(rng.integers(0, len(agents)))]
    if source in ("bank_out", "restock", "salary_in"):
        if contacts.bank < 0:
            raise ValueError(f"account {acc_idx} has no bank contact for {source!r}")
        return contacts.bank
    raise ValueError(f"unknown source {source!r}")  # pragma: no cover


# --------------------------------------------------------------------------- #
# Fraud pending events (planned by generator.fraud, applied by the engine)
# --------------------------------------------------------------------------- #

MODE_LNORM = "lnorm"
MODE_LNORM_RESERVE = "lnorm_reserve"
MODE_FIXED = "fixed"
MODE_FRAC_LIMIT = "frac_limit"
MODE_FRAC_BAL = "frac_balance"
MODE_MULT_HISTORY = "mult_history"
MODE_CYCLE = "cycle"


@dataclass(slots=True)
class PendingEvent:
    """One fraud event scheduled at plan time; amounts resolved at apply."""

    ts_ms: int
    scenario: str
    run_id: str
    txn: TxnType
    sender: int
    recipient: int
    mode: str
    param: object  # mode-specific: (mu, sigma) | int | float | (lo, hi)
    cell_idx: int | None = None
    set_sim: str | None = None  # persist a new SIM on the sender (ATO)
    set_device: str | None = None  # persist a new device on the sender (ATO)
    device_once: str | None = None  # one-off device override (mule ring)
    members: tuple[int, ...] = ()  # cycle members for uniform clamp (CIRCULAR)
    #: Fraud sends still to come in this run; those amounts are reserved so a
    #: velocity burst always produces its full 10-30 events (Audit 6).
    remaining: int = 0


class Engine:
    """Applies plan items and fraud pending events, emits records to a sink."""

    def __init__(
        self,
        config: GeneratorConfig,
        world: World,
        ledger: Ledger,
        bank: object,
        sink: object,
    ) -> None:
        from generator.rng import RngBank

        assert isinstance(bank, RngBank)  # noqa: S101 - narrow the duck type
        self.config = config
        self.world = world
        self.ledger = ledger
        self.sink = sink
        self.amount_rng: Generator = bank.get("amounts")
        self.ulid_rng: Generator = bank.get("ulid")
        self.channel_rng: Generator = bank.get("channel")
        self.labels_rng: Generator = bank.get("labels")
        self.day = 0
        self.emitted = 0
        self.skipped = 0
        self.skip_reasons: dict[str, int] = {}
        self._links: dict[str, tuple[int, int]] = {}  # link -> (ts, amount)
        self._send_history: dict[int, list[tuple[int, int]]] = {}  # outgoing events per sender
        self._cycle_x: dict[str, int] = {}  # run_id -> uniform cycle amount
        # Resolve the 60-cell distance table once instead of re-entering the
        # lazy global lookup for every emitted event.
        self._cell_cums = _cell_cum_table()
        self._channels = CHANNELS_BY_TYPE
        self._session_prefix = SESSION_PREFIX
        self._label_noise = config.label_noise > 0
        # ULIDs emitted in the same millisecond get consecutive random parts,
        # so (event_time, event_id) order equals ledger application order.
        # Without this, a same-millisecond tie could be replayed in the wrong
        # order and break balance_before (found by the Audit 4 replay).
        self._last_id_ts = -1
        self._last_id_rand = -1

    def _skip(self, reason: str) -> bool:
        """Record a dropped event and its reason (reported in the manifest)."""
        self.skipped += 1
        self.skip_reasons[reason] = self.skip_reasons.get(reason, 0) + 1
        return False

    # ------------------------------------------------------------------ #
    # day loop
    # ------------------------------------------------------------------ #

    def process_day(self, day: int, items: list[PlanItem], pending: list[PendingEvent]) -> None:
        """Run one simulation day: merge legit items and fraud events by time."""
        self.day = day
        self.ledger.reset_daily_volume()
        heap: list[tuple[int, int, int, tuple[str, object]]] = []
        seq = 0
        for it in items:
            heap.append((it.ts_ms, 0, seq, ("legit", it)))
            seq += 1
        for pe in pending:
            heap.append((pe.ts_ms, 1, seq, ("pending", pe)))
            seq += 1
        heapq.heapify(heap)
        while heap:
            ts, _, _, (kind, obj) = heapq.heappop(heap)
            self.sink.drain_before(ts)  # type: ignore[attr-defined]
            if kind == "legit":
                self.apply_plan_item(obj)  # type: ignore[arg-type]
            else:
                self.apply_pending(obj)  # type: ignore[arg-type]

    # ------------------------------------------------------------------ #
    # legitimate events
    # ------------------------------------------------------------------ #

    def apply_plan_item(self, item: PlanItem) -> bool:
        """Resolve amounts/balances for one planned event and emit it."""
        cfg = self.config
        world = self.world
        acc = world.accounts[item.account]
        source = item.source
        txn = SOURCE_TXN_TYPES[source]
        inbound = source in INBOUND_SOURCES
        sender_idx = item.cp if inbound else item.account
        rec_idx = item.account if inbound else item.cp

        rng = self.amount_rng

        # ---- amount -----------------------------------------------------
        if source == "first_large":
            typical = np.exp(acc.amount_mu.get("p2p_out", 9.0))
            amount = _round50(typical * float(rng.uniform(5.0, 15.0)))
        elif source == "school_fee":
            mu = acc.amount_mu[source]
            sigma = acc.amount_sigma[source]
            amount = _round50(float(np.exp(rng.normal(mu, sigma))) * float(rng.uniform(3.0, 10.0)))
        elif source == "restock":
            stored = self._links.get(item.link)
            if stored is not None and item.ts_ms - stored[0] <= 30 * _MIN_MS:
                amount = stored[1]
            else:
                mu = acc.amount_mu.get(source, 12.0)
                sigma = acc.amount_sigma.get(source, 0.7)
                amount = sample_amount(rng, mu, sigma, 10**12)
        else:
            mu = acc.amount_mu.get(source, 9.0)
            sigma = acc.amount_sigma.get(source, 0.8)
            amount = sample_amount(rng, mu, sigma, 10**12)

        if amount <= 0:
            return self._skip("empty_draw")

        # ---- caps (daily limit, balance, agent float) -------------------
        if not world.is_special(sender_idx) and txn in SENDER_DEBIT_TYPES:
            tier = world.accounts[sender_idx].kyc_tier
            limit = cfg.tier_limits(tier).daily_limit
            remaining = limit - self.ledger.spent_today(sender_idx)
            if remaining < MIN_AMOUNT:
                return self._skip("daily_limit")
            ub = remaining - fee_for(remaining)
            if ub < MIN_AMOUNT:
                return self._skip("daily_limit")
            if amount > ub:
                amount = ub - (ub % 50)
        if txn is not TxnType.CASH_IN:
            bal = self.ledger.balance(sender_idx)
            afford = bal - fee_for(bal)
            if afford < MIN_AMOUNT:
                return self._skip("balance")
            if amount > afford:
                amount = afford - (afford % 50)
        if txn is TxnType.CASH_OUT:
            agent_bal = self.ledger.balance(rec_idx)
            if amount > agent_bal:
                amount = agent_bal - (agent_bal % 50)
        if amount < MIN_AMOUNT:
            return self._skip("amount")

        # remember restock companions
        if item.link:
            self._links[item.link] = (item.ts_ms, amount)

        return self.emit(
            txn,
            amount,
            sender_idx,
            rec_idx,
            item.ts_ms,
            scenario=None,
            run_id=None,
        )

    # ------------------------------------------------------------------ #
    # fraud events
    # ------------------------------------------------------------------ #

    def apply_pending(self, pe: PendingEvent) -> bool:
        """Resolve a fraud event's amount from current state and emit it."""
        rng = self.amount_rng
        mode = pe.mode
        param = pe.param

        if mode in (MODE_LNORM, MODE_LNORM_RESERVE):
            mu, sigma = _as_pair(param)
            amount = sample_amount(rng, mu, sigma, 10**12)
        elif mode == MODE_FIXED:
            amount = int(_as_number(param))
        elif mode == MODE_FRAC_LIMIT:
            tier = self.world.accounts[pe.sender].kyc_tier
            limit = self.config.tier_limits(tier).daily_limit
            amount = _round50(_as_number(param) * limit)
        elif mode == MODE_FRAC_BAL:
            bal = self.ledger.balance(pe.sender)
            amount = _round50(_as_number(param) * bal)
        elif mode == MODE_MULT_HISTORY:
            lo, hi = _as_pair(param)
            count, mean = self.prior_sends(pe.sender, pe.ts_ms)
            if count < 1 or mean <= 0:
                # A fresh sender has no observed average yet: use the
                # sender's segment typical P2P amount as the baseline so the
                # run still produces its documented 5-15x pattern instead of
                # silently dropping every event (found in Audit 6).
                acc = self.world.accounts[pe.sender]
                mean = float(np.exp(acc.amount_mu.get("p2p_out", 9.0)))
            amount = _round50(mean * float(rng.uniform(lo, hi)))
        elif mode == MODE_CYCLE:
            # One uniform amount for the whole cycle: clamp once at the run's
            # first event so every hop uses exactly the same value.
            cached = self._cycle_x.get(pe.run_id)
            if cached is None:
                want = int(_as_number(param))
                if pe.members:
                    headroom = min(self.ledger.balance(m) for m in pe.members) - 4_000
                    want = min(want, headroom)
                want = max(MIN_AMOUNT, want)
                want -= want % 50
                cached = want
                self._cycle_x[pe.run_id] = cached
            amount = cached
        else:  # pragma: no cover - guarded by the planner
            raise ValueError(f"unknown fraud amount mode {mode!r}")

        # balance / float safety nets (fraud may exceed daily limits only)
        if pe.txn is not TxnType.CASH_IN:
            bal = self.ledger.balance(pe.sender)
            afford = bal - fee_for(bal)
            if afford < MIN_AMOUNT:
                return self._skip("fraud_balance")
            if amount > afford:
                amount = afford - (afford % 50)
        if pe.txn is TxnType.CASH_OUT:
            agent_bal = self.ledger.balance(pe.recipient)
            if amount > agent_bal:
                amount = agent_bal - (agent_bal % 50)
        if pe.remaining > 0:
            # Keep enough balance for every remaining send in the run, so the
            # documented event count survives a low-balance subject.
            afford = self.ledger.balance(pe.sender) - fee_for(self.ledger.balance(pe.sender))
            cap = afford // (pe.remaining + 1)
            if amount > cap:
                amount = cap - (cap % 50)
        if amount < MIN_AMOUNT:
            return self._skip("fraud_amount")

        return self.emit(
            pe.txn,
            amount,
            pe.sender,
            pe.recipient,
            pe.ts_ms,
            scenario=pe.scenario,
            run_id=pe.run_id,
            cell_idx=pe.cell_idx,
            set_sim=pe.set_sim,
            set_device=pe.set_device,
            device_once=pe.device_once,
        )

    # ------------------------------------------------------------------ #
    # emission
    # ------------------------------------------------------------------ #

    def prior_sends(self, sender: int, ts_ms: int, window_days: int = 30) -> tuple[int, float]:
        """Count and mean amount of the sender's outgoing events in-window.

        Used by SOCIAL_ENG ("amount 5-15x sender average") and mirrored by
        the difficulty feature ``amount_vs_avg_30d`` in the audits.
        """
        hist = self._send_history.get(sender)
        if not hist:
            return 0, 0.0
        cutoff = ts_ms - window_days * _DAY_MS
        vals = [a for t, a in hist if t >= cutoff and t < ts_ms]
        if not vals:
            return 0, 0.0
        return len(vals), float(sum(vals)) / len(vals)

    def _draw_cell(self, acc: Account, rng: Generator) -> int:
        """Legit geo: mostly near home, 2% long trips across Rwanda."""
        cum = self._cell_cums[acc.home_cell]
        return int(np.searchsorted(cum, rng.random(), side="right"))

    def emit(
        self,
        txn: TxnType,
        amount: int,
        sender_idx: int,
        rec_idx: int,
        ts_ms: int,
        *,
        scenario: str | None,
        run_id: str | None,
        cell_idx: int | None = None,
        set_sim: str | None = None,
        set_device: str | None = None,
        device_once: str | None = None,
    ) -> bool:
        """Finalise and emit one event; returns False if it was skipped."""
        world = self.world
        ledger = self.ledger
        day = self.day

        if set_sim is not None and not world.is_special(sender_idx):
            world.accounts[sender_idx].cur_sim = set_sim
        if set_device is not None and not world.is_special(sender_idx):
            world.accounts[sender_idx].cur_device = set_device

        # sender block
        if world.is_special(sender_idx):
            spec = world.specials[sender_idx - len(world.accounts)]
            account_id = spec.account_id
            msisdn = spec.msisdn_hash
            device = f"dev_{999900 + (sender_idx - len(world.accounts)):06d}_0"
            from generator.models import hash_identifier

            sim = hash_identifier(self.config.seed, "sim", f"special:{spec.account_id}")
            account_age = 3650
            tier = 3
            cell = cell_idx if cell_idx is not None else 0
        else:
            acc = world.accounts[sender_idx]
            account_id = acc.account_id
            msisdn = acc.msisdn_hash
            if device_once is not None:
                device = device_once
            else:
                if (
                    scenario is None
                    and len(acc.devices) > 1
                    and self.channel_rng.random() < DEVICE_SWITCH_RATE
                ):
                    acc.cur_device = next(d for d in acc.devices if d != acc.cur_device)
                device = acc.cur_device
            sim = acc.cur_sim
            account_age = day - acc.created_day
            tier = acc.kyc_tier
            if cell_idx is not None:
                cell = cell_idx
            else:
                cell = self._draw_cell(acc, self.amount_rng)

        rec_acc = None if world.is_special(rec_idx) else world.accounts[rec_idx]
        rec_spec = None if rec_acc is not None else world.specials[rec_idx - len(world.accounts)]

        first_seen = not ledger.has_seen(sender_idx, rec_idx)
        balance_before = ledger.balance(sender_idx)
        cell_obj = CELLS[cell]
        geo = Geo(lat=cell_obj.lat, lon=cell_obj.lon, cell_id=cell_obj.cell_id)
        allowed = self._channels[txn]
        channel = allowed[int(self.channel_rng.integers(len(allowed)))]
        rec_id = world.display_id(rec_idx)
        rec_is_agent = rec_acc is not None and rec_acc.is_agent

        event = Event(
            ts_ms=ts_ms,
            txn_type=txn,
            amount=amount,
            currency="RWF",
            fee=fee_for(amount),
            sender=Sender(
                account_id=account_id,
                msisdn_hash=msisdn,
                device_id=device,
                sim_serial_hash=sim,
                geo=geo,
                account_age_days=account_age,
                kyc_tier=tier,
                balance_before=balance_before,
            ),
            recipient=Recipient(
                account_id=rec_id,
                msisdn_hash=rec_acc.msisdn_hash if rec_acc else rec_spec.msisdn_hash,  # type: ignore[union-attr]
                is_agent=rec_is_agent,
                first_seen_by_sender=first_seen,
            ),
            channel=channel,
            agent_id=rec_id if txn in (TxnType.CASH_IN, TxnType.CASH_OUT) else None,
            session_id="",  # filled below from the same 128-bit draw as the ULID
        )
        # One 128-bit draw supplies both the ULID random part and the 40-bit
        # session suffix (previously two separate bit_generator calls).
        raw = self.ulid_rng.bit_generator.random_raw(2)
        bits = (int(raw[0]) << 64) | int(raw[1])
        if ts_ms == self._last_id_ts:
            rand = self._last_id_rand + 1
        else:
            rand = bits >> 48
        self._last_id_ts, self._last_id_rand = ts_ms, rand
        event.event_id = encode_ulid((int(ts_ms) & 0xFFFFFFFFFFFF) << 80 | rand)
        event.event_time = format_event_time(ts_ms)
        event.session_id = f"{self._session_prefix[txn]}-{crockford_b32(bits & ((1 << 40) - 1), 8)}"

        ledger.apply(event, sender_idx, rec_idx, day=day)

        # labels (optional label noise flips labels.parquet only)
        label = LabelValue.LEGIT if scenario is None else LabelValue.FRAUD
        scen, run = scenario, run_id
        if self._label_noise and self.labels_rng.random() < self.config.label_noise:
            if label is LabelValue.FRAUD:
                label, scen, run = LabelValue.LEGIT, None, None
            else:
                label, scen, run = LabelValue.FRAUD, "LABEL_NOISE", None
        rec = LabelRecord(
            event_id=event.event_id,
            label=label,
            scenario=scen,
            scenario_run_id=run,
        )

        if txn is not None and not world.is_special(sender_idx):
            hist = self._send_history.setdefault(sender_idx, [])
            hist.append((ts_ms, amount))
            cutoff = ts_ms - 30 * _DAY_MS
            while hist and hist[0][0] < cutoff:
                hist.pop(0)

        self.emitted += 1
        self.sink.on_event(event, rec)  # type: ignore[attr-defined]
        return True


__all__ = [
    "AGENT_RATES",
    "CELL_LAT",
    "CELL_LON",
    "Engine",
    "MODE_CYCLE",
    "MODE_FIXED",
    "MODE_FRAC_BAL",
    "MODE_FRAC_LIMIT",
    "MODE_LNORM",
    "MODE_LNORM_RESERVE",
    "MODE_MULT_HISTORY",
    "PendingEvent",
    "PlanItem",
    "local_day_start_ms",
    "plan_day",
]
