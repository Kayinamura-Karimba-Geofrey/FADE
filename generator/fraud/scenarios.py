"""Fraud scenario planning: budget allocation and pending-event scheduling.

The fraud budget is ``F = round(fraud_rate / (1 - fraud_rate) * N_legit)``
events, split over the seven scenarios by ``config.scenario_shares``.  Run
sizes come from per-scenario part sets (see ``PARTS``) so every run matches
its documented pattern; each scenario's total is snapped to the nearest
achievable sum within 10% of its config-derived target.

Drift: runs scheduled on ``day >= drift.start_day`` scale their timing
spans and amount parameters by ``1 + drift.intensity``.  ATO_SIMSWAP and
STRUCTURING additionally use fewer, larger events after drift (documented
in docs/phase-1-audit.md).

All planning is pure: it reads opening balances only, never the live
ledger, so it can run before the day loop.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from math import ceil, floor, log

import numpy as np
from numpy.random import Generator

from generator.behavior.amounts import fee_for
from generator.behavior.engine import (
    MODE_CYCLE,
    MODE_FIXED,
    MODE_FRAC_BAL,
    MODE_FRAC_LIMIT,
    MODE_LNORM,
    MODE_LNORM_RESERVE,
    MODE_MULT_HISTORY,
    PendingEvent,
    local_day_start_ms,
)
from generator.config import GeneratorConfig
from generator.models import TxnType, hash_identifier
from generator.population.accounts import CELLS, World, haversine_km

_MIN_MS = 60_000
_HOUR_MS = 3_600_000

#: Minimum events a scenario may ever be allocated (one valid run).
FLOORS: dict[str, int] = {
    "VELOCITY": 10,
    "STRUCTURING": 8,
    "ATO_SIMSWAP": 4,
    "MULE": 9,
    "AGENT_FRAUD": 7,
    "CIRCULAR": 4,
    "SOCIAL_ENG": 2,
}

#: Circular run sizes = hops x reps with hops in 2..4, reps in 2..6.
CIRC_SIZES: tuple[int, ...] = tuple(
    sorted({h * r for h in range(2, 5) for r in range(2, 7)})
)

#: Allowed run sizes per scenario (pre-drift).
PARTS_PRE: dict[str, tuple[int, ...]] = {
    "VELOCITY": tuple(range(10, 31)),
    "STRUCTURING": tuple(range(8, 11)),  # n transfers + 1 funding event
    "ATO_SIMSWAP": tuple(range(4, 8)),  # drain transfers
    "MULE": tuple(range(9, 42)),  # k inbounds + 1 cash-out
    "AGENT_FRAUD": tuple(range(7, 17)),  # k cash-ins + 1 bank transfer
    "CIRCULAR": CIRC_SIZES,
    "SOCIAL_ENG": tuple(range(2, 21)),
}

#: Allowed run sizes after drift (ATO/STRUCTURING shrink for larger means).
PARTS_POST: dict[str, tuple[int, ...]] = {
    "VELOCITY": PARTS_PRE["VELOCITY"],
    "STRUCTURING": (6, 7),  # 5-6 transfers + funding
    "ATO_SIMSWAP": (2, 3),
    "MULE": PARTS_PRE["MULE"],
    "AGENT_FRAUD": PARTS_PRE["AGENT_FRAUD"],
    "CIRCULAR": PARTS_PRE["CIRCULAR"],
    "SOCIAL_ENG": PARTS_PRE["SOCIAL_ENG"],
}

#: Scenarios whose run sizes differ before/after drift.
PHASED: frozenset[str] = frozenset({"STRUCTURING", "ATO_SIMSWAP"})

#: Fixed processing order (deterministic pool consumption).
SCENARIO_ORDER: tuple[str, ...] = (
    "VELOCITY",
    "STRUCTURING",
    "ATO_SIMSWAP",
    "MULE",
    "AGENT_FRAUD",
    "CIRCULAR",
    "SOCIAL_ENG",
)

#: Fraud-touched share of the customer population (pool sizing) and of the
#: agent population (agent pool sizing).  The customer share comes from
#: ``config.fraud_account_share``; this is only the agent-side default.
AGENT_POOL_SHARE = 0.02
MIN_CUSTOMER_POOL = 40
MIN_AGENT_POOL = 10

#: Members of a circular run must open with at least this much so the cycle
#: amount (10% of the smallest opening) survives interleaved spending.
CIRC_MIN_OPENING = 400_000


# --------------------------------------------------------------------------- #
# run-size allocation
# --------------------------------------------------------------------------- #


def reachable_sums(parts: tuple[int, ...], limit: int) -> set[int]:
    """All sums <= limit achievable as a sum of ``parts`` (unbounded DP)."""
    dp = [False] * (limit + 1)
    dp[0] = True
    for s in range(1, limit + 1):
        for p in parts:
            if p <= s and dp[s - p]:
                dp[s] = True
                break
    return {i for i, v in enumerate(dp) if v}


def split_sizes(alloc: int, parts: tuple[int, ...], rng: Generator) -> list[int]:
    """Decompose ``alloc`` into run sizes drawn from ``parts`` (exact DP)."""
    if alloc <= 0:
        return []
    if alloc in parts:
        return [alloc]
    order = [int(p) for p in parts]
    rng.shuffle(order)
    reach: dict[int, int] = {0: 0}
    for s in range(1, alloc + 1):
        for p in order:
            if p <= s and (s - p) in reach:
                reach[s] = p
                break
    if alloc not in reach:  # pragma: no cover - callers pre-validate
        raise ValueError(f"cannot split {alloc} into {parts}")
    sizes: list[int] = []
    cur = alloc
    while cur > 0:
        part = reach[cur]
        sizes.append(part)
        cur -= part
    return sizes


def allocate_budget(
    n_legit: int,
    config: GeneratorConfig,
    rng: Generator,
) -> tuple[int, dict[str, int], dict[str, tuple[int, int]]]:
    """Split the fraud budget over scenarios, snapped to achievable sums.

    Returns ``(n_fraud, alloc, phase_split)`` where ``phase_split[s]`` is
    the (pre, post-drift) event split for phased scenarios (others: all in
    pre).
    """
    rate = config.fraud_rate
    n_fraud = round(rate / (1.0 - rate) * n_legit) if n_legit else 0
    if n_fraud <= 0:
        return 0, {s: 0 for s in SCENARIO_ORDER}, {s: (0, 0) for s in SCENARIO_ORDER}

    days_pre = config.days if not config.drift.enabled else min(config.days, config.drift.start_day)
    days_post = config.days - days_pre
    allow_post = days_post > 0

    alloc: dict[str, int] = {}
    phase: dict[str, tuple[int, int]] = {}
    for scenario in SCENARIO_ORDER:
        target = max(FLOORS[scenario], round(config.scenario_shares[scenario] * n_fraud))
        if scenario in PHASED and allow_post:
            total, pre, post = _phase_split(
                target, PARTS_PRE[scenario], PARTS_POST[scenario], days_pre, days_post, rng
            )
            phase[scenario] = (pre, post)
        else:
            total = _snap_total(target, PARTS_PRE[scenario], rng)
            pre, post = total, 0
            phase[scenario] = (0, 0)
        alloc[scenario] = total

    return n_fraud, alloc, phase


def _snap_total(target: int, parts: tuple[int, ...], rng: Generator) -> int:
    """Nearest achievable single-phase total within 10% of ``target``."""
    limit = ceil(target * 1.1) + max(parts)
    ok = reachable_sums(parts, limit)
    lo, hi = floor(target * 0.9), ceil(target * 1.1)
    candidates = sorted(s for s in ok if lo <= s <= hi)
    if not candidates:  # pragma: no cover - guarded by share validation
        raise ValueError(f"no achievable total within 10% of {target} for parts {parts}")
    best = min(candidates, key=lambda s: (abs(s - target), s))
    # deterministic tie-break draw for equal-distance candidates
    tied = [c for c in candidates if abs(c - target) == abs(best - target)]
    return int(tied[int(rng.integers(len(tied)))])


def _phase_split(
    target: int,
    parts_pre: tuple[int, ...],
    parts_post: tuple[int, ...],
    days_pre: int,
    days_post: int,
    rng: Generator,
) -> tuple[int, int, int]:
    """Snap ``target`` to (total, pre, post) achievable sums within 10%."""
    limit = ceil(target * 1.1) + max(max(parts_pre), max(parts_post))
    ok_pre = reachable_sums(parts_pre, limit)
    ok_post = reachable_sums(parts_post, limit)
    lo, hi = floor(target * 0.9), ceil(target * 1.1)
    expected_post = target * (days_post / max(days_pre + days_post, 1))

    best: tuple[int, int, int, int] | None = None  # (dtotal, dpost, total, post)
    for total in range(max(lo, min(parts_pre)), hi + 1):
        for post in sorted(ok_post):
            pre = total - post
            if pre < 0:
                break
            if post and post < min(parts_post):
                continue
            if pre and pre not in ok_pre:
                continue
            if post == 0 and days_post > 0 and pre == 0:  # pragma: no cover
                continue
            score = (abs(total - target), int(abs(post - expected_post)), total, post)
            if best is None or score[:2] < best[:2]:
                best = score
    if best is None:  # pragma: no cover - guarded by share validation
        raise ValueError(f"no phased split within 10% of {target}")
    _, _, total, post = best
    return total, total - post, post


# --------------------------------------------------------------------------- #
# fraud plan
# --------------------------------------------------------------------------- #


@dataclass(slots=True)
class FraudPlan:
    """Everything the pipeline needs about scheduled fraud."""

    events_by_day: dict[int, list[PendingEvent]] = field(default_factory=dict)
    alloc: dict[str, int] = field(default_factory=dict)
    phase_split: dict[str, tuple[int, int]] = field(default_factory=dict)
    runs: dict[str, int] = field(default_factory=dict)
    n_fraud: int = 0
    customer_pool: int = 0
    agent_pool: int = 0


class _Cursor:
    """Cycling cursor over a shuffled pool that yields distinct batches."""

    def __init__(self, pool: np.ndarray) -> None:
        self.pool = pool
        self.pos = 0

    def take(self, k: int) -> list[int]:
        n = len(self.pool)
        out = [int(self.pool[(self.pos + i) % n]) for i in range(k)]
        self.pos = (self.pos + k) % n
        return out

    def take_distinct(self, k: int) -> list[int]:
        return sorted(set(self.take(k)))


def plan_fraud(config: GeneratorConfig, world: World, n_legit: int, rng: Generator) -> FraudPlan:
    """Schedule every fraud event of the run (pure: opening balances only)."""
    n_fraud, alloc, phase = allocate_budget(n_legit, config, rng)

    # Fraud participants are established wallets: an in-run account must not
    # be the sender of a run scheduled before its creation day.
    customers = np.array(
        [a.idx for a in world.accounts if not a.is_agent and a.created_day < 0],
        dtype=np.int64,
    )
    agents = np.array([a.idx for a in world.accounts if a.is_agent], dtype=np.int64)
    pool_size = min(
        len(customers),
        max(MIN_CUSTOMER_POOL, round(config.fraud_account_share * len(customers))),
    )
    agent_size = min(len(agents), max(MIN_AGENT_POOL, round(AGENT_POOL_SHARE * len(agents))))
    # Prefer funded wallets: a burst that runs out of balance silently loses
    # events and breaks the documented scenario counts.
    funded = [i for i in customers if world.accounts[int(i)].opening_balance >= 150_000]
    general = np.array(funded if len(funded) >= pool_size else customers, dtype=np.int64)
    cust_pool = rng.permutation(general)[:pool_size]
    agent_pool = rng.permutation(agents)[:agent_size]
    # Affluent subjects for the scenarios that send a lot of money (velocity).
    by_balance = sorted(customers, key=lambda i: -world.accounts[int(i)].opening_balance)
    heavy_pool = np.array(by_balance[: max(20, round(0.01 * len(customers)))], dtype=np.int64)
    if pool_size == 0 or len(cust_pool) == 0 or len(agent_pool) == 0:
        raise ValueError("population too small to host fraud participants")

    cur = _Cursor(cust_pool)
    agent_cur = _Cursor(agent_pool)
    heavy_cur = _Cursor(heavy_pool)

    days_pre = config.days if not config.drift.enabled else min(config.days, config.drift.start_day)
    plan = FraudPlan(n_fraud=n_fraud, alloc=alloc, phase_split=phase, customer_pool=pool_size,
                     agent_pool=agent_size)

    for scenario in SCENARIO_ORDER:
        if alloc[scenario] <= 0:
            plan.runs[scenario] = 0
            continue
        pre_target, post_target = phase[scenario]
        day_sizes: list[tuple[int, int]] = []
        if scenario in PHASED and (pre_target or post_target):
            for size in split_sizes(pre_target, PARTS_PRE[scenario], rng):
                day_sizes.append((int(rng.integers(0, max(days_pre, 1))), size))
            for size in split_sizes(post_target, PARTS_POST[scenario], rng):
                day_sizes.append((int(rng.integers(days_pre, config.days)), size))
        else:
            lo_day = 1 if scenario == "SOCIAL_ENG" and config.days > 1 else 0
            for size in split_sizes(alloc[scenario], PARTS_PRE[scenario], rng):
                day_sizes.append((int(rng.integers(lo_day, config.days)), size))
        rng.shuffle(day_sizes)

        run_counter = 0
        for day, size in day_sizes:
            run_counter += 1
            run_id = f"{scenario}-{run_counter:05d}"
            post = config.drift.enabled and day >= config.drift.start_day
            scale = 1.0 + config.drift.intensity if post else 1.0
            maker = _MAKERS[scenario]
            events = maker(config, world, day, size, run_id, scale, cur, agent_cur, heavy_cur,
                           rng, run_counter)
            plan.events_by_day.setdefault(day, []).extend(events)
        plan.runs[scenario] = run_counter

    for day_events in plan.events_by_day.values():
        day_events.sort(key=lambda pe: (pe.ts_ms, pe.run_id))
    return plan


# --------------------------------------------------------------------------- #
# per-scenario makers
# --------------------------------------------------------------------------- #


def _day_ms(config: GeneratorConfig, day: int) -> int:
    return local_day_start_ms(config, day)


def _ts(day_ms: int, hour: int, minute: int, ms: int = 0) -> int:
    return day_ms + hour * _HOUR_MS + minute * _MIN_MS + ms


def _make_velocity(
    config: GeneratorConfig,
    world: World,
    day: int,
    size: int,
    run_id: str,
    scale: float,
    cur: _Cursor,
    agent_cur: _Cursor,
    heavy_cur: _Cursor,
    rng: Generator,
    run_counter: int,
) -> list[PendingEvent]:
    accounts = world.accounts
    subject = heavy_cur.take(1)[0]
    acc = accounts[subject]
    n = size
    d = int(rng.integers(5, 16))
    d = min(d, n)
    recips = cur.take(d)
    rng.shuffle(recips)
    hour = int(rng.integers(6, 22))
    start = _ts(_day_ms(config, day), hour, int(rng.integers(0, 60)), int(rng.integers(0, 60_000)))
    gmax = min(60.0, 460.0 / max(n - 1, 1))
    gaps = rng.uniform(5.0, gmax, size=max(n - 1, 0)) * 1000.0 * scale
    mu = acc.amount_mu["p2p_out"] + log(scale)
    sigma = acc.amount_sigma["p2p_out"]
    out: list[PendingEvent] = []
    ts = start
    for k in range(n):
        out.append(
            PendingEvent(
                ts_ms=int(ts),
                scenario="VELOCITY",
                run_id=run_id,
                txn=TxnType.P2P_SEND,
                sender=subject,
                recipient=recips[k % d],
                mode=MODE_LNORM_RESERVE,
                param=(mu, sigma),
                remaining=n - 1 - k,
            )
        )
        if k < n - 1:
            ts += int(gaps[k])
    return out


def _bounded_gaps(rng: Generator, k: int, total: float, lo: float, hi: float) -> np.ndarray:
    """``k`` gaps in ``[lo, hi]`` minutes summing exactly to ``total``.

    Requires ``lo * k <= total <= hi * k`` (callers guarantee it).
    """
    gaps = np.empty(k, dtype=np.float64)
    remaining = float(total)
    for i in range(k):
        others = k - i - 1
        if others == 0:
            gaps[i] = remaining
            break
        lo_i = max(lo, remaining - hi * others)
        hi_i = min(hi, remaining - lo * others)
        g = float(rng.uniform(lo_i, hi_i))
        gaps[i] = g
        remaining -= g
    return gaps


def _make_structuring(
    config: GeneratorConfig,
    world: World,
    day: int,
    size: int,
    run_id: str,
    scale: float,
    cur: _Cursor,
    agent_cur: _Cursor,
    heavy_cur: _Cursor,
    rng: Generator,
    run_counter: int,
) -> list[PendingEvent]:
    accounts = world.accounts
    subject = cur.take(1)[0]
    benef = cur.take(1)[0]
    bank = world.index_of[f"spec_bank_{int(rng.integers(0, 3)):03d}"]
    n_transfers = size - 1
    tier = accounts[subject].kyc_tier
    limit = config.tier_limits(tier).daily_limit

    us = rng.uniform(0.902, 0.988, size=n_transfers)
    amounts = [int(u * limit) - int(u * limit) % 50 for u in us]
    fees = [fee_for(a) for a in amounts]
    funding = sum(a + f for a, f in zip(amounts, fees, strict=True)) + 2_000

    k_gaps = max(n_transfers - 1, 1)
    span_lo, span_hi = 60.0, min(130.0, 15.3 * max(n_transfers - 1, 1))
    span_lo = max(span_lo, 5.0 * k_gaps)
    total = float(rng.uniform(span_lo, span_hi))
    minutes = _bounded_gaps(rng, k_gaps, total, 5.0, 15.3) * scale
    lead = float(rng.uniform(5.0, 15.3)) * scale  # funding -> first transfer
    hour = int(rng.integers(8, 15))
    start = _ts(_day_ms(config, day), hour, int(rng.integers(0, 60)), int(rng.integers(0, 60_000)))

    out: list[PendingEvent] = [
        PendingEvent(
            ts_ms=start,
            scenario="STRUCTURING",
            run_id=run_id,
            txn=TxnType.BANK_TRANSFER,
            sender=bank,
            recipient=subject,
            mode=MODE_FIXED,
            param=funding,
        )
    ]
    ts = start + int(lead * 60_000)
    for k in range(n_transfers):
        out.append(
            PendingEvent(
                ts_ms=ts,
                scenario="STRUCTURING",
                run_id=run_id,
                txn=TxnType.BANK_TRANSFER,
                sender=subject,
                recipient=benef,
                mode=MODE_FRAC_LIMIT,
                param=float(us[k]),
            )
        )
        if k < n_transfers - 1:
            ts += int(minutes[k] * 60_000)
    return out


def _far_cells(home_cell: int) -> list[int]:
    home = CELLS[home_cell]
    return [
        i
        for i, c in enumerate(CELLS)
        if 50.0 <= haversine_km(home.lat, home.lon, c.lat, c.lon) <= 400.0
    ]


def _make_ato(
    config: GeneratorConfig,
    world: World,
    day: int,
    size: int,
    run_id: str,
    scale: float,
    cur: _Cursor,
    agent_cur: _Cursor,
    heavy_cur: _Cursor,
    rng: Generator,
    run_counter: int,
) -> list[PendingEvent]:
    victim = cur.take(1)[0]
    recipient = cur.take(1)[0]
    dev_src = cur.take(1)[0]
    acc = world.accounts[victim]
    n = size
    remainder = float(rng.uniform(0.05, 0.25))
    frac = 1.0 - remainder ** (1.0 / n)
    far = _far_cells(acc.home_cell)
    cell = far[int(rng.integers(0, len(far)))]
    sim = hash_identifier(config.seed, "sim", f"ato:{run_id}")
    device = world.accounts[dev_src].devices[0]
    hour = int(rng.integers(0, 23))
    start = _ts(_day_ms(config, day), hour, int(rng.integers(0, 31)), int(rng.integers(0, 60_000)))
    gaps = rng.uniform(0.5, 4.0, size=max(n - 1, 1)) * _MIN_MS * scale
    out: list[PendingEvent] = []
    ts = start
    for k in range(n):
        out.append(
            PendingEvent(
                ts_ms=int(ts),
                scenario="ATO_SIMSWAP",
                run_id=run_id,
                txn=TxnType.P2P_SEND,
                sender=victim,
                recipient=recipient,
                mode=MODE_FRAC_BAL,
                param=frac,
                cell_idx=cell,
                set_sim=sim,
                set_device=device,
            )
        )
        if k < n - 1:
            ts += int(gaps[k])
    return out


def _make_mule(
    config: GeneratorConfig,
    world: World,
    day: int,
    size: int,
    run_id: str,
    scale: float,
    cur: _Cursor,
    agent_cur: _Cursor,
    heavy_cur: _Cursor,
    rng: Generator,
    run_counter: int,
) -> list[PendingEvent]:
    mule = cur.take(1)[0]
    k = size - 1
    senders = cur.take(k)
    agent = agent_cur.take(1)[0]
    ring = run_counter == 1  # guarantee at least one ring run per dataset
    ring_device = world.accounts[mule].cur_device
    hour = int(rng.integers(4, 11))
    start = _ts(_day_ms(config, day), hour, int(rng.integers(0, 60)), int(rng.integers(0, 60_000)))
    span_h = float(rng.uniform(3.0, 10.0)) * scale
    span_ms = span_h * _HOUR_MS
    jitter = rng.uniform(0.0, 0.6, size=k) * (span_ms / max(k, 1))
    out: list[PendingEvent] = []
    last_in = start
    for j, sender in enumerate(senders):
        acc = world.accounts[sender]
        ts = int(start + (j * span_ms) / max(k, 1) + jitter[j])
        last_in = max(last_in, ts)
        out.append(
            PendingEvent(
                ts_ms=ts,
                scenario="MULE",
                run_id=run_id,
                txn=TxnType.P2P_SEND,
                sender=sender,
                recipient=mule,
                mode=MODE_LNORM,
                param=(acc.amount_mu["p2p_out"] + log(scale), acc.amount_sigma["p2p_out"]),
                device_once=ring_device if ring else None,
            )
        )
    gap = float(rng.uniform(2.0, 10.0)) * _MIN_MS * scale
    out.append(
        PendingEvent(
            ts_ms=int(last_in + gap),
            scenario="MULE",
            run_id=run_id,
            txn=TxnType.CASH_OUT,
            sender=mule,
            recipient=agent,
            mode=MODE_FRAC_BAL,
            param=float(rng.uniform(0.80, 0.95)),
        )
    )
    return out


def _make_agent(
    config: GeneratorConfig,
    world: World,
    day: int,
    size: int,
    run_id: str,
    scale: float,
    cur: _Cursor,
    agent_cur: _Cursor,
    heavy_cur: _Cursor,
    rng: Generator,
    run_counter: int,
) -> list[PendingEvent]:
    agent = agent_cur.take(1)[0]
    k = size - 1
    colluders = cur.take(k)
    bank = world.index_of[f"spec_bank_{int(rng.integers(0, 3)):03d}"]
    hour = int(rng.integers(8, 15))
    start = _ts(_day_ms(config, day), hour, int(rng.integers(0, 60)), int(rng.integers(0, 60_000)))
    jump = float(rng.uniform(1_500_000, 4_000_000)) * scale
    out: list[PendingEvent] = [
        PendingEvent(
            ts_ms=start,
            scenario="AGENT_FRAUD",
            run_id=run_id,
            txn=TxnType.BANK_TRANSFER,
            sender=bank,
            recipient=agent,
            mode=MODE_FIXED,
            param=int(jump - jump % 50),
        )
    ]
    gap = rng.uniform(1.0, 4.0, size=k) * _MIN_MS * scale
    mu = 10.6 + log(scale)
    for j, sender in enumerate(colluders):
        out.append(
            PendingEvent(
                ts_ms=int(start + (j + 1) * gap[j]),
                scenario="AGENT_FRAUD",
                run_id=run_id,
                txn=TxnType.CASH_IN,
                sender=sender,
                recipient=agent,
                mode=MODE_LNORM,
                param=(mu, 0.9),
            )
        )
    return out


def _make_circular(
    config: GeneratorConfig,
    world: World,
    day: int,
    size: int,
    run_id: str,
    scale: float,
    cur: _Cursor,
    agent_cur: _Cursor,
    heavy_cur: _Cursor,
    rng: Generator,
    run_counter: int,
) -> list[PendingEvent]:
    pairs = [(h, r) for h in range(2, 5) for r in range(2, 7) if h * r == size]
    hops, reps = pairs[int(rng.integers(len(pairs)))]
    # members must be able to fund the cycle
    members: list[int] = []
    for _ in range(20):
        cand = cur.take(1)[0]
        if world.accounts[cand].opening_balance >= CIRC_MIN_OPENING or len(members) >= 3:
            if cand not in members:
                members.append(cand)
        if len(members) == hops:
            break
    while len(members) < hops:
        cand = cur.take(1)[0]
        if cand not in members:
            members.append(cand)
    min_open = min(world.accounts[m].opening_balance for m in members)
    amount = max(10_000, int(0.10 * min_open))
    amount = int(amount * scale) - int(amount * scale) % 50
    hour = int(rng.integers(6, 17))
    start = _ts(_day_ms(config, day), hour, int(rng.integers(0, 60)), int(rng.integers(0, 60_000)))
    rep_gap = float(rng.uniform(35.0, 55.0)) * _MIN_MS * scale
    hop_gap = float(rng.uniform(1.5, 4.0)) * _MIN_MS * scale
    out: list[PendingEvent] = []
    for r in range(reps):
        for h in range(hops):
            ts = int(start + r * rep_gap + h * hop_gap)
            out.append(
                PendingEvent(
                    ts_ms=ts,
                    scenario="CIRCULAR",
                    run_id=run_id,
                    txn=TxnType.P2P_SEND,
                    sender=members[h],
                    recipient=members[(h + 1) % hops],
                    mode=MODE_CYCLE,
                    param=amount,
                    members=tuple(members),
                )
            )
    return out


def _make_social(
    config: GeneratorConfig,
    world: World,
    day: int,
    size: int,
    run_id: str,
    scale: float,
    cur: _Cursor,
    agent_cur: _Cursor,
    heavy_cur: _Cursor,
    rng: Generator,
    run_counter: int,
) -> list[PendingEvent]:
    # Prefer an established, funded sender with history.  The heavy pool is
    # the top 1% by opening balance, so the 5-15x-average transfers are not
    # immediately clamped to a tiny balance (an earlier version picked any
    # established wallet and produced 1-2k transfers instead of 5-15x the
    # sender's usual amount).
    sender = heavy_cur.take(1)[0]
    known = set(world.contacts[sender].peers) | set(world.contacts[sender].merchants)
    # Draw recipients from the whole customer base, not the fraud pool: the
    # pattern needs *first-time* payees, and the pool's 3% of customers is
    # often entirely inside the sender's existing contact set.
    candidates = [a.idx for a in world.accounts if not a.is_agent and a.idx != sender]
    fresh: list[int] = []
    for _ in range(max(60, size * 40)):
        cand = candidates[int(rng.integers(0, len(candidates)))]
        if cand not in known and cand not in fresh and world.accounts[cand].created_day <= day:
            fresh.append(cand)
        if len(fresh) == size:
            break
    if not fresh:  # pragma: no cover - populations always have a stranger
        fresh = [c for c in candidates if world.accounts[c].created_day <= day][:size]
    n_events = min(size, len(fresh))
    hour = int(rng.integers(0, 2))
    start = _ts(_day_ms(config, day), hour, int(rng.integers(0, 60)), int(rng.integers(0, 60_000)))
    span = float(rng.uniform(0.5, 2.5)) * _HOUR_MS * scale  # ends before 05:00 local
    lo, hi = 5.0 * scale, 15.0 * scale
    out: list[PendingEvent] = []
    for j in range(n_events):
        ts = int(start + (j * span) / max(n_events, 1) + rng.uniform(0.0, 0.6) * (span / max(n_events, 1)))
        out.append(
            PendingEvent(
                ts_ms=ts,
                scenario="SOCIAL_ENG",
                run_id=run_id,
                txn=TxnType.P2P_SEND,
                sender=sender,
                recipient=fresh[j % len(fresh)],
                mode=MODE_MULT_HISTORY,
                param=(lo, hi),
                remaining=n_events - 1 - j,
            )
        )
    return out


_MAKERS = {
    "VELOCITY": _make_velocity,
    "STRUCTURING": _make_structuring,
    "ATO_SIMSWAP": _make_ato,
    "MULE": _make_mule,
    "AGENT_FRAUD": _make_agent,
    "CIRCULAR": _make_circular,
    "SOCIAL_ENG": _make_social,
}
