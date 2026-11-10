"""Static contact graph and the special (non-customer) ledger wallets.

Built once per run from the ``population`` RNG stream, so every account
gets a deterministic set of relationships:

* **peers**     - other customers this account exchanges P2P with
* **merchants** - trader customers this account pays (``merchant_out``);
                  the reverse index (``World.merchant_payers``) drives the
                  inbound ``merchant_in`` events of traders
* **agents**    - agents this account may cash in/out at (and the reverse
                  ``World.agent_customers``)
* **billers**   - utility/school wallets this account pays bills to
* **bank**      - the external bank wallet used for salary in and
                  transfers/restocking out

Special wallets (telecom, 8 billers, 3 banks) live in the ledger after the
population accounts; their ids and opening balances are fixed constants so
the dataset manifest describes them exactly.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from numpy.random import Generator

from generator.config import GeneratorConfig
from generator.models import hash_identifier
from generator.population.accounts import Account, SpecialAccount, World

#: Number of special wallets per kind (spec: 1 telecom, several billers,
#: 3 external banks).
N_TELECOM = 1
N_BILLERS = 8
N_BANKS = 3

#: Opening balances of the special wallets (RWF) - large enough that system
#: counterparties never approach zero or an overdraft.
OPENING_TELECOM = 100_000_000_000
OPENING_BILLER = 10_000_000_000
OPENING_BANK = 1_000_000_000_000

BILLER_NAMES: tuple[str, ...] = (
    "electricity",
    "water",
    "school",
    "tv",
    "internet",
    "clinic",
    "insurance",
    "sacco",
)


@dataclass(slots=True)
class Contacts:
    """Static relationships of one population account (ledger indices)."""

    peers: list[int] = field(default_factory=list)
    merchants: list[int] = field(default_factory=list)
    agents: list[int] = field(default_factory=list)
    billers: list[int] = field(default_factory=list)
    bank: int = -1


def _choose_distinct(
    rng: Generator, pool: list[int], k: int, avoid: int = -1
) -> list[int]:
    """Sample ``k`` distinct indices from ``pool`` (excluding ``avoid``).

    Works on positions instead of copying/filtering the pool: the old
    ``[i for i in pool if i != avoid]`` built a 47k-item list for every one
    of 50k accounts, which dominated population build time (Audit 9).
    """
    if k <= 0:
        return []
    if len(pool) <= k:
        return sorted(int(i) for i in pool if i != avoid)
    picks = rng.choice(len(pool), size=min(len(pool), k + 1), replace=False)
    out: list[int] = []
    for pos in np.atleast_1d(picks):
        value = int(pool[int(pos)])
        if value != avoid:
            out.append(value)
        if len(out) == k:
            break
    return sorted(out)


def build_specials(config: GeneratorConfig) -> list[SpecialAccount]:
    """Create the telecom, biller and bank wallets (deterministic ids)."""
    specials: list[SpecialAccount] = []
    idx = config.accounts
    for i in range(N_TELECOM):
        specials.append(
            SpecialAccount(
                idx=idx,
                account_id=f"spec_telecom_{i:03d}",
                kind="telecom",
                name="MTN mobile money float",
                msisdn_hash=hash_identifier(config.seed, "msisdn", f"special:telecom:{i}"),
                opening_balance=OPENING_TELECOM,
            )
        )
        idx += 1
    for i in range(N_BILLERS):
        specials.append(
            SpecialAccount(
                idx=idx,
                account_id=f"spec_biller_{i:03d}",
                kind="biller",
                name=BILLER_NAMES[i],
                msisdn_hash=hash_identifier(config.seed, "msisdn", f"special:biller:{i}"),
                opening_balance=OPENING_BILLER,
            )
        )
        idx += 1
    for i in range(N_BANKS):
        specials.append(
            SpecialAccount(
                idx=idx,
                account_id=f"spec_bank_{i:03d}",
                kind="bank",
                name=f"external bank {i}",
                msisdn_hash=hash_identifier(config.seed, "msisdn", f"special:bank:{i}"),
                opening_balance=OPENING_BANK,
            )
        )
        idx += 1
    return specials


def build_world(config: GeneratorConfig, accounts: list[Account], rng: Generator) -> World:
    """Assemble specials, indices and every contact relationship."""
    specials = build_specials(config)
    n_total = len(accounts) + len(specials)

    index_of = {a.account_id: a.idx for a in accounts}
    index_of.update({s.account_id: s.idx for s in specials})

    region_members: dict[str, list[int]] = {}
    region_customers: dict[str, list[int]] = {}
    region_traders: dict[str, list[int]] = {}
    region_agents: dict[str, list[int]] = {}
    for a in accounts:
        region_members.setdefault(a.region, []).append(a.idx)
        if a.is_agent:
            region_agents.setdefault(a.region, []).append(a.idx)
        elif a.created_day < 0:
            # Only pre-existing wallets may be counterparties: an in-run
            # account must not receive or send an event before its
            # created_day (the old graph allowed it and produced negative
            # account_age_days, rejected by the strict schema audit).
            region_customers.setdefault(a.region, []).append(a.idx)
            if a.segment == "small_trader":
                region_traders.setdefault(a.region, []).append(a.idx)

    customers = [a.idx for a in accounts if not a.is_agent and a.created_day < 0]
    agents = [a.idx for a in accounts if a.is_agent]
    traders = [
        a.idx for a in accounts if not a.is_agent and a.created_day < 0 and a.segment == "small_trader"
    ]
    if not traders:  # tiny fixtures may draw no traders - fall back to customers
        traders = list(customers)
        region_traders = {k: list(v) for k, v in region_customers.items()}
    if not customers:
        raise ValueError("population has no pre-existing customer wallets")
    if not agents:
        raise ValueError("population has no agents; raise agent_share")

    biller_specials = [s.idx for s in specials if s.kind == "biller"]
    bank_specials = [s.idx for s in specials if s.kind == "bank"]

    merchant_payers: dict[int, list[int]] = {t: [] for t in traders}
    contacts: dict[int, Contacts] = {}
    customer_agents: dict[int, list[int]] = {}
    agent_customers: dict[int, list[int]] = {a: [] for a in agents}

    for a in accounts:
        if a.is_agent:
            continue

        # peers: 3-10 other customers, mostly same region
        n_peers = int(rng.integers(3, 11))
        local = region_customers.get(a.region, customers)
        if rng.random() < 0.60 and local:
            n_local = min(n_peers, max(0, len(local) - 1))
            peers = _choose_distinct(rng, local, n_local, avoid=a.idx)
            if len(peers) < n_peers:
                peers += _choose_distinct(rng, customers, n_peers - len(peers), avoid=a.idx)
                peers = sorted(set(peers))
        else:
            peers = _choose_distinct(rng, customers, n_peers, avoid=a.idx)

        # merchants: 2-5 traders, 70% same region
        n_merch = int(rng.integers(2, 6))
        traders_local = region_traders.get(a.region, traders)
        if rng.random() < 0.70 and traders_local:
            n_local = min(n_merch, max(0, len(traders_local) - 1))
            merchants = _choose_distinct(rng, traders_local, n_local, avoid=a.idx)
            if len(merchants) < n_merch:
                merchants += _choose_distinct(rng, traders, n_merch - len(merchants), avoid=a.idx)
                merchants = sorted(set(merchants))
        else:
            merchants = _choose_distinct(rng, traders, n_merch, avoid=a.idx)
        if a.created_day < 0:
            for m in merchants:
                merchant_payers[m].append(a.idx)

        # attached agents: 1-3, 70% same region
        n_ag = int(rng.integers(1, 4))
        agents_local = region_agents.get(a.region, agents)
        if rng.random() < 0.70 and agents_local:
            n_local = min(n_ag, len(agents_local))
            attached = _choose_distinct(rng, agents_local, n_local)
            if len(attached) < n_ag:
                attached += _choose_distinct(rng, agents, n_ag - len(attached))
                attached = sorted(set(attached))
        else:
            attached = _choose_distinct(rng, agents, n_ag)

        # billers: 2-4 of the 8; one bank: employer / supplier account
        n_bill = int(rng.integers(2, 5))
        billers = _choose_distinct(rng, biller_specials, n_bill)
        bank = int(rng.choice(bank_specials))

        contacts[a.idx] = Contacts(
            peers=peers,
            merchants=merchants,
            agents=attached,
            billers=billers,
            bank=bank,
        )
        customer_agents[a.idx] = attached
        for ag in attached:
            agent_customers[ag].append(a.idx)

    # Agents need a real bank wallet for float sweeps (``bank_out``) and a
    # biller list for their own bills.  Leaving ``bank`` at the -1 default
    # made Python's negative indexing credit the last ledger entry (a bank
    # wallet) instead of an agent's bank - a silent ledger corruption found
    # by the Audit 4 replay.
    for a in accounts:
        if a.is_agent:
            contacts[a.idx] = Contacts(
                billers=_choose_distinct(rng, biller_specials, 2),
                bank=int(rng.choice(bank_specials)),
            )

    world = World(
        accounts=accounts,
        specials=specials,
        index_of=index_of,
        region_members=region_members,
        merchant_payers=merchant_payers,
        customer_agents=customer_agents,
        agent_customers=agent_customers,
        contacts=contacts,
    )
    assert len(index_of) == n_total  # noqa: S101 - index integrity
    return world


def salary_bank_for(config: GeneratorConfig, contacts_bank: int) -> int:
    """Deterministically map an account's bank contact (kept for symmetry)."""
    return contacts_bank
