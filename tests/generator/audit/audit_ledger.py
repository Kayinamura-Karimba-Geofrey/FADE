"""Audit 4 - ledger and business invariants.

Rebuilds every wallet balance from the event stream alone and checks that

* ``sender.balance_before`` equals the running balance at that point,
* no wallet ever goes negative (agents' float included),
* fees are deducted and inbound amounts credited per the documented ledger,
* LEGIT events respect the KYC daily limit,
* STRUCTURING transfers stay at 90-99% of the targeted daily limit,
* CASH_IN/CASH_OUT always carry an agent id and channel AGENT,
* there are no self-transfers, unknown counterparties, or agent P2P sends,
* per-account event_time is non-decreasing in ULID order.

Run:  uv run python -m tests.generator.audit.audit_ledger --data data/small
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from generator.behavior.amounts import fee_for
from tests.generator.audit.common import (
    Report,
    data_dir,
    load_accounts,
    load_labels,
    load_manifest,
    run_script,
)

COLUMNS = [
    "event_id",
    "event_time",
    "txn_type",
    "amount",
    "fee",
    "sender.account_id",
    "sender.balance_before",
    "sender.kyc_tier",
    "recipient.account_id",
    "channel",
    "agent_id",
]


def _load(path: Path) -> tuple[pd.DataFrame, dict[str, int], dict[str, int], dict[str, bool]]:
    manifest = load_manifest(path)
    events = pq.read_table(path / "events.parquet", columns=COLUMNS).to_pandas()
    events = events.drop_duplicates("event_id", keep="first")
    events["event_time"] = pd.to_datetime(events["event_time"], utc=True)
    events["local_day"] = (events["event_time"].astype("int64") // 10**9 + 7200) // 86400
    labels = load_labels(path)[["event_id", "label", "scenario", "scenario_run_id"]]
    events = events.merge(labels, on="event_id", how="left", validate="one_to_one")
    events = events.sort_values(["event_time", "event_id"], kind="mergesort").reset_index(drop=True)
    accounts = load_accounts(path)
    opening = dict(zip(accounts["account_id"], accounts["opening_balance"], strict=True))
    opening.update({s["account_id"]: s["opening_balance"] for s in manifest["specials"]})
    agents = dict(zip(accounts["account_id"], accounts["is_agent"], strict=True))
    tiers = dict(zip(accounts["account_id"], accounts["kyc_tier"], strict=True))
    return events, opening, tiers, agents


def audit(path: Path) -> Report:
    report = Report("Audit 4 - ledger and business invariants")
    manifest = load_manifest(path)
    events, opening, tiers, agents = _load(path)
    limits = {int(k): v["daily_limit"] for k, v in manifest["config"]["kyc_tiers"].items()}

    balances = dict(opening)
    daily_spent: dict[tuple[str, int], int] = {}
    mismatch = 0
    negative = 0
    fee_errors = 0
    daily_violations = 0
    self_transfers = 0
    unknown = 0
    agent_p2p = 0
    cash_channel = 0
    examples: list[str] = []

    for row in events.itertuples(index=False):
        sender = row._5  # sender.account_id
        recipient = row._8  # recipient.account_id
        amount = int(row.amount)
        fee = int(row.fee)
        txn = row.txn_type
        if sender == recipient:
            self_transfers += 1
        if sender not in balances or recipient not in balances:
            unknown += 1
            continue
        if txn in ("CASH_IN", "CASH_OUT"):
            if row.channel != "AGENT" or row.agent_id != recipient:
                cash_channel += 1
        if agents.get(sender) and txn == "P2P_SEND":
            agent_p2p += 1
        if fee != fee_for(amount):
            fee_errors += 1

        before = balances[sender]
        if int(row._6) != before:  # sender.balance_before
            mismatch += 1
            if len(examples) < 5:
                examples.append(f"{row.event_time} {row.event_id} {txn} recorded={row._6} replayed={before}")
        if txn == "CASH_IN":
            balances[sender] = before + amount - fee
            balances[recipient] += amount
        else:
            cost = amount + fee
            balances[sender] = before - cost
            if txn == "CASH_OUT":
                balances[recipient] -= amount
            else:
                balances[recipient] += amount
        if balances[sender] < 0 or balances[recipient] < 0:
            negative += 1
            if len(examples) < 5:
                examples.append(f"negative balance {row.event_id}: {txn}")

        if txn != "CASH_IN":
            key = (sender, int(row.local_day))
            spent = daily_spent.get(key, 0)
            if row.label == "LEGIT" and sender in tiers:
                remaining = limits[tiers[sender]] - spent
                if amount + fee > remaining:
                    daily_violations += 1
                    if len(examples) < 5:
                        examples.append(f"daily limit {row.event_id}: {amount + fee} > {remaining}")
            daily_spent[key] = spent + amount + fee

    report.check(
        "balance_before equals the replayed running balance",
        mismatch == 0,
        f"{len(events) - mismatch}/{len(events)} events",
        "0 mismatches",
        detail=f" ({examples[0]})" if examples else "",
    )
    report.check(
        "no wallet balance goes negative",
        negative == 0,
        f"{negative} negative postings",
        "0",
    )
    report.check(
        "fees match the documented schedule",
        fee_errors == 0,
        f"{len(events) - fee_errors}/{len(events)}",
        "0 mismatches",
    )
    report.check(
        "LEGIT amount+fee within the KYC daily limit",
        daily_violations == 0,
        f"{daily_violations} violations",
        "0",
    )
    report.check(
        "no self-transfers",
        self_transfers == 0,
        f"{self_transfers}",
        "0",
    )
    report.check(
        "every counterparty exists",
        unknown == 0,
        f"{unknown} unknown ids",
        "0",
    )
    report.check(
        "CASH_IN/CASH_OUT use channel AGENT with agent_id == recipient",
        cash_channel == 0,
        f"{cash_channel} violations",
        "0",
    )
    report.check(
        "agent accounts never send P2P",
        agent_p2p == 0,
        f"{agent_p2p} agent P2P sends",
        "0",
    )

    # STRUCTURING: each outbound transfer is 90-99% of the subject's limit and
    # one run moves 3-20x that limit.
    struct = events[(events["scenario"] == "STRUCTURING") & (events["txn_type"] == "BANK_TRANSFER")]
    struct = struct[~struct["sender.account_id"].str.startswith("spec_")]
    fractions: list[float] = []
    for row in struct.itertuples(index=False):
        limit = limits[tiers[row._5]]
        fractions.append(int(row.amount) / limit)
    bad_fraction = sum(1 for f in fractions if not 0.899 <= f <= 0.99)
    totals = struct.groupby("scenario_run_id")["amount"].sum()
    bad_totals = 0
    for run_id, total in totals.items():
        first = struct[struct["scenario_run_id"] == run_id].iloc[0]
        limit = limits[tiers[first["sender.account_id"]]]
        if not 3.0 <= float(total) / limit <= 20.0:
            bad_totals += 1
    report.check(
        "STRUCTURING transfers are 90-99% of the targeted limit",
        bad_fraction == 0,
        f"{len(fractions) - bad_fraction}/{len(fractions)} in band",
        "0 outside",
    )
    report.check(
        "STRUCTURING runs move 3-20x the daily limit",
        bad_totals == 0,
        f"{len(totals) - bad_totals}/{len(totals)} runs in band",
        "0 outside",
    )

    # ULID order == event_time order per account (time-sortable ids).
    ordered = events.sort_values(["sender.account_id", "event_id"], kind="mergesort")
    time_ns = ordered["event_time"].astype("int64").to_numpy()
    senders = ordered["sender.account_id"].to_numpy()
    backwards = int(
        sum(
            time_ns[i] < time_ns[i - 1]
            for i in range(1, len(time_ns))
            if senders[i] == senders[i - 1]
        )
    )
    report.check(
        "per-account event_time non-decreasing when sorted by ULID",
        backwards == 0,
        f"{backwards} inversions",
        "0",
    )

    counts = Counter(events["label"].fillna("UNLABELLED"))
    report.check(
        "every event has a label",
        counts.get("UNLABELLED", 0) == 0,
        f"{dict(counts)}",
        "0 unlabelled",
    )
    return report


def test_ledger() -> None:
    report = audit(data_dir())
    assert report.passed, report.render()


if __name__ == "__main__":
    raise SystemExit(run_script("Audit 4 - ledger and business invariants", audit))
