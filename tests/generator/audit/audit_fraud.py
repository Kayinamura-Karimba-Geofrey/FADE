"""Audit 6 - fraud scenario realism.

Every one of the seven scenarios must be present and each injected run must
show its documented micro-pattern, not merely carry a FRAUD label:

* VELOCITY       - 10-30 P2P sends by one account within ~8 minutes,
* STRUCTURING    - one funding credit, then 90-99% of the daily limit moved
                   to a single beneficiary 3-20x over,
* ATO_SIMSWAP    - a handful of sequential drains from a far cell with a new
                   SIM and a device the victim never used,
* MULE           - many distinct senders into one wallet, then one cash-out,
* AGENT_FRAUD    - a bank float jump to an agent followed by collusive cash-ins,
* CIRCULAR       - equal amounts cycling through 2-4 wallets 2-6 times,
* SOCIAL_ENG     - 5-15x the sender's usual amount to fresh payees at night.

Run:  uv run python -m tests.generator.audit.audit_fraud --data data/run42
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from generator.fraud.scenarios import CIRC_SIZES
from generator.population.accounts import CELLS, haversine_km
from tests.generator.audit.common import (
    Report,
    data_dir,
    load_accounts,
    load_events,
    load_labels,
    load_manifest,
    run_script,
    skip_if_missing,
)

LOCAL_OFFSET = pd.Timedelta(hours=2)
CELL_BY_ID = {c.cell_id: c for c in CELLS}


def _limit(config: dict, tier: int) -> int:
    return int(config["kyc_tiers"][str(tier)]["daily_limit"])


def _check_velocity(run: pd.DataFrame) -> str | None:
    if not 10 <= len(run) <= 30:
        return f"{len(run)} events"
    if run["sender.account_id"].nunique() != 1:
        return "not a single sender"
    if not (run["txn_type"] == "P2P_SEND").all():
        return "non-P2P event"
    if run["recipient.account_id"].nunique() < 2:
        return "fewer than 2 recipients"
    span = (run["event_time"].max() - run["event_time"].min()).total_seconds()
    if span > 600:
        return f"span {span:.0f}s"
    if bool(run["sender_is_agent"].any()):
        return "agent sender"
    return None


def _check_structuring(run: pd.DataFrame, config: dict) -> str | None:
    if not 6 <= len(run) <= 10:
        return f"{len(run)} events"
    if not (run["txn_type"] == "BANK_TRANSFER").all():
        return "non-BANK_TRANSFER event"
    funding = run[run["sender.account_id"].str.startswith("spec_bank")]
    transfers = run[~run["sender.account_id"].str.startswith("spec_bank")]
    if len(funding) != 1:
        return f"{len(funding)} funding credits"
    if len(transfers) != len(run) - 1:
        return "unexpected event split"
    if transfers["sender.account_id"].nunique() != 1:
        return "multiple subjects"
    if transfers["recipient.account_id"].nunique() != 1:
        return "multiple beneficiaries"
    if funding.iloc[0]["recipient.account_id"] != transfers.iloc[0]["sender.account_id"]:
        return "funding does not reach the subject"
    if funding.iloc[0]["event_time"] >= transfers["event_time"].min():
        return "transfers start before funding"
    limit = _limit(config, int(transfers.iloc[0]["acct_kyc_tier"]))
    fractions = transfers["amount"] / limit
    if not ((fractions >= 0.899) & (fractions <= 0.99)).all():
        return f"fraction out of band ({fractions.min():.3f}-{fractions.max():.3f})"
    total = float(transfers["amount"].sum()) / limit
    if not 3.0 <= total <= 20.0:
        return f"{total:.1f}x limit"
    if float(funding.iloc[0]["amount"]) < float(
        (transfers["amount"] + transfers["fee"]).sum()
    ):
        return "funding below transfer costs"
    return None


def _check_ato(run: pd.DataFrame, accounts: pd.DataFrame) -> str | None:
    if not 2 <= len(run) <= 7:
        return f"{len(run)} events"
    if run["sender.account_id"].nunique() != 1:
        return "multiple victims"
    if run["recipient.account_id"].nunique() != 1:
        return "multiple recipients"
    if not (run["txn_type"] == "P2P_SEND").all():
        return "non-P2P event"
    if run["sender.geo.cell_id"].nunique() != 1:
        return "geo moves during the drain"
    victim = run.iloc[0]["sender.account_id"]
    account = accounts[accounts["account_id"] == victim].iloc[0]
    cell = CELL_BY_ID[run.iloc[0]["sender.geo.cell_id"]]
    home = CELL_BY_ID[account["home_cell"]]
    km = haversine_km(home.lat, home.lon, cell.lat, cell.lon)
    if km < 50.0:
        return f"only {km:.0f} km from home"
    if (run["sender.sim_serial_hash"] == account["sim_serial_hash"]).any():
        return "victim's original SIM still used"
    if run["sender.device_id"].isin(account["devices"]).any():
        return "victim's own device used"
    if run["amount"].iloc[-1] >= run["amount"].iloc[0]:
        return "amounts do not drain"
    return None


def _check_mule(run: pd.DataFrame, accounts: pd.DataFrame) -> str | None:
    if not 9 <= len(run) <= 41:
        return f"{len(run)} events"
    cash = run[run["txn_type"] == "CASH_OUT"]
    inbound = run[run["txn_type"] == "P2P_SEND"]
    if len(cash) != 1:
        return f"{len(cash)} cash-outs"
    if len(inbound) < 8:
        return f"{len(inbound)} inbounds"
    mule = cash.iloc[0]["sender.account_id"]
    if not (inbound["recipient.account_id"] == mule).all():
        return "inbound not all to the mule"
    if inbound["sender.account_id"].nunique() != len(inbound):
        return "duplicate inbound senders"
    if not bool(cash.iloc[0]["recipient.is_agent"]):
        return "cash-out not at an agent"
    if cash.iloc[0]["event_time"] <= inbound["event_time"].max():
        return "cash-out before last inbound"
    run_id = str(run.iloc[0]["scenario_run_id"])
    if run_id.endswith("-00001"):
        devices = set(inbound["sender.device_id"])
        if len(devices) != 1:
            return "ring run does not share one device"
        device = next(iter(devices))
        for row in inbound.to_dict("records"):
            owner = accounts[accounts["account_id"] == row["sender.account_id"]].iloc[0]
            if device in owner["devices"]:
                return "ring device belongs to an inbound sender"
    return None


def _check_agent(run: pd.DataFrame) -> str | None:
    if not 7 <= len(run) <= 16:
        return f"{len(run)} events"
    cash = run[run["txn_type"] == "CASH_IN"]
    bank = run[run["txn_type"] == "BANK_TRANSFER"]
    if len(bank) != 1:
        return f"{len(bank)} float transfers"
    if len(cash) != len(run) - 1:
        return "unexpected event split"
    agent = bank.iloc[0]["recipient.account_id"]
    if not bool(bank.iloc[0]["recipient.is_agent"]):
        return "float jump does not target an agent"
    if float(bank.iloc[0]["amount"]) < 1_500_000:
        return "float jump below 1.5M"
    if not (cash["recipient.account_id"] == agent).all():
        return "cash-ins target another agent"
    if not bool(cash["recipient.is_agent"].all()):
        return "cash-in recipient not an agent"
    if bank.iloc[0]["event_time"] > cash["event_time"].min():
        return "cash-ins before the float jump"
    return None


def _check_circular(run: pd.DataFrame) -> str | None:
    if len(run) not in CIRC_SIZES:
        return f"{len(run)} events is not hops x reps"
    if not (run["txn_type"] == "P2P_SEND").all():
        return "non-P2P event"
    members = sorted(set(run["sender.account_id"]) | set(run["recipient.account_id"]))
    if not 2 <= len(members) <= 4:
        return f"{len(members)} members"
    if len(run) % len(members) != 0:
        return "uneven cycle"
    reps = len(run) // len(members)
    if not 2 <= reps <= 6:
        return f"{reps} repetitions"
    if run["amount"].nunique() != 1:
        return "amounts differ inside the cycle"
    edges = run.groupby("sender.account_id")["recipient.account_id"].nunique()
    if not bool((edges == 1).all()) or len(edges) != len(members):
        return "not a single cycle"
    successor = {
        row["sender.account_id"]: row["recipient.account_id"] for row in run.to_dict("records")
    }
    current = members[0]
    visited = [current]
    for _ in range(len(members) - 1):
        current = successor[current]
        visited.append(current)
    if successor[current] != members[0] or sorted(visited) != members:
        return "cycle does not close through every member"
    return None


def _check_social(run: pd.DataFrame, history: pd.DataFrame) -> str | None:
    if not 2 <= len(run) <= 20:
        return f"{len(run)} events"
    if run["sender.account_id"].nunique() != 1:
        return "multiple senders"
    if not (run["txn_type"] == "P2P_SEND").all():
        return "non-P2P event"
    if run["recipient.account_id"].nunique() != len(run):
        return "payees repeat"
    if not bool(run["recipient.first_seen_by_sender"].all()):
        return "payee already known"
    hours = run["event_time"].dt.hour
    if not bool(hours.between(0, 5).all()):
        return "not at night"
    sender = run.iloc[0]["sender.account_id"]
    prior = history[(history["sender.account_id"] == sender)].sort_values("event_time")
    ratios: list[float] = []
    for row in run.to_dict("records"):
        window = prior[
            (prior["event_time"] >= row["event_time"] - pd.Timedelta(days=30))
            & (prior["event_time"] < row["event_time"])
        ]
        if window.empty:
            continue
        ratios.append(float(row["amount"]) / float(window["amount"].mean()))
    if not ratios:
        return None
    inside = sum(1 for r in ratios if 5.0 <= r <= 15.0)
    if inside / len(ratios) < 0.80:
        return f"{inside}/{len(ratios)} amounts in 5-15x band"
    return None


def audit(path: Path) -> Report:
    report = Report("Audit 6 - fraud scenario realism")
    manifest = load_manifest(path)
    config = manifest["config"]
    events = load_events(path)
    labels = load_labels(path)
    accounts = load_accounts(path)
    events = events.merge(
        labels[["event_id", "label", "scenario", "scenario_run_id"]],
        on="event_id",
        how="left",
        validate="many_to_one",
    ).drop_duplicates("event_id")
    events = events.merge(
        accounts[["account_id", "kyc_tier", "home_cell", "sim_serial_hash", "devices", "is_agent"]]
        .rename(
            columns={
                "account_id": "sender.account_id",
                "kyc_tier": "acct_kyc_tier",
                "home_cell": "acct_home_cell",
                "sim_serial_hash": "acct_sim",
                "devices": "acct_devices",
                "is_agent": "sender_is_agent",
            }
        ),
        on="sender.account_id",
        how="left",
    )
    events["event_time"] = pd.to_datetime(events["event_time"], utc=True)
    events["local"] = events["event_time"] + LOCAL_OFFSET
    events["event_time"] = events["local"]
    fraud = events[
        events["scenario"].notna() & (events["scenario"] != "LABEL_NOISE")
    ].copy()

    present = sorted(fraud["scenario"].unique())
    expected = sorted(manifest["fraud"]["alloc"])
    report.check(
        "all seven scenarios are present",
        present == expected and all(manifest["fraud"]["runs"].get(s, 0) >= 1 for s in expected),
        f"{present}",
        f"{expected}",
    )

    alloc = manifest["fraud"]["alloc"]
    actual = {s: int((fraud["scenario"] == s).sum()) for s in expected}
    count_problems = {
        s: (actual[s], alloc[s])
        for s in expected
        if alloc[s] and abs(actual[s] - alloc[s]) / alloc[s] > 0.10
    }
    report.check(
        "scenario event counts are within 10% of the fraud budget",
        not count_problems,
        f"{actual}",
        f"alloc {alloc}",
        detail=f" (off: {count_problems})" if count_problems else "",
    )

    checkers = {
        "VELOCITY": lambda run, hist: _check_velocity(run),
        "STRUCTURING": lambda run, hist: _check_structuring(run, config),
        "ATO_SIMSWAP": lambda run, hist: _check_ato(run, accounts),
        "MULE": lambda run, hist: _check_mule(run, accounts),
        "AGENT_FRAUD": lambda run, hist: _check_agent(run),
        "CIRCULAR": lambda run, hist: _check_circular(run),
        "SOCIAL_ENG": lambda run, hist: _check_social(run, hist),
    }
    for scenario in expected:
        runs = fraud[fraud["scenario"] == scenario]
        if runs.empty:
            report.check(f"{scenario} run pattern", False, "0 runs", ">= 1 run")
            continue
        problems: list[str] = []
        for run_id, run in runs.groupby("scenario_run_id", sort=True):
            problem = checkers[scenario](run.sort_values("event_time"), events)
            if problem:
                problems.append(f"{run_id}: {problem}")
        report.check(
            f"{scenario} run pattern",
            not problems,
            f"{runs['scenario_run_id'].nunique()} runs, {len(runs)} events",
            "every run matches the documented pattern",
            detail=f" ({problems[0]})" if problems else "",
        )

    # Statistical checks only when the dataset is large enough to measure them.
    if int(config["accounts"]) >= 10_000 and int(config["days"]) >= 30:
        rate = float(config["fraud_rate"])
        measured = len(fraud) / max(len(events[events["scenario"].isna()]), 1)
        report.check(
            "realised fraud rate is within 10% relative of config",
            abs(measured - rate) / rate <= 0.10,
            f"{measured:.4%}",
            f"{rate:.4%} +/-10%",
        )
        custom = accounts[~accounts["is_agent"]]
        touched = set(fraud["sender.account_id"]) | set(fraud["recipient.account_id"])
        touched &= set(custom["account_id"])
        share = len(touched) / len(custom)
        target = float(config["fraud_account_share"])
        report.check(
            "fraud-touched customer share is within 20% relative of config",
            abs(share - target) / target <= 0.20,
            f"{share:.3%} ({len(touched)}/{len(custom)})",
            f"{target:.3%} +/-20%",
        )
    else:
        report.check(
            "fraud rate / touched-account statistics",
            True,
            "n/a on fixture dataset (requires >=10k accounts, >=30 days)",
            "run42 scale",
        )
    return report


def test_fraud() -> None:
    path = data_dir()
    skip_if_missing(path)
    report = audit(path)
    assert report.passed, report.render()


if __name__ == "__main__":
    raise SystemExit(run_script("Audit 6 - fraud scenario realism", audit))
