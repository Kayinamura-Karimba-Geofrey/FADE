"""Audit 1 - population and reference data.

Checks that ``accounts.parquet`` is a complete, valid description of the
simulated population: every configured wallet exists exactly once, segment
and KYC mixes follow the documented weights, identifiers and hashes are
well-formed and unique, home cells are inside the six documented regions,
opening balances respect the tier caps, agents hold the documented float
range, and account creation days agree with the ages seen in the event
stream.

Run:  uv run python -m tests.generator.audit.audit_population --data data/small
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from generator.population.accounts import (
    AGENT_FLOAT_RANGE,
    CELLS,
    MIN_OPENING,
    REGION_WEIGHTS,
)
from generator.population.segments import SEGMENT_KYC_WEIGHTS, largest_remainder
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

ACCOUNT_ID_RE = re.compile(r"^acct_(\d{6})$")
DEVICE_RE = re.compile(r"^dev_(\d{6})_(\d)$")
MSISDN_RE = re.compile(r"^h_[0-9a-f]{32}$")
SIM_RE = re.compile(r"^sim_[0-9a-f]{32}$")
CELL_IDS = {c.cell_id for c in CELLS}


def audit(path: Path) -> Report:
    report = Report("Audit 1 - population and reference data")
    manifest = load_manifest(path)
    cfg = manifest["config"]
    accounts = load_accounts(path)
    events = load_events(path)
    labels = load_labels(path)

    n_cfg = int(cfg["accounts"])
    n_agents = int(round(n_cfg * cfg["agent_share"]))
    agents = accounts[accounts["is_agent"]]
    customers = accounts[~accounts["is_agent"]]

    report.check(
        "account count and agent count match the config",
        len(accounts) == n_cfg and len(agents) == n_agents,
        f"{len(accounts)} accounts, {len(agents)} agents",
        f"{n_cfg} accounts, {n_agents} agents",
    )

    expected_segments = largest_remainder(len(customers), cfg["segments"])
    actual_segments = customers["segment"].value_counts().to_dict()
    segment_diff = {
        name: (actual_segments.get(name, 0), want)
        for name, want in expected_segments.items()
        if actual_segments.get(name, 0) != want
    }
    report.check(
        "customer segment counts follow the configured shares",
        not segment_diff,
        f"{actual_segments}",
        f"{expected_segments}",
        detail=f" (off: {segment_diff})" if segment_diff else "",
    )

    # KYC mix: compare each segment's tier shares with its documented weights.
    tier_problems: list[str] = []
    for segment, weights in sorted(SEGMENT_KYC_WEIGHTS.items()):
        rows = customers[customers["segment"] == segment]
        if rows.empty:
            tier_problems.append(f"{segment}: no accounts")
            continue
        for tier, weight in weights.items():
            share = float((rows["kyc_tier"] == tier).mean())
            if abs(share - weight) > 0.12:
                tier_problems.append(f"{segment}/tier{tier}: {share:.3f} vs {weight:.2f}")
    report.check(
        "KYC tiers are valid and follow segment weights",
        customers["kyc_tier"].isin([1, 2, 3]).all()
        and agents["kyc_tier"].eq(3).all()
        and not tier_problems,
        "valid" if not tier_problems else "; ".join(tier_problems[:3]),
        "tiers 1-3 within 0.12 of weights; agents tier 3",
    )

    # Identifier formats and uniqueness.
    id_match = accounts["account_id"].map(lambda v: bool(ACCOUNT_ID_RE.match(v)))
    seq_ok = (
        accounts["account_id"].tolist()
        == [f"acct_{i:06d}" for i in range(len(accounts))]
    )
    device_problems = 0
    for idx, devices in enumerate(accounts["devices"]):
        if not 1 <= len(devices) <= 2 or len(set(devices)) != len(devices):
            device_problems += 1
            continue
        for device in devices:
            match = DEVICE_RE.match(device)
            if not match or int(match.group(1)) != idx:
                device_problems += 1
                break
    id_problems = (
        int((~id_match).sum())
        + int(not seq_ok)
        + int(accounts["account_id"].duplicated().any())
        + int(accounts["msisdn_hash"].duplicated().any())
        + int(accounts["sim_serial_hash"].duplicated().any())
        + int(not accounts["msisdn_hash"].map(lambda v: bool(MSISDN_RE.match(v))).all())
        + int(not accounts["sim_serial_hash"].map(lambda v: bool(SIM_RE.match(v))).all())
        + device_problems
    )
    report.check(
        "account, device, msisdn and SIM identifiers are well-formed and unique",
        id_problems == 0,
        f"{id_problems} problems",
        "0",
    )

    # Home cells and regional mix.
    unknown_cells = sorted(set(accounts["home_cell"]) - CELL_IDS)
    regions = accounts["home_cell"].map({c.cell_id: c.region for c in CELLS})
    region_shares = regions.value_counts(normalize=True).to_dict()
    region_diff = {
        region: round(float(region_shares.get(region, 0.0) - weight), 3)
        for region, weight in REGION_WEIGHTS.items()
        if abs(region_shares.get(region, 0.0) - weight) > 0.05
    }
    report.check(
        "every home cell exists and regional shares match the weights",
        not unknown_cells and not region_diff,
        f"unknown={unknown_cells[:3]} diffs={region_diff}",
        "0 unknown, |diff| <= 0.05",
    )

    # Opening balances and agent floats.
    caps = {int(tier): limits["balance_cap"] for tier, limits in cfg["kyc_tiers"].items()}
    customer_caps = customers["kyc_tier"].map(caps)
    bad_balance = int(
        (
            (customers["opening_balance"] < MIN_OPENING)
            | (customers["opening_balance"] > customer_caps)
            | (customers["opening_balance"] % 50 != 0)
        ).sum()
    )
    bad_float = int(
        (
            (agents["opening_balance"] < AGENT_FLOAT_RANGE[0])
            | (agents["opening_balance"] > AGENT_FLOAT_RANGE[1])
        ).sum()
    )
    report.check(
        "opening balances respect tier caps and agent float range",
        bad_balance == 0 and bad_float == 0,
        f"{bad_balance} customer / {bad_float} agent violations",
        "0",
    )

    # Creation days: 70% pre-existing, the rest inside the simulated range.
    created = accounts["created_day"]
    pre_existing = created < 0
    in_run = created >= 0
    pre_share = float(pre_existing.mean())
    bad_created = int(
        (
            (pre_existing & (created < -730))
            | (in_run & (created >= int(cfg["days"])))
        ).sum()
    )
    report.check(
        "created_day is either pre-existing or inside the simulation window",
        bad_created == 0 and 0.65 <= pre_share <= 0.75,
        f"{pre_share:.1%} pre-existing, {bad_created} out of range",
        "65-75% pre-existing, 0 out of range",
    )

    # Event-side ages must equal local day minus created_day and never be negative.
    local = pd.to_datetime(events["event_time"], utc=True) + pd.Timedelta(hours=2)
    start = pd.Timestamp(cfg["start_date"], tz="UTC")
    day = (local.dt.normalize() - start).dt.days
    senders = events[["sender.account_id", "sender.account_age_days"]].copy()
    senders["day"] = day
    senders = senders.merge(
        accounts[["account_id", "created_day", "is_agent"]],
        left_on="sender.account_id",
        right_on="account_id",
        how="left",
    )
    population_events = senders[senders["created_day"].notna()]
    expected_age = population_events["day"] - population_events["created_day"]
    age_mismatch = int((population_events["sender.account_age_days"] != expected_age).sum())
    report.check(
        "sender.account_age_days equals local day minus created_day",
        age_mismatch == 0,
        f"{age_mismatch} mismatches out of {len(population_events)}",
        "0",
    )

    before_creation = int((population_events["day"] < population_events["created_day"]).sum())
    used = set(events["sender.account_id"]) | set(events["recipient.account_id"])
    pre_existing_unused = int(
        ((accounts["created_day"] < 0) & ~accounts["account_id"].isin(used)).sum()
    )
    unused_share = float((~accounts["account_id"].isin(used)).mean())
    report.check(
        "no account acts before creation and every established wallet is exercised",
        before_creation == 0 and pre_existing_unused == 0 and unused_share <= 0.05,
        f"{before_creation} pre-creation events, {pre_existing_unused} established "
        f"unused, {unused_share:.1%} total unused",
        "0 / 0 / <= 5%",
    )

    # Special wallets are exactly the documented system counterparties.
    kinds = pd.Series([s["kind"] for s in manifest["specials"]]).value_counts().to_dict()
    report.check(
        "special wallets are 1 telecom, 8 billers and 3 banks",
        kinds == {"biller": 8, "bank": 3, "telecom": 1},
        f"{kinds}",
        "{telecom: 1, biller: 8, bank: 3}",
    )

    # Labels must only ever mention accounts that exist.
    labelled = labels["event_id"].isin(set(events["event_id"]))
    report.check(
        "every label points at an emitted event",
        bool(labelled.all()) and labels["event_id"].is_unique,
        f"{int(labelled.sum())}/{len(labels)} labels valid",
        "all valid and unique",
    )
    return report


def test_population() -> None:
    path = data_dir()
    skip_if_missing(path)
    report = audit(path)
    assert report.passed, report.render()


if __name__ == "__main__":
    raise SystemExit(run_script("Audit 1 - population and reference data", audit))
