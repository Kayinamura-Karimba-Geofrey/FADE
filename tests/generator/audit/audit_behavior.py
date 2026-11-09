"""Audit 5 - legitimate behaviour realism.

The legitimate stream must look like a real mobile-money population, not a
uniform random generator: a diurnal cycle, segment-specific weekend activity,
round amounts, the documented salary/term/restock calendar gates, the fee
schedule, correctly maintained ``first_seen_by_sender`` flags, and a positive
but bounded amount distribution.

Run:  uv run python -m tests.generator.audit.audit_behavior --data data/small
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from generator.behavior.amounts import fee_for
from generator.behavior.calendar import is_salary_window, is_term_window
from tests.generator.audit.common import (
    Report,
    data_dir,
    load_accounts,
    load_events,
    load_labels,
    run_script,
    skip_if_missing,
)


def _local(events: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(events["event_time"], utc=True) + pd.Timedelta(hours=2)


def audit(path: Path) -> Report:
    report = Report("Audit 5 - legitimate behaviour realism")
    events = load_events(path)
    labels = load_labels(path)
    accounts = load_accounts(path)
    events = events.merge(
        labels[["event_id", "label", "scenario"]], on="event_id", how="left", validate="many_to_one"
    ).drop_duplicates("event_id")
    events = events.merge(
        accounts[["account_id", "segment"]].rename(columns={"account_id": "sender.account_id"}),
        on="sender.account_id",
        how="left",
    )
    events["local"] = _local(events)
    events["hour"] = events["local"].dt.hour
    legit = events[events["scenario"].isna() & (events["label"] == "LEGIT")].copy()

    report.check(
        "legitimate events carry the documented fee schedule",
        bool((legit["fee"] == legit["amount"].map(fee_for)).all()),
        f"{int((legit['fee'] == legit['amount'].map(fee_for)).sum())}/{len(legit)}",
        "all events",
    )
    report.check(
        "legitimate amounts are >= 100 RWF and multiples of 50",
        bool((legit["amount"] >= 100).all() and (legit["amount"] % 50 == 0).all()),
        f"min={legit['amount'].min()}, non-multiples={int((legit['amount'] % 50 != 0).sum())}",
        "min >= 100, 0 non-multiples",
    )

    round_shares = {
        txn: float((legit.loc[legit["txn_type"] == txn, "amount"] % 1000 == 0).mean())
        for txn in ("P2P_SEND", "MERCHANT_PAY")
    }
    report.check(
        "P2P and merchant amounts include realistic round-thousand values",
        all(0.25 <= share <= 0.60 for share in round_shares.values()),
        ", ".join(f"{k}={v:.1%}" for k, v in round_shares.items()),
        "each in 25-60%",
    )

    day_share = float(legit["hour"].between(6, 21).mean())
    night_share = float(legit["hour"].isin([0, 1, 2, 3, 4, 5]).mean())
    report.check(
        "activity follows a diurnal cycle",
        day_share >= 0.85 and night_share <= 0.10,
        f"06-21={day_share:.1%}, 00-05={night_share:.1%}",
        ">= 85% day, <= 10% overnight",
    )

    # Weekend uplift for urban youth (per-day rates, not raw counts).
    weekdays = legit["local"].dt.weekday
    youth = legit[legit["segment"] == "urban_youth"]
    youth_weekend = youth[youth["local"].dt.weekday >= 5]
    youth_weekday = youth[youth["local"].dt.weekday < 5]
    all_days = legit["local"].dt.normalize()
    weekend_days = max(int(all_days[weekdays >= 5].nunique()), 1)
    weekday_days = max(int(all_days[weekdays < 5].nunique()), 1)
    weekend_rate = len(youth_weekend) / weekend_days
    weekday_rate = len(youth_weekday) / weekday_days
    ratio = weekend_rate / weekday_rate if weekday_rate else 0.0
    report.check(
        "urban youth are more active per day on weekends",
        ratio >= 1.10,
        f"{ratio:.2f}x weekend/weekday rate",
        ">= 1.10x",
    )

    # Salary credits: bank -> salaried customer, only in the 25-28 window.
    cfg_days = sorted({d.date() for d in legit["local"]})
    has_salary_window = any(is_salary_window(d) for d in cfg_days)
    salary = legit[
        (legit["txn_type"] == "BANK_TRANSFER")
        & legit["sender.account_id"].str.startswith("spec_bank")
        & ~legit["recipient.account_id"].str.startswith("spec_")
    ]
    if has_salary_window:
        outside = int((~salary["local"].dt.date.map(is_salary_window)).sum())
        report.check(
            "salary credits land only on days 25-28",
            outside == 0 and len(salary) > 0,
            f"{len(salary)} salary credits, {outside} outside the window",
            "0 outside, >= 1 credit",
        )
    else:
        report.check(
            "salary credits land only on days 25-28",
            True,
            "no salary window in this date range",
            "0 outside",
        )

    # School fees: large student-parent bills to the school biller only in
    # Jan/May/Sep days 1-10.
    school = legit[
        (legit["segment"] == "student_parent")
        & (legit["txn_type"] == "BILL_PAY")
        & (legit["recipient.account_id"] == "spec_biller_002")
        & (legit["amount"] >= 100_000)
    ]
    if school.empty:
        report.check(
            "large school-fee bills land only in term windows",
            True,
            "no large school bills in this date range",
            "0 outside",
        )
    else:
        outside_school = int((~school["local"].dt.date.map(is_term_window)).sum())
        report.check(
            "large school-fee bills land only in term windows",
            outside_school == 0,
            f"{len(school)} bills, {outside_school} outside Jan/May/Sep 1-10",
            "0 outside",
        )

    # Night restocking: a trader cash-in immediately precedes the bank payment.
    night = legit[
        (legit["segment"] == "small_trader")
        & (legit["txn_type"] == "BANK_TRANSFER")
        & legit["recipient.account_id"].str.startswith("spec_bank")
        & legit["hour"].between(1, 3)
    ]
    ordered = legit.sort_values(["local", "event_id"], kind="mergesort")
    matched = 0
    for row in night.to_dict("records"):
        cash = ordered[
            (ordered["sender.account_id"] == row["sender.account_id"])
            & (ordered["txn_type"] == "CASH_IN")
            & (ordered["amount"] == row["amount"])
            & (ordered["local"] <= row["local"])
            & (ordered["local"] >= row["local"] - pd.Timedelta(minutes=30))
        ]
        if not cash.empty:
            matched += 1
    night_share_matched = matched / len(night) if len(night) else 1.0
    report.check(
        "night restocking is funded by a cash-in minutes earlier",
        night_share_matched >= 0.95,
        f"{matched}/{len(night)} funded transfers",
        ">= 95%",
    )

    # first_seen_by_sender: true exactly for the first (event_time, ULID) pair.
    ordered = events.sort_values(["event_time", "event_id"], kind="mergesort")
    seen: set[tuple[str, str]] = set()
    bad_first_seen = 0
    for sender, recipient, flag in zip(
        ordered["sender.account_id"],
        ordered["recipient.account_id"],
        ordered["recipient.first_seen_by_sender"],
        strict=True,
    ):
        pair = (sender, recipient)
        expected = pair not in seen
        seen.add(pair)
        if bool(flag) != expected:
            bad_first_seen += 1
    report.check(
        "recipient.first_seen_by_sender marks exactly the first transfer per pair",
        bad_first_seen == 0,
        f"{bad_first_seen} wrong flags of {len(events)} events",
        "0",
    )

    report.check(
        "every legitimate event is below the per-event amount ceiling",
        bool((legit["amount"] <= 50_000_000).all()),
        f"max={int(legit['amount'].max())}",
        "<= 50,000,000",
    )
    return report


def test_behavior() -> None:
    path = data_dir()
    skip_if_missing(path)
    report = audit(path)
    assert report.passed, report.render()


if __name__ == "__main__":
    raise SystemExit(run_script("Audit 5 - legitimate behaviour realism", audit))
