"""Audit 7 - labels, label noise and leakage.

``labels.parquet`` is the training ground truth, so it must be complete,
unique and self-consistent: one row per unique event, FRAUD rows carry one
of the seven scenarios and a run id, LEGIT rows carry neither, the realised
fraud rate matches the config (also when label noise is enabled), and no
label information ever appears inside ``events.jsonl.zst``.

Run:  uv run python -m tests.generator.audit.audit_labels --data data/run42
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path

from generator.config import SCENARIO_NAMES
from tests.generator.audit.common import (
    Report,
    data_dir,
    iter_jsonl,
    load_events,
    load_labels,
    load_manifest,
    run_script,
    skip_if_missing,
)

LABEL_KEYS = {"label", "scenario", "scenario_run_id", "fraud", "is_fraud"}
EVENT_KEYS = {
    "event_id",
    "event_time",
    "txn_type",
    "amount",
    "currency",
    "fee",
    "sender",
    "recipient",
    "channel",
    "agent_id",
    "session_id",
}


def audit(path: Path) -> Report:
    report = Report("Audit 7 - labels, label noise and leakage")
    manifest = load_manifest(path)
    config = manifest["config"]
    events = load_events(path)
    labels = load_labels(path)
    unique = events.drop_duplicates("event_id")
    merged = unique.merge(labels, on="event_id", how="left", validate="one_to_one")

    report.check(
        "labels cover every unique event exactly once",
        len(labels) == len(unique)
        and labels["event_id"].is_unique
        and merged["label"].notna().all(),
        f"{len(labels)} labels / {len(unique)} unique events",
        "equal, unique, none missing",
    )
    report.check(
        "label values and sources are valid",
        set(labels["label"]) <= {"LEGIT", "FRAUD"}
        and bool((labels["source"] == "GENERATOR").all()),
        f"values={sorted(set(labels['label']))}, sources={sorted(set(labels['source']))}",
        "LEGIT/FRAUD, source GENERATOR",
    )

    fraud = labels[labels["label"] == "FRAUD"]
    legit = labels[labels["label"] == "LEGIT"]
    bad_scenario = fraud["scenario"].isna() | ~fraud["scenario"].isin(
        [*SCENARIO_NAMES, "LABEL_NOISE"]
    )
    legit_has_scenario = legit["scenario"].notna()
    report.check(
        "FRAUD rows carry a known scenario and LEGIT rows carry none",
        not bad_scenario.any() and not legit_has_scenario.any(),
        f"{int(bad_scenario.sum())} bad fraud scenarios, "
        f"{int(legit_has_scenario.sum())} legit scenarios",
        "0 / 0",
    )

    scenario_rows = fraud[fraud["scenario"] != "LABEL_NOISE"]
    run_missing = scenario_rows["scenario_run_id"].isna()
    run_unexpected = fraud[fraud["scenario"] == "LABEL_NOISE"]["scenario_run_id"].notna()
    report.check(
        "scenario_run_id is set for every injected fraud run and only there",
        not run_missing.any() and not run_unexpected.any(),
        f"{int(run_missing.sum())} missing, {int(run_unexpected.sum())} unexpected",
        "0 / 0",
    )

    prefix_bad = 0
    run_sizes: list[int] = []
    for run_id, group in scenario_rows.groupby("scenario_run_id", sort=True):
        scenario = str(group["scenario"].iloc[0])
        expected_prefix = f"{scenario}-"
        if not str(run_id).startswith(expected_prefix):
            prefix_bad += 1
        run_sizes.append(len(group))
    report.check(
        "run ids are named after their scenario and every run has >= 2 events",
        prefix_bad == 0 and bool(run_sizes) and min(run_sizes) >= 2,
        f"{len(run_sizes)} runs, sizes {min(run_sizes) if run_sizes else 0}-"
        f"{max(run_sizes) if run_sizes else 0}",
        "prefix matches, every run >= 2",
    )

    # Realised label distribution vs the configured rate, including label noise.
    rate = float(config["fraud_rate"])
    noise = float(config["label_noise"])
    expected = (1.0 - noise) * rate + noise * (1.0 - rate)
    measured = len(fraud) / len(labels)
    report.check(
        "realised FRAUD share matches config (accounting for label noise)",
        abs(measured - expected) / expected <= 0.10 if expected else measured == 0,
        f"{measured:.4%}",
        f"{expected:.4%} +/-10%",
    )

    if noise > 0:
        noisy = int((labels["scenario"] == "LABEL_NOISE").sum())
        expected_noisy = noise * len(labels) * (1.0 - rate)
        report.check(
            "label-noise flips are present at the configured rate",
            abs(noisy - expected_noisy) / expected_noisy <= 0.20 if expected_noisy else noisy == 0,
            f"{noisy} noisy rows",
            f"~{expected_noisy:.0f} +/-20%",
        )
    else:
        report.check(
            "label_noise = 0 leaves no LABEL_NOISE rows",
            not bool((labels["scenario"] == "LABEL_NOISE").any()),
            f"{int((labels['scenario'] == 'LABEL_NOISE').sum())} noisy rows",
            "0",
        )

    # Leakage: labels must never be part of the event stream.
    leaked = 0
    wrong_keys = 0
    scanned = 0
    for obj in iter_jsonl(path):
        scanned += 1
        keys = set(obj)
        if keys & LABEL_KEYS:
            leaked += 1
        if keys != EVENT_KEYS:
            wrong_keys += 1
        if leaked or wrong_keys:
            break
    report.check(
        "events.jsonl.zst never contains label or scenario fields",
        leaked == 0 and wrong_keys == 0,
        f"{scanned} lines scanned, {leaked} leaked, {wrong_keys} key mismatches",
        "0 leaked, 0 mismatches",
    )

    counts = Counter(labels["label"])
    report.check(
        "both label classes are present",
        counts.get("FRAUD", 0) > 0 and counts.get("LEGIT", 0) > 0,
        f"{dict(counts)}",
        "both > 0",
    )
    return report


def test_labels() -> None:
    path = data_dir()
    skip_if_missing(path)
    report = audit(path)
    assert report.passed, report.render()


if __name__ == "__main__":
    raise SystemExit(run_script("Audit 7 - labels, label noise and leakage", audit))
