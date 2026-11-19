#!/usr/bin/env python3
"""Turn a replay ``decisions.parquet`` into detection-quality evidence.

The replay command writes one row per (event, config).  This script computes
the operating-point confusion matrices, Wilson confidence intervals, recall at
a 1% false-positive budget, per-scenario recall and segment fairness from those
rows.  Results are written as ``detection.json`` and ``detection.md`` so the
README tables and the release report can quote committed evidence.

Usage:
  python3 scripts/detection_report.py \
      --decisions /tmp/replay-eval-ignore/decisions.parquet \
      --dataset data/eval --out bench/results/2026-11-19/detection.json

A replay window that ends early (crash, timeout) is analysed as-is; the output
records the observed event count and window so the number is never mistaken for
a full-dataset result.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq


def wilson(k: int, n: int, z: float = 1.96) -> list[float]:
    if n == 0:
        return [0.0, 0.0]
    p = k / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return [max(0.0, centre - margin), min(1.0, centre + margin)]


def point(n: int, d: int) -> dict:
    value = (n / d) if d else 0.0
    return {"point": value, "n": n, "d": d, "ci95": wilson(n, d)}


def recall_at_fpr(legit_risk, fraud_risk, budget: float = 0.01) -> dict:
    """Highest recall whose empirical FPR is <= budget, using risk as score."""
    if not fraud_risk:
        return {"point": 0.0, "ci95": [0.0, 0.0], "threshold": None, "fpr": 0.0, "n": 0, "d": 0}
    ordered = sorted(legit_risk)
    # Allow at most floor(budget * |legit|) false positives.
    allowed = int(len(ordered) * budget)
    threshold = None
    if allowed < len(ordered):
        # Risk >= threshold is an alert; pick the threshold that flags <= allowed.
        threshold = ordered[-allowed - 1] if allowed + 1 <= len(ordered) else 0
        if allowed == 0:
            threshold = ordered[-1] + 1
    hits = sum(1 for r in fraud_risk if r >= threshold)
    flagged = sum(1 for r in legit_risk if r >= threshold)
    return {
        "point": hits / len(fraud_risk),
        "ci95": wilson(hits, len(fraud_risk)),
        "threshold": threshold,
        "fpr": flagged / len(ordered) if ordered else 0.0,
        "n": hits,
        "d": len(fraud_risk),
    }


def mcnemar(baseline_alert: dict, combined_alert: dict) -> dict:
    b01 = sum(1 for e in baseline_alert if not baseline_alert[e] and combined_alert.get(e))
    b10 = sum(1 for e in baseline_alert if baseline_alert[e] and not combined_alert.get(e))
    # Exact binomial two-sided p-value.
    n = b01 + b10
    if n == 0:
        p = 1.0
    else:
        k = min(b01, b10)
        p = min(1.0, 2 * sum(math.comb(n, i) for i in range(k + 1)) / (2**n))
    return {"baseline_only": b10, "combined_only": b01, "p_value": p}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--decisions", required=True)
    ap.add_argument("--dataset", default="data/eval")
    ap.add_argument("--out", required=True)
    ap.add_argument("--fpr-budget", type=float, default=0.01)
    args = ap.parse_args()

    table = pq.read_table(args.decisions).to_pandas()
    required = {"config_label", "event_id", "label", "scenario", "segment", "decision", "risk", "event_time_ms"}
    missing = required - set(table.columns)
    if missing:
        raise SystemExit(f"decisions file missing columns: {sorted(missing)}")

    configs: dict[str, dict] = {}
    alerts_by_config: dict[str, dict[str, bool]] = {}
    for name in sorted(table.config_label.unique()):
        rows = table[table.config_label == name]
        fraud = rows[rows.label == "FRAUD"]
        legit = rows[rows.label == "LEGIT"]
        alert = rows.decision != "ALLOW"
        tp = int((alert & (rows.label == "FRAUD")).sum())
        fp = int((alert & (rows.label == "LEGIT")).sum())
        fn = len(fraud) - tp
        tn = len(legit) - fp
        alerts_by_config[name] = dict(zip(rows.event_id, alert, strict=False))
        configs[name] = {
            "events": len(rows),
            "fraud_events": len(fraud),
            "legit_events": len(legit),
            "alerts": int(alert.sum()),
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": tn,
            "recall": point(tp, len(fraud)),
            "fpr": point(fp, len(legit)),
            "precision": point(tp, tp + fp),
            "recall_at_fpr": recall_at_fpr(
                legit.risk.tolist(), fraud.risk.tolist(), args.fpr_budget
            ),
            "per_scenario": {},
            "per_day": {},
        }
        for scenario, group in fraud.groupby("scenario"):
            caught = int((group.decision != "ALLOW").sum())
            configs[name]["per_scenario"][scenario] = point(caught, len(group))
        for day, group in rows.groupby(rows.event_time_ms // 86_400_000):
            f = group[group.label == "FRAUD"]
            configs[name]["per_day"][str(int(day))] = point(
                int((f.decision != "ALLOW").sum()), len(f)
            )

    # Segment fairness: FPR in the worst segment divided by overall FPR.
    fairness = {}
    combined_rows = table[table.config_label == "combined"]
    legit = combined_rows[combined_rows.label == "LEGIT"]
    overall_fpr = (legit.decision != "ALLOW").mean() if len(legit) else 0.0
    for segment, group in legit.groupby("segment"):
        fpr = (group.decision != "ALLOW").mean()
        fairness[segment] = {
            "fpr": fpr,
            "n": len(group),
            "ratio_to_overall": (fpr / overall_fpr) if overall_fpr else None,
        }

    base = configs.get("baseline", {})
    comb = configs.get("combined", {})
    lift = {
        "recall_points": 100
        * (comb.get("recall", {}).get("point", 0) - base.get("recall", {}).get("point", 0))
    }
    if "baseline" in alerts_by_config and "combined" in alerts_by_config:
        mcn = mcnemar(alerts_by_config["baseline"], alerts_by_config["combined"])
    else:
        mcn = {}

    span_start = int(table.event_time_ms.min())
    span_end = int(table.event_time_ms.max())
    manifest = Path(args.dataset) / "manifest.json"
    decisions_hash = hashlib.sha256(Path(args.decisions).read_bytes()).hexdigest()
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "dataset": {
            "name": str(Path(args.dataset)),
            "manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest() if manifest.exists() else None,
            "events_in_window": int(len(table) // max(1, table.config_label.nunique())),
            "fraud_events_in_window": len(table[(table.config_label == "combined") & (table.label == "FRAUD")]),
            "window_start_ms": span_start,
            "window_end_ms": span_end,
            "window_days": round((span_end - span_start) / 86_400_000, 3),
            "sampled": False,
            "note": "Partial replay window: the run was stopped before the full 90-day dataset was replayed.",
        },
        "operating_point": "decision != ALLOW (STEP_UP, HOLD or BLOCK)",
        "decisions_sha256": decisions_hash,
        "command": "python3 scripts/detection_report.py --decisions <replay>/decisions.parquet --dataset data/eval --out bench/results/2026-11-19/detection.json",
        "configs": configs,
        "lift_over_rules_only": lift,
        "fairness": {
            "max_fpr_ratio": max(
                (v["ratio_to_overall"] or 0.0 for v in fairness.values()), default=0.0
            ),
            "segments": fairness,
        },
        "robustness": {
            "cold_start": {
                "recall": None,
                "note": "Segment labels in the evaluation manifest do not identify account age; not measured in this window.",
            }
        },
        "mcnemar_baseline_vs_combined": mcn,
        "fpr_budget": args.fpr_budget,
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2) + "\n")

    md = [
        "# Detection quality (partial replay window)",
        "",
        f"Generated {payload['generated_at']} from `{args.decisions}`.",
        "",
        f"Window: {payload['dataset']['window_days']:.2f} days, "
        f"{payload['dataset']['events_in_window']:,} events, "
        f"{payload['dataset']['fraud_events_in_window']} fraud events.",
        "",
        "| config | events | recall | 95% CI | FPR | precision | recall @ ≤1% FPR |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for name, c in configs.items():
        md.append(
            f"| {name} | {c['events']:,} | {c['recall']['point']:.1%} | "
            f"{c['recall']['ci95'][0]:.1%}–{c['recall']['ci95'][1]:.1%} | "
            f"{c['fpr']['point']:.2%} | {c['precision']['point']:.1%} | "
            f"{c['recall_at_fpr']['point']:.1%} |"
        )
    md += ["", "## Per-scenario recall", ""]
    scenarios = sorted({s for c in configs.values() for s in c["per_scenario"]})
    md.append("| scenario | fraud events | " + " | ".join(configs) + " |")
    md.append("| --- | --- | " + " | ".join("---" for _ in configs) + " |")
    for scenario in scenarios:
        counts = [configs[n]["per_scenario"].get(scenario, {}).get("d", 0) for n in configs]
        cells = []
        for n in configs:
            p = configs[n]["per_scenario"].get(scenario)
            cells.append(
                f"{p['point']:.1%} ({p['n']}/{p['d']}, CI {p['ci95'][0]:.1%}–{p['ci95'][1]:.1%})"
                if p
                else "n/a"
            )
        md.append(f"| {scenario} | {max(counts)} | " + " | ".join(cells) + " |")
    md += [
        "",
        f"Recall lift over rules-only: **{lift['recall_points']:+.1f} points**.",
        f"McNemar baseline vs combined: {mcn.get('combined_only', 0)} newly caught, "
        f"{mcn.get('baseline_only', 0)} lost, p={mcn.get('p_value', 1):.3g}.",
        "",
        f"Worst-segment FPR ratio: **{payload['fairness']['max_fpr_ratio']:.2f}×**.",
        "",
        "_Caveat: this is a partial window (the replay was stopped before the full 90 days). "
        "Confidence intervals are correspondingly wide; treat the numbers as evidence of the "
        "measurement pipeline, not as the final release recall._",
    ]
    Path(out.with_suffix(".md")).write_text("\n".join(md) + "\n")
    print("\n".join(md))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
