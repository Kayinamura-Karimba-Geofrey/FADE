#!/usr/bin/env python3
"""Independent cross-check of a FADE replay report from its decision parquet.

Usage: python tests/replay/crosscheck.py JOB_DIR

Recomputes counts and rates with pandas/numpy and compares them with
report.json. Counts must match exactly and rates within 1e-9. Confidence
intervals use an independent NumPy bootstrap, so they are compared with a
1e-2 tolerance rather than the Go RNG sequence.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: crosscheck.py JOB_DIR", file=sys.stderr)
        return 2
    job = Path(sys.argv[1])
    report = json.loads((job / "report.json").read_text())
    df = pd.read_parquet(job / "decisions.parquet")
    failures: list[str] = []

    def close(name: str, got: float, want: float, tol: float = 1e-9) -> None:
        if abs(got - want) > tol:
            failures.append(f"{name}: go={want:.12f} py={got:.12f}")

    for label, m in report["summary"].items():
        sub = df[df.config_label == label]
        fraud = sub[sub.label == "FRAUD"]
        legit = sub[sub.label == "LEGIT"]
        alerted = sub.decision.isin(["STEP_UP", "HOLD", "BLOCK"])
        intervened = sub.decision.isin(["HOLD", "BLOCK"])
        fraud_alerted = fraud.decision.isin(["STEP_UP", "HOLD", "BLOCK"])
        fraud_intervened = fraud.decision.isin(["HOLD", "BLOCK"])
        legit_alerted = legit.decision.isin(["STEP_UP", "HOLD", "BLOCK"])
        legit_intervened = legit.decision.isin(["HOLD", "BLOCK"])

        if len(sub) != m["scored"]:
            failures.append(f"{label} scored: go={m['scored']} py={len(sub)}")
        if len(fraud) != m["fraud"] or len(legit) != m["legit"]:
            failures.append(f"{label} label counts differ")
        close(f"{label} recall", fraud_alerted.mean(), m["estimates"]["fraud_caught_rate"]["point"])
        close(f"{label} recall_intervened", fraud_intervened.mean(),
              m["estimates"]["fraud_intervened_rate"]["point"])
        close(f"{label} fpr", legit_alerted.mean(), m["estimates"]["false_positive_rate"]["point"])
        close(f"{label} friction", alerted.mean(), m["estimates"]["customer_friction"]["point"])
        close(f"{label} precision",
              fraud_alerted.sum() / max(1, fraud_alerted.sum() + legit_alerted.sum()),
              m["estimates"]["precision_alerted"]["point"])
        saved = int(fraud.loc[fraud_intervened, "amount"].sum())
        if saved != m["amounts"]["FRAUD:INTERVENED"]:
            failures.append(f"{label} amount saved: go={m['amounts']['FRAUD:INTERVENED']} py={saved}")
        missed = int(fraud.loc[~fraud_intervened, "amount"].sum())
        if missed != m["amounts"]["FRAUD:MISSED"]:
            failures.append(f"{label} amount missed: go={m['amounts']['FRAUD:MISSED']} py={missed}")

        # Independent bootstrap of recall over accounts (1,000 resamples).
        acc = fraud.assign(hit=fraud_alerted).groupby("account_id").agg(
            fraud=("hit", "size"), hit=("hit", "sum"))
        if len(acc):
            rng = np.random.default_rng(11)
            idx = rng.integers(0, len(acc), size=(1000, len(acc)))
            vals = np.array([acc.hit.to_numpy()[i].sum() / acc.fraud.to_numpy()[i].sum()
                             for i in idx])
            lo, hi = np.quantile(vals, [0.025, 0.975])
            go = m["estimates"]["fraud_caught_rate"]
            if abs(lo - go["lo"]) > 0.02 or abs(hi - go["hi"]) > 0.02:
                failures.append(f"{label} recall CI differs: go=[{go['lo']:.3f},{go['hi']:.3f}] "
                                f"py=[{lo:.3f},{hi:.3f}]")

        for scen in report["scenarios"][label]:
            s = fraud[fraud.scenario == scen["scenario"]]
            close(f"{label} scenario {scen['scenario']} recall",
                  (s.decision.isin(["STEP_UP", "HOLD", "BLOCK"])).mean(),
                  scen["recall_alerted"])

        for rule in report["rules"][label]:
            fired = sub.fired_rules.str.split(",").apply(lambda xs: rule["rule_id"] in xs)
            if int(fired.sum()) != rule["fires"]:
                failures.append(f"{label} rule {rule['rule_id']} fires differ")

    if failures:
        print("crosscheck FAILED")
        for f in failures:
            print(" -", f)
        return 1
    print(f"crosscheck OK: {len(report['summary'])} configs, {len(df)} decision rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
