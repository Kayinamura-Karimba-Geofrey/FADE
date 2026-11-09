"""Dataset summary: events, accounts, scenarios, fraud rate, noise rates."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _pct(x: float) -> str:
    return f"{100.0 * x:.3f}%"


def build_report(out_dir: Path | str) -> str:
    """Build the human-readable summary of a generated dataset."""
    out = Path(out_dir)
    manifest: dict[str, Any] = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    counts = manifest["counts"]
    fraud = manifest["fraud"]
    noise = manifest["noise"]
    cfg = manifest["config"]

    lines: list[str] = []
    lines.append("# FADE dataset report")
    lines.append("")
    lines.append(f"- seed: {cfg['seed']}  accounts: {counts['accounts']}  "
                 f"days: {cfg['days']}  start: {cfg['start_date']}")
    lines.append(f"- events: {counts['events']} (unique {counts['events_unique']}, "
                 f"duplicates {counts['duplicates']})")
    lines.append(f"- planned legit: {counts['planned_legit']}  applied: {counts['applied']}  "
                 f"skipped: {counts['skipped']}")
    lines.append(f"- fraud rate: {_pct(counts['fraud_rate'])} "
                 f"(target {_pct(cfg['fraud_rate'])})")
    lines.append("- labels: " + ", ".join(f"{k}={v}" for k, v in counts["labels"].items()))
    lines.append("")
    lines.append("## Per-scenario counts")
    lines.append("")
    lines.append("| scenario | events | runs | share of fraud |")
    lines.append("| --- | ---: | ---: | ---: |")
    total_fraud = max(counts["labels"].get("FRAUD", 0), 1)
    for scenario in sorted(counts["per_scenario"]):
        n = counts["per_scenario"][scenario]
        runs = fraud["runs"].get(scenario, 0)
        lines.append(f"| {scenario} | {n} | {runs} | {_pct(n / total_fraud)} |")
    lines.append("")
    lines.append("## Delivery noise (drawn)")
    lines.append("")
    total = max(sum(noise.values()), 1)
    lines.append("| category | count | rate | config |")
    lines.append("| --- | ---: | ---: | ---: |")
    ncfg = cfg["noise"]
    lines.append(f"| duplicate | {noise['duplicates']} | {_pct(noise['duplicates'] / total)} "
                 f"| {_pct(ncfg['duplicate_rate'])} |")
    lines.append(f"| delayed | {noise['delays']} | {_pct(noise['delays'] / total)} "
                 f"| {_pct(ncfg['delay_rate'])} |")
    lines.append(f"| late | {noise['late']} | {_pct(noise['late'] / total)} "
                 f"| {_pct(ncfg['late_rate'])} |")
    lines.append(f"| on time | {noise['normal']} | {_pct(noise['normal'] / total)} | - |")
    lines.append("")
    lines.append("## Files")
    lines.append("")
    lines.append("| file | bytes | sha256 |")
    lines.append("| --- | ---: | --- |")
    for name in sorted(manifest["files"]):
        meta = manifest["files"][name]
        lines.append(f"| {name} | {meta['bytes']} | `{meta['sha256'][:16]}...` |")
    return "\n".join(lines)
