#!/usr/bin/env python3
"""Regenerate the README result tables from committed bench JSON.

The README is the only place a human reads the headline numbers; the JSON in
bench/results is the source of truth.  This script rewrites the block between
``<!-- BEGIN BENCH:TABLES -->`` and ``<!-- END BENCH:TABLES -->`` and fails
(``--check``) when the README has drifted.

Usage:
  python3 scripts/readme_tables.py            # rewrite README.md
  python3 scripts/readme_tables.py --check    # exit 1 if README differs
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "bench" / "results"
README = ROOT / "README.md"
BEGIN = "<!-- BEGIN BENCH:TABLES -->"
END = "<!-- END BENCH:TABLES -->"


def load(name: str) -> dict | None:
    path = RESULTS / "latest" / name
    if not path.exists():
        path = RESULTS / name
    return json.loads(path.read_text()) if path.exists() else None


def load_latest(*candidates: str) -> tuple[str, dict] | tuple[None, None]:
    """Pick the newest dated result directory that holds any candidate file."""
    dates = sorted((p for p in RESULTS.iterdir() if p.is_dir()), reverse=True)
    for directory in dates:
        for name in candidates:
            path = directory / name
            if path.exists():
                return str(path.relative_to(ROOT)), json.loads(path.read_text())
    # legacy flat layout
    for name in candidates:
        path = RESULTS / name
        if path.exists():
            return str(path.relative_to(ROOT)), json.loads(path.read_text())
    return None, None


def fmt(value, digits: int = 1, suffix: str = "") -> str:
    if value is None:
        return "n/a"
    if isinstance(value, list):
        return f"{value[0]:.{digits}f}–{value[1]:.{digits}f}{suffix}"
    return f"{value:.{digits}f}{suffix}"


def latency_table() -> list[str]:
    rows = []
    for scenario in ("s1", "s2", "s3", "s6", "s7", "s8"):
        source, data = load_latest(f"{scenario}.json")
        if not data:
            continue
        runs = data.get("runs", [])
        if not runs:
            continue
        def med(key: str, runs=runs) -> str:
            values = [r["k6"].get(key) for r in runs if r["k6"].get(key) is not None]
            if not values:
                return "n/a"
            values.sort()
            middle = values[len(values) // 2]
            return f"{middle:.2f}"
        errors = [r["k6"]["error_rate"] for r in runs]
        rps = [r["k6"]["throughput_rps"] for r in runs]
        rows.append(
            "| {s} | {rps:.0f} | {p50} | {p95} | {p99} | {p999} | {err:.4%} | [{src}]({src}) |".format(
                s=scenario.upper(),
                rps=sum(rps) / len(rps),
                p50=med("p50_ms"), p95=med("p95_ms"), p99=med("p99_ms"), p999=med("p999_ms"),
                err=sum(errors) / len(errors), src=source,
            )
        )
    if not rows:
        return ["_No benchmark results committed yet._"]
    return [
        "| scenario | rps | p50 ms | p95 ms | p99 ms | p99.9 ms | error rate | evidence |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
        *rows,
        "",
        "Median of the recorded runs per scenario (see each evidence file for the full 3-run range).",
    ]


def detection_table() -> list[str]:
    source, data = load_latest("detection.json")
    if not data:
        return ["_No detection evaluation committed yet._"]
    configs = data.get("configs", {})
    best = configs.get("combined", {})
    cold = data.get("robustness", {}).get("cold_start", {}).get("recall")
    cold_text = f"{cold:.1%}" if cold is not None else "n/a (not identified in partial window)"
    rfpr = best.get("recall_at_fpr", {})
    ci = rfpr.get("ci95", [0, 0])
    lines = [
        "| metric | measured | target |",
        "| --- | --- | --- |",
        f"| Recall @ ≤1% FPR (combined) | {rfpr.get('point', 0):.1%} "
        f"(95% CI {ci[0]:.1%}–{ci[1]:.1%}) | ≥ 85% |",
        f"| FPR (combined) | {best.get('fpr', {}).get('point', 0):.2%} | ≤ 1% |",
        f"| Recall lift over rules-only | {data.get('lift_over_rules_only', {}).get('recall_points', 0):+.1f} points | ≥ +10 |",
        f"| Cold-start recall (accounts < 7 d) | {cold_text} | reported |",
        f"| Worst segment FPR ratio | {data.get('fairness', {}).get('max_fpr_ratio', 0):.2f}× | ≤ 2× |",
        "",
        "| scenario | fraud events | combined recall (95% CI) |",
        "| --- | --- | --- |",
    ]
    for scenario, p in sorted(best.get("per_scenario", {}).items()):
        lines.append(
            f"| {scenario} | {p.get('d', 0)} | {p.get('point', 0):.1%} "
            f"(CI {p.get('ci95', [0, 0])[0]:.1%}–{p.get('ci95', [0, 0])[1]:.1%}) |"
        )
    lines += [
        "",
        f"Dataset: `{data.get('dataset', {}).get('name')}` "
        f"hash `{str(data.get('dataset', {}).get('manifest_sha256'))[:16]}…`, "
        f"window {data.get('dataset', {}).get('window_days', 0):.2f} days. "
        f"Evidence: [{source}]({source}).",
    ]
    return lines


def chaos_table() -> list[str]:
    source, data = load_latest("chaos.json")
    if not data:
        return ["_No chaos run committed yet._"]
    names = {
        "C1": "Redis outage", "C2": "Redis stall", "C3": "Redis restart",
        "C4": "ML scorer down", "C5": "Postgres down", "C6": "Kafka down",
        "C7": "Kafka partition loss", "C8": "network partition", "C9": "disk full",
        "C10": "bad rules push", "C11": "clock skew", "C12": "pod restart",
        "C13": "volume loss", "C14": "connection flood",
    }
    rows = ["| id | scenario | result | measured |", "| --- | --- | --- | --- |"]
    for item in data.get("scenarios", []):
        rows.append(
            f"| {item['id']} | {names.get(item['id'], item.get('name', ''))} | "
            f"{item.get('result', 'FAIL')} | {item.get('detail', '')} |"
        )
    rows.append("")
    rows.append(f"Evidence: [{source}]({source}).")
    return rows


def operations_block() -> list[str]:
    source, data = load_latest("operations.json")
    if not data:
        return []
    rpo = data.get("backup_restore", {})
    return [
        "### Operations",
        "",
        f"- Backup/restore drill: **{'PASS' if rpo.get('passed') else 'FAIL'}**, "
        f"RPO {rpo.get('rpo_minutes', 'n/a')} min (target ≤ 15), RTO {rpo.get('rto_minutes', 'n/a')} min (target ≤ 30).",
        f"- Upgrade from v0.8.0 data: **{'PASS' if data.get('upgrade', {}).get('passed') else 'FAIL'}**; "
        f"zero-downtime rolling upgrade: **{'PASS' if data.get('rolling_upgrade', {}).get('passed') else 'FAIL'}**.",
        f"- Scaling: {data.get('scaling', {}).get('summary', 'n/a')}.",
        "",
        f"Evidence: [{source}]({source}).",
    ]


def build_block() -> str:
    lines = [BEGIN, "### Measured performance", ""]
    lines += latency_table()
    lines += ["", "### Detection quality (unseen evaluation dataset)", ""]
    lines += detection_table()
    lines += ["", "### Failure-mode verification", ""]
    lines += chaos_table()
    extra = operations_block()
    if extra:
        lines += ["", *extra]
    lines.append(END)
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()
    if not README.exists():
        print("README.md missing", file=sys.stderr)
        return 2
    text = README.read_text()
    block = build_block()
    if BEGIN in text and END in text:
        head, rest = text.split(BEGIN, 1)
        _, tail = rest.split(END, 1)
        new = head + block + tail
    else:
        new = text.rstrip() + "\n\n" + block + "\n"
    if args.check:
        if new != text:
            print("README result tables are stale: run `make readme-tables`", file=sys.stderr)
            return 1
        print("readme-tables: README result tables match committed JSON")
        return 0
    README.write_text(new)
    print("readme-tables: README updated from committed JSON")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
