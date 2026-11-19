#!/usr/bin/env python3
"""Aggregate bench/results/**/*.json into bench/results/<date>/bench-report.md."""
from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "bench" / "results"


def main() -> int:
    date = sys.argv[1] if len(sys.argv) > 1 else datetime.now(UTC).date().isoformat()
    out_dir = RESULTS / date
    out_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(out_dir.glob("*.json"))
    lines = [f"# Benchmark report {date}", ""]
    if not files:
        lines.append("_No results._")
    for path in files:
        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        if "runs" not in data:
            continue
        lines.append(f"## {data.get('scenario', path.stem)}")
        lines.append("")
        lines.append(f"- command: `{data.get('command_line')}`")
        lines.append(f"- git: `{data.get('git_sha')}` engine {data.get('engine_version')}")
        lines.append(f"- evidence: `{path.relative_to(ROOT)}`")
        lines.append("")
        lines.append("| run | rps | p50 | p95 | p99 | p99.9 | errors | server p99 | Δ% |")
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
        for run in data["runs"]:
            k = run["k6"]
            lines.append(
                f"| {run['run']} | {k['throughput_rps']} | {k['p50_ms']} | {k['p95_ms']} | {k['p99_ms']} | "
                f"{k['p999_ms']} | {k['error_rate']} | {run['server']['http'].get('p99')} | "
                f"{run['k6_vs_server_p99_delta_pct']} |"
            )
        median = data.get("median", {})
        lines.append("")
        lines.append(f"Median p99 **{median.get('p99_ms')} ms** (range {median.get('p99_range_ms')}).")
        lines.append("")
    report = out_dir / "bench-report.md"
    report.write_text("\n".join(lines) + "\n")
    print(f"wrote {report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
