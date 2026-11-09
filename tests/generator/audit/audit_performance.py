"""Audit 9 - performance and dataset size.

The full ``data/run42`` dataset (50k accounts x 90 days) must generate in
under 15 minutes on the reference machine, stay inside the 200 bytes/event
JSONL budget, and keep peak memory below 2 GB.  Running the full job inside
an audit would make the suite unusable, so this audit times a scaled
``configs/run42.yaml`` job (5k accounts x 10 days), measures its throughput
and peak RSS, and projects linearly to the full event count.

Set ``FADE_PERF_FULL=1`` to generate the full run42 configuration instead of
projecting (used for the release exit check, not in CI).

Run:  uv run python -m tests.generator.audit.audit_performance --data data/run42
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from generator.config import dump_config, load_config
from tests.generator.audit.common import (
    Report,
    data_dir,
    load_manifest,
    run_script,
    skip_if_missing,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
RUN42_CONFIG = REPO_ROOT / "configs" / "run42.yaml"
FULL_SECONDS = 900.0
BYTES_PER_EVENT = 200.0
TOTAL_BYTES_PER_EVENT = 300.0
MAX_RSS_GB = 2.0


def _bench(out: Path, config: Path) -> dict[str, float]:
    """Generate one dataset in a child process and return timing and RSS."""
    code = (
        "import json,resource,sys,time\n"
        "from generator.config import load_config\n"
        "from generator.pipeline import run\n"
        "cfg=load_config(sys.argv[1])\n"
        "t=time.monotonic(); m=run(cfg,sys.argv[2]); seconds=time.monotonic()-t\n"
        "print(json.dumps({'seconds':seconds,'rss_kb':"
        "resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,"
        "'events':m['counts']['events_unique'],'accounts':m['counts']['accounts'],"
        "'days':m['config']['days']}))\n"
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT)
    proc = subprocess.run(
        [sys.executable, "-c", code, str(config), str(out)],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(proc.stdout.strip().splitlines()[-1])


def audit(path: Path) -> Report:
    report = Report("Audit 9 - performance and dataset size")
    manifest = load_manifest(path)
    counts = manifest["counts"]
    files = manifest["files"]
    events = int(counts["events"])

    jsonl = files["events.jsonl.zst"]["bytes"] / events if events else 0.0
    total_bytes = sum(meta["bytes"] for meta in files.values()) / events if events else 0.0
    report.check(
        "events.jsonl.zst is within the 200 bytes/event budget",
        jsonl <= BYTES_PER_EVENT,
        f"{jsonl:.1f} bytes/event",
        f"<= {BYTES_PER_EVENT:.0f}",
    )
    report.check(
        "the whole dataset is within 300 bytes per event",
        total_bytes <= TOTAL_BYTES_PER_EVENT,
        f"{total_bytes:.1f} bytes/event",
        f"<= {TOTAL_BYTES_PER_EVENT:.0f}",
    )

    account_days = int(manifest["config"]["accounts"]) * int(manifest["config"]["days"])
    density = events / account_days if account_days else 0.0
    report.check(
        "event density is plausible for the population and period",
        density >= 0.8,
        f"{density:.2f} events per account-day",
        ">= 0.80",
    )

    full = os.environ.get("FADE_PERF_FULL") == "1"
    base = load_config(RUN42_CONFIG)
    with tempfile.TemporaryDirectory(prefix="fade-perf-") as tmp:
        root = Path(tmp)
        if full:
            bench_cfg = base
            label = "50k x 90d"
        else:
            bench_cfg = base.model_copy(update={"accounts": 5_000, "days": 10})
            label = "5k x 10d (projected)"
        cfg_path = root / "bench.yaml"
        cfg_path.write_text(dump_config(bench_cfg), encoding="utf-8")
        result = _bench(root / "data", cfg_path)

    seconds = float(result["seconds"])
    bench_events = float(result["events"])
    rss_gb = float(result["rss_kb"]) / (1024.0 * 1024.0)
    target_events = bench_events
    if not full and bench_events:
        target_events = bench_events * (base.accounts / bench_cfg.accounts) * (
            base.days / bench_cfg.days
        )
        projected = 1.0 + max(seconds - 1.0, 0.1) * (target_events / bench_events)
    else:
        projected = seconds
    report.check(
        f"full run42 generation fits the 15-minute budget ({label})",
        projected <= FULL_SECONDS,
        f"{projected:.0f} s projected ({bench_events:.0f} events in {seconds:.1f} s)",
        f"<= {FULL_SECONDS:.0f} s",
    )
    report.check(
        "peak resident memory stays under 2 GB",
        rss_gb <= MAX_RSS_GB,
        f"{rss_gb:.2f} GB",
        f"<= {MAX_RSS_GB:.1f} GB",
    )
    report.check(
        "the measured dataset has the expected order of magnitude",
        events >= 1_000 or int(manifest["config"]["accounts"]) < 1_000,
        f"{events} events",
        "> 0",
    )
    return report


def test_performance() -> None:
    path = data_dir()
    skip_if_missing(path)
    report = audit(path)
    assert report.passed, report.render()


if __name__ == "__main__":
    raise SystemExit(run_script("Audit 9 - performance and dataset size", audit))
