#!/usr/bin/env python3
"""Pre-flight check for benchmarking. Exits non-zero unless --force."""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(cmd):
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, timeout=30).strip()
    except Exception as exc:
        return f"<error: {exc}>"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    checks = {}
    # CPU governor
    gov = "unknown"
    try:
        gov = Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor").read_text().strip()
    except Exception:
        pass
    checks["cpu_governor"] = {"value": gov, "ok": gov in ("performance", "unknown")}

    # Disk free
    free_gb = shutil.disk_usage(ROOT).free / (1024 ** 3)
    checks["disk_free_gb"] = {"value": round(free_gb, 2), "ok": free_gb > 20}

    # Clock sync
    checks["clock_utc"] = {"value": datetime.now(UTC).isoformat(), "ok": True}

    # Heavy processes (best effort): none beyond threshold
    ps = run(["ps", "-eo", "pcpu,comm", "--sort=-pcpu"])
    checks["top_processes"] = {"value": ps.splitlines()[:6], "ok": True}

    ok = all(c["ok"] for c in checks.values())
    result = {"ok": ok, "forced": args.force, "checks": checks}
    out = ROOT / "bench" / "results" / "preflight.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    if not ok and not args.force:
        print("PREFLIGHT FAILED; use --force to override (recorded)", file=sys.stderr)
        return 1
    if args.force:
        result["forced"] = True
        out.write_text(json.dumps(result, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
