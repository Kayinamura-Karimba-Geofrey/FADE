#!/usr/bin/env python3
"""Capture the reference environment into bench/results/<date>/environment.json.

Usage: python scripts/env_report.py [--date YYYY-MM-DD] [--out DIR]
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str]) -> str:
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, timeout=60).strip()
    except Exception as exc:  # noqa: BLE001
        return f"<error: {exc}>"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=datetime.now(UTC).date().isoformat())
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    out_dir = Path(args.out) if args.out else ROOT / "bench" / "results" / args.date
    out_dir.mkdir(parents=True, exist_ok=True)

    version = (ROOT / "VERSION").read_text().strip()

    # CPU
    cpu_model = "unknown"
    cpu_cores = os.cpu_count()
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu_model = line.split(":", 1)[1].strip()
                break
    except Exception:
        pass

    # RAM
    mem_kb = None
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                mem_kb = int(line.split()[1])
                break
    except Exception:
        pass

    # Disk type
    disk_type = run(["lsblk", "-d", "-o", "NAME,TYPE,ROTA,SIZE,MODEL"])
    # Image digests
    images = run(["docker", "images", "--format", "{{.Repository}}:{{.Tag}}\t{{.ID}}\t{{.Digest}}"])

    env = {
        "captured_at": datetime.now(UTC).isoformat(),
        "git_sha": run(["git", "-C", str(ROOT), "rev-parse", "HEAD"]),
        "engine_version": version,
        "date": args.date,
        "os": {
            "kernel": platform.release(),
            "system": platform.system(),
            "machine": platform.machine(),
            "distro": run(["cat", "/etc/os-release"]),
        },
        "cpu": {"model": cpu_model, "cores": cpu_cores},
        "memory": {"total_kb": mem_kb},
        "disk": {"lsblk": disk_type},
        "tool_versions": {
            "go": run(["go", "version"]),
            "python": run(["python3", "--version"]),
            "node": run(["node", "--version"]),
            "docker": run(["docker", "--version"]),
            "compose": run(["docker", "compose", "version"]),
            "k6": run(["k6", "version"]) if shutil.which("k6") else "MISSING",
        },
        "image_digests_raw": images,
        "command_line": "python scripts/env_report.py --date " + args.date,
    }

    out_path = out_dir / "environment.json"
    out_path.write_text(json.dumps(env, indent=2, sort_keys=True) + "\n")
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
