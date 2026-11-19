#!/usr/bin/env python3
"""Run one k6 benchmark scenario and record machine-readable evidence.

Every run writes ``bench/results/<date>/<scenario>-run<N>.json`` and a matching
``.md`` summary.  The JSON carries everything needed to reproduce the number:
git sha, engine version, hardware/OS/Docker versions, dataset hash, seeds,
config hashes and the exact command lines.  Client-side numbers come from k6;
server-side numbers are computed from the decision-api Prometheus histogram
scraped immediately before and after the run.

Usage:
  python3 scripts/bench_run.py --scenario S1 --script bench/k6/s1-steady.js \
      --rate 1000 --duration 10m --runs 3
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BENCH_RESULTS = ROOT / "bench" / "results"

#: Documented targets per scenario (see docs/benchmarks.md).
TARGETS = {
    "S1": {"p99_ms": 50.0, "errors": 0.0, "degraded_rate": 0.0},
    "S2": {"p99_ms_at_3000rps": 100.0},
    "S3": {"recovery_s": 30.0},
    "S4": {"rss_growth_pct_per_hour": 1.0},
    "S5": {"double_effects": 0},
    "S6": {"p99_ms": 60.0},
    "S7": {"p99_ms": 50.0},
    "S8": {"p99_ms": 30.0, "recovery_s": 15.0},
    "S9": {"first_decision_s": 60.0},
}


def sh(cmd: list[str], timeout: int = 120) -> str:
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, timeout=timeout).strip()
    except Exception as exc:  # noqa: BLE001
        return f"<error: {exc}>"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def dataset_hash(name: str = "eval") -> str:
    manifest = ROOT / "data" / name / "manifest.json"
    if not manifest.exists():
        return "missing"
    return sha256_file(manifest)


def config_hashes() -> dict[str, str]:
    out = {}
    for rel in ("rules/prod.json", "config/features.yaml", "config/gate.yaml", "VERSION"):
        p = ROOT / rel
        if p.exists():
            out[rel] = sha256_file(p)
    reg = ROOT / "ml" / "models" / "registry.json"
    if reg.exists():
        raw = json.loads(reg.read_text())
        for version, meta in raw.get("versions", {}).items():
            if meta.get("status") == "active":
                out["model.active"] = f"{version}:{meta.get('artifact_hash', '')}"
    return out


def environment() -> dict:
    cpu_model = "unknown"
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu_model = line.split(":", 1)[1].strip()
                break
    except Exception:  # noqa: BLE001
        pass
    mem_kb = 0
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                mem_kb = int(line.split()[1])
                break
    except Exception:  # noqa: BLE001
        pass
    return {
        "cpu_model": cpu_model,
        "cpu_cores": os.cpu_count(),
        "ram_kb": mem_kb,
        "kernel": platform.release(),
        "system": platform.system(),
        "machine": platform.machine(),
        "go": sh(["go", "version"]),
        "python": sh(["python3", "--version"]),
        "node": sh(["node", "--version"]),
        "docker": sh(["docker", "--version"]),
        "compose": sh(["docker", "compose", "version"]),
        "k6": sh(["k6", "version"]),
        # The reference host runs the stack in Docker on this machine; the
        # documented limits live in deploy/docker-compose.bench.yml.
        "shared_host": True,
    }


def scrape_metrics(url: str) -> dict[str, float]:
    import urllib.request

    try:
        with urllib.request.urlopen(url, timeout=10) as resp:  # noqa: S310 - fixed local URL
            text = resp.read().decode()
    except Exception as exc:  # noqa: BLE001
        return {"__error__": str(exc)}  # type: ignore[dict-item]
    out: dict[str, float] = {}
    for line in text.splitlines():
        if line.startswith("#") or not line:
            continue
        try:
            key, value = line.rsplit(" ", 1)
        except ValueError:
            continue
        out[key] = float(value)
    return out


def histogram_quantiles(before: dict, after: dict, metric: str, labels: dict[str, str] | None = None) -> dict:
    """Quantiles from the delta of a cumulative Prometheus histogram."""
    labels = labels or {}
    label_part = ",".join(f'{k}="{v}"' for k, v in labels.items())
    suffix = "{" + label_part + "}" if label_part else ""
    buckets: list[tuple[float, float]] = []
    for key in after:
        if not key.startswith(metric + "_bucket"):
            continue
        if suffix and suffix not in key:
            continue
        m = re.search(r'le="([+0-9.eE]+)"', key)
        if not m:
            continue
        le = math.inf if m.group(1) == "+Inf" else float(m.group(1))
        delta = after[key] - before.get(key, 0.0)
        buckets.append((le, delta))
    if not buckets:
        return {"count": 0}
    buckets.sort()
    total = buckets[-1][1]
    result: dict[str, float | int] = {"count": int(total)}
    if total <= 0:
        return result
    for q in (0.5, 0.95, 0.99, 0.999):
        prev_le, prev_cum = 0.0, 0.0
        value = buckets[-1][0]
        for le, cum in buckets:
            if cum / total >= q:
                if math.isinf(le):
                    value = prev_le
                else:
                    # linear interpolation inside the bucket
                    span = cum - prev_cum
                    frac = 0.0 if span <= 0 else (q * total - prev_cum) / span
                    value = prev_le + (le - prev_le) * frac
                break
            prev_le, prev_cum = le, cum
        result[f"p{str(q * 100).replace('.', '_')}"] = round(value * 1000, 3)
    return result


def counter_delta(before: dict, after: dict, prefix: str) -> dict[str, float]:
    out = {}
    for key, value in after.items():
        if key.startswith(prefix):
            delta = value - before.get(key, 0.0)
            if delta or value:
                out[key] = round(delta, 3)
    return out


def mint_token() -> str:
    env = dict(os.environ)
    if not env.get("JWT_SECRET"):
        env_file = ROOT / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("JWT_SECRET="):
                    env["JWT_SECRET"] = line.split("=", 1)[1]
                    break
    raw = subprocess.check_output(["go", "run", "./cmd/dev-token"], cwd=ROOT, env=env, text=True).strip()
    return raw


def pct(values: list[float], q: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round(q * (len(ordered) - 1)))))
    return ordered[idx]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", required=True)
    ap.add_argument("--script", required=True)
    ap.add_argument("--rate", default="1000")
    ap.add_argument("--duration", default="10m")
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--base-url", default=os.environ.get("BASE_URL", "http://localhost:8080"))
    ap.add_argument("--date", default=datetime.now(UTC).date().isoformat())
    ap.add_argument("--seed", default="11")
    ap.add_argument("--dataset", default="eval")
    ap.add_argument("--tag", default="")
    args = ap.parse_args()

    out_dir = BENCH_RESULTS / args.date
    out_dir.mkdir(parents=True, exist_ok=True)
    token = mint_token()
    env = dict(os.environ)
    env.update({"BASE_URL": args.base_url, "TOKEN": token, "RATE": args.rate, "DURATION": args.duration})

    runs = []
    for i in range(1, args.runs + 1):
        summary = out_dir / f"{args.scenario.lower()}-run{i}.k6.json"
        cmd = [
            "k6", "run",
            "--summary-trend-stats", "avg,min,med,max,p(90),p(95),p(99),p(99.9)",
            "--summary-export", str(summary),
            str(ROOT / args.script),
        ]
        before = scrape_metrics(args.base_url + "/metrics")
        started = datetime.now(UTC).isoformat()
        t0 = time.time()
        proc = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True, text=True)  # noqa: S603
        elapsed = time.time() - t0
        after = scrape_metrics(args.base_url + "/metrics")
        (out_dir / f"{args.scenario.lower()}-run{i}.log").write_text(proc.stdout + proc.stderr)

        data = json.loads(summary.read_text()) if summary.exists() else {"metrics": {}}
        m = data.get("metrics", {})
        dur = m.get("http_req_duration", {})
        failed = m.get("http_req_failed", {})
        reqs = m.get("http_reqs", {})
        stages = {}
        for stage in ("idem", "features", "ml", "rules", "combine"):
            q = histogram_quantiles(before, after, "fade_decision_latency_seconds", {"stage": stage})
            if q.get("count"):
                stages[stage] = q
        decisions = counter_delta(before, after, "fade_decisions_total")
        shed = counter_delta(before, after, "fade_shed_total")
        requests = counter_delta(before, after, "fade_http_requests_total")
        gh = histogram_quantiles(before, after, "fade_http_request_duration_seconds")
        client_p99 = dur.get("p(99)")
        server_p99 = gh.get("p99")
        agreement = None
        if client_p99 and server_p99:
            agreement = round(abs(client_p99 - server_p99) / max(client_p99, 1e-9) * 100, 2)

        runs.append({
            "run": i,
            "started_at": started,
            "elapsed_s": round(elapsed, 2),
            "command": " ".join(cmd),
            "k6": {
                "requests": reqs.get("count"),
                "throughput_rps": round(reqs.get("rate", 0.0), 3),
                "error_rate": failed.get("value", 0.0),
                "p50_ms": dur.get("med"),
                "p95_ms": dur.get("p(95)"),
                "p99_ms": dur.get("p(99)"),
                "p999_ms": dur.get("p(99.9)"),
                "max_ms": dur.get("max"),
                "thresholds": {k: v.get("thresholds") for k, v in m.items() if v.get("thresholds")},
            },
            "server": {
                "requests": requests,
                "decisions": decisions,
                "shed": shed,
                "http": gh,
                "stages_ms": stages,
            },
            "k6_vs_server_p99_delta_pct": agreement,
            "exit_code": proc.returncode,
        })

    p99s = [r["k6"]["p99_ms"] for r in runs if r["k6"]["p99_ms"] is not None]
    errs = [r["k6"]["error_rate"] for r in runs]
    record = {
        "scenario": args.scenario,
        "tag": args.tag,
        "metric_targets": TARGETS.get(args.scenario, {}),
        "generated_at": datetime.now(UTC).isoformat(),
        "git_sha": sh(["git", "rev-parse", "HEAD"]),
        "git_dirty": bool(sh(["git", "status", "--porcelain"])),
        "engine_version": (ROOT / "VERSION").read_text().strip(),
        "environment": environment(),
        "dataset": {"name": args.dataset, "manifest_sha256": dataset_hash(args.dataset)},
        "seed": args.seed,
        "config_hashes": config_hashes(),
        "command_line": " ".join(sys.argv),
        "runs": runs,
        "median": {
            "p99_ms": pct(p99s, 0.5) if p99s else None,
            "p99_range_ms": [min(p99s), max(p99s)] if p99s else None,
            "error_rate": pct(errs, 0.5),
        },
    }
    json_path = out_dir / f"{args.scenario.lower()}.json"
    json_path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")

    md = [
        f"# {args.scenario} benchmark ({args.date})",
        "",
        f"- git: `{record['git_sha']}` (dirty={record['git_dirty']})",
        f"- engine: {record['engine_version']}",
        f"- host: {record['environment']['cpu_model']} × {record['environment']['cpu_cores']} cores, "
        f"{record['environment']['ram_kb'] // 1024 // 1024} GiB RAM, kernel {record['environment']['kernel']}",
        f"- k6: {record['environment']['k6']}",
        f"- dataset: {args.dataset} manifest sha256 `{record['dataset']['manifest_sha256']}`",
        f"- command: `{record['command_line']}`",
        "",
        "| run | rps | p50 ms | p95 ms | p99 ms | p99.9 ms | errors | server p99 ms | Δ client/server |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for r in runs:
        md.append(
            f"| {r['run']} | {r['k6']['throughput_rps']} | {r['k6']['p50_ms']} | {r['k6']['p95_ms']} | "
            f"{r['k6']['p99_ms']} | {r['k6']['p999_ms']} | {r['k6']['error_rate']} | "
            f"{r['server']['http'].get('p99')} | {r['k6_vs_server_p99_delta_pct']}% |"
        )
    md += ["", f"Median p99: {record['median']['p99_ms']} ms "
               f"(range {record['median']['p99_range_ms']})"]
    (out_dir / f"{args.scenario.lower()}.md").write_text("\n".join(md) + "\n")
    print(json.dumps({"scenario": args.scenario, "median_p99_ms": record["median"]["p99_ms"],
                      "json": str(json_path)}, indent=2))
    return 0 if all(r["exit_code"] == 0 for r in runs) else 1


if __name__ == "__main__":
    raise SystemExit(main())
