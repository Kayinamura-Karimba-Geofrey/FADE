"""Audit 2 - determinism.

Proves that a dataset is a pure function of (config, seed): same seed gives
byte-identical files across runs, output paths, working directories and
``PYTHONHASHSEED`` values; a different seed changes the data.  Also scans the
output-affecting modules for wall-clock/uuid/global-RNG use with a tokenizer
(docstrings and comments are ignored, so prose mentioning ``time.time()`` does
not create a false positive).

Run:  uv run python -m tests.generator.audit.audit_determinism --data data/small
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import tempfile
import tokenize
from pathlib import Path

from tests.generator.audit.common import Report, data_dir, run_script

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG = REPO_ROOT / "configs" / "small.yaml"
OUTPUT_FILES = (
    "events.jsonl.zst",
    "events.parquet",
    "labels.parquet",
    "arrivals.parquet",
    "accounts.parquet",
    "manifest.json",
)

#: Modules whose output is part of the dataset.  The publisher is excluded:
#: it uses the wall clock for pacing, never to produce data.
OUTPUT_MODULES = (
    "generator/behavior",
    "generator/population",
    "generator/fraud",
    "generator/delivery/noise.py",
    "generator/io",
    "generator/models.py",
    "generator/rng.py",
    "generator/pipeline.py",
    "generator/config.py",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _hashes(out: Path) -> dict[str, str]:
    return {name: _sha256(out / name) for name in OUTPUT_FILES}


def _generate(
    out: Path,
    *,
    seed: int | None = None,
    cwd: Path | None = None,
    hashseed: str | None = None,
) -> dict[str, str]:
    cmd = [
        sys.executable,
        "-m",
        "generator",
        "run",
        "--out",
        str(out),
        "--config",
        str(CONFIG),
        "--quiet",
    ]
    if seed is not None:
        cmd += ["--seed", str(seed)]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT)
    if hashseed is not None:
        env["PYTHONHASHSEED"] = hashseed
    else:
        env.pop("PYTHONHASHSEED", None)
    subprocess.run(cmd, cwd=cwd or REPO_ROOT, env=env, check=True, capture_output=True)
    return _hashes(out)


def _python_files() -> list[Path]:
    files: list[Path] = []
    for spec in OUTPUT_MODULES:
        path = REPO_ROOT / spec
        if path.is_dir():
            files.extend(sorted(path.rglob("*.py")))
        else:
            files.append(path)
    return files


def _token_hits(path: Path) -> list[str]:
    """Flag wall-clock / uuid / global-random calls using real code tokens."""
    hits: list[str] = []
    try:
        tokens = list(tokenize.generate_tokens(path.open("r", encoding="utf-8").readline))
    except (tokenize.TokenError, IndentationError):  # pragma: no cover - defensive
        return hits
    significant = [
        t
        for t in tokens
        if t.type not in (tokenize.COMMENT, tokenize.NL, tokenize.NEWLINE, tokenize.STRING)
    ]
    for i, tok in enumerate(significant):
        name = tok.string
        prev = significant[i - 1].string if i else ""
        nxt = significant[i + 1].string if i + 1 < len(significant) else ""
        if tok.type == tokenize.NAME:
            if name == "time" and nxt == "." and i + 2 < len(significant):
                if significant[i + 2].string in ("time", "monotonic", "perf_counter"):
                    hits.append(f"{path.relative_to(REPO_ROOT)}:{tok.start[0]}: {name}.{significant[i+2].string}")
            if name == "datetime" and nxt == "." and i + 2 < len(significant):
                if significant[i + 2].string in ("now", "utcnow", "today"):
                    hits.append(f"{path.relative_to(REPO_ROOT)}:{tok.start[0]}: datetime.now")
            if name == "uuid4":
                hits.append(f"{path.relative_to(REPO_ROOT)}:{tok.start[0]}: uuid4")
            if name == "random" and prev in ("import", "from"):
                hits.append(f"{path.relative_to(REPO_ROOT)}:{tok.start[0]}: global random import")
            if name == "random" and prev == "np" and nxt == "." and i + 2 < len(significant):
                attr = significant[i + 2].string
                if attr != "default_rng":
                    hits.append(f"{path.relative_to(REPO_ROOT)}:{tok.start[0]}: np.random.{attr}")
    return hits


def _set_ordering_hits() -> list[str]:
    """Textual review aid: set iteration that feeds ordering.

    All ordering-sensitive set iterations in the generator are wrapped in
    ``sorted(...)`` (see the allowlist below); these are the only matches and
    they are ordered.  A new unsorted set iteration should fail this check.
    """
    allow = {
        "generator/fraud/scenarios.py": {"sorted(set(self.take(k)))", "sorted(set("},
        "generator/population/graph.py": {"sorted(set(peers))", "sorted(set(merchants))", "sorted(set(attached))"},
        "generator/config.py": {"sorted(unknown)"},
    }
    hits: list[str] = []
    for path in _python_files():
        rel = str(path.relative_to(REPO_ROOT))
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#") or " set(" not in f" {stripped}":
                continue
            # Only iteration over a set (or a set literal) can affect order;
            # membership sets and ``sorted(set(...))`` are safe.
            if "for " not in stripped or " in " not in stripped:
                continue
            if "sorted(" in stripped or "frozenset" in stripped:
                continue
            allowed = allow.get(rel, set())
            if any(fragment in line for fragment in allowed):
                continue
            hits.append(f"{rel}:{lineno}: {stripped}")
    return hits


def audit(path: Path) -> Report:
    report = Report("Audit 2 - determinism")
    with tempfile.TemporaryDirectory(prefix="fade-det-") as tmp:
        root = Path(tmp)
        a = _generate(root / "a")
        b = _generate(root / "b")
        report.check(
            "same seed, same config -> identical sha256 (all files)",
            a == b,
            f"{sum(a[k] == b[k] for k in OUTPUT_FILES)}/{len(OUTPUT_FILES)} files equal",
            "all 6 files equal",
        )

        c = _generate(root / "c", seed=8)
        report.check(
            "different seed -> events differ",
            c["events.jsonl.zst"] != a["events.jsonl.zst"],
            "events.jsonl.zst differs" if c["events.jsonl.zst"] != a["events.jsonl.zst"] else "IDENTICAL",
            "events.jsonl.zst differs",
        )

        d = _generate(root / "different-out-path", cwd=root / "a")
        report.check(
            "different --out path and working directory -> identical",
            d == a,
            f"{sum(d[k] == a[k] for k in OUTPUT_FILES)}/{len(OUTPUT_FILES)} files equal",
            "all 6 files equal",
        )

        e = _generate(root / "hashseed0", hashseed="0")
        f = _generate(root / "hashseed1", hashseed="1")
        report.check(
            "PYTHONHASHSEED=0 vs 1 -> identical",
            e == f,
            f"{sum(e[k] == f[k] for k in OUTPUT_FILES)}/{len(OUTPUT_FILES)} files equal",
            "all 6 files equal",
        )

        help_text = subprocess.run(
            [sys.executable, "-m", "generator", "run", "--help"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        ).stdout
        has_workers = "--workers" in help_text
        report.check(
            "1 vs 4 workers",
            True,
            "no parallel path (single process)" if not has_workers else "--workers present",
            "identical if parallelism exists",
            detail="(generator is single-process; no worker flag)" if not has_workers else "",
        )

    hits = [hit for path in _python_files() for hit in _token_hits(path)]
    report.check(
        "no time.time / datetime.now / uuid4 / global random in output modules",
        not hits,
        f"{len(hits)} hits" + (f": {hits[:3]}" if hits else ""),
        "0 hits",
    )
    set_hits = _set_ordering_hits()
    report.check(
        "no unsorted set iteration feeding output order",
        not set_hits,
        f"{len(set_hits)} un-allowlisted uses" + (f": {set_hits[:3]}" if set_hits else ""),
        "0 hits",
    )
    return report


def test_determinism() -> None:
    report = audit(data_dir())
    assert report.passed, report.render()


if __name__ == "__main__":
    raise SystemExit(run_script("Audit 2 - determinism", audit))
