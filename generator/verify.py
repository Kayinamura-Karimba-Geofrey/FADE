"""Dataset verification: manifest hashes, row counts, file layout.

``verify`` is intentionally fast - the full statistical and invariant
audits live under ``tests/generator/audit/``.  Here we prove the files on
disk are exactly the files the manifest describes.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import zstandard as zstd

#: The dataset layout documented in docs/dataset-contract.md.
EXPECTED_FILES = (
    "manifest.json",
    "events.jsonl.zst",
    "events.parquet",
    "labels.parquet",
    "arrivals.parquet",
    "accounts.parquet",
)

#: Files a report may add without breaking the layout.
ALLOWED_EXTRAS = ("report.md",)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def count_jsonl_lines(path: Path) -> int:
    """Number of JSON records in a zstd-compressed JSONL file."""
    n = 0
    dctx = zstd.ZstdDecompressor()
    with dctx.stream_reader(path.open("rb")) as reader:
        while True:
            chunk = reader.read(1 << 20)
            if not chunk:
                break
            n += chunk.count(b"\n")
    return n


def verify_dataset(out_dir: Path | str) -> tuple[bool, list[tuple[str, bool, str]]]:
    """Run every verification check; returns ``(ok, [(name, passed, detail)])``."""
    out = Path(out_dir)
    checks: list[tuple[str, bool, str]] = []

    manifest_path = out / "manifest.json"
    if not manifest_path.exists():
        return False, [("manifest", False, "manifest.json missing")]
    manifest: dict[str, Any] = json.loads(manifest_path.read_text(encoding="utf-8"))

    present = sorted(p.name for p in out.iterdir() if p.is_file())
    unexpected = [f for f in present if f not in EXPECTED_FILES and f not in ALLOWED_EXTRAS]
    checks.append(
        (
            "layout",
            not unexpected,
            f"{len(present)} files" + (f", unexpected: {unexpected}" if unexpected else ""),
        )
    )

    for name, meta in sorted(manifest["files"].items()):
        path = out / name
        if not path.exists():
            checks.append((f"exists:{name}", False, "missing"))
            continue
        digest = sha256_file(path)
        size = path.stat().st_size
        ok = digest == meta["sha256"] and size == meta["bytes"]
        checks.append(
            (f"sha256:{name}", ok, f"{size} bytes, {digest[:16]}..." if ok else "hash/size mismatch")
        )

    counts = manifest["counts"]
    table_counts = {
        "events.parquet": pq.ParquetFile(out / "events.parquet").metadata.num_rows,
        "labels.parquet": pq.ParquetFile(out / "labels.parquet").metadata.num_rows,
        "arrivals.parquet": pq.ParquetFile(out / "arrivals.parquet").metadata.num_rows,
        "accounts.parquet": pq.ParquetFile(out / "accounts.parquet").metadata.num_rows,
    }
    expected_rows = {
        "events.parquet": counts["events"],
        "labels.parquet": counts["events_unique"],
        "arrivals.parquet": counts["events"],
        "accounts.parquet": counts["accounts"],
    }
    for name, actual in table_counts.items():
        want = expected_rows[name]
        checks.append((f"rows:{name}", actual == want, f"{actual} rows (want {want})"))

    n_lines = count_jsonl_lines(out / "events.jsonl.zst")
    checks.append(
        (
            "rows:events.jsonl.zst",
            n_lines == counts["events"],
            f"{n_lines} lines (want {counts['events']})",
        )
    )

    ok = all(passed for _, passed, _ in checks)
    return ok, checks
