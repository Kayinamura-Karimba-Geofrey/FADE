"""Shared helpers for the Phase 1 generator audits.

Every audit has the same shape: it loads a generated dataset, measures a
small set of quantities, compares them to the thresholds in the Phase 1
spec, prints a table and (for pytest) asserts that nothing failed.
"""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq
import zstandard as zstd

DEFAULT_DATA = Path("data/small")


def data_dir(explicit: str | Path | None = None) -> Path:
    """Resolve the dataset directory (CLI --data, env FADE_DATA_DIR, default)."""
    if explicit:
        return Path(explicit)
    return Path(os.environ.get("FADE_DATA_DIR", DEFAULT_DATA))


def require_dataset(path: Path) -> Path:
    """Raise a clear error when a generated dataset is missing."""
    if not (path / "manifest.json").exists():
        raise FileNotFoundError(
            f"{path} has no manifest.json - run `make gen-small` (or `make gen`) first"
        )
    return path


def load_manifest(path: Path) -> dict[str, Any]:
    return json.loads((path / "manifest.json").read_text(encoding="utf-8"))


def load_events(path: Path) -> pd.DataFrame:
    """Load events.parquet with parsed timestamps (arrival order preserved)."""
    table = pq.read_table(path / "events.parquet")
    df = table.to_pandas()
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    return df


def load_arrivals(path: Path) -> pd.DataFrame:
    df = pq.read_table(path / "arrivals.parquet").to_pandas()
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    df["arrival_time"] = pd.to_datetime(df["arrival_time"], utc=True)
    return df


def load_labels(path: Path) -> pd.DataFrame:
    return pq.read_table(path / "labels.parquet").to_pandas()


def load_accounts(path: Path) -> pd.DataFrame:
    return pq.read_table(path / "accounts.parquet").to_pandas()


def joined(path: Path) -> pd.DataFrame:
    """Events with sender segment and ground-truth labels attached."""
    events = load_events(path)
    labels = load_labels(path)
    events = events.merge(
        labels[["event_id", "label", "scenario", "scenario_run_id"]],
        on="event_id",
        how="left",
        validate="many_to_one",
    )
    accounts = load_accounts(path)[["account_id", "segment", "home_cell", "created_day"]]
    events = events.merge(
        accounts.rename(columns={"account_id": "sender.account_id", "home_cell": "home_cell"}),
        on="sender.account_id",
        how="left",
    )
    return events


def iter_jsonl(path: Path) -> Iterator[Any]:
    """Yield parsed event objects from events.jsonl.zst without loading all."""
    dctx = zstd.ZstdDecompressor()
    with dctx.stream_reader((path / "events.jsonl.zst").open("rb")) as reader:
        pending = b""
        while True:
            chunk = reader.read(1 << 20)
            if not chunk:
                break
            pending += chunk
            while b"\n" in pending:
                line, pending = pending.split(b"\n", 1)
                if line:
                    yield json.loads(line)
        if pending.strip():
            yield json.loads(pending)


def read_contract_block(path: Path = Path("docs/dataset-contract.md")) -> dict[str, Any]:
    """Parse the machine-readable contract embedded in the dataset doc."""
    text = path.read_text(encoding="utf-8")
    start = "<!-- contract-json:start -->"
    end = "<!-- contract-json:end -->"
    if start not in text or end not in text:
        raise ValueError("docs/dataset-contract.md is missing the contract-json block")
    block = text.split(start, 1)[1].split(end, 1)[0]
    block = block.strip()
    if block.startswith("```json"):
        block = block[len("```json") :].rsplit("```", 1)[0]
    parsed: dict[str, Any] = json.loads(block)
    return parsed


@dataclass
class Check:
    name: str
    passed: bool
    measured: str
    threshold: str
    detail: str = ""


@dataclass
class Report:
    """Collects PASS/FAIL checks and renders the audit table."""

    title: str
    checks: list[Check] = field(default_factory=list)

    def check(
        self,
        name: str,
        passed: bool,
        measured: str,
        threshold: str,
        detail: str = "",
    ) -> bool:
        self.checks.append(Check(name, bool(passed), measured, threshold, detail))
        return bool(passed)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def render(self) -> str:
        lines = [f"# {self.title}", "", "| check | measured | threshold | result |",
                 "| --- | --- | --- | --- |"]
        for c in self.checks:
            result = "PASS" if c.passed else "FAIL"
            detail = f" {c.detail}" if c.detail else ""
            lines.append(f"| {c.name} | {c.measured}{detail} | {c.threshold} | {result} |")
        failed = [c for c in self.checks if not c.passed]
        lines.append("")
        lines.append(f"{len(self.checks) - len(failed)}/{len(self.checks)} checks passed")
        return "\n".join(lines)

    def exit_code(self) -> int:
        return 0 if self.passed else 1


def run_script(title: str, audit: Any, argv: list[str] | None = None) -> int:
    """CLI entry point shared by every audit module."""
    parser = argparse.ArgumentParser(description=title)
    parser.add_argument("--data", default=None, help="dataset directory")
    args = parser.parse_args(argv)
    path = data_dir(args.data)
    report = audit(require_dataset(path))
    print(report.render())
    return report.exit_code()


def skip_if_missing(path: Path) -> None:
    """pytest helper: skip when the dataset has not been generated."""
    if not (path / "manifest.json").exists():
        import pytest

        pytest.skip(f"{path} not generated; run make gen-small / make gen")
