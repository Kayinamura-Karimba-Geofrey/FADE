"""Audit 8 - delivery noise.

Checks duplicate/delay/late rates against the config (within 20% relative),
duplicate byte equality, arrival ordering, one arrivals row per emitted event,
and the per-account reordering depth.

Run:  uv run python -m tests.generator.audit.audit_noise --data data/small
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from tests.generator.audit.common import (
    Report,
    data_dir,
    load_arrivals,
    load_events,
    load_manifest,
    run_script,
)


def _rate_ok(measured: float, target: float) -> bool:
    if target == 0:
        return measured == 0
    return abs(measured - target) / target <= 0.20


def audit(path: Path) -> Report:
    report = Report("Audit 8 - delivery noise")
    manifest = load_manifest(path)
    noise = manifest["config"]["noise"]
    arrivals = load_arrivals(path)
    events = load_events(path)
    unique = events.drop_duplicates("event_id")
    n_unique = len(unique)
    n_events = len(events)

    report.check(
        "arrivals has exactly one row per emitted event (duplicates included)",
        len(arrivals) == n_events and manifest["counts"]["events"] == n_events,
        f"{len(arrivals)} arrivals / {n_events} events",
        "equal",
    )

    seq_ok = arrivals["seq"].tolist() == list(range(n_events))
    arrival_sorted = arrivals["arrival_time"].is_monotonic_increasing
    report.check(
        "events are in arrival order (gapless seq, non-decreasing arrival_time)",
        seq_ok and arrival_sorted,
        f"seq_ok={seq_ok} sorted={arrival_sorted}",
        "both true",
    )

    # First arrival per event_id determines the event's noise category.
    first = arrivals.sort_values("seq", kind="mergesort").drop_duplicates("event_id", keep="first")
    first = first.copy()
    first["delta_s"] = (first["arrival_time"] - first["event_time"]).dt.total_seconds()
    dup_ids = arrivals[arrivals.duplicated("event_id", keep=False)]["event_id"].nunique()
    n_dup_rows = int(manifest["counts"]["duplicates"])
    dup_rate = n_dup_rows / n_unique if n_unique else 0.0
    delay = first[(first["delta_s"] >= 1) & (first["delta_s"] <= 45)]
    late = first[(first["delta_s"] > 60) & (first["delta_s"] <= 300)]
    delay_rate = len(delay) / n_unique
    late_rate = len(late) / n_unique
    report.check(
        "duplicate rate within 20% relative of config",
        _rate_ok(dup_rate, noise["duplicate_rate"]),
        f"{dup_rate:.4%} ({n_dup_rows}/{n_unique})",
        f"{noise['duplicate_rate']:.4%} +/-20%",
    )
    report.check(
        "delayed rate within 20% relative of config",
        _rate_ok(delay_rate, noise["delay_rate"]),
        f"{delay_rate:.4%} ({len(delay)}/{n_unique})",
        f"{noise['delay_rate']:.4%} +/-20%",
    )
    report.check(
        "late rate within 20% relative of config",
        _rate_ok(late_rate, noise["late_rate"]),
        f"{late_rate:.4%} ({len(late)}/{n_unique})",
        f"{noise['late_rate']:.4%} +/-20%",
    )
    report.check(
        "delayed arrivals stay inside delay_range_s",
        len(first[(first["delta_s"] >= 1) & (first["delta_s"] <= noise["delay_range_s"][1])])
        == len(delay),
        f"{len(delay)} delayed events",
        "all within range",
    )
    report.check(
        "late arrivals are >60 s and <=600 s",
        bool((first.loc[first["delta_s"] > 60, "delta_s"] <= 600).all()),
        f"{len(late)} late events",
        "all in (60, 600]",
    )

    # Byte equality of duplicate JSON lines.
    dup_set = set(
        arrivals[arrivals.duplicated("event_id", keep=False)]["event_id"].unique().tolist()
    )
    seen: dict[str, bytes] = {}
    unequal = 0
    compared = 0
    for raw in _iter_raw_lines(path):
        import json

        event_id = json.loads(raw)["event_id"]
        if event_id not in dup_set:
            continue
        if event_id in seen:
            compared += 1
            if seen[event_id] != raw:
                unequal += 1
        else:
            seen[event_id] = raw
    report.check(
        "duplicates are byte-identical to the original",
        unequal == 0 and compared == n_dup_rows,
        f"{compared - unequal}/{compared} pairs equal",
        "0 unequal",
        detail=f" ({n_dup_rows} duplicate rows)",
    )
    report.check(
        "duplicate ids counted consistently",
        dup_ids == n_dup_rows,
        f"{dup_ids} duplicated ids / {n_dup_rows} duplicate rows",
        "equal",
    )

    # Event-time order only breaks where a delivery delay applies.
    ordered = arrivals.sort_values("seq", kind="mergesort")
    on_time = ordered[ordered["arrival_time"] == ordered["event_time"]]
    report.check(
        "on-time rows preserve event_time order",
        on_time["event_time"].is_monotonic_increasing,
        f"{len(on_time)} on-time rows",
        "monotonic",
    )

    # Per-account reordering depth <= 300 s.
    sender = events[["event_id", "sender.account_id"]].drop_duplicates("event_id")
    seq = ordered.merge(sender, on="event_id", how="left")
    seq["event_ns"] = seq["event_time"].astype("int64")
    depth_ns = 0
    for _account, group in seq.groupby("sender.account_id", sort=False):
        values = group.sort_values("seq", kind="mergesort")["event_ns"].to_numpy()
        running = -1
        for value in values:
            if running - value > depth_ns:
                depth_ns = int(running - value)
            if value > running:
                running = int(value)
    depth_s = depth_ns / 1e9
    report.check(
        "per-account out-of-order depth <= 300 s",
        depth_s <= 300.0,
        f"{depth_s:.1f} s",
        "<= 300 s",
    )
    return report


def _iter_raw_lines(path: Path) -> Iterator[bytes]:
    import zstandard as zstd

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
                    yield line
        if pending.strip():
            yield pending


def test_noise() -> None:
    report = audit(data_dir())
    assert report.passed, report.render()


if __name__ == "__main__":
    raise SystemExit(run_script("Audit 8 - delivery noise", audit))
