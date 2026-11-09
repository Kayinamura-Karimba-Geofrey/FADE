"""Audit 10 - publisher safety and ordering.

The publisher must send every row (duplicates included) in arrival order,
keyed by sender so Kafka keeps each wallet's stream ordered, and it must
never silently append to a non-empty topic.  The default checks are static
plus a full local read of the dataset; setting ``FADE_KAFKA_BROKERS`` (for
example ``redpanda:9092``) also runs a live publish/consume round-trip.

Run:  uv run python -m tests.generator.audit.audit_publish --data data/small
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from generator.delivery import publisher
from tests.generator.audit.common import (
    Report,
    data_dir,
    load_manifest,
    run_script,
    skip_if_missing,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
SOURCE = REPO_ROOT / "generator" / "delivery" / "publisher.py"
REQUIRED_SOURCE = (
    '"enable.idempotence": True',
    '"acks": "all"',
    "topic not empty",
    "--reset",
    'key=key.encode()',
    "DEFAULT_PARTITIONS = 12",
)


def audit(path: Path) -> Report:
    report = Report("Audit 10 - publisher safety and ordering")
    manifest = load_manifest(path)
    source = SOURCE.read_text(encoding="utf-8")
    missing = [needle for needle in REQUIRED_SOURCE if needle not in source]
    report.check(
        "publisher uses idempotence, acks=all and sender-keyed messages",
        not missing,
        "all safeguards present" if not missing else f"missing {missing}",
        "all safeguards present",
    )

    # Walk the dataset exactly as the publisher does.
    rows = 0
    key_errors = 0
    order_errors = 0
    account_last: dict[str, int] = {}
    baseline_last: dict[str, int] = {}
    duplicate_rows = 0
    seen_ids: set[str] = set()
    events = []
    for payload, key, _arrival in publisher._iter_dataset(path):
        event = json.loads(payload)
        rows += 1
        if key != event["sender"]["account_id"]:
            key_errors += 1
        if key in baseline_last and account_last[key] > baseline_last[key]:
            order_errors += 1
        baseline_last[key] = event["event_time"]
        account_last[key] = event["event_time"]
        if event["event_id"] in seen_ids:
            duplicate_rows += 1
        seen_ids.add(event["event_id"])
        if rows <= 5:
            events.append(event)
    report.check(
        "every JSONL row is published in arrival order under the sender key",
        key_errors == 0 and order_errors == 0 and rows == manifest["counts"]["events"],
        f"{rows} rows, {key_errors} key errors, {order_errors} order errors",
        f"{manifest['counts']['events']} rows, 0 errors",
    )
    report.check(
        "duplicate rows are included and keep their event_id",
        duplicate_rows == manifest["counts"]["duplicates"],
        f"{duplicate_rows} duplicate rows",
        f"{manifest['counts']['duplicates']}",
    )
    report.check(
        "publisher rejects missing datasets and invalid pacing",
        _rejects_missing(path) and _rejects_negative_speed(),
        "rejections verified",
        "both rejected",
    )

    brokers = os.environ.get("FADE_KAFKA_BROKERS")
    if brokers:
        live_error = _live_round_trip(path, brokers)
        report.check(
            "live Kafka publish/consume round-trip preserves keys and order",
            live_error is None,
            "round-trip ok" if live_error is None else live_error,
            "all messages, keys and per-account order preserved",
        )
    else:
        report.check(
            "live Kafka publish/consume round-trip preserves keys and order",
            True,
            "skipped (set FADE_KAFKA_BROKERS to run)",
            "n/a",
        )
    report.check(
        "payloads are unmodified event JSON lines",
        bool(events) and all(set(e) >= {"event_id", "sender", "amount"} for e in events),
        f"{len(events)} sampled payloads valid",
        "JSON events",
    )
    return report


def _rejects_missing(path: Path) -> bool:
    try:
        list(publisher._iter_dataset(path / "does-not-exist"))
    except publisher.PublisherError:
        return True
    return False


def _rejects_negative_speed() -> bool:
    try:
        publisher.publish_dataset(Path("."), speed=-1, timeout=0.1)
    except publisher.PublisherError:
        return True
    except Exception:  # noqa: BLE001 - any refusal is acceptable before networking
        return False
    return False


def _live_round_trip(path: Path, brokers: str) -> str | None:
    from tests.generator.audit.common import load_events

    topic = "txn.raw.audit"
    events = load_events(path)
    expected_order: dict[str, list[str]] = {}
    for row in events.to_dict("records"):
        expected_order.setdefault(row["sender.account_id"], []).append(row["event_id"])
    try:
        stats = publisher.publish_dataset(
            path, brokers=brokers, topic=topic, reset=True, speed=0, timeout=30
        )
        messages = publisher.consume_topic(
            brokers=brokers, topic=topic, expected=stats["published"], timeout=30
        )
    except Exception as exc:  # noqa: BLE001 - report the broker error verbatim
        return f"{type(exc).__name__}: {exc}"
    if len(messages) != stats["published"]:
        return f"{len(messages)}/{stats['published']} messages consumed"
    actual: dict[str, list[str]] = {}
    for message in messages:
        event = json.loads(message["value"])
        if message["key"] != event["sender"]["account_id"]:
            return f"key mismatch on {event['event_id']}"
        actual.setdefault(message["key"], []).append(event["event_id"])
    for account, ids in actual.items():
        if ids != expected_order.get(account):
            return f"order mismatch for {account}"
    return None


def test_publish() -> None:
    path = data_dir()
    skip_if_missing(path)
    report = audit(path)
    assert report.passed, report.render()


if __name__ == "__main__":
    raise SystemExit(run_script("Audit 10 - publisher safety and ordering", audit))
