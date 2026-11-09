"""Publish a generated dataset to Kafka in simulated arrival order.

The publisher sends *every* row of ``events.jsonl.zst`` (including delivery
duplicates) keyed by ``sender.account_id``.  Because the JSONL stream is
already written in arrival order, and Kafka preserves order per key/partition,
consumers see each account's events in the same order as ``arrivals.parquet``.

Safety rules (spec: Audit 10):

* publishing into a non-empty topic is refused unless ``reset=True``;
* ``reset=True`` deletes and recreates the topic;
* broker problems are retried with exponential backoff and then surfaced as
  :class:`PublisherError` (the CLI exits non-zero);
* ``speed=0`` publishes as fast as possible, ``speed=N`` paces the stream at
  ``N`` times the recorded arrival clock.
"""

from __future__ import annotations

import json
import time
import zlib
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import zstandard as zstd
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

DEFAULT_PARTITIONS = 12
DEFAULT_REPLICATION = 1
PRODUCE_RETRIES = 30
BROKER_RETRIES = 20


class PublisherError(RuntimeError):
    """Raised when a dataset cannot be published safely."""


def _backoff(attempt: int) -> float:
    return min(2.0, 0.1 * (2**attempt))


def _wait_for_broker(admin: AdminClient, timeout: float) -> None:
    """Retry ``list_topics`` until the broker answers or the budget expires."""
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    for attempt in range(BROKER_RETRIES):
        try:
            admin.list_topics(timeout=max(0.5, min(5.0, deadline - time.monotonic())))
            return
        except Exception as exc:  # confluent_kafka.KafkaException and friends
            last = exc
            if time.monotonic() >= deadline:
                break
            time.sleep(_backoff(attempt))
    raise PublisherError(
        f"broker unavailable after {timeout:.0f}s: {last}. "
        "Start Redpanda (make up-infra) and check --brokers."
    )


def _topic_partitions(admin: AdminClient, topic: str) -> list[int] | None:
    """Partition ids for ``topic``, or ``None`` when the topic is absent."""
    metadata = admin.list_topics(timeout=10)
    info = metadata.topics.get(topic)
    if info is None or info.error is not None:
        return None
    return sorted(info.partitions)


def _topic_is_empty(admin: AdminClient, topic: str, brokers: str) -> bool:
    """True when every partition of ``topic`` starts at its high watermark."""
    partitions = _topic_partitions(admin, topic)
    if partitions is None:
        return True
    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"fade-publisher-preflight-{zlib.crc32(topic.encode())}",
            "enable.auto.commit": False,
            "socket.timeout.ms": 5000,
        }
    )
    try:
        tps = [TopicPartition(topic, p) for p in partitions]
        watermarks = consumer.get_watermark_offsets(tps[0], timeout=5)
        for tp in tps[1:]:
            watermarks = consumer.get_watermark_offsets(tp, timeout=5)
            if watermarks[1] > watermarks[0]:
                return False
        return watermarks[1] <= watermarks[0]
    finally:
        consumer.close()


def _reset_topic(admin: AdminClient, topic: str) -> None:
    """Delete (if present) and recreate ``topic`` with a single replica."""
    partitions = _topic_partitions(admin, topic)
    if partitions is not None:
        futures = admin.delete_topics([topic], request_timeout=20)
        for future in futures.values():
            try:
                future.result(timeout=30)
            except Exception as exc:  # deleted between check and call
                if "Unknown topic" not in str(exc):
                    raise PublisherError(f"cannot delete topic {topic!r}: {exc}") from exc
        # The broker needs a moment before the name can be recreated.
        for _ in range(50):
            if _topic_partitions(admin, topic) is None:
                break
            time.sleep(0.2)
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=DEFAULT_PARTITIONS, replication_factor=DEFAULT_REPLICATION)],
        request_timeout=20,
    )
    for future in futures.values():
        try:
            future.result(timeout=30)
        except KafkaException as exc:
            if "already exists" not in str(exc):
                raise PublisherError(f"cannot create topic {topic!r}: {exc}") from exc


def _iter_dataset(out_dir: Path) -> Any:
    """Yield ``(payload_json, key, arrival_time_str)`` in arrival order."""
    arrivals_path = out_dir / "arrivals.parquet"
    events_path = out_dir / "events.jsonl.zst"
    if not arrivals_path.exists() or not events_path.exists():
        raise PublisherError(f"{out_dir} is missing arrivals.parquet or events.jsonl.zst")
    arrivals = pq.read_table(
        arrivals_path, columns=["event_id", "arrival_time", "seq"]
    ).to_pylist()
    dctx = zstd.ZstdDecompressor()
    with dctx.stream_reader(events_path.open("rb")) as reader:
        pending = b""
        rows = 0
        while True:
            chunk = reader.read(1 << 20)
            if not chunk:
                break
            pending += chunk
            while b"\n" in pending:
                line, pending = pending.split(b"\n", 1)
                if not line:
                    continue
                event = json.loads(line)
                if rows >= len(arrivals):
                    raise PublisherError("events.jsonl.zst has more rows than arrivals.parquet")
                arrival = arrivals[rows]
                if arrival["event_id"] != event["event_id"]:
                    raise PublisherError(
                        f"arrival/event mismatch at seq {rows}: "
                        f"{arrival['event_id']} != {event['event_id']}"
                    )
                yield line, event["sender"]["account_id"], arrival["arrival_time"]
                rows += 1
        if pending.strip():
            event = json.loads(pending)
            arrival = arrivals[rows]
            if arrival["event_id"] != event["event_id"]:
                raise PublisherError(f"arrival/event mismatch at seq {rows}")
            yield pending, event["sender"]["account_id"], arrival["arrival_time"]
            rows += 1
    if rows != len(arrivals):
        raise PublisherError(
            f"events.jsonl.zst has {rows} rows, arrivals.parquet has {len(arrivals)}"
        )


def publish_dataset(
    out_dir: Path,
    *,
    brokers: str = "localhost:9092",
    topic: str = "txn.raw",
    reset: bool = False,
    speed: float = 1.0,
    max_seconds: float = 0.0,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Publish a dataset; see the module docstring for semantics."""
    out_dir = Path(out_dir)
    if speed < 0:
        raise PublisherError("--speed must be >= 0 (0 = max)")

    admin = AdminClient({"bootstrap.servers": brokers})
    _wait_for_broker(admin, timeout)

    exists = _topic_partitions(admin, topic) is not None
    if exists and not reset and not _topic_is_empty(admin, topic, brokers):
        raise PublisherError(
            f"topic {topic!r} is not empty; pass --reset to truncate/recreate it"
        )
    if reset:
        _reset_topic(admin, topic)
    elif not exists:
        futures = admin.create_topics(
            [
                NewTopic(
                    topic,
                    num_partitions=DEFAULT_PARTITIONS,
                    replication_factor=DEFAULT_REPLICATION,
                )
            ],
            request_timeout=20,
        )
        for future in futures.values():
            try:
                future.result(timeout=30)
            except KafkaException as exc:
                if "already exists" not in str(exc):
                    raise PublisherError(f"cannot create topic {topic!r}: {exc}") from exc

    producer = Producer(
        {
            "bootstrap.servers": brokers,
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
            "linger.ms": 5,
            "message.timeout.ms": 30_000,
        }
    )
    failures: list[str] = []

    def _delivered(err: Any, _msg: Any) -> None:
        if err is not None:
            failures.append(str(err))

    started = time.monotonic()
    published = 0
    first_arrival: float | None = None
    last_arrival = 0.0
    for payload, key, arrival in _iter_dataset(out_dir):
        if speed > 0:
            from datetime import datetime

            stamp = datetime.fromisoformat(arrival.replace("Z", "+00:00")).timestamp()
            if first_arrival is None:
                first_arrival = stamp
            last_arrival = stamp
            target = (stamp - first_arrival) / speed
            if max_seconds > 0 and target > max_seconds:
                break
            wait = target - (time.monotonic() - started)
            if wait > 0:
                time.sleep(wait)
        for attempt in range(PRODUCE_RETRIES):
            try:
                producer.produce(topic, value=payload, key=key.encode(), callback=_delivered)
                break
            except BufferError:
                producer.poll(0.5)
                time.sleep(_backoff(attempt))
        else:
            raise PublisherError("producer queue full for 30 retries; broker is not draining")
        published += 1
        if published % 5000 == 0:
            producer.poll(0)
            if failures:
                raise PublisherError(f"delivery failed: {failures[0]}")
    remaining = producer.flush(60)
    if remaining:
        raise PublisherError(f"{remaining} messages still undelivered after 60s")
    if failures:
        raise PublisherError(f"delivery failed: {failures[0]}")
    return {
        "published": published,
        "failed": len(failures),
        "elapsed_s": time.monotonic() - started,
        "topic": topic,
        "simulated_s": max(0.0, last_arrival - (first_arrival or last_arrival)),
    }


def consume_topic(
    *,
    brokers: str = "localhost:9092",
    topic: str = "txn.raw",
    expected: int | None = None,
    timeout: float = 30.0,
) -> list[dict[str, Any]]:
    """Read ``topic`` from the beginning and return messages in partition order.

    Each item is ``{"key", "value", "partition", "offset"}``.  The consumer
    keeps polling until ``expected`` messages arrive or the topic has been
    quiet for ``timeout`` seconds.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"fade-audit-{zlib.crc32(topic.encode())}-{time.monotonic_ns()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "socket.timeout.ms": 5000,
        }
    )
    admin = AdminClient({"bootstrap.servers": brokers})
    _wait_for_broker(admin, timeout)
    partitions = _topic_partitions(admin, topic) or []
    consumer.assign([TopicPartition(topic, p, 0) for p in partitions])
    out: list[dict[str, Any]] = []
    idle = time.monotonic()
    while time.monotonic() - idle < timeout:
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if msg.error():
            continue
        idle = time.monotonic()
        out.append(
            {
                "key": msg.key().decode() if msg.key() else None,
                "value": msg.value(),
                "partition": msg.partition(),
                "offset": msg.offset(),
            }
        )
        if expected is not None and len(out) >= expected:
            break
    consumer.close()
    return out


__all__ = ["PublisherError", "consume_topic", "publish_dataset"]
