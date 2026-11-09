"""File writers for the dataset: JSONL.zst, parquet tables, manifest.

Parquet schemas are the public contract (docs/dataset-contract.md); the
column lists below are the single source of truth and are asserted against
the contract in the schema audit.  Everything is written in emission
(= arrival) order; labels are written once per unique event in generation
(event_time) order.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd

from generator.models import Event, LabelRecord, format_event_time

#: Compression level for events.jsonl.zst.  Level 3 is ~25% faster to write
#: than level 6 at this data's entropy and still leaves the file far under
#: the 200 bytes/event budget (Audit 9).
ZSTD_LEVEL = 3

#: Rows per parquet write (memory bound for the 50k x 90d run).
BATCH = 10_000

EVENT_COLUMNS: list[tuple[str, str]] = [
    ("event_id", "string"),
    ("event_time", "string"),
    ("txn_type", "string"),
    ("amount", "int64"),
    ("currency", "string"),
    ("fee", "int64"),
    ("sender.account_id", "string"),
    ("sender.msisdn_hash", "string"),
    ("sender.device_id", "string"),
    ("sender.sim_serial_hash", "string"),
    ("sender.geo.lat", "float64"),
    ("sender.geo.lon", "float64"),
    ("sender.geo.cell_id", "string"),
    ("sender.account_age_days", "int32"),
    ("sender.kyc_tier", "int32"),
    ("sender.balance_before", "int64"),
    ("recipient.account_id", "string"),
    ("recipient.msisdn_hash", "string"),
    ("recipient.is_agent", "bool"),
    ("recipient.first_seen_by_sender", "bool"),
    ("channel", "string"),
    ("agent_id", "string"),
    ("session_id", "string"),
]

LABEL_COLUMNS: list[tuple[str, str]] = [
    ("event_id", "string"),
    ("label", "string"),
    ("scenario", "string"),
    ("scenario_run_id", "string"),
    ("source", "string"),
]

ARRIVAL_COLUMNS: list[tuple[str, str]] = [
    ("event_id", "string"),
    ("event_time", "string"),
    ("arrival_time", "string"),
    ("seq", "int64"),
]

ACCOUNT_COLUMNS: list[tuple[str, str]] = [
    ("account_id", "string"),
    ("msisdn_hash", "string"),
    ("segment", "string"),
    ("kyc_tier", "int32"),
    ("created_day", "int32"),
    ("home_cell", "string"),
    ("devices", "list<string>"),
    ("sim_serial_hash", "string"),
    ("is_agent", "bool"),
    ("opening_balance", "int64"),
]

_ARROW_TYPES = {
    "string": pa.string(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "float64": pa.float64(),
    "bool": pa.bool_(),
    "list<string>": pa.list_(pa.string()),
}


#: The only nullable columns in the contract (plus label scenario fields).
NULLABLE_COLUMNS = frozenset({"agent_id", "scenario", "scenario_run_id"})


def schema_for(columns: list[tuple[str, str]]) -> pa.Schema:
    """Build a pyarrow schema from (name, dtype) pairs.

    Nullability is part of the contract: only ``agent_id`` and the two label
    scenario fields may be null.  pyarrow defaults fields to nullable=True,
    which made the schema audit correctly reject the first build.
    """
    return pa.schema(
        [
            pa.field(name, _ARROW_TYPES[dtype], nullable=name in NULLABLE_COLUMNS)
            for name, dtype in columns
        ]
    )


#: Schemas are immutable and rebuilt per parquet flush otherwise; cache them.
EVENT_SCHEMA = schema_for(EVENT_COLUMNS)
LABEL_SCHEMA = schema_for(LABEL_COLUMNS)
ARRIVAL_SCHEMA = schema_for(ARRIVAL_COLUMNS)
ACCOUNT_SCHEMA = schema_for(ACCOUNT_COLUMNS)

#: Flush buffered JSON lines to zstd once this many bytes have accumulated.
_JSON_FLUSH_BYTES = 1 << 20


def event_row(event: Event) -> dict[str, Any]:
    """Flatten one event into the contract's dotted columns."""
    s = event.sender
    r = event.recipient
    return {
        "event_id": event.event_id,
        "event_time": event.event_time,
        "txn_type": event.txn_type.value,
        "amount": event.amount,
        "currency": event.currency,
        "fee": event.fee,
        "sender.account_id": s.account_id,
        "sender.msisdn_hash": s.msisdn_hash,
        "sender.device_id": s.device_id,
        "sender.sim_serial_hash": s.sim_serial_hash,
        "sender.geo.lat": s.geo.lat,
        "sender.geo.lon": s.geo.lon,
        "sender.geo.cell_id": s.geo.cell_id,
        "sender.account_age_days": s.account_age_days,
        "sender.kyc_tier": s.kyc_tier,
        "sender.balance_before": s.balance_before,
        "recipient.account_id": r.account_id,
        "recipient.msisdn_hash": r.msisdn_hash,
        "recipient.is_agent": r.is_agent,
        "recipient.first_seen_by_sender": r.first_seen_by_sender,
        "channel": event.channel.value,
        "agent_id": event.agent_id,
        "session_id": event.session_id,
    }


def canonical_json(obj: dict[str, Any]) -> bytes:
    """Deterministic one-line JSON encoding (reference encoder)."""
    return json.dumps(obj, separators=(",", ":"), sort_keys=False).encode("utf-8") + b"\n"


def event_json(event: Event) -> bytes:
    """Encode one event as the contract JSON line.

    A hand-written encoder instead of ``json.dumps(event.to_dict())``: the
    generator emits millions of events, and dict construction plus the
    generic encoder was the single largest writer cost in the Audit 9
    profile.  Every encoded value is a generator-controlled identifier,
    enum value, integer or finite float, so no JSON escaping is required;
    the schema audit re-parses every line to prove it.
    """
    s = event.sender
    g = s.geo
    r = event.recipient
    agent = "null" if event.agent_id is None else f'"{event.agent_id}"'
    return (
        f'{{"event_id":"{event.event_id}","event_time":"{event.event_time}",'
        f'"txn_type":"{event.txn_type.value}","amount":{event.amount},'
        f'"currency":"{event.currency}","fee":{event.fee},'
        f'"sender":{{"account_id":"{s.account_id}","msisdn_hash":"{s.msisdn_hash}",'
        f'"device_id":"{s.device_id}","sim_serial_hash":"{s.sim_serial_hash}",'
        f'"geo":{{"lat":{g.lat!r},"lon":{g.lon!r},"cell_id":"{g.cell_id}"}},'
        f'"account_age_days":{s.account_age_days},"kyc_tier":{s.kyc_tier},'
        f'"balance_before":{s.balance_before}}},'
        f'"recipient":{{"account_id":"{r.account_id}","msisdn_hash":"{r.msisdn_hash}",'
        f'"is_agent":{"true" if r.is_agent else "false"},'
        f'"first_seen_by_sender":{"true" if r.first_seen_by_sender else "false"}}},'
        f'"channel":"{event.channel.value}","agent_id":{agent},'
        f'"session_id":"{event.session_id}"}}\n'
    ).encode()


def _file_meta(path: Path) -> dict[str, Any]:
    """sha256 and byte size of a finished file."""
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return {"sha256": digest.hexdigest(), "bytes": path.stat().st_size}


class DatasetWriter:
    """Owns every output file of one generation run."""

    def __init__(self, out_dir: Path) -> None:
        out_dir.mkdir(parents=True, exist_ok=True)
        self.out_dir = out_dir
        self._jsonl_fh = (out_dir / "events.jsonl.zst").open("wb")
        self._comp = zstd.ZstdCompressor(level=ZSTD_LEVEL).stream_writer(self._jsonl_fh)
        self._jsonl_closed = False
        # Batch JSON bytes: one zstd write per ~1 MB instead of per event.
        self._json_buf: list[bytes] = []
        self._json_buf_bytes = 0

        self._events_writer = pq.ParquetWriter(
            out_dir / "events.parquet", EVENT_SCHEMA, compression="zstd"
        )
        self._labels_writer = pq.ParquetWriter(
            out_dir / "labels.parquet", LABEL_SCHEMA, compression="zstd"
        )
        self._arrivals_writer = pq.ParquetWriter(
            out_dir / "arrivals.parquet", ARRIVAL_SCHEMA, compression="zstd"
        )

        # Columnar buffers: appending to per-column lists and building arrow
        # arrays is markedly faster than pa.Table.from_pylist for 50k x 90d.
        self._event_cols: dict[str, list[Any]] = {name: [] for name, _ in EVENT_COLUMNS}
        self._label_cols: dict[str, list[Any]] = {name: [] for name, _ in LABEL_COLUMNS}
        self._arrival_cols: dict[str, list[Any]] = {name: [] for name, _ in ARRIVAL_COLUMNS}

        self.n_events = 0
        self.n_labels = 0
        self.n_duplicate = 0
        self.label_counts: dict[str, int] = {}
        self.scenario_counts: dict[str, int] = {}

    # ------------------------------------------------------------------ #

    def add_label(self, label: LabelRecord) -> None:
        cols = self._label_cols
        cols["event_id"].append(label.event_id)
        cols["label"].append(label.label.value)
        cols["scenario"].append(label.scenario)
        cols["scenario_run_id"].append(label.scenario_run_id)
        cols["source"].append(label.source)
        self.n_labels += 1
        self.label_counts[label.label.value] = self.label_counts.get(label.label.value, 0) + 1
        if label.scenario:
            self.scenario_counts[label.scenario] = self.scenario_counts.get(label.scenario, 0) + 1
        if self.n_labels % BATCH == 0:
            self._flush_labels()

    def add_event(self, event: Event, arrival_ms: int, is_duplicate: bool) -> None:
        payload = event_json(event)
        self._json_buf.append(payload)
        self._json_buf_bytes += len(payload)
        if self._json_buf_bytes >= _JSON_FLUSH_BYTES:
            self._comp.write(b"".join(self._json_buf))
            self._json_buf.clear()
            self._json_buf_bytes = 0
        s = event.sender
        g = s.geo
        r = event.recipient
        cols = self._event_cols
        cols["event_id"].append(event.event_id)
        cols["event_time"].append(event.event_time)
        cols["txn_type"].append(event.txn_type.value)
        cols["amount"].append(event.amount)
        cols["currency"].append(event.currency)
        cols["fee"].append(event.fee)
        cols["sender.account_id"].append(s.account_id)
        cols["sender.msisdn_hash"].append(s.msisdn_hash)
        cols["sender.device_id"].append(s.device_id)
        cols["sender.sim_serial_hash"].append(s.sim_serial_hash)
        cols["sender.geo.lat"].append(g.lat)
        cols["sender.geo.lon"].append(g.lon)
        cols["sender.geo.cell_id"].append(g.cell_id)
        cols["sender.account_age_days"].append(s.account_age_days)
        cols["sender.kyc_tier"].append(s.kyc_tier)
        cols["sender.balance_before"].append(s.balance_before)
        cols["recipient.account_id"].append(r.account_id)
        cols["recipient.msisdn_hash"].append(r.msisdn_hash)
        cols["recipient.is_agent"].append(r.is_agent)
        cols["recipient.first_seen_by_sender"].append(r.first_seen_by_sender)
        cols["channel"].append(event.channel.value)
        cols["agent_id"].append(event.agent_id)
        cols["session_id"].append(event.session_id)

        acols = self._arrival_cols
        acols["event_id"].append(event.event_id)
        acols["event_time"].append(event.event_time)
        # ~97.5% of rows are on time: reuse the event timestamp string rather
        # than formatting the same millisecond a second time.
        acols["arrival_time"].append(
            event.event_time if arrival_ms == event.ts_ms else format_event_time(arrival_ms)
        )
        acols["seq"].append(self.n_events)

        self.n_events += 1
        if is_duplicate:
            self.n_duplicate += 1
        if self.n_events % BATCH == 0:
            self._flush_events()

    # ------------------------------------------------------------------ #

    @staticmethod
    def _table(
        cols: dict[str, list[Any]], columns: list[tuple[str, str]], schema: pa.Schema
    ) -> pa.Table:
        arrays = [pa.array(cols[name], type=_ARROW_TYPES[dtype]) for name, dtype in columns]
        return pa.Table.from_arrays(arrays, schema=schema)

    def _flush_events(self) -> None:
        if self._event_cols["event_id"]:
            self._events_writer.write_table(
                self._table(self._event_cols, EVENT_COLUMNS, EVENT_SCHEMA), row_group_size=BATCH
            )
            for col in self._event_cols.values():
                col.clear()
        if self._arrival_cols["event_id"]:
            self._arrivals_writer.write_table(
                self._table(self._arrival_cols, ARRIVAL_COLUMNS, ARRIVAL_SCHEMA),
                row_group_size=BATCH,
            )
            for col in self._arrival_cols.values():
                col.clear()

    def _flush_labels(self) -> None:
        if self._label_cols["event_id"]:
            self._labels_writer.write_table(
                self._table(self._label_cols, LABEL_COLUMNS, LABEL_SCHEMA), row_group_size=BATCH
            )
            for col in self._label_cols.values():
                col.clear()

    def close(self) -> dict[str, dict[str, Any]]:
        """Close every file and return {name: {sha256, bytes}}."""
        self._flush_events()
        self._flush_labels()
        if self._json_buf:
            self._comp.write(b"".join(self._json_buf))
            self._json_buf.clear()
            self._json_buf_bytes = 0
        self._events_writer.close()
        self._labels_writer.close()
        self._arrivals_writer.close()
        if not self._jsonl_closed:
            self._comp.close()
            self._jsonl_fh.close()
            self._jsonl_closed = True
        meta = {
            "events.jsonl.zst": _file_meta(self.out_dir / "events.jsonl.zst"),
            "events.parquet": _file_meta(self.out_dir / "events.parquet"),
            "labels.parquet": _file_meta(self.out_dir / "labels.parquet"),
            "arrivals.parquet": _file_meta(self.out_dir / "arrivals.parquet"),
        }
        return meta


def write_accounts(path: Path, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Write accounts.parquet (population wallets) and return its meta."""
    table = pa.Table.from_pylist(rows, schema=ACCOUNT_SCHEMA)
    pq.write_table(table, path, compression="zstd")
    return _file_meta(path)
