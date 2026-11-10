"""Audit 3 - schema contract.

Validates *every* line of events.jsonl.zst against the strict pydantic model,
checks the cross-field rules in the spec (ULID timestamp, hashes, geo, KYC,
session, amounts), and proves the Parquet files match docs/dataset-contract.md
and the JSONL stream row for row.

Run:  uv run python -m tests.generator.audit.audit_schema --data data/small
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import ValidationError
from ulid import ULID

from generator.io.writers import (
    ACCOUNT_COLUMNS,
    ARRIVAL_COLUMNS,
    EVENT_COLUMNS,
    LABEL_COLUMNS,
)
from generator.schema import EventModel
from tests.generator.audit.common import (
    Report,
    data_dir,
    iter_jsonl,
    load_labels,
    load_manifest,
    read_contract_block,
    run_script,
)


def _arrow_dtype(t: pa.DataType) -> str:
    if pa.types.is_string(t):
        return "string"
    if pa.types.is_int64(t):
        return "int64"
    if pa.types.is_int32(t):
        return "int32"
    if pa.types.is_float64(t):
        return "float64"
    if pa.types.is_boolean(t):
        return "bool"
    if pa.types.is_list(t) and pa.types.is_string(t.value_type):
        return "list<string>"
    return str(t)


def _check_parquet_contract(path: Path, contract: dict[str, Any]) -> list[str]:
    problems: list[str] = []
    for filename, expected in contract.items():
        pf = pq.ParquetFile(path / filename)
        actual = pf.schema_arrow
        expected_names = [c["name"] for c in expected]
        if actual.names != expected_names:
            problems.append(f"{filename}: columns {actual.names} != {expected_names}")
            continue
        for field, spec in zip(actual, expected, strict=True):
            dtype = _arrow_dtype(field.type)
            if dtype != spec["dtype"]:
                problems.append(f"{filename}.{field.name}: dtype {dtype} != {spec['dtype']}")
            if field.nullable != spec["nullable"]:
                problems.append(
                    f"{filename}.{field.name}: nullable {field.nullable} != {spec['nullable']}"
                )
    return problems


def _check_code_matches_contract(contract: dict[str, Any]) -> list[str]:
    declared = {
        "events.parquet": EVENT_COLUMNS,
        "labels.parquet": LABEL_COLUMNS,
        "arrivals.parquet": ARRIVAL_COLUMNS,
        "accounts.parquet": ACCOUNT_COLUMNS,
    }
    problems: list[str] = []
    for filename, columns in declared.items():
        expected = [(c["name"], c["dtype"]) for c in contract[filename]]
        if list(columns) != expected:
            problems.append(f"{filename}: writer columns differ from docs/dataset-contract.md")
    return problems


def audit(path: Path) -> Report:
    report = Report("Audit 3 - schema contract")
    contract = read_contract_block()

    problems = _check_code_matches_contract(contract)
    report.check(
        "writer column lists match docs/dataset-contract.md",
        not problems,
        "match" if not problems else "; ".join(problems[:3]),
        "identical",
    )

    schema = EventModel.model_json_schema()
    strict = schema.get("additionalProperties") is False
    report.check(
        "JSON Schema generated from pydantic is strict (additionalProperties=false)",
        strict,
        f"additionalProperties={schema.get('additionalProperties')}",
        "false",
    )

    violations: Counter[str] = Counter()
    examples: list[str] = []
    n = 0
    ulid_mismatch = 0
    for event in iter_jsonl(path):
        n += 1
        try:
            model = EventModel.model_validate(event)
        except ValidationError as exc:
            violations["pydantic"] += 1
            if len(examples) < 5:
                examples.append(str(exc).splitlines()[0])
            continue
        # Cross-field checks not expressible in JSON Schema.
        if model.amount <= 0:
            violations["amount"] += 1
        if model.fee < 0:
            violations["fee"] += 1
        if model.currency != "RWF":
            violations["currency"] += 1
        if not model.session_id:
            violations["session_id"] += 1
        if not (
            model.sender.msisdn_hash.startswith("h_")
            and model.sender.sim_serial_hash.startswith("sim_")
        ):
            violations["hash_prefix"] += 1
        if model.sender.kyc_tier not in (1, 2, 3):
            violations["kyc_tier"] += 1
        stamp = datetime.strptime(model.event_time, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
        if int(stamp.timestamp() * 1000) != int(ULID.from_str(model.event_id).timestamp * 1000):
            ulid_mismatch += 1
        if ulid_mismatch and len(examples) < 5:
            examples.append(f"ULID timestamp != event_time at {model.event_id}")

    report.check(
        "every JSONL event validates against strict EventModel",
        violations["pydantic"] == 0,
        f"{n - violations['pydantic']}/{n} valid",
        "0 violations",
        detail=f" ({examples[0]})" if examples and violations["pydantic"] else "",
    )
    report.check(
        "amount/fee/currency/session/hash/kyc field rules",
        not any(
            violations[k]
            for k in ("amount", "fee", "currency", "session_id", "hash_prefix", "kyc_tier")
        ),
        ", ".join(
            f"{k}={v}" for k, v in sorted(violations.items()) if k != "pydantic"
        )
        or "0",
        "0 violations",
    )
    report.check(
        "ULID timestamp component == event_time (ms)",
        ulid_mismatch == 0,
        f"{n - ulid_mismatch}/{n}",
        "0 mismatches",
    )

    parquet_problems = _check_parquet_contract(path, contract)
    report.check(
        "Parquet columns, dtypes and nullability match the contract",
        not parquet_problems,
        "match" if not parquet_problems else "; ".join(parquet_problems[:3]),
        "0 differences",
    )

    # Same events, same order: stream both sides in batches.
    order_mismatch = 0
    compared = 0
    pf = pq.ParquetFile(path / "events.parquet")
    batch_iter = pf.iter_batches(batch_size=50_000, columns=["event_id"])
    jsonl_iter = iter_jsonl(path)
    for batch in batch_iter:
        ids = batch.column(0).to_pylist()
        for pid in ids:
            try:
                obj = next(jsonl_iter)
            except StopIteration:
                order_mismatch += 1
                break
            if obj.get("event_id") != pid:
                order_mismatch += 1
            compared += 1
        if order_mismatch:
            break
    if not order_mismatch:
        for _ in jsonl_iter:
            order_mismatch += 1
            break
    report.check(
        "Parquet and JSONL contain the same events in the same order",
        order_mismatch == 0 and compared == n,
        f"{compared - order_mismatch}/{n} event_ids equal",
        "0 mismatches",
    )

    labels = load_labels(path)
    unique_ids = n - int(load_manifest(path)["counts"]["duplicates"])
    report.check(
        "labels cover every unique event exactly once",
        len(labels) == unique_ids and labels["event_id"].is_unique,
        f"{len(labels)} label rows / {unique_ids} unique events",
        "equal and unique",
    )
    return report


def test_schema() -> None:
    report = audit(data_dir())
    assert report.passed, report.render()


if __name__ == "__main__":
    raise SystemExit(run_script("Audit 3 - schema contract", audit))
