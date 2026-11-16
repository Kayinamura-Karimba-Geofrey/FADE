"""Build a leakage-safe, time-split training table for FADE."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from ml.config import (
    BOOL_FEATURES,
    FEATURE_NAMES,
    FEATURE_SCHEMA_VERSION,
    PHASE2_FEATURE_NAMES,
    SEED,
    TARGET_COLUMN,
    encode_category,
    load_vocab,
    split_for_timestamp,
)

FEATURE_COLUMN = "feature_schema_version"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_inputs(data_dir: Path, features_path: Path | None = None) -> pd.DataFrame:
    data_dir = Path(data_dir)
    features_path = features_path or (data_dir / "features.parquet")
    labels_path = data_dir / "labels.parquet"
    events_path = data_dir / "events.parquet"

    features = pq.read_table(features_path).to_pandas()
    labels = pq.read_table(labels_path).to_pandas()
    events = pq.read_table(events_path, columns=["event_id", "event_time", "txn_type", "channel", "amount", "sender.account_id"]).to_pandas()
    events = events.rename(columns={"sender.account_id": "account_id"})
    # Events parquet can contain byte-identical duplicate rows for retried publishes; collapse them.
    events = events.drop_duplicates(subset=["event_id"], keep="first")

    if features["event_id"].duplicated().any():
        raise ValueError("duplicate event_id in feature table")
    if labels["event_id"].duplicated().any():
        raise ValueError("duplicate event_id in label table")
    if events["event_id"].duplicated().any():
        raise ValueError("duplicate event_id in event table")

    frame = features.merge(
        labels[["event_id", "label", "scenario"]],
        on="event_id",
        how="inner",
        validate="one_to_one",
    )
    frame = frame.merge(events, on="event_id", how="left", validate="one_to_one")
    if frame[["event_time", "txn_type", "channel", "account_id", "amount"]].isna().any().any():
        raise ValueError("missing event metadata after join")
    if not set(frame["label"]).issubset({"FRAUD", "LEGIT"}):
        raise ValueError(f"unexpected labels: {sorted(set(frame['label']) - {'FRAUD', 'LEGIT'})}")
    if (frame[FEATURE_COLUMN] != FEATURE_SCHEMA_VERSION).any():
        raise ValueError("feature schema version mismatch")
    return frame


def encode_features(frame: pd.DataFrame) -> pd.DataFrame:
    vocab = load_vocab()
    categories = vocab["categories"]
    out = frame.copy()
    for name in BOOL_FEATURES:
        out[name] = out[name].astype("int8")
    for name in PHASE2_FEATURE_NAMES:
        if name not in out:
            raise ValueError(f"missing Phase 2 feature {name}")
    # Encode event-derived categorical fields with the versioned vocabulary.
    out["txn_type"] = out["txn_type"].map(lambda x: encode_category(x, categories["txn_type"]))
    out["channel"] = out["channel"].map(lambda x: encode_category(x, categories["channel"]))
    out[TARGET_COLUMN] = (out["label"] == "FRAUD").astype("int8")
    # Cast to the model dtypes.
    int32_fields = {
        "velocity_1m", "velocity_10m", "velocity_1h", "distinct_recip_24h", "small_txn_run_1h",
        "time_since_sim_change_min", "acc_per_device_7d", "recip_distinct_senders_24h",
        "recip_cashout_lag_min", "cycle_len_2_4", "account_age_days", "kyc_tier",
        "txn_type", "channel",
    }
    for name in int32_fields:
        out[name] = out[name].astype("int32")
    for name in ("amount_sum_10m", "amount_sum_24h"):
        out[name] = out[name].astype("int64")
    for name in ("amount_vs_avg_30d", "amount_zscore_30d", "near_limit_ratio", "new_geo_km",
                 "inbound_outbound_ratio_1h", "agent_cico_ratio_24h", "agent_float_delta_pct"):
        out[name] = out[name].astype("float32")
    for name in BOOL_FEATURES:
        out[name] = out[name].astype("int8")
    if out[list(FEATURE_NAMES)].isna().any().any():
        raise ValueError("NaN in model features")
    if np.isinf(out[list(FEATURE_NAMES)].select_dtypes(include=[np.number]).to_numpy()).any():
        raise ValueError("Inf in model features")
    return out


def add_splits(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    parsed = pd.to_datetime(out["event_time"], utc=True, errors="raise")
    out["split"] = parsed.map(lambda ts: split_for_timestamp(ts.to_pydatetime()))
    out["event_time_dt"] = parsed
    return out


def build_table(data_dir: Path, features_path: Path | None = None) -> pd.DataFrame:
    frame = encode_features(load_inputs(data_dir, features_path))
    frame = add_splits(frame)
    return frame


def write_table(frame: pd.DataFrame, output: Path) -> None:
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    # Keep metadata columns alongside features for reporting and auditing.
    keep = ["event_id", "event_time", "account_id", "amount", "scenario", "label", "split"] + list(FEATURE_NAMES) + [TARGET_COLUMN]
    frame[keep].to_parquet(output, index=False)


def imbalance_report(frame: pd.DataFrame) -> dict[str, object]:
    report: dict[str, object] = {}
    for split in ("train", "valid", "test", "purge"):
        subset = frame[frame["split"] == split]
        positives = int((subset[TARGET_COLUMN] == 1).sum())
        negatives = int((subset[TARGET_COLUMN] == 0).sum())
        report[split] = {
            "rows": int(len(subset)),
            "fraud": positives,
            "legit": negatives,
            "fraud_rate": float(positives / len(subset)) if len(subset) else 0.0,
        }
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Build FADE ML training table")
    parser.add_argument("--data-dir", type=Path, default=Path("data/run42"))
    parser.add_argument("--features", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    data_dir = args.data_dir
    output = args.output or (data_dir / "ml_table.parquet")
    frame = build_table(data_dir, args.features)
    write_table(frame, output)

    report = imbalance_report(frame)
    manifest = {
        "data_dir": str(data_dir),
        "output": str(output),
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "seed": SEED,
        "splits": report,
        "split_boundaries_utc": {
            key: value.isoformat()
            for key, value in __import__("ml.config", fromlist=["split_boundaries"]).split_boundaries().items()
        },
        "input_hashes": {
            "events": _sha256(data_dir / "events.parquet"),
            "labels": _sha256(data_dir / "labels.parquet"),
            "features": _sha256(args.features or (data_dir / "features.parquet")),
        },
    }
    manifest_path = output.with_suffix(".manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
