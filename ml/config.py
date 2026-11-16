"""Shared configuration for FADE Phase 4 training and serving.

The feature order here is frozen alongside ``config/ml_vocab.yaml``. Every
artifact writes the expanded contract to ``features.json`` and the Go client
checks the same order at build time.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml

ROOT = Path(__file__).resolve().parents[1]
VOCAB_PATH = ROOT / "config" / "ml_vocab.yaml"
MODELS_DIR = ROOT / "ml" / "models"
REPORTS_DIR = ROOT / "ml" / "reports"
FEATURE_SCHEMA_VERSION = "1.0.0"
TARGET_COLUMN = "y"
SEED = 20261111


@dataclass(frozen=True)
class FeatureSpec:
    name: str
    dtype: str
    default: float | int
    vocab: str | None = None

    def manifest(self, vocab: dict[str, dict[str, int]] | None = None) -> dict[str, Any]:
        return {
            "name": self.name,
            "dtype": self.dtype,
            "default": self.default,
            "vocab": vocab.get(self.vocab) if self.vocab and vocab else None,
        }


# Order is normative. Numeric Phase 2 features first, then encoded raw fields.
FEATURES: tuple[FeatureSpec, ...] = (
    FeatureSpec("velocity_1m", "int32", 1),
    FeatureSpec("velocity_10m", "int32", 1),
    FeatureSpec("velocity_1h", "int32", 1),
    FeatureSpec("amount_sum_10m", "int64", 0),
    FeatureSpec("amount_sum_24h", "int64", 0),
    FeatureSpec("distinct_recip_24h", "int32", 1),
    FeatureSpec("amount_vs_avg_30d", "float32", 1.0),
    FeatureSpec("amount_zscore_30d", "float32", 0.0),
    FeatureSpec("near_limit_ratio", "float32", 0.0),
    FeatureSpec("small_txn_run_1h", "int32", 0),
    FeatureSpec("new_device", "int8", 0),
    FeatureSpec("new_geo_km", "float32", 0.0),
    FeatureSpec("sim_changed_72h", "int8", 0),
    FeatureSpec("time_since_sim_change_min", "int32", -1),
    FeatureSpec("acc_per_device_7d", "int32", 1),
    FeatureSpec("recip_new_for_sender", "int8", 1),
    FeatureSpec("recip_distinct_senders_24h", "int32", 0),
    FeatureSpec("recip_cashout_lag_min", "int32", -1),
    FeatureSpec("inbound_outbound_ratio_1h", "float32", 1.0),
    FeatureSpec("night_hour", "int8", 0),
    FeatureSpec("cycle_len_2_4", "int32", 0),
    FeatureSpec("agent_cico_ratio_24h", "float32", 0.0),
    FeatureSpec("agent_float_delta_pct", "float32", 0.0),
    FeatureSpec("account_age_days", "int32", 0),
    FeatureSpec("kyc_tier", "int32", 0),
    FeatureSpec("txn_type", "int32", 0, vocab="txn_type"),
    FeatureSpec("channel", "int32", 0, vocab="channel"),
)
FEATURE_NAMES: tuple[str, ...] = tuple(f.name for f in FEATURES)
BOOL_FEATURES = frozenset({"new_device", "sim_changed_72h", "recip_new_for_sender", "night_hour"})

# The 25 Phase 2 fields before txn_type/channel are joined by event_id.
PHASE2_FEATURE_NAMES: tuple[str, ...] = FEATURE_NAMES[:25]
JOIN_COLUMNS = ("event_id", "event_time", "account_id", "scenario", "txn_type", "channel")


@dataclass(frozen=True)
class SplitConfig:
    start_date: str = "2026-01-01"
    timezone: str = "Africa/Kigali"
    train_days: tuple[int, int] = (1, 56)
    valid_days: tuple[int, int] = (57, 70)
    test_days: tuple[int, int] = (71, 90)
    purge_hours: int = 24


SPLITS = SplitConfig()


def load_vocab(path: Path = VOCAB_PATH) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict) or "categories" not in data:
        raise ValueError(f"{path}: missing categories")
    return data


def encode_category(value: object, table: dict[str, int]) -> int:
    return int(table.get(str(value), 0))


def split_boundaries(cfg: SplitConfig = SPLITS) -> dict[str, datetime]:
    """Return UTC boundaries. Purge removes the final 24h of train and valid."""
    tz = ZoneInfo(cfg.timezone)
    start = datetime.fromisoformat(cfg.start_date).replace(tzinfo=tz).astimezone(UTC)
    train_end = start + timedelta(days=cfg.train_days[1])
    valid_start = start + timedelta(days=cfg.valid_days[0] - 1)
    valid_end = start + timedelta(days=cfg.valid_days[1])
    test_start = start + timedelta(days=cfg.test_days[0] - 1)
    test_end = start + timedelta(days=cfg.test_days[1])
    purge = timedelta(hours=cfg.purge_hours)
    return {
        "dataset_start": start,
        "train_start": start,
        "train_end": train_end - purge,
        "train_purge_end": train_end,
        "valid_start": valid_start,
        "valid_end": valid_end - purge,
        "valid_purge_end": valid_end,
        "test_start": test_start,
        "test_end": test_end,
    }


def split_for_timestamp(value: datetime, cfg: SplitConfig = SPLITS) -> str:
    b = split_boundaries(cfg)
    if value < b["train_end"]:
        return "train"
    if value < b["train_purge_end"]:
        return "purge"
    if b["valid_start"] <= value < b["valid_end"]:
        return "valid"
    if value < b["valid_purge_end"]:
        return "purge"
    if b["test_start"] <= value < b["test_end"]:
        return "test"
    if value < b["dataset_start"]:
        return "pre"
    return "post"


def feature_manifest(vocab: dict[str, Any] | None = None) -> dict[str, Any]:
    vocab = vocab or load_vocab()
    return {
        "schema_version": FEATURE_SCHEMA_VERSION,
        "features": [f.manifest(vocab["categories"]) for f in FEATURES],
    }


def model_dir(version: str, root: Path = MODELS_DIR) -> Path:
    return root / version


def version_name(prefix: str, year: int, month: int, revision: int) -> str:
    return f"{prefix}-{year:04d}.{month:02d}.{revision}"
