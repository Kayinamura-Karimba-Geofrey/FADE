"""Pydantic v2 configuration for the synthetic transaction generator.

The configuration is the single source of truth for a run: population size,
date range, fraud budget, noise rates, drift and KYC limits.  It round-trips
through YAML (``--config file.yaml``) and is embedded verbatim in
``manifest.json`` so a dataset can always be regenerated exactly.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

#: Segment names the behaviour engine knows how to simulate.
KNOWN_SEGMENTS: frozenset[str] = frozenset(
    {"salaried", "small_trader", "student_parent", "rural_saver", "urban_youth"}
)

#: Default population mix.  Segment shares are fractions of the *whole*
#: population; agents are a separate share, so shares + agent_share == 1.
DEFAULT_SEGMENTS: dict[str, float] = {
    "salaried": 0.25,
    "small_trader": 0.20,
    "student_parent": 0.15,
    "rural_saver": 0.20,
    "urban_youth": 0.15,
}

_DEFAULT_TIERS: dict[int, dict[str, int]] = {
    1: {"daily_limit": 200_000, "balance_cap": 500_000},
    2: {"daily_limit": 1_000_000, "balance_cap": 2_000_000},
    3: {"daily_limit": 5_000_000, "balance_cap": 10_000_000},
}


class NoiseConfig(BaseModel):
    """Delivery-noise rates.  ``event_time`` is never modified - only arrival."""

    model_config = ConfigDict(extra="forbid")

    duplicate_rate: float = 0.005
    delay_rate: float = 0.02
    delay_range_s: tuple[int, int] = (1, 45)
    late_rate: float = 0.001
    late_range_s: tuple[int, int] = (61, 600)

    @field_validator("duplicate_rate", "delay_rate", "late_rate")
    @classmethod
    def _rate_in_unit_interval(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError("rate must be within [0, 1]")
        return v

    @field_validator("delay_range_s", "late_range_s")
    @classmethod
    def _range_ordered(cls, v: tuple[int, int]) -> tuple[int, int]:
        lo, hi = v
        if lo < 0 or hi < lo:
            raise ValueError("range must satisfy 0 <= low <= high")
        return v

    @model_validator(mode="after")
    def _rates_together(self) -> NoiseConfig:
        if self.duplicate_rate + self.delay_rate + self.late_rate > 1.0:
            raise ValueError("duplicate_rate + delay_rate + late_rate must be <= 1")
        return self

    @property
    def max_delay_s(self) -> int:
        """Largest possible arrival shift - sizes the writer reorder window."""
        return max(self.delay_range_s[1], self.late_range_s[1])


class DriftConfig(BaseModel):
    """Post-``start_day`` parameter drift applied to fraud injectors."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    start_day: int = 60
    intensity: float = 0.3

    @field_validator("start_day")
    @classmethod
    def _non_negative_day(cls, v: int) -> int:
        if v < 0:
            raise ValueError("start_day must be >= 0")
        return v

    @field_validator("intensity")
    @classmethod
    def _intensity_range(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError("intensity must be within [0, 1]")
        return v


class TierLimits(BaseModel):
    """Per-KYC-tier wallet limits in RWF."""

    model_config = ConfigDict(extra="forbid")

    daily_limit: int = Field(gt=0)
    balance_cap: int = Field(gt=0)

    @model_validator(mode="after")
    def _cap_covers_limit(self) -> TierLimits:
        if self.balance_cap < self.daily_limit:
            raise ValueError("balance_cap must be >= daily_limit")
        return self


class GeneratorConfig(BaseModel):
    """Full configuration of one generator run."""

    model_config = ConfigDict(extra="forbid")

    seed: int = 42
    accounts: int = 50_000
    days: int = 90
    start_date: date = date(2026, 1, 1)
    fraud_rate: float = 0.004
    fraud_account_share: float = 0.03
    agent_share: float = 0.05
    currency: str = "RWF"
    timezone: str = "Africa/Kigali"
    segments: dict[str, float] = Field(default_factory=lambda: dict(DEFAULT_SEGMENTS))
    noise: NoiseConfig = Field(default_factory=NoiseConfig)
    drift: DriftConfig = Field(default_factory=DriftConfig)
    kyc_tiers: dict[int, TierLimits] = Field(
        default_factory=lambda: {k: TierLimits(**v) for k, v in _DEFAULT_TIERS.items()}
    )

    @field_validator("accounts")
    @classmethod
    def _accounts_positive(cls, v: int) -> int:
        if v < 10:
            raise ValueError("accounts must be >= 10")
        return v

    @field_validator("days")
    @classmethod
    def _days_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("days must be >= 1")
        return v

    @field_validator("fraud_rate")
    @classmethod
    def _fraud_rate_range(cls, v: float) -> float:
        if not 0.0 <= v < 1.0:
            raise ValueError("fraud_rate must be within [0, 1)")
        return v

    @field_validator("fraud_account_share")
    @classmethod
    def _share_range(cls, v: float) -> float:
        if not 0.0 < v <= 1.0:
            raise ValueError("fraud_account_share must be within (0, 1]")
        return v

    @field_validator("agent_share")
    @classmethod
    def _agent_share_range(cls, v: float) -> float:
        if not 0.0 <= v < 1.0:
            raise ValueError("agent_share must be within [0, 1)")
        return v

    @field_validator("segments")
    @classmethod
    def _segments_known(cls, v: dict[str, float]) -> dict[str, float]:
        if not v:
            raise ValueError("segments must not be empty")
        unknown = set(v) - KNOWN_SEGMENTS
        if unknown:
            raise ValueError(f"unknown segments: {sorted(unknown)}")
        for name, share in v.items():
            if not 0.0 <= share <= 1.0:
                raise ValueError(f"segment share for {name!r} must be within [0, 1]")
        return v

    @field_validator("kyc_tiers")
    @classmethod
    def _tiers_valid(cls, v: dict[int, TierLimits]) -> dict[int, TierLimits]:
        if not v:
            raise ValueError("kyc_tiers must not be empty")
        if not set(v) <= {1, 2, 3}:
            raise ValueError("kyc_tiers keys must be a subset of {1, 2, 3}")
        return v

    @model_validator(mode="after")
    def _shares_sum_to_one(self) -> GeneratorConfig:
        # Segment shares are fractions of the whole population; agents take
        # agent_share, so together every account belongs to exactly one group.
        total = sum(self.segments.values()) + self.agent_share
        if abs(total - 1.0) > 1e-9:
            raise ValueError(f"segment shares + agent_share must sum to 1, got {total:.6f}")
        return self

    # ------------------------------------------------------------------ #
    # derived values
    # ------------------------------------------------------------------ #

    @property
    def n_agents(self) -> int:
        """Number of agent accounts in the population."""
        return int(round(self.accounts * self.agent_share))

    @property
    def n_customers(self) -> int:
        """Number of non-agent customer accounts."""
        return self.accounts - self.n_agents

    @property
    def end_date(self) -> date:
        """Exclusive end of the simulated date range."""
        from datetime import timedelta

        return self.start_date + timedelta(days=self.days)

    def tier_limits(self, tier: int) -> TierLimits:
        """Limits for a KYC tier, falling back to the lowest configured tier."""
        try:
            return self.kyc_tiers[tier]
        except KeyError:
            return self.kyc_tiers[min(self.kyc_tiers)]

    def model_dump_jsonable(self) -> dict[str, Any]:
        """Plain-JSON dump (dates as ISO strings) for manifest embedding."""
        return self.model_dump(mode="json")


# --------------------------------------------------------------------------- #
# YAML round-trip
# --------------------------------------------------------------------------- #


def load_config(path: str | Path) -> GeneratorConfig:
    """Load and validate a YAML configuration file."""
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return GeneratorConfig.model_validate(raw)


def dump_config(config: GeneratorConfig) -> str:
    """Serialize a configuration to deterministic YAML text."""
    data = config.model_dump(mode="json")
    return yaml.safe_dump(data, sort_keys=True, default_flow_style=False)


def build_config(
    config_path: str | Path | None = None,
    overrides: dict[str, Any] | None = None,
) -> GeneratorConfig:
    """Build a config from an optional YAML file plus explicit overrides.

    Overrides (CLI flags) win over the file; unset flags are ``None`` and
    are ignored so defaults survive.
    """
    base = load_config(config_path) if config_path else GeneratorConfig()
    clean = {k: v for k, v in (overrides or {}).items() if v is not None}
    if not clean:
        return base
    merged = {**base.model_dump(mode="json"), **clean}
    return GeneratorConfig.model_validate(merged)
