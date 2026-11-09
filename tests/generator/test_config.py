"""Config validation and YAML round-trip tests."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from pydantic import ValidationError

from generator.config import (
    DEFAULT_SEGMENTS,
    SCENARIO_NAMES,
    GeneratorConfig,
    NoiseConfig,
    TierLimits,
    build_config,
    dump_config,
    load_config,
)


def test_defaults_are_valid() -> None:
    cfg = GeneratorConfig()
    assert cfg.seed == 42
    assert cfg.accounts == 50_000
    assert cfg.days == 90
    assert cfg.start_date == date(2026, 1, 1)
    assert cfg.fraud_rate == 0.004
    assert cfg.fraud_account_share == 0.03
    assert cfg.agent_share == 0.05
    assert cfg.currency == "RWF"
    assert cfg.timezone == "Africa/Kigali"
    assert cfg.segments == DEFAULT_SEGMENTS
    assert sum(cfg.segments.values()) + cfg.agent_share == pytest.approx(1.0)
    assert cfg.n_agents == 2500
    assert cfg.n_customers == 47_500
    assert cfg.noise.duplicate_rate == 0.005
    assert cfg.drift.enabled and cfg.drift.start_day == 60 and cfg.drift.intensity == 0.3
    assert cfg.kyc_tiers[1].daily_limit == 200_000
    assert cfg.kyc_tiers[3].balance_cap == 10_000_000


def test_shares_must_sum_with_agent_share() -> None:
    with pytest.raises(ValidationError, match="must sum to 1"):
        GeneratorConfig(segments={**DEFAULT_SEGMENTS, "salaried": 0.50})


def test_unknown_segment_rejected() -> None:
    with pytest.raises(ValidationError, match="unknown segments"):
        GeneratorConfig(segments={**DEFAULT_SEGMENTS, "tourist": 0.0})


def test_days_must_be_positive() -> None:
    with pytest.raises(ValidationError, match="days"):
        GeneratorConfig(days=0)


def test_accounts_minimum() -> None:
    with pytest.raises(ValidationError, match="accounts"):
        GeneratorConfig(accounts=3)


@pytest.mark.parametrize("rate", [-0.1, 1.0, 1.5])
def test_fraud_rate_bounds(rate: float) -> None:
    with pytest.raises(ValidationError, match="fraud_rate"):
        GeneratorConfig(fraud_rate=rate)


def test_noise_rate_bounds() -> None:
    with pytest.raises(ValidationError, match=r"\[0, 1\]"):
        NoiseConfig(delay_rate=1.5)
    with pytest.raises(ValidationError, match=r"must be <= 1"):
        NoiseConfig(duplicate_rate=0.6, delay_rate=0.5, late_rate=0.1)


def test_noise_range_ordering() -> None:
    with pytest.raises(ValidationError, match="range"):
        NoiseConfig(delay_range_s=(50, 10))
    # Late arrivals use (61, 300] s: inside the documented (60, 600] band and
    # small enough that per-account reordering depth stays <= 300 s (Audit 8).
    assert NoiseConfig().max_delay_s == 300


def test_default_scenario_shares_sum_to_one() -> None:
    cfg = GeneratorConfig()
    assert sum(cfg.scenario_shares.values()) == pytest.approx(1.0)
    assert set(cfg.scenario_shares) == set(SCENARIO_NAMES)


def test_tier_validation() -> None:
    with pytest.raises(ValidationError, match="balance_cap"):
        GeneratorConfig(kyc_tiers={1: TierLimits(daily_limit=500, balance_cap=100)})
    with pytest.raises(ValidationError, match="subset"):
        GeneratorConfig(kyc_tiers={4: TierLimits(daily_limit=500, balance_cap=1000)})


def test_extra_fields_rejected() -> None:
    with pytest.raises(ValidationError):
        GeneratorConfig(bogus=1)  # type: ignore[call-arg]


def test_yaml_round_trip(tmp_path: Path) -> None:
    cfg = GeneratorConfig(seed=7, accounts=1234, days=10)
    path = tmp_path / "cfg.yaml"
    path.write_text(dump_config(cfg), encoding="utf-8")
    assert load_config(path) == cfg


def test_build_config_overrides_win(tmp_path: Path) -> None:
    path = tmp_path / "cfg.yaml"
    path.write_text(dump_config(GeneratorConfig(seed=1)), encoding="utf-8")
    cfg = build_config(path, {"seed": 9, "fraud_rate": None, "days": 5})
    assert cfg.seed == 9
    assert cfg.days == 5
    assert cfg.fraud_rate == 0.004  # None override ignored


def test_build_config_without_file() -> None:
    cfg = build_config(None, {"accounts": 100})
    assert cfg.accounts == 100
    assert cfg.seed == 42
