"""Determinism tests for the RNG bank."""

from __future__ import annotations

import numpy as np
import pytest

from generator.rng import DEFAULT_COMPONENTS, RngBank


def test_same_seed_same_stream() -> None:
    a = RngBank(42)
    b = RngBank(42)
    for name in DEFAULT_COMPONENTS:
        np.testing.assert_array_equal(a.get(name).random(20), b.get(name).random(20))


def test_different_seed_different_streams() -> None:
    a = RngBank(42)
    b = RngBank(43)
    assert not np.array_equal(a.get("plan").random(20), b.get("plan").random(20))


def test_get_resets_stream() -> None:
    bank = RngBank(7)
    first = bank.get("plan").integers(0, 1000, size=50)
    second = bank.get("plan").integers(0, 1000, size=50)
    np.testing.assert_array_equal(first, second)


def test_components_are_isolated() -> None:
    bank = RngBank(1)
    # drawing heavily from one component must not move another's stream
    baseline = bank.get("noise").random(10)
    other = RngBank(1)
    other.get("plan").random(10_000)
    np.testing.assert_array_equal(baseline, other.get("noise").random(10))


def test_spawn_children_deterministic_and_distinct() -> None:
    a = RngBank(5).spawn("fraud", 3)
    b = RngBank(5).spawn("fraud", 3)
    draws_a = [g.random(5) for g in a]
    draws_b = [g.random(5) for g in b]
    for x, y in zip(draws_a, draws_b, strict=True):
        np.testing.assert_array_equal(x, y)
    assert not np.array_equal(draws_a[0], draws_a[1])


def test_unknown_component_raises() -> None:
    bank = RngBank(1)
    with pytest.raises(KeyError, match="unknown rng component"):
        bank.get("nope")


def test_duplicate_components_rejected() -> None:
    with pytest.raises(ValueError, match="unique"):
        RngBank(1, components=("a", "a"))
