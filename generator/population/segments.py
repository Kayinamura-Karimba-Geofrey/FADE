"""Segment and KYC-tier assignment for the customer population."""

from __future__ import annotations

import numpy as np
from numpy.random import Generator

from generator.config import GeneratorConfig

#: KYC tier weights per segment (tiers 1..3).  Traders and salaried
#: customers reach higher tiers; rural customers mostly stay at tier 1.
SEGMENT_KYC_WEIGHTS: dict[str, dict[int, float]] = {
    "salaried": {1: 0.10, 2: 0.50, 3: 0.40},
    "small_trader": {1: 0.30, 2: 0.50, 3: 0.20},
    "student_parent": {1: 0.70, 2: 0.30, 3: 0.00},
    "rural_saver": {1: 0.80, 2: 0.20, 3: 0.00},
    "urban_youth": {1: 0.60, 2: 0.40, 3: 0.00},
}

#: Agents run business wallets: tier 3 limits apply.
AGENT_KYC_TIER = 3


def largest_remainder(n: int, shares: dict[str, float]) -> dict[str, int]:
    """Allocate ``n`` items over ``shares`` (largest-remainder method).

    Guarantees the counts sum to exactly ``n`` with deterministic rounding.
    """
    total = sum(shares.values())
    if total <= 0:
        raise ValueError("shares must sum to a positive value")
    exact = {k: n * (v / total) for k, v in shares.items()}
    counts = {k: int(np.floor(v)) for k, v in exact.items()}
    remainder = n - sum(counts.values())
    order = sorted(exact, key=lambda k: (-(exact[k] % 1), k))
    for key in order[:remainder]:
        counts[key] += 1
    return counts


def assign_segments(rng: Generator, config: GeneratorConfig) -> list[str]:
    """Assign a segment to every customer account (deterministically)."""
    counts = largest_remainder(config.n_customers, config.segments)
    segments: list[str] = []
    for name in sorted(counts):
        segments.extend([name] * counts[name])
    order = rng.permutation(len(segments))
    return [segments[i] for i in order]


def assign_kyc_tiers(rng: Generator, segments: list[str]) -> list[int]:
    """Draw a KYC tier per customer from segment-weighted distributions."""
    tiers: list[int] = []
    for segment in segments:
        weights = SEGMENT_KYC_WEIGHTS[segment]
        tier = int(rng.choice(sorted(weights), p=[weights[t] for t in sorted(weights)]))
        tiers.append(tier)
    return tiers
