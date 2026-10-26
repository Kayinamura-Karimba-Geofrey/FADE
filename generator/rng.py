"""Deterministic randomness for the generator.

Everything the generator draws comes from :class:`numpy.random.Generator`
instances derived from a single :class:`numpy.random.SeedSequence` created
from the run seed.  Each *component* (population, behaviour planning, fraud,
noise, ...) gets its own child ``SeedSequence`` via ``SeedSequence.spawn()``
so that:

* the whole run is reproducible from ``--seed`` alone,
* adding draws in one component cannot shift the stream of another,
* a component can be *reset* to the beginning of its stream (the behaviour
  planner runs twice - once as a cheap counting pass, once for real - and
  must draw identical values in both passes).

No module may use the global ``random`` state, ``time.time()`` or ``uuid4``.
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
from numpy.random import Generator, SeedSequence

#: Ordered component names.  Order matters: ``SeedSequence.spawn`` derives
#: children positionally, so the list must stay stable for a given seed.
DEFAULT_COMPONENTS: tuple[str, ...] = (
    "population",  # accounts, contact graph, agent placement
    "plan",  # per account-day behaviour planning (run twice, reset each pass)
    "fraud",  # fraud scenario parameters (spawned once per scenario)
    "noise",  # delivery-noise draws (duplicate / delay / late)
    "ulid",  # ULID randomness bytes, consumed in event_time order
)


class RngBank:
    """Named, resettable child generators derived from one seed."""

    def __init__(self, seed: int, components: Sequence[str] = DEFAULT_COMPONENTS) -> None:
        if len(set(components)) != len(components):
            raise ValueError("component names must be unique")
        root = SeedSequence(seed)
        children = root.spawn(len(components))
        self._seed = seed
        self._sequences: dict[str, SeedSequence] = dict(zip(components, children, strict=True))

    @property
    def seed(self) -> int:
        """The run seed this bank was created from."""
        return self._seed

    def sequence(self, name: str) -> SeedSequence:
        """The child ``SeedSequence`` of a component (for sub-spawning)."""
        try:
            return self._sequences[name]
        except KeyError:
            raise KeyError(f"unknown rng component {name!r}") from None

    def get(self, name: str) -> Generator:
        """A fresh generator positioned at the start of ``name``'s stream.

        Calling ``get`` twice for the same component returns two independent
        generators with *identical* streams - this is what makes the
        counting pass and the real behaviour pass agree.
        """
        return np.random.default_rng(self.sequence(name))

    def spawn(self, name: str, n: int) -> list[Generator]:
        """Derive ``n`` child generators from component ``name``'s sequence."""
        return [np.random.default_rng(child) for child in self.sequence(name).spawn(n)]
