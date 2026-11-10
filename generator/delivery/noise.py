"""Delivery noise: duplicates, delays and late arrivals, in arrival order.

``event_time`` is never modified - only ``arrival_time``.  Categories are
drawn exclusively so the realised rates match the config exactly:

* ``duplicate_rate``  - a byte-identical second copy of the event, arriving
  1-2000 ms after the original (same ``event_id``),
* ``delay_rate``      - arrival ``event_time + U[1, 45] s``,
* ``late_rate``       - arrival ``event_time + U[61, 300] s`` (the upper
  bound keeps the per-account out-of-order depth <= 300 s - see
  docs/phase-1-audit.md),
* otherwise normal    - arrival == event_time.

Category assignment is **stratified**, not i.i.d.: the stream is cut into
blocks of ``NOISE_BLOCK`` events and each block receives exactly
``block_size * rate`` (cumulative-rounding) slots of every category, placed
at random positions within the block.  Realised rates therefore match the
config to within one block of rounding instead of drifting by binomial
noise (an i.i.d. draw put the 0.1% late rate 22% below target on
data/small, which Audit 8 correctly rejected).  Random placement keeps the
noise uncorrelated with event content.

Events are offered in event_time order; a min-heap keyed by arrival time
emits them in arrival order using a watermark of the next event's
timestamp, so an event can never be overtaken by a future one.
"""

from __future__ import annotations

import heapq
from math import floor

from numpy.random import Generator

from generator.config import NoiseConfig
from generator.models import Event, LabelRecord

_MS = 1000

#: Stratification block for exact delivery-noise rates.
NOISE_BLOCK = 1_000


class ArrivalBuffer:
    """Turns a stream in event_time order into a stream in arrival order."""

    def __init__(self, noise: NoiseConfig, rng: Generator) -> None:
        self.noise = noise
        self.rng = rng
        self._buf: list[tuple[int, int, Event, LabelRecord, bool]] = []
        self._seq = 0
        self.n_duplicate = 0
        self.n_delay = 0
        self.n_late = 0
        self.n_normal = 0
        self._labels: list[str] = []
        self._block_start = 0
        # Random phase per category keeps the cumulative rounding from
        # locking every block to the same offset.
        self._phase = {"duplicate": float(rng.random()), "delay": float(rng.random()),
                       "late": float(rng.random())}

    def _refill(self) -> None:
        """Queue one block of category labels with exact cumulative counts."""
        noise = self.noise
        start, end = self._block_start, self._block_start + NOISE_BLOCK
        rates = {
            "duplicate": noise.duplicate_rate,
            "delay": noise.delay_rate,
            "late": noise.late_rate,
        }
        counts: dict[str, int] = {}
        for name, rate in rates.items():
            phase = self._phase[name]
            counts[name] = int(floor(end * rate + phase) - floor(start * rate + phase))
        excess = sum(counts.values()) - NOISE_BLOCK
        if excess > 0:  # cumulative rounding can overshoot by one or two
            counts["late"] = max(0, counts["late"] - min(excess, counts["late"]))
            excess = sum(counts.values()) - NOISE_BLOCK
        if excess > 0:  # pragma: no cover - rates <= 1 keeps this unreachable
            counts["duplicate"] = max(0, counts["duplicate"] - excess)
        labels = (
            ["duplicate"] * counts["duplicate"]
            + ["delay"] * counts["delay"]
            + ["late"] * counts["late"]
        )
        labels += ["normal"] * (NOISE_BLOCK - len(labels))
        self.rng.shuffle(labels)
        self._labels = labels
        self._block_start = end

    def _next_category(self) -> str:
        if not self._labels:
            self._refill()
        return self._labels.pop()

    def offer(self, event: Event, label: LabelRecord) -> None:
        """Queue one event (plus its duplicate, if drawn)."""
        noise = self.noise
        category = self._next_category()
        arrival = event.ts_ms
        if category == "duplicate":
            self.n_duplicate += 1
            dup_gap = int(self.rng.integers(1, 2001))  # 1-2000 ms after original
            self._push(arrival, event, label, False)
            self._push(arrival + dup_gap, event, label, True)
            return
        if category == "delay":
            self.n_delay += 1
            arrival += int(self.rng.integers(noise.delay_range_s[0], noise.delay_range_s[1] + 1)) * _MS
        elif category == "late":
            self.n_late += 1
            arrival += int(self.rng.integers(noise.late_range_s[0], noise.late_range_s[1] + 1)) * _MS
        else:
            self.n_normal += 1
        self._push(arrival, event, label, False)

    def _push(self, arrival: int, event: Event, label: LabelRecord, is_dup: bool) -> None:
        self._seq += 1
        heapq.heappush(self._buf, (arrival, self._seq, event, label, is_dup))

    def drain_before(self, ts_ms: int) -> list[tuple[Event, LabelRecord, int, bool]]:
        """Emit everything whose arrival is strictly before ``ts_ms``."""
        out: list[tuple[Event, LabelRecord, int, bool]] = []
        while self._buf and self._buf[0][0] < ts_ms:
            arrival, _, event, label, is_dup = heapq.heappop(self._buf)
            out.append((event, label, arrival, is_dup))
        return out

    def flush(self) -> list[tuple[Event, LabelRecord, int, bool]]:
        """Emit everything still buffered, in arrival order."""
        out: list[tuple[Event, LabelRecord, int, bool]] = []
        while self._buf:
            arrival, _, event, label, is_dup = heapq.heappop(self._buf)
            out.append((event, label, arrival, is_dup))
        return out
