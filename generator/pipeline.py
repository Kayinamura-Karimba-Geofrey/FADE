"""End-to-end generation pipeline: population, planning, fraud, delivery.

Pipeline steps (all deterministic from ``config.seed``):

1. build the population and contact graph (``population`` stream),
2. counting pass over ``plan_day`` (``plan`` stream, reset for pass 2) to
   size the fraud budget,
3. schedule every fraud event (``fraud`` stream),
4. real pass: day loop merging legit plan items and fraud events by
   ``event_time``, emitting through delivery noise into the writers,
5. close files and write ``manifest.json`` (hashes, counts, parameters).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

from generator.behavior.engine import (
    Engine,
    HourlyCums,
    build_plan_tables,
    estimate_legit_events,
    plan_day,
)
from generator.config import GeneratorConfig
from generator.delivery.noise import ArrivalBuffer
from generator.fraud.scenarios import plan_fraud
from generator.io.writers import DatasetWriter, write_accounts
from generator.models import Event, LabelRecord
from generator.population.accounts import CELLS, Ledger, create_accounts
from generator.population.graph import build_world
from generator.rng import RngBank


class DatasetSink:
    """Routes emitted events through the arrival-order noise buffer."""

    def __init__(self, config: GeneratorConfig, out_dir: Path, bank: RngBank) -> None:
        self.config = config
        self.writer = DatasetWriter(out_dir)
        self.buffer = ArrivalBuffer(config.noise, bank.get("noise"))

    def on_event(self, event: Event, label: LabelRecord) -> None:
        self.writer.add_label(label)
        self.buffer.offer(event, label)

    def drain_before(self, ts_ms: int) -> None:
        for event, _label, arrival, is_dup in self.buffer.drain_before(ts_ms):
            self.writer.add_event(event, arrival, is_dup)

    def finish(self) -> dict[str, dict[str, Any]]:
        for event, _label, arrival, is_dup in self.buffer.flush():
            self.writer.add_event(event, arrival, is_dup)
        return self.writer.close()


def run(config: GeneratorConfig, out_dir: Path, *, quiet: bool = True) -> dict[str, Any]:
    """Generate a full dataset into ``out_dir`` and return its manifest."""
    out_dir = Path(out_dir)
    bank = RngBank(config.seed)
    pop_rng = bank.get("population")

    accounts = create_accounts(config, pop_rng)
    world = build_world(config, accounts, pop_rng)
    opening = [a.opening_balance for a in accounts] + [s.opening_balance for s in world.specials]
    ledger = Ledger(opening)
    sink = DatasetSink(config, out_dir, bank)

    weekday_cums = tuple(np.cumsum(a.hourly) for a in accounts)
    weekend_cums = tuple(np.cumsum(a.hourly_weekend) for a in accounts)
    cums: HourlyCums = (weekday_cums, weekend_cums)
    tables = build_plan_tables(world)

    # ---- expected size of the legitimate stream -------------------------
    # Analytic expectation: running a full planning pass just to count events
    # doubled the planner cost without changing the data.
    n_plan = estimate_legit_events(config, world, tables)
    if not quiet:
        print(f"expected legit events: {n_plan}")

    # ---- fraud scheduling ----------------------------------------------
    fplan = plan_fraud(config, world, n_plan, bank.get("fraud"))
    if not quiet:
        print(f"fraud budget: {fplan.n_fraud} events, alloc={fplan.alloc}")

    # ---- emission -------------------------------------------------------
    engine = Engine(config, world, ledger, bank, sink)
    plan_rng = bank.get("plan")
    for day in range(config.days):
        items = plan_day(config, world, day, plan_rng, cums, tables)
        pending = fplan.events_by_day.get(day, [])
        engine.process_day(day, items, pending)
        if not quiet and (day + 1) % max(1, config.days // 5) == 0:
            print(f"day {day + 1}/{config.days}: emitted={engine.emitted}")

    files = sink.finish()
    w = sink.writer

    account_rows = [
        {
            "account_id": a.account_id,
            "msisdn_hash": a.msisdn_hash,
            "segment": a.segment,
            "kyc_tier": a.kyc_tier,
            "created_day": a.created_day,
            "home_cell": CELLS[a.home_cell].cell_id,
            "devices": list(a.devices),
            "sim_serial_hash": a.sim_serial_hash,
            "is_agent": a.is_agent,
            "opening_balance": a.opening_balance,
        }
        for a in accounts
    ]
    files["accounts.parquet"] = write_accounts(out_dir / "accounts.parquet", account_rows)

    n_unique = w.n_labels
    n_fraud = w.label_counts.get("FRAUD", 0)
    manifest: dict[str, Any] = {
        "config": config.model_dump_jsonable(),
        "files": files,
        "counts": {
            "events": w.n_events,
            "events_unique": n_unique,
            "duplicates": w.n_duplicate,
            "labels": dict(sorted(w.label_counts.items())),
            "fraud_rate": round(n_fraud / n_unique, 6) if n_unique else 0.0,
            "per_scenario": dict(sorted(w.scenario_counts.items())),
            "accounts": len(accounts),
            "planned_legit": n_plan,
            "applied": engine.emitted,
            "skipped": engine.skipped,
            "skip_reasons": dict(sorted(engine.skip_reasons.items())),
        },
        "fraud": {
            "budget": fplan.n_fraud,
            "alloc": dict(sorted(fplan.alloc.items())),
            "phase_split": {k: list(v) for k, v in sorted(fplan.phase_split.items())},
            "runs": dict(sorted(fplan.runs.items())),
            "customer_pool": fplan.customer_pool,
            "agent_pool": fplan.agent_pool,
        },
        "noise": {
            "duplicates": sink.buffer.n_duplicate,
            "delays": sink.buffer.n_delay,
            "late": sink.buffer.n_late,
            "normal": sink.buffer.n_normal,
        },
        "specials": [
            {"account_id": s.account_id, "kind": s.kind, "opening_balance": s.opening_balance}
            for s in world.specials
        ],
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest
