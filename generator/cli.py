"""Typer CLI for the FADE synthetic transaction generator.

Commands:

* ``run``     - generate a dataset into ``--out``
* ``verify``  - recompute manifest hashes, row counts and file layout
* ``report``  - summarise a dataset (stdout table + ``report.md``)
* ``publish`` - stream a dataset to a Kafka topic (generator.delivery)
* ``features`` - compute the independent Phase 2 reference feature vectors
* ``version`` - print the package version
"""

from __future__ import annotations

from pathlib import Path

import typer

from generator import __version__
from generator.config import build_config

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="FADE synthetic mobile-money transaction generator.",
)


@app.callback()
def main() -> None:
    """Generate synthetic mobile-money transactions for FADE."""


@app.command()
def version() -> None:
    """Print the generator version."""
    typer.echo(__version__)


@app.command()
def run(
    out: Path = typer.Option(..., "--out", help="Output directory for the dataset"),
    config: Path | None = typer.Option(None, "--config", help="YAML config file"),
    seed: int | None = typer.Option(None, "--seed", help="Override the run seed"),
    accounts: int | None = typer.Option(None, "--accounts", help="Population size"),
    days: int | None = typer.Option(None, "--days", help="Simulation length in days"),
    start_date: str | None = typer.Option(None, "--start-date", help="First day, YYYY-MM-DD"),
    fraud_rate: float | None = typer.Option(None, "--fraud-rate", help="Target fraud rate"),
    label_noise: float | None = typer.Option(None, "--label-noise", help="Label flip rate"),
    quiet: bool = typer.Option(False, "--quiet", help="Suppress progress output"),
) -> None:
    """Generate a dataset into --out and write manifest.json."""
    from generator.pipeline import run as run_pipeline

    cfg = build_config(
        config,
        {
            "seed": seed,
            "accounts": accounts,
            "days": days,
            "start_date": start_date,
            "fraud_rate": fraud_rate,
            "label_noise": label_noise,
        },
    )
    manifest = run_pipeline(cfg, out, quiet=quiet)
    counts = manifest["counts"]
    typer.echo(
        f"wrote {counts['events']} events ({counts['events_unique']} unique, "
        f"{counts['duplicates']} duplicates) to {out}"
    )


@app.command()
def verify(
    out: Path = typer.Option(..., "--out", help="Dataset directory to verify"),
) -> None:
    """Recompute file hashes and row counts against manifest.json."""
    from generator.verify import verify_dataset

    ok, checks = verify_dataset(out)
    for name, passed, detail in checks:
        mark = "PASS" if passed else "FAIL"
        typer.echo(f"[{mark}] {name}: {detail}")
    if not ok:
        raise typer.Exit(code=1)
    typer.echo("verify: all checks passed")


@app.command()
def report(
    out: Path = typer.Option(..., "--out", help="Dataset directory to summarise"),
    write_md: bool = typer.Option(True, "--md/--no-md", help="Write report.md"),
) -> None:
    """Print the dataset summary (events, labels, scenarios, noise rates)."""
    from generator.report import build_report

    text = build_report(out)
    typer.echo(text)
    if write_md:
        (Path(out) / "report.md").write_text(text + "\n", encoding="utf-8")


@app.command()
def publish(
    out: Path = typer.Option(..., "--out", help="Dataset directory to publish"),
    brokers: str = typer.Option("localhost:9092", "--brokers", help="Kafka bootstrap servers"),
    topic: str = typer.Option("txn.raw", "--topic", help="Target topic"),
    reset: bool = typer.Option(False, "--reset", help="Delete/recreate a non-empty topic first"),
    speed: float = typer.Option(1.0, "--speed", help="Arrival pacing multiplier (0 = max)"),
    max_seconds: float = typer.Option(
        0.0, "--max-seconds", help="Cap the simulated arrival window (0 = no cap)"
    ),
    timeout: float = typer.Option(60.0, "--timeout", help="Seconds to wait for the broker"),
) -> None:
    """Stream events.jsonl.zst to a Kafka topic in arrival order."""
    from generator.delivery.publisher import publish_dataset

    stats = publish_dataset(
        Path(out),
        brokers=brokers,
        topic=topic,
        reset=reset,
        speed=speed,
        max_seconds=max_seconds,
        timeout=timeout,
    )
    typer.echo(
        f"published {stats['published']} messages to {topic} "
        f"({stats['failed']} failed, {stats['elapsed_s']:.1f}s)"
    )
    if stats["failed"]:
        raise typer.Exit(code=1)


@app.command()
def features(
    out: Path = typer.Option(..., "--out", help="Dataset directory containing events.parquet"),
    config: Path = typer.Option(
        Path("config/features.yaml"), "--config", help="Shared feature YAML"
    ),
    events_jsonl: Path | None = typer.Option(
        None, "--events-jsonl", help="Optional path to export events for the Go parity CLI"
    ),
) -> None:
    """Write features.parquet and features.jsonl from the Python reference."""
    from generator.reference_features import write_features as run_reference

    count = run_reference(out, out, config, events_jsonl)
    typer.echo(f"wrote {count} reference feature vectors to {out}")


__all__ = [
    "app",
    "main",
    "run",
    "version",
    "verify",
    "report",
    "publish",
    "features",
    "__version__",
]

if __name__ == "__main__":  # pragma: no cover - exercised via __main__
    app()
