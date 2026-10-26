"""Typer CLI for the FADE synthetic transaction generator.

Commands:

* ``run``     - generate a dataset into ``--out``
* ``publish`` - stream a dataset to a Kafka topic
* ``report``  - summarise a dataset (stdout table + ``report.md``)
* ``verify``  - recompute manifest hashes and check every invariant
* ``version`` - print the package version
"""

from __future__ import annotations

import typer

from generator import __version__

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


# `run`, `publish`, `report` and `verify` are attached in generator.cli once
# the pipeline modules land (Phase 1 steps 7-10); keeping the app object here
# lets `python -m generator version` work from day one.

__all__ = ["app", "main", "version", "__version__"]

if __name__ == "__main__":  # pragma: no cover - exercised via __main__
    app()
