"""Command-line entrypoint for the FADE transaction generator.

Phase 0 ships a version command only; scenario generation lands in later
phases.
"""

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


if __name__ == "__main__":
    app()
