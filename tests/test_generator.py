"""Tests for the generator package (Phase 0: version command only)."""

from typer.testing import CliRunner

from generator import __version__
from generator.__main__ import app

runner = CliRunner()


def test_version_command_prints_version() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.output.strip() == __version__


def test_no_args_shows_help() -> None:
    result = runner.invoke(app, [])
    # invoking the group without arguments prints full usage help
    assert result.exit_code == 0
    assert "Usage" in result.output
    assert "version" in result.output
