"""Golden-feature oracle: compare the Python reference with a committed fixture.

Usage:
    python -m tests.golden.oracle
    python -m tests.golden.oracle --update
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from generator.reference_features import (
    FEATURE_FIELDS,
    compare_features,
    read_feature_jsonl,
    run_reference_jsonl,
)

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EVENTS = ROOT / "tests" / "golden" / "events.jsonl"
DEFAULT_EXPECTED = ROOT / "tests" / "golden" / "expected_features.jsonl"
DEFAULT_CONFIG = ROOT / "config" / "features.yaml"


def expected_vectors(
    events_path: Path = DEFAULT_EVENTS,
    config_path: Path = DEFAULT_CONFIG,
) -> list[dict[str, object]]:
    """Compute the reference vectors for the committed golden events."""
    return list(run_reference_jsonl(events_path, config_path))


def check(
    events_path: Path = DEFAULT_EVENTS,
    expected_path: Path = DEFAULT_EXPECTED,
    config_path: Path = DEFAULT_CONFIG,
) -> list[str]:
    expected = read_feature_jsonl(expected_path)
    actual = expected_vectors(events_path, config_path)
    problems = compare_features(expected, actual)
    for vector in expected:
        missing = [name for name in FEATURE_FIELDS if name not in vector]
        if missing:
            problems.append(f"{vector.get('event_id')}: missing fields {missing}")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, default=DEFAULT_EVENTS)
    parser.add_argument("--expected", type=Path, default=DEFAULT_EXPECTED)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--update", action="store_true", help="rewrite the expected fixture")
    args = parser.parse_args(argv)

    if args.update:
        vectors = expected_vectors(args.events, args.config)
        args.expected.write_text(
            "".join(json.dumps(v, separators=(",", ":")) + "\n" for v in vectors),
            encoding="utf-8",
        )
        print(f"updated {args.expected} with {len(vectors)} vectors")
        return 0

    problems = check(args.events, args.expected, args.config)
    if problems:
        print(f"golden feature oracle FAILED ({len(problems)} mismatches)")
        for problem in problems:
            print(" ", problem)
        return 1
    print(f"golden feature oracle PASS ({len(read_feature_jsonl(args.expected))} vectors)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
