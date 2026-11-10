"""Compare two feature JSONL streams and exit non-zero on any mismatch."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from generator.reference_features import compare_features


def iter_jsonl(path: str | Path) -> Iterator[dict[str, Any]]:
    with Path(path).open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--actual", type=Path, required=True)
    parser.add_argument("--tolerance", type=float, default=1e-9)
    args = parser.parse_args(argv)

    problems = compare_features(
        iter_jsonl(args.reference), iter_jsonl(args.actual), float_tolerance=args.tolerance
    )
    if problems:
        print(f"parity FAILED ({len(problems)} mismatches)")
        for problem in problems:
            print(" ", problem)
        return 1
    print(f"parity PASS ({args.reference} == {args.actual})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
