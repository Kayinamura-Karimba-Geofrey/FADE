#!/usr/bin/env python3
"""Generate JCS cross-check fixtures with the Python `rfc8785` package.

The Go implementation in internal/canonical is checked against this fixture in
`go test ./internal/canonical`. Regenerate with `make canonical-parity`.

Each case stores the input JSON text and the canonical output computed by the
reference implementation, so the Go test needs no Python at run time.
"""

from __future__ import annotations

import json
import math
import random
import struct
import sys
from pathlib import Path

import rfc8785

OUT = Path(__file__).resolve().parents[1] / "internal" / "canonical" / "testdata" / "jcs_cases.json"

# RFC 8785 Appendix B style vectors plus the corner cases that differ between
# Go's encoding/json and ECMAScript number formatting.
FIXED_INPUTS = [
    "{}",
    "[]",
    '{"b":1,"a":2}',
    '{"a":{"c":3,"b":[true,false,null]}}',
    '{"key":"value","nested":{"arr":[1,2,3]}}',
    '{"unicode":"é","emoji":"😀","control":"\\u0001\\u001f"}',
    '{"quote":"\\"","backslash":"\\\\","tab":"\\t"}',
    '{"n":1e21}',
    '{"n":1e-7}',
    '{"n":1e-6}',
    '{"n":0.000001}',
    '{"n":123.456}',
    '{"n":-0.0}',
    '{"n":9007199254740991}',
    '{"n":1.7976931348623157e308}',
    '{"n":5e-324}',
    '{"a":1,"A":2,"á":3,"ä":4,"z":5}',
    '{"\\u00e9":1,"e":2}',
    '{"nested":{"a":[{"b":1},{"b":2}]}}',
]


def rand_string(rng: random.Random) -> str:
    alphabet = "abcXYZ019 _-éü€😀\t\n\"\\"
    return "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 12)))


def rand_number(rng: random.Random):
    kind = rng.randrange(6)
    if kind == 0:
        return rng.randint(-(2**31), 2**31)
    if kind == 1:
        return rng.randint(-(10**6), 10**6)
    if kind == 2:
        return struct.unpack(">d", struct.pack(">Q", rng.getrandbits(64)))[0] / 10 ** rng.randint(0, 12)
    if kind == 3:
        return rng.choice([0, 0.0, -0.0, 1e-7, 1e21, 1.5, -2.25, 1e-6])
    if kind == 4:
        return rng.randint(0, 1) / rng.choice([1, 3, 7, 10, 1000])
    return rng.uniform(-1e6, 1e6)


def rand_value(rng: random.Random, depth: int = 0):
    choices = ["null", "bool", "num", "str"]
    if depth < 3:
        choices += ["obj", "arr"]
    kind = rng.choice(choices)
    if kind == "null":
        return None
    if kind == "bool":
        return rng.choice([True, False])
    if kind == "num":
        return rand_number(rng)
    if kind == "str":
        return rand_string(rng)
    if kind == "arr":
        return [rand_value(rng, depth + 1) for _ in range(rng.randint(0, 5))]
    return {rand_string(rng) or f"k{i}": rand_value(rng, depth + 1) for i in range(rng.randint(0, 5))}


def main() -> int:
    rng = random.Random(20261116)
    cases: list[dict[str, str]] = []
    for text in FIXED_INPUTS:
        obj = json.loads(text)
        cases.append({"input": text, "expected": rfc8785.dumps(obj).decode("utf-8")})

    generated = 0
    while generated < 1000:
        value = rand_value(rng)
        # Skip values the reference implementation rejects (NaN/Inf from random
        # doubles) and duplicates that JSON encoded silently drops.
        try:
            text = json.dumps(value, ensure_ascii=False, allow_nan=False)
            obj = json.loads(text)
            expected = rfc8785.dumps(obj).decode("utf-8")
        except Exception:
            continue
        cases.append({"input": text, "expected": expected})
        generated += 1

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({"reference": f"rfc8785=={getattr(rfc8785, '__version__', '0.1.4')}", "cases": cases}, ensure_ascii=False, indent=0) + "\n", encoding="utf-8")
    print(f"wrote {len(cases)} cases to {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
