#!/usr/bin/env python3
"""Lightweight docs checks: local links and referenced make targets.

This intentionally does not try to execute every command in the docs. It
catches the common drift: a renamed file, a removed make target, or a link
that only exists in the author's working tree.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = [ROOT / "README.md", *sorted((ROOT / "docs").rglob("*.md"))]
LINK = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
MAKE_REF = re.compile(r"`make ([a-zA-Z0-9_-]+)`")


def make_targets() -> set[str]:
    text = (ROOT / "Makefile").read_text()
    return set(re.findall(r"^([a-zA-Z0-9_-]+):", text, re.MULTILINE))


def main() -> int:
    errors: list[str] = []
    targets = make_targets()
    for path in DOCS:
        text = path.read_text()
        rel = path.relative_to(ROOT)
        for link in LINK.findall(text):
            target = link.split("#", 1)[0].strip()
            if not target or target.startswith(("http://", "https://", "mailto:")):
                continue
            resolved = (path.parent / target).resolve()
            if not resolved.exists():
                errors.append(f"{rel}: missing link target {target}")
        for target in MAKE_REF.findall(text):
            if target not in targets:
                errors.append(f"{rel}: make target `{target}` is not defined")
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"docs-test: {len(DOCS)} documents OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
