"""CLI entrypoint: ``python -m ml.train <command>``."""

from __future__ import annotations

import sys


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: python -m ml.train {build-table,train}", file=sys.stderr)
        raise SystemExit(2)
    cmd = sys.argv[1]
    sys.argv = [sys.argv[0]] + sys.argv[2:]
    if cmd == "build-table":
        from ml.train.dataset import main as _main

        _main()
    elif cmd == "train":
        from ml.train.train import main as _main

        _main()
    else:
        print(f"unknown command: {cmd}", file=sys.stderr)
        raise SystemExit(2)


if __name__ == "__main__":
    main()
