#!/usr/bin/env python3
"""Dependency-free PNG line-chart renderer for benchmark/chaos timelines.

The release environment cannot assume matplotlib, so this module renders a
simple but readable multi-series line chart with nothing but the standard
library (struct + zlib).  It is used by the chaos harness to turn the exported
Prometheus CSV timeline into a committed PNG.

Usage:
  python3 scripts/plot_png.py --csv timeline.csv --out timeline.png \
      --title "C1 Redis kill" --y-label "p99 ms"
"""
from __future__ import annotations

import argparse
import zlib
from pathlib import Path

WIDTH, HEIGHT = 900, 420
MARGIN_L, MARGIN_R, MARGIN_T, MARGIN_B = 70, 20, 40, 50

FONT = {
    "0": ["01110", "10001", "10011", "10101", "11001", "10001", "01110"],
    "1": ["00100", "01100", "00100", "00100", "00100", "00100", "01110"],
    "2": ["01110", "10001", "00001", "00010", "00100", "01000", "11111"],
    "3": ["11110", "00001", "00001", "01110", "00001", "00001", "11110"],
    "4": ["00010", "00110", "01010", "10010", "11111", "00010", "00010"],
    "5": ["11111", "10000", "11110", "00001", "00001", "10001", "01110"],
    "6": ["00110", "01000", "10000", "11110", "10001", "10001", "01110"],
    "7": ["11111", "00001", "00010", "00100", "01000", "01000", "01000"],
    "8": ["01110", "10001", "10001", "01110", "10001", "10001", "01110"],
    "9": ["01110", "10001", "10001", "01111", "00001", "00010", "01100"],
    ".": ["00000", "00000", "00000", "00000", "00000", "00110", "00110"],
    "-": ["00000", "00000", "00000", "11111", "00000", "00000", "00000"],
    ":": ["00000", "00110", "00110", "00000", "00110", "00110", "00000"],
    " ": ["00000", "00000", "00000", "00000", "00000", "00000", "00000"],
    "A": ["01110", "10001", "10001", "11111", "10001", "10001", "10001"],
    "B": ["11110", "10001", "10001", "11110", "10001", "10001", "11110"],
    "C": ["01110", "10001", "10000", "10000", "10000", "10001", "01110"],
    "D": ["11110", "10001", "10001", "10001", "10001", "10001", "11110"],
    "E": ["11111", "10000", "10000", "11110", "10000", "10000", "11111"],
    "F": ["11111", "10000", "10000", "11110", "10000", "10000", "10000"],
    "G": ["01110", "10001", "10000", "10111", "10001", "10001", "01110"],
    "H": ["10001", "10001", "10001", "11111", "10001", "10001", "10001"],
    "I": ["01110", "00100", "00100", "00100", "00100", "00100", "01110"],
    "J": ["00111", "00010", "00010", "00010", "00010", "10010", "01100"],
    "K": ["10001", "10010", "10100", "11000", "10100", "10010", "10001"],
    "L": ["10000", "10000", "10000", "10000", "10000", "10000", "11111"],
    "M": ["10001", "11011", "10101", "10101", "10001", "10001", "10001"],
    "N": ["10001", "11001", "10101", "10011", "10001", "10001", "10001"],
    "O": ["01110", "10001", "10001", "10001", "10001", "10001", "01110"],
    "P": ["11110", "10001", "10001", "11110", "10000", "10000", "10000"],
    "Q": ["01110", "10001", "10001", "10001", "10101", "10010", "01101"],
    "R": ["11110", "10001", "10001", "11110", "10100", "10010", "10001"],
    "S": ["01111", "10000", "10000", "01110", "00001", "00001", "11110"],
    "T": ["11111", "00100", "00100", "00100", "00100", "00100", "00100"],
    "U": ["10001", "10001", "10001", "10001", "10001", "10001", "01110"],
    "V": ["10001", "10001", "10001", "10001", "10001", "01010", "00100"],
    "W": ["10001", "10001", "10001", "10101", "10101", "11011", "10001"],
    "X": ["10001", "10001", "01010", "00100", "01010", "10001", "10001"],
    "Y": ["10001", "10001", "01010", "00100", "00100", "00100", "00100"],
    "Z": ["11111", "00001", "00010", "00100", "01000", "10000", "11111"],
    "_": ["00000", "00000", "00000", "00000", "00000", "00000", "11111"],
    "/": ["00001", "00010", "00010", "00100", "01000", "01000", "10000"],
    "%": ["11001", "11010", "00010", "00100", "01000", "01011", "10011"],
    "(": ["00010", "00100", "01000", "01000", "01000", "00100", "00010"],
    ")": ["01000", "00100", "00010", "00010", "00010", "00100", "01000"],
}

PALETTE = [(31, 119, 180), (214, 39, 40), (44, 160, 44), (148, 103, 189), (140, 86, 75)]
BG = (250, 250, 250)
FG = (30, 30, 30)
GRID = (220, 220, 220)


class Canvas:
    def __init__(self, width: int, height: int) -> None:
        self.w, self.h = width, height
        self.px = bytearray(bytes(BG) * (width * height))

    def set(self, x: int, y: int, color: tuple[int, int, int]) -> None:
        if 0 <= x < self.w and 0 <= y < self.h:
            i = (y * self.w + x) * 3
            self.px[i:i + 3] = bytes(color)

    def hline(self, x0: int, x1: int, y: int, color=FG) -> None:
        for x in range(min(x0, x1), max(x0, x1) + 1):
            self.set(x, y, color)

    def vline(self, x: int, y0: int, y1: int, color=FG) -> None:
        for y in range(min(y0, y1), max(y0, y1) + 1):
            self.set(x, y, color)

    def line(self, x0: int, y0: int, x1: int, y1: int, color) -> None:
        dx, dy = abs(x1 - x0), -abs(y1 - y0)
        sx = 1 if x0 < x1 else -1
        sy = 1 if y0 < y1 else -1
        err = dx + dy
        while True:
            self.set(x0, y0, color)
            if x0 == x1 and y0 == y1:
                break
            e2 = 2 * err
            if e2 >= dy:
                err += dy
                x0 += sx
            if e2 <= dx:
                err += dx
                y0 += sy

    def text(self, x: int, y: int, text: str, color=FG, scale: int = 2) -> None:
        for ch in text.upper():
            rows = FONT.get(ch, FONT[" "])
            for ry, row in enumerate(rows):
                for rx, bit in enumerate(row):
                    if bit == "1":
                        for a in range(scale):
                            for b in range(scale):
                                self.set(x + rx * scale + a, y + ry * scale + b, color)
            x += 6 * scale

    def save(self, path: Path) -> None:
        raw = bytearray()
        for y in range(self.h):
            raw.append(0)
            raw.extend(self.px[y * self.w * 3:(y + 1) * self.w * 3])

        def chunk(tag: bytes, data: bytes) -> bytes:
            return (len(data).to_bytes(4, "big") + tag + data +
                    zlib.crc32(tag + data).to_bytes(4, "big"))

        png = (b"\x89PNG\r\n\x1a\n" +
               chunk(b"IHDR", self.w.to_bytes(4, "big") + self.h.to_bytes(4, "big") + bytes([8, 2, 0, 0, 0])) +
               chunk(b"IDAT", zlib.compress(bytes(raw), 9)) +
               chunk(b"IEND", b""))
        path.write_bytes(png)


def read_csv(path: Path) -> tuple[list[str], list[list[float]]]:
    lines = [ln for ln in path.read_text().splitlines() if ln.strip()]
    header = lines[0].split(",")
    rows = []
    for line in lines[1:]:
        parts = line.split(",")
        rows.append([float(p) if p not in ("", "nan") else float("nan") for p in parts])
    return header, rows


def render(csv_path: Path, out_path: Path, title: str, y_label: str, series: list[str] | None = None) -> None:
    header, rows = read_csv(csv_path)
    if series is None:
        series = [h for h in header[1:]]
    idx = {name: header.index(name) for name in series}
    values = [[r[idx[name]] for name in series] for r in rows]
    flat = [v for row in values for v in row if v == v]
    if not flat:
        flat = [0.0, 1.0]
    lo, hi = min(flat), max(flat)
    if hi - lo < 1e-9:
        hi = lo + 1.0
    # pad the top by 10%
    hi += (hi - lo) * 0.1

    c = Canvas(WIDTH, HEIGHT)
    plot_w = WIDTH - MARGIN_L - MARGIN_R
    plot_h = HEIGHT - MARGIN_T - MARGIN_B
    c.text(10, 10, title[:70], scale=2)

    # axes + grid
    c.hline(MARGIN_L, WIDTH - MARGIN_R, HEIGHT - MARGIN_B)
    c.vline(MARGIN_L, MARGIN_T, HEIGHT - MARGIN_B)
    for i in range(5):
        y = MARGIN_T + int(plot_h * i / 4)
        c.hline(MARGIN_L, WIDTH - MARGIN_R, y, GRID)
        label = f"{hi - (hi - lo) * i / 4:.1f}"
        c.text(6, y - 6, label, scale=1)
    c.text(6, HEIGHT - MARGIN_B + 20, y_label[:12], scale=1)

    n = max(len(values) - 1, 1)
    for si, name in enumerate(series):
        color = PALETTE[si % len(PALETTE)]
        prev = None
        for i, row in enumerate(values):
            v = row[si]
            if v != v:  # NaN: gap
                prev = None
                continue
            x = MARGIN_L + int(plot_w * i / n)
            y = MARGIN_T + int(plot_h * (1 - (v - lo) / (hi - lo)))
            if prev is not None:
                c.line(prev[0], prev[1], x, y, color)
            prev = (x, y)
        c.text(MARGIN_L + plot_w - 150, MARGIN_T + 12 * si, name[:18], color=color, scale=1)

    # x axis ticks (seconds)
    if rows:
        c.text(MARGIN_L, HEIGHT - MARGIN_B + 8, "0S", scale=1)
        c.text(WIDTH - MARGIN_R - 40, HEIGHT - MARGIN_B + 8, f"{len(rows)}S", scale=1)
    c.save(out_path)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--title", default="timeline")
    ap.add_argument("--y-label", default="value")
    ap.add_argument("--series", default="")
    args = ap.parse_args()
    series = [s for s in args.series.split(",") if s] or None
    render(Path(args.csv), Path(args.out), args.title, args.y_label, series)
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
