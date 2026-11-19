#!/usr/bin/env python3
"""Scrape decision-api /metrics and print per-stage percentile estimates
from the fade_decision_latency_seconds histogram buckets."""
import re
import sys
import urllib.request


def scrape(url="http://localhost:8080/metrics"):
    text = urllib.request.urlopen(url).read().decode()
    # parse buckets: fade_decision_latency_seconds_bucket{stage="X",le="Y"} N
    stages = {}
    for m in re.finditer(r'fade_decision_latency_seconds_bucket\{stage="([^"]+)",le="([^"]+)"\}\s+(\d+)', text):
        stage, le, val = m.group(1), m.group(2), int(m.group(3))
        stages.setdefault(stage, {})[le] = val
    for stage, buckets in stages.items():
        # cumulative -> delta not needed for snapshot estimate; print p50-ish via total
        total = buckets.get("+Inf", 0)
        if total == 0:
            continue
        # find le where cumulative >= 0.99*total
        target = 0.99 * total
        p99_le = None
        for le in sorted(buckets, key=lambda x: (x == "+Inf", float(x) if x != "+Inf" else float("inf"))):
            if buckets[le] >= target:
                p99_le = le
                break
        print(f"{stage:12s} n={total:7d} p99_le={p99_le}")
    # also ml degraded rate
    for line in text.splitlines():
        if "fade_ml_status_total" in line and not line.startswith("#"):
            print(line)

if __name__ == "__main__":
    scrape(sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080/metrics")
