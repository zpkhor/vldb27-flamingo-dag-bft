#!/usr/bin/env python3
"""Parse f+1 Commit latency (workers) and Committed TPS from a scenario sweep results dir.

Outputs CSV with columns: label, n, rate, run, latency_ms, tps

Usage:
    python parse_latency_tps.py <results_dir> [-o output.csv]
"""
import argparse
import csv
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from plot_common import TPS_RE

# Overall weighted p95 from PER-VALIDATOR COMMIT METRICS table
#  Overall      33,026        1,839 (wtd)  2,175 (wtd) 1
LATENCY_P95_WTD_RE = re.compile(
    r"Overall\s+([\d,]+)\s+([\d,]+)\s+\(wtd\)\s+([\d,]+)\s+\(wtd\)"
)

RUN_DIR_RE = re.compile(r"^n(\d+)_(.+)_r(\d+)_run_(\d+)$")

FIELDS = ["label", "n", "rate", "run", "latency_ms", "tps"]


def parse_log(log_path):
    """Return (latency_p95_wtd_ms: int, tps: int) or (None, None) on failure."""
    text = log_path.read_text(encoding="utf-8")
    lat_m = LATENCY_P95_WTD_RE.search(text)
    tps_m = TPS_RE.search(text)
    if not lat_m or not tps_m:
        return None, None
    return int(lat_m.group(3).replace(",", "")), int(tps_m.group(1).replace(",", ""))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("results_dir")
    parser.add_argument("-o", "--output", help="Output CSV path (default: <results_dir>/latency_tps.csv)")
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    assert results_dir.exists(), f"Path not found: {results_dir}"

    outfile = Path(args.output) if args.output else results_dir / "latency_tps.csv"

    rows = []
    for subdir in sorted(results_dir.iterdir()):
        if not subdir.is_dir():
            continue
        m = RUN_DIR_RE.match(subdir.name)
        if not m:
            continue
        n, label, rate, run = int(m.group(1)), m.group(2), int(m.group(3)), int(m.group(4))
        log = subdir / "output.log"
        if not log.exists():
            print(f"WARNING: missing {log}", file=sys.stderr)
            continue
        latency, tps = parse_log(log)
        if latency is None:
            print(f"WARNING: could not parse metrics from {log}", file=sys.stderr)
            continue
        rows.append({"label": label, "n": n, "rate": rate, "run": run, "latency_ms": latency, "tps": tps})

    assert rows, f"No parseable run directories found in {results_dir}"

    with open(outfile, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {outfile.resolve()}")


if __name__ == "__main__":
    main()
