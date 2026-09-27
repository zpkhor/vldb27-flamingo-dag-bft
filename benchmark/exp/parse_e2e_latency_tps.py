#!/usr/bin/env python3
"""Parse E2E latency and E2E TPS from a scenario sweep results dir.

Outputs CSV with columns: label, n, rate, run, latency_ms, tps

Usage:
    python parse_latency_tps.py <results_dir> [-o output.csv] [--latency p50]
"""
import argparse
import csv
import re
import sys
from pathlib import Path

E2E_TPS_RE = re.compile(r" E2E TPS: ([\d,]+) tx/s")

E2E_LATENCY_RE = re.compile(
    r" E2E latency \(send -> exec reply\) \((\w+)\): ([\d,]+) ms"
)

RUN_DIR_RE = re.compile(r"^n(\d+)_(.+)_r(\d+)_run_(\d+)$")

MERGED_CMD_RE = re.compile(r"^CMD: LABEL=(n(\d+)_(.+)_r(\d+)_run_(\d+))\b")


def parse_log(log_path, latency_type):
    """Return (latency_ms: int, tps: int) or (None, None) on failure."""
    text = log_path.read_text(encoding="utf-8")
    tps_m = E2E_TPS_RE.search(text)
    lat_val = None
    for m in E2E_LATENCY_RE.finditer(text):
        if m.group(1) == latency_type:
            lat_val = int(m.group(2).replace(",", ""))
            break
    if lat_val is None or not tps_m:
        return None, None
    return lat_val, int(tps_m.group(1).replace(",", ""))


def parse_merged_log(log_path, latency_type):
    """Parse a merged_output.log containing CMD: LABEL=... markers. Returns list of row dicts."""
    lat_col = f"{latency_type}_ms"
    rows = []
    current = None
    buf = []

    def flush():
        if current is None:
            return
        text = "\n".join(buf)
        tps_m = E2E_TPS_RE.search(text)
        lat_val = None
        for m in E2E_LATENCY_RE.finditer(text):
            if m.group(1) == latency_type:
                lat_val = int(m.group(2).replace(",", ""))
                break
        if lat_val is None or not tps_m:
            print(f"WARNING: could not parse metrics for {current['label']}", file=sys.stderr)
            return
        rows.append({**current, lat_col: lat_val, "tps": int(tps_m.group(1).replace(",", ""))})

    for line in log_path.read_text(encoding="utf-8").splitlines():
        m = MERGED_CMD_RE.match(line)
        if m:
            flush()
            label = f"n{m.group(2)}_{m.group(3)}"
            current = {"label": label, "n": int(m.group(2)), "rate": int(m.group(4)), "run": int(m.group(5))}
            buf = []
        else:
            buf.append(line)
    flush()
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("results_dir")
    parser.add_argument("-o", "--output", help="Output CSV path (default: <results_dir>/latency_tps.csv)")
    parser.add_argument("--latency", choices=["mean", "p50", "p90", "p95", "p99"], default="p50")
    args = parser.parse_args()

    results_path = Path(args.results_dir)
    assert results_path.exists(), f"Path not found: {results_path}"

    lat_col = f"{args.latency}_ms"
    fields = ["label", "n", "rate", "run", lat_col, "tps"]

    if results_path.is_file():
        rows = parse_merged_log(results_path, args.latency)
        assert rows, f"No parseable runs found in {results_path}"
        outfile = Path(args.output) if args.output else results_path.parent / "latency_tps.csv"
    else:
        outfile = Path(args.output) if args.output else results_path / "latency_tps.csv"
        rows = []
        for subdir in sorted(results_path.iterdir()):
            if not subdir.is_dir():
                continue
            m = RUN_DIR_RE.match(subdir.name)
            if not m:
                continue
            n, label, rate, run = int(m.group(1)), f"n{m.group(1)}_{m.group(2)}", int(m.group(3)), int(m.group(4))
            log = subdir / "output.log"
            if not log.exists():
                print(f"WARNING: missing {log}", file=sys.stderr)
                continue
            latency, tps = parse_log(log, args.latency)
            if latency is None:
                print(f"WARNING: could not parse metrics from {log}", file=sys.stderr)
                continue
            rows.append({"label": label, "n": n, "rate": rate, "run": run, lat_col: latency, "tps": tps})
        assert rows, f"No parseable run directories found in {results_path}"

    rows.sort(key=lambda r: (r["label"], r["n"], r["rate"]))

    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {outfile.resolve()}")


if __name__ == "__main__":
    main()
