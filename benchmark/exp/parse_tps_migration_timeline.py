#!/usr/bin/env python3
"""Parse TPS_TIMELINE_CSV and MIGRATION_EVENTS_CSV blocks from output logs.

Scans a results directory for run subdirectories and extracts the two embedded
CSV blocks written by logs.py into each output.log. Outputs:
  <results_dir>/tps_timeline.csv
  <results_dir>/migration_events.csv

Usage:
    python parse_tps_migration_timeline.py <results_dir>
"""
import csv
import re
import sys
from pathlib import Path

RUN_DIR_RE = re.compile(r"^n(\d+)_(.+)_r(\d+)_run_(\d+)$")

TPS_BLOCK_RE = re.compile(
    r'\+ TPS_TIMELINE_CSV:\n(.*?)(?=\n \+|\n-{5}|\Z)', re.DOTALL
)
MIG_BLOCK_RE = re.compile(
    r'\+ MIGRATION_EVENTS_CSV:\n(.*?)(?=\n \+|\n-{5}|\Z)', re.DOTALL
)


def parse_csv_block(text, block_re):
    m = block_re.search(text)
    if not m:
        return None
    lines = [l for l in m.group(1).strip().splitlines() if l.strip()]
    if len(lines) < 2:
        return None
    reader = csv.DictReader(lines)
    return list(reader)


def main():
    assert len(sys.argv) == 2, f"Usage: {sys.argv[0]} <results_dir>"
    results_dir = Path(sys.argv[1])
    assert results_dir.exists(), f"Path not found: {results_dir}"

    tps_rows = []
    mig_rows = []

    for subdir in sorted(results_dir.iterdir()):
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
        text = log.read_text(encoding="utf-8")

        tps_data = parse_csv_block(text, TPS_BLOCK_RE)
        if tps_data is None:
            print(f"WARNING: no TPS_TIMELINE_CSV in {log}", file=sys.stderr)
        else:
            for row in tps_data:
                tps_rows.append({"label": label, "n": n, "rate": rate, "run": run, **row})

        mig_data = parse_csv_block(text, MIG_BLOCK_RE)
        if mig_data is None:
            print(f"WARNING: no MIGRATION_EVENTS_CSV in {log}", file=sys.stderr)
        else:
            for row in mig_data:
                mig_rows.append({"label": label, "n": n, "rate": rate, "run": run, **row})

    def write_csv(rows, outfile):
        assert rows, f"No data to write to {outfile}"
        with open(outfile, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        print(f"Wrote {len(rows)} rows to {outfile.resolve()}")

    if tps_rows:
        write_csv(tps_rows, results_dir / "tps_timeline.csv")
    if mig_rows:
        write_csv(mig_rows, results_dir / "migration_events.csv")


if __name__ == "__main__":
    main()
