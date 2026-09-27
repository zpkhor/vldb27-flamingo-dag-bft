#!/usr/bin/env python3
"""Summarise a hot-spot-shift run so attempts can be compared on the same windows.

The figure (benchmark/hotspot_shift.pdf) has five regions of interest: the balanced
warmup plateau, the dip when the load imbalance starts, the plateau the LB algorithm
recovers to, the dip when the hot region moves, and the plateau after that recovery.
This prints mean/CV per window plus the dip minima and total migrations.

Windows are seconds since the first TPS bin, matching hotspot.py's time origin.

Usage:
    python benchmark/exp/check_hotspot_shift.py <run_dir> [<run_dir> ...]

Each run dir needs tps_timeline.csv (from benchmark/tps_timeline.py) and output.log.
"""
import csv
import re
import statistics
import sys
from pathlib import Path

# (label, start_s, end_s, is_dip)
WINDOWS = [
    ("warmup plateau",     60, 220, False),
    ("imbalance dip",     220, 260, True),
    ("recovered plateau", 450, 570, False),
    ("shift dip",         580, 620, True),
    ("post-shift plateau", 720, 880, False),
]

MIG_BLOCK_RE = re.compile(r'\+ MIGRATION_EVENTS_CSV:\n(.*?)(?=\n \+|\n-{5}|\Z)', re.DOTALL)


def load_tps(run_dir):
    tps_path = run_dir / "tps_timeline.csv"
    assert tps_path.exists(), (
        f"Missing {tps_path.resolve()}\n"
        f"  -> run: python benchmark/tps_timeline.py {run_dir.resolve()} > {tps_path.resolve()}"
    )
    rows = [r for r in csv.DictReader(tps_path.read_text().splitlines())
            if (r.get("timestamp_s") or "").strip()]
    assert rows, f"No TPS rows in {tps_path.resolve()}"
    t0 = float(rows[0]["timestamp_s"])
    return [(float(r["timestamp_s"]) - t0, float(r["tps"]), float(r["lat_mean"] or 0)) for r in rows]


def total_migrations(run_dir):
    log_path = run_dir / "output.log"
    assert log_path.exists(), f"Missing {log_path.resolve()}"
    m = MIG_BLOCK_RE.search(log_path.read_text(encoding="utf-8"))
    assert m, f"No MIGRATION_EVENTS_CSV block in {log_path.resolve()}"
    rows = csv.DictReader([l for l in m.group(1).strip().splitlines() if l.strip()])
    return sum(int(r["n_migrations"]) for r in rows)


def main():
    assert len(sys.argv) >= 2, f"Usage: {sys.argv[0]} <run_dir> [<run_dir> ...]"
    for arg in sys.argv[1:]:
        run_dir = Path(arg)
        series = load_tps(run_dir)
        print(f"\n=== {run_dir.resolve()} ===")
        print(f"  span={series[-1][0]:.0f}s  total_migrations={total_migrations(run_dir):,}")
        for label, start, end, is_dip in WINDOWS:
            vals = [tps for t, tps, _ in series if start <= t < end]
            lats = [lat for t, _, lat in series if start <= t < end]
            assert vals, f"No TPS bins in window {label} [{start}-{end}s] of {run_dir.resolve()}"
            mean = statistics.mean(vals)
            extra = (f"min={min(vals):>9,.0f}" if is_dip
                     else f"cv={statistics.pstdev(vals) / mean:>8.3f}")
            print(f"  {label:<18} [{start:>3}-{end:>3}s] tps={mean:>9,.0f}  {extra}"
                  f"  lat_mean={statistics.mean(lats):>7,.0f}ms")


if __name__ == "__main__":
    main()
