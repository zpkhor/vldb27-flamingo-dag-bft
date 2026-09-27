#!/usr/bin/env python3
"""Pass/fail check: did a run's LB throughput recover to its offered rate and stay flat?

A run passes when, over the recovery window (the tail of the plotted region), the
mean TPS is within --tol of the offered rate AND the coefficient of variation is
below --max-cv. Offered rate is parsed from the run dir name (..._r<rate>_run_N).

Usage:
    python benchmark/exp/check_tps_recovered.py <RESULTS_DIR> [RUN_SUBDIR ...]
        [--tol 0.05] [--max-cv 0.10] [--tail 300]
"""
import argparse
import re
import sys
from pathlib import Path

import numpy as np
np.Inf = np.inf  # patch for matplotlib compatibility with NumPy 2.0

sys.path.insert(0, str(Path(__file__).parent))
from plot_lb_configs import load_run, TRIM_LEFT_DUR, TRIM_RIGHT_DUR

DIR_RE = re.compile(r"_r(?P<rate>\d+)_run_(?P<run>\d+)$")


def evaluate(run_dir, tol, max_cv, tail_s):
    """Return (passed, detail_dict) for one run dir."""
    run_dir = Path(run_dir)
    m = DIR_RE.search(run_dir.name)
    assert m, f"Cannot parse offered rate from dir name: {run_dir.name}"
    rate = int(m.group("rate"))

    times, vals, _lat, _mt, _mc = load_run(run_dir, "mean")
    assert times, f"No TPS samples in {run_dir}"

    t0 = times[0]
    rel = [t - t0 for t in times]
    end = rel[-1] - TRIM_RIGHT_DUR
    start = max(TRIM_LEFT_DUR, end - tail_s)
    window = [v for t, v in zip(rel, vals) if start <= t <= end]
    assert window, (
        f"Empty recovery window for {run_dir.name} "
        f"(span={rel[-1]:.0f}s, need > TRIM_RIGHT_DUR+{tail_s})"
    )

    arr = np.array(window, dtype=float)
    mean = float(arr.mean())
    cv = float(arr.std() / mean) if mean > 0 else float("inf")
    frac = mean / rate
    passed = frac >= (1.0 - tol) and cv <= max_cv
    return passed, {
        "run": run_dir.name,
        "rate": rate,
        "mean": mean,
        "frac": frac,
        "cv": cv,
        "n": len(window),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("results_dir")
    ap.add_argument("runs", nargs="*", help="Run subdir names (default: all in results_dir).")
    ap.add_argument("--tol", type=float, default=0.05,
                    help="Allowed shortfall below offered rate (default 0.05 = 95%%).")
    ap.add_argument("--max-cv", type=float, default=0.10,
                    help="Max coefficient of variation for flatness (default 0.10).")
    ap.add_argument("--tail", type=float, default=300.0,
                    help="Seconds of recovery window to evaluate (default 300).")
    args = ap.parse_args()

    results_dir = Path(args.results_dir)
    if args.runs:
        run_dirs = [results_dir / r for r in args.runs]
    else:
        run_dirs = sorted(d for d in results_dir.iterdir()
                          if d.is_dir() and DIR_RE.search(d.name))

    failures = []
    print(f"{'RUN':<36} {'RATE':>8} {'MEAN':>9} {'%RATE':>7} {'CV':>6}  VERDICT")
    for d in run_dirs:
        passed, info = evaluate(d, args.tol, args.max_cv, args.tail)
        if not passed:
            failures.append(info["run"])
        print(
            f"{info['run']:<36} {info['rate']:>8,} {info['mean']:>9,.0f} "
            f"{info['frac']*100:>6.1f}% {info['cv']:>6.3f}  {'PASS' if passed else 'FAIL'}"
        )

    print()
    if failures:
        print(f"FAILED ({len(failures)}): " + " ".join(failures))
    else:
        print("All runs recovered to offered rate and are flat.")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
