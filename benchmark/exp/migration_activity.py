#!/usr/bin/env python3
"""Is the load balancer still migrating at the end of a run, or has it converged?

Splits each run's post-warmup window into three equal thirds and reports the
migration rate in each, alongside the throughput plateau. A converged run does
its migrations early and goes quiet; a run that keeps churning shows a late-third
rate comparable to its early-third rate.

Warmup comes from <run_dir>/bench-params.json; the tail is trimmed by
TRIM_RIGHT_DUR so the shutdown ramp-down is excluded (same window the plots use).

Usage:
    python benchmark/exp/migration_activity.py <RESULTS_DIR> [RUN_SUBDIR ...]
"""
import argparse
import json
import re
import sys
from pathlib import Path

import numpy as np
np.Inf = np.inf  # patch for matplotlib compatibility with NumPy 2.0

sys.path.insert(0, str(Path(__file__).parent))
from plot_lb_configs import load_run, TRIM_RIGHT_DUR

DIR_RE = re.compile(r"_r(?P<rate>\d+)_run_(?P<run>\d+)$")

# late/early migration-rate ratio boundaries for the verdict column
SUSTAINED_RATIO = 0.5
CONVERGED_RATIO = 0.05

# A run whose commits stop early still yields a narrow window and a plausible-looking
# plateau. Refuse to report one rather than let a stalled run pass as data.
MIN_WINDOW_S = 300.0

# Width alone is not enough: a run that dies mid-window but emits a few late stragglers
# spans a long interval with a hole in it, and the empty tail reads as "no migrations",
# i.e. as convergence. Require the samples inside the window to be near-contiguous.
# 30 s because healthy reference runs have no gap that large at all; 60 s let a visibly
# degrading n21 cell through (see tps_timeline_lb_nodes_cloud_20260812_114149).
MAX_GAP_S = 30.0


def analyze(run_dir):
    run_dir = Path(run_dir)
    m = DIR_RE.search(run_dir.name)
    assert m, f"Cannot parse offered rate from dir name: {run_dir.name}"
    rate = int(m.group("rate"))

    params = json.loads((run_dir / "bench-params.json").read_text())
    warmup = float(params["warmup"])

    times, vals, _lat, mig_times, mig_cum = load_run(run_dir, "mean")
    assert times, f"No TPS samples in {run_dir}"

    t0 = times[0]
    rel = [t - t0 for t in times]
    end = rel[-1] - TRIM_RIGHT_DUR
    assert end - warmup >= MIN_WINDOW_S, (
        f"{run_dir.name}: steady window is {end - warmup:.0f}s, below the {MIN_WINDOW_S:.0f}s "
        f"minimum (commit span={rel[-1]:.0f}s, warmup={warmup:.0f}s, trim={TRIM_RIGHT_DUR}s). "
        f"Commits likely stopped before the run ended - check the tail of the worker logs."
    )

    in_window = [(t, v) for t, v in zip(rel, vals) if warmup <= t <= end]
    assert in_window, f"Empty TPS window for {run_dir.name}"

    # Include the leading and trailing holes: a run that dies inside the window leaves
    # its last sample well short of `end`, and no in-window pair spans that hole.
    wt = [t for t, _ in in_window]
    gaps = [(wt[i], wt[i + 1] - wt[i]) for i in range(len(wt) - 1)]
    gaps.append((warmup, wt[0] - warmup))
    gaps.append((wt[-1], end - wt[-1]))
    worst = max(gaps, key=lambda g: g[1], default=(0.0, 0.0))
    assert worst[1] <= MAX_GAP_S, (
        f"{run_dir.name}: {worst[1]:.0f}s gap in committed output at t={worst[0]:.0f}s, "
        f"above the {MAX_GAP_S:.0f}s limit. Commits stopped mid-run and resumed (or the run "
        f"died and emitted stragglers); the empty stretch would read as zero migrations."
    )

    tps_window = [v for _, v in in_window]
    arr = np.array(tps_window, dtype=float)
    mean = float(arr.mean())
    cv = float(arr.std() / mean) if mean > 0 else float("inf")

    # Migration counts are per-event; bucket them into thirds of the steady window.
    third = (end - warmup) / 3.0
    counts = [0.0, 0.0, 0.0]
    per_event = np.diff([0.0] + list(mig_cum)) if mig_cum else []
    for t, n in zip(mig_times, per_event):
        if not (warmup <= t <= end):
            continue
        idx = min(int((t - warmup) / third), 2)
        counts[idx] += n
    rates = [c / third for c in counts]
    total = sum(counts)

    early, late = rates[0], rates[2]
    if early <= 0:
        verdict = "SUSTAINED" if late > 0 else "NONE"
        ratio = float("inf") if late > 0 else 0.0
    else:
        ratio = late / early
        if ratio >= SUSTAINED_RATIO:
            verdict = "SUSTAINED"
        elif ratio <= CONVERGED_RATIO:
            verdict = "CONVERGED"
        else:
            verdict = "DECAYING"

    return {
        "run": run_dir.name,
        "rate": rate,
        "mean": mean,
        "frac": mean / rate,
        "cv": cv,
        "window": (warmup, end),
        "total": total,
        "rates": rates,
        "ratio": ratio,
        "verdict": verdict,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("results_dir")
    ap.add_argument("runs", nargs="*", help="Run subdir names (default: all in results_dir).")
    args = ap.parse_args()

    results_dir = Path(args.results_dir)
    if args.runs:
        run_dirs = [results_dir / r for r in args.runs]
    else:
        run_dirs = sorted(d for d in results_dir.iterdir()
                          if d.is_dir() and DIR_RE.search(d.name))
    assert run_dirs, f"No run dirs found in {results_dir.resolve()}"

    print(f"{'RUN':<28} {'RATE':>7} {'PLATEAU':>8} {'%RATE':>6} {'CV':>6} "
          f"{'MIGS':>10} {'EARLY/s':>9} {'MID/s':>9} {'LATE/s':>9} {'LATE/EARLY':>10}  VERDICT")
    for d in run_dirs:
        i = analyze(d)
        ratio_str = "inf" if i["ratio"] == float("inf") else f"{i['ratio']:.2f}"
        print(
            f"{i['run']:<28} {i['rate']:>7,} {i['mean']:>8,.0f} {i['frac']*100:>5.1f}% "
            f"{i['cv']:>6.3f} {i['total']:>10,.0f} "
            f"{i['rates'][0]:>9,.1f} {i['rates'][1]:>9,.1f} {i['rates'][2]:>9,.1f} "
            f"{ratio_str:>10}  {i['verdict']}"
        )
    print()
    print(f"Window: [warmup, end-{TRIM_RIGHT_DUR}s], split into equal thirds. "
          f"SUSTAINED if late/early >= {SUSTAINED_RATIO}, CONVERGED if <= {CONVERGED_RATIO}.")


if __name__ == "__main__":
    main()
