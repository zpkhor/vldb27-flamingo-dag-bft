#!/usr/bin/env python3
"""Plot per-validator TPS and per-event account migrations over real time.

Reads tps_timeline.csv and migration_events.csv produced by
parse_tps_migration_timeline.py.  Filters to a single label/run combination
and produces a two-panel figure:
  Top:    per-validator TPS (tx/s) over time
  Bottom: account migrations per reroute event (bar chart)

Time axis is normalised to seconds from the first TPS sample.

Usage:
    python plot_tps_migration_timeline.py <tps_csv> <migration_csv>
        [--label LABEL] [--run RUN] [-o output.png]
"""
import argparse
import csv
import sys
from pathlib import Path

import numpy as np
np.Inf = np.inf  # patch for matplotlib compatibility with NumPy 2.0
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


VALIDATOR_COLORS = ["tab:blue", "tab:orange", "tab:green", "tab:red",
                    "tab:purple", "tab:brown", "tab:pink", "tab:gray"]


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def filter_rows(rows, label, run):
    if label:
        rows = [r for r in rows if r["label"] == label]
    if run is not None:
        rows = [r for r in rows if int(r["run"]) == run]
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("tps_csv")
    parser.add_argument("migration_csv")
    parser.add_argument("--label", default=None, help="Filter to this label (e.g. n4_v_rate_imb60)")
    parser.add_argument("--run", type=int, default=None, help="Filter to this run number (default: first)")
    parser.add_argument("-o", "--output", default=None)
    parser.add_argument("--per-validator", action="store_true",
                        help="Plot per-validator TPS lines instead of summed total")
    args = parser.parse_args()

    tps_rows = read_csv(args.tps_csv)
    mig_rows = read_csv(args.migration_csv)

    # Determine label and run to plot.
    if args.label is None:
        labels = sorted({r["label"] for r in tps_rows})
        assert labels, "No rows in TPS CSV"
        args.label = labels[0]
        if len(labels) > 1:
            print(f"INFO: multiple labels found, plotting {args.label}. Use --label to select.", file=sys.stderr)

    tps_rows = filter_rows(tps_rows, args.label, args.run)
    mig_rows = filter_rows(mig_rows, args.label, args.run)

    if args.run is None and tps_rows:
        runs = sorted({int(r["run"]) for r in tps_rows})
        args.run = runs[0]
        if len(runs) > 1:
            print(f"INFO: multiple runs found, plotting run {args.run}. Use --run to select.", file=sys.stderr)
        tps_rows = [r for r in tps_rows if int(r["run"]) == args.run]
        mig_rows = [r for r in mig_rows if int(r["run"]) == args.run]

    assert tps_rows, f"No TPS rows for label={args.label} run={args.run}"

    tps_rows = sorted(tps_rows, key=lambda r: float(r["timestamp_s"]))

    # Normalise time to seconds from first sample.
    t0 = float(tps_rows[0]["timestamp_s"])
    tps_times = [float(r["timestamp_s"]) - t0 for r in tps_rows]

    validator_cols = [k for k in tps_rows[0].keys() if k.startswith("v") and k[1:].isdigit()]
    validator_cols = sorted(validator_cols, key=lambda c: int(c[1:]))

    fig, (ax_tps, ax_mig) = plt.subplots(2, 1, figsize=(10, 6), sharex=True,
                                          gridspec_kw={"height_ratios": [3, 1]})

    # --- Top panel: TPS ---
    if args.per_validator:
        for i, vcol in enumerate(validator_cols):
            tps_vals = [float(r[vcol]) for r in tps_rows]
            color = VALIDATOR_COLORS[i % len(VALIDATOR_COLORS)]
            ax_tps.plot(tps_times, tps_vals, color=color, label=f"V{vcol[1:]}", linewidth=1.5, marker="o", markersize=3)
        ax_tps.legend(loc="lower right", fontsize=8)
    else:
        total_tps = [sum(float(r[vcol]) for vcol in validator_cols) for r in tps_rows]
        ax_tps.plot(tps_times, total_tps, color="tab:blue", linewidth=1.5, marker="o", markersize=3)

    ax_tps.set_ylabel("Certified TPS (tx/s)")
    ax_tps.yaxis.set_major_formatter(ticker.FuncFormatter(
        lambda x, _: f"{x/1000:.0f}k" if x >= 1000 else f"{x:.0f}"
    ))
    ax_tps.grid(True, alpha=0.3)
    ax_tps.set_title(f"{args.label}  run {args.run}")

    # --- Bottom panel: migrations per event ---
    if mig_rows:
        mig_rows = sorted(mig_rows, key=lambda r: float(r["timestamp_s"]))
        mig_times = [float(r["timestamp_s"]) - t0 for r in mig_rows]
        mig_counts = [int(r["n_migrations"]) for r in mig_rows]
        # bar width = half the typical interval between events
        if len(mig_times) >= 2:
            intervals = [mig_times[i+1] - mig_times[i] for i in range(len(mig_times)-1)]
            bar_w = min(intervals) * 0.4
        else:
            bar_w = 5.0
        ax_mig.bar(mig_times, mig_counts, width=bar_w, color="tab:red", alpha=0.8)
    else:
        ax_mig.text(0.5, 0.5, "no migrations", transform=ax_mig.transAxes,
                    ha="center", va="center", fontsize=9, color="gray")

    ax_mig.set_ylabel("Accounts migrated")
    ax_mig.set_xlabel("Time (s)")
    ax_mig.grid(True, alpha=0.3)

    plt.tight_layout()

    outpath = args.output
    if outpath is None:
        outpath = str(Path(args.tps_csv).parent / f"tps_migration_{args.label}_run{args.run}.png")
    plt.savefig(outpath, dpi=150, bbox_inches="tight")
    print(f"Saved to {Path(outpath).resolve()}")


if __name__ == "__main__":
    main()
