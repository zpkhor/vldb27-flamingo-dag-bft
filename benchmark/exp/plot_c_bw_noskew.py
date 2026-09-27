#!/usr/bin/env python3
"""Plot the config-(c) bandwidth sweep run without validator rate skew.

Three rows per cell, sharing a time axis:
  1. committed TPS against the offered rate
  2. migration rate, binned
  3. which validators the reroute pass classified as donors at each evaluation

Row 3 is the point of the figure: it shows whether the donor set stays on the
bandwidth-limited validators or flips onto the fast ones.

Usage:
    python benchmark/exp/plot_c_bw_noskew.py <RESULTS_DIR> [--out FIG_STEM]
"""
import argparse
import csv
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
np.Inf = np.inf  # patch for matplotlib compatibility with NumPy 2.0
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

sys.path.insert(0, str(Path(__file__).parent))
from plot_lb_configs import load_run, TRIM_RIGHT_DUR

MIG_BIN = 120.0  # seconds per migration-rate bin

# Okabe-Ito subset, validated for CVD separation against a light surface.
FAST_COLOR = "#0072B2"
SLOW_COLOR = "#D55E00"

# (run_subdir, offered_rate, slow_validator_indices, column title)
CELLS = [
    ("n4_balanced_r110000_run_1", 110_000, [],     "Balanced BW\n110k"),
    ("n4_bw_f_r110000_run_1",     110_000, [3],    "f BW-limited\n110k"),
    ("n4_bw_f1_r65000_run_1",      65_000, [2, 3], "f+1 BW-limited\n65k"),
    ("n4_bw_f1_r80000_run_1",      80_000, [2, 3], "f+1 BW-limited\n80k"),
    ("n4_bw_f1_r110000_run_1",    110_000, [2, 3], "f+1 BW-limited\n110k"),
]

REROUTE_RE = re.compile(
    r'^\[(\S+) INFO  consensus\] reroute: donors=(\d+) receivers=(\d+) \(donor_pks: \[(.*)\]\)'
)
VALIDATOR_RE = re.compile(r'v(\d+)')


def parse_reroute(primary_log, t0_abs):
    """Return [(t_rel, {donor validator indices}), ...] from a primary log."""
    out = []
    for line in primary_log.read_text().splitlines():
        m = REROUTE_RE.match(line)
        if not m:
            continue
        ts = datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc).timestamp()
        donors = {int(v) for v in VALIDATOR_RE.findall(m.group(4))}
        out.append((ts - t0_abs, donors))
    assert out, f"No reroute lines in {primary_log}"
    return out


def load_cell(run_dir):
    run_dir = Path(run_dir)
    warmup = float(json.loads((run_dir / "bench-params.json").read_text())["warmup"])

    with open(run_dir / "tps_timeline.csv") as f:
        t0_abs = float(next(csv.DictReader(f))["timestamp_s"])

    times, vals, _lat, mig_times, mig_cum = load_run(run_dir, "mean")
    t0 = times[0]
    rel = [t - t0 for t in times]
    end = rel[-1] - TRIM_RIGHT_DUR

    per_event = np.diff([0.0] + list(mig_cum)) if mig_cum else []
    nbins = max(int((end - warmup) // MIG_BIN), 1)
    mig = np.zeros(nbins)
    for t, n in zip(mig_times, per_event):
        i = int((t - warmup) // MIG_BIN)
        if t >= warmup and 0 <= i < nbins:
            mig[i] += n
    mig_edges = warmup + np.arange(nbins + 1) * MIG_BIN
    # Total counts only the binned window, matching migration_activity.py.
    mig_total = float(mig.sum())

    donors = parse_reroute(run_dir / "primary-0.log", t0_abs)
    return {
        "rel": rel, "tps": vals, "warmup": warmup, "end": end,
        "mig_rate": mig / MIG_BIN, "mig_edges": mig_edges,
        "mig_total": mig_total, "donors": donors,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("results_dir")
    ap.add_argument("--out", default="c_bw_noskew",
                    help="Output file stem (written next to results_dir).")
    args = ap.parse_args()

    results_dir = Path(args.results_dir)
    cells = [(name, rate, slow, title, load_cell(results_dir / name))
             for name, rate, slow, title in CELLS]

    plt.rcParams.update({
        "font.size": 7, "axes.labelsize": 7, "axes.titlesize": 8,
        "xtick.labelsize": 6, "ytick.labelsize": 6, "legend.fontsize": 6,
        "axes.spines.top": False, "axes.spines.right": False,
        "figure.dpi": 160,
    })

    ncol = len(cells)
    fig, axes = plt.subplots(
        3, ncol, figsize=(2.35 * ncol, 5.4), sharex="col",
        gridspec_kw={"height_ratios": [2.0, 1.5, 1.3], "hspace": 0.22, "wspace": 0.26},
    )

    for c, (name, rate, slow, title, d) in enumerate(cells):
        ax_tps, ax_mig, ax_don = axes[0][c], axes[1][c], axes[2][c]
        xmax = d["end"]

        for ax in (ax_tps, ax_mig, ax_don):
            ax.axvspan(0, d["warmup"], color="0.92", zorder=0, linewidth=0)
            ax.set_xlim(0, xmax)

        # Row 1: committed throughput vs offered rate
        ax_tps.plot(d["rel"], np.array(d["tps"]) / 1000.0, color=FAST_COLOR,
                    linewidth=1.1, zorder=3)
        ax_tps.axhline(rate / 1000.0, color="0.35", linestyle="--", linewidth=0.8, zorder=2)
        ax_tps.set_ylim(0, 145)
        ax_tps.set_title(title, fontsize=7.5, pad=4)

        window = [v for t, v in zip(d["rel"], d["tps"]) if d["warmup"] <= t <= xmax]
        mean = float(np.mean(window))
        ax_tps.annotate(f"{mean/1000:.1f}k ({mean/rate*100:.0f}%)",
                        (0.5, 0.06), xycoords="axes fraction", ha="center",
                        fontsize=6, color=FAST_COLOR, fontweight="bold")

        # Row 2: migration rate
        step_y = np.append(d["mig_rate"], d["mig_rate"][-1])
        ax_mig.fill_between(d["mig_edges"], step_y, step="post",
                            color=SLOW_COLOR, alpha=0.75, linewidth=0, zorder=3)
        ax_mig.set_ylim(0, 460)
        ax_mig.annotate(f"{d['mig_total']:,.0f} migs",
                        (0.04, 0.84), xycoords="axes fraction", ha="left",
                        fontsize=6, color="0.25")

        # Row 3: donor membership per reroute evaluation
        for t, donor_set in d["donors"]:
            if not (0 <= t <= xmax):
                continue
            for v in donor_set:
                ax_don.plot([t], [v], marker="s", markersize=2.6,
                            color=SLOW_COLOR if v in slow else FAST_COLOR, zorder=3)
        ax_don.set_ylim(-0.6, 3.6)
        ax_don.set_yticks(range(4))
        ax_don.set_yticklabels([f"v{v}" for v in range(4)])
        for v in range(4):
            ax_don.get_yticklabels()[v].set_color(SLOW_COLOR if v in slow else "0.3")
        ax_don.set_xlabel("Time (s)")

        if c == 0:
            ax_tps.set_ylabel("Committed\n[ktrans/s]")
            ax_mig.set_ylabel("Migrations/s")
            ax_don.set_ylabel("Donors")
        else:
            for ax in (ax_tps, ax_mig, ax_don):
                ax.set_yticklabels([])

    handles = [
        Line2D([], [], marker="s", linestyle="none", markersize=4, color=SLOW_COLOR,
               label="bandwidth-limited validator (200 mbit)"),
        Line2D([], [], marker="s", linestyle="none", markersize=4, color=FAST_COLOR,
               label="full-bandwidth validator (600 mbit)"),
        Line2D([], [], color="0.35", linestyle="--", linewidth=0.8, label="offered rate"),
        Patch(facecolor="0.92", label="warmup (excluded)"),
    ]
    fig.legend(handles=handles, loc="lower center", ncol=4, frameon=False,
               bbox_to_anchor=(0.5, -0.005))
    fig.suptitle("Config (c) bandwidth scenarios at uniform submission rate (n=4, no validator skew)",
                 fontsize=8.5, y=0.985)
    fig.tight_layout(rect=[0, 0.035, 1, 0.965])

    for ext in ("png", "pdf"):
        out = results_dir / f"{args.out}.{ext}"
        fig.savefig(out, bbox_inches="tight")
        print(f"Wrote {out.resolve()}")


if __name__ == "__main__":
    main()
