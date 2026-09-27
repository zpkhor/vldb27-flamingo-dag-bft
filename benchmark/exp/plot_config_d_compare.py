#!/usr/bin/env python3
"""Side-by-side config (d) comparison: reference sweep vs a reproduction sweep.

Two columns share a y-axis per row so the panels are directly comparable:
throughput, mean latency, and cumulative migrations against committee size.

The n=4 cell is not part of either nodes sweep - it is shared with config (b) -
so its run directory is named per column.

Usage:
    python benchmark/exp/plot_config_d_compare.py [--out FIG_STEM] [--output-dir DIR]
"""
import argparse
import sys
from pathlib import Path

import numpy as np
np.Inf = np.inf  # patch for matplotlib compatibility with NumPy 2.0
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import matplotlib.ticker as ticker
from matplotlib.ticker import MaxNLocator

sys.path.insert(0, str(Path(__file__).parent))
from plot_lb_configs import load_run, _smoothed_series, TRIM_LEFT_DUR, TRIM_RIGHT_DUR

_RESULTS = Path(__file__).parent / "results"

REF_DIR   = _RESULTS / "tps_timeline_lb_nodes_cloud_20260415_114941"
# 2026-08-13 sweep on b1f416a + the analyze()/synchronizer fixes: the first sweep in which
# all three cells commit for the full run. Supersedes 20260813_005238 (n21 stalled at 770s)
# and the 2026-08-12 11:41 daytime sweep (n16 and n21 stalled).
TODAY_DIR = _RESULTS / "tps_timeline_lb_nodes_cloud_20260813_113650"
# n=4 under current code comes from the config-b sweep; the April sweep carries its own.
TODAY_N4  = _RESULTS / "tps_timeline_lb_cloud_20260810_161630"

# Okabe-Ito subset, validated for CVD separation against a light surface.
COLORS = ["#0072B2", "#E69F00", "#009E73", "#D55E00"]

# (run_subdir, legend label)
SERIES = [
    ("n4_v_rate_imb60_r110000_run_1",  "n=4"),
    ("n10_v_rate_imb60_r110000_run_1", "n=10"),
    ("n16_v_rate_imb60_r110000_run_1", "n=16"),
    ("n21_v_rate_imb60_r110000_run_1", "n=21"),
]

WARMUP_S = 240.0

# Today's n21/n16 timelines are shorter than the reference's, so a per-run
# TRIM_RIGHT_DUR still lets their shutdown ramp reach the last bin. Draw every
# series over one window bounded by the shortest run, minus a bin of margin.
COMMON_MARGIN_S = 20.0

COLUMNS = [
    ("Reference (2026-04-15)", REF_DIR,   REF_DIR),
    ("Reproduction (2026-08-13)", TODAY_DIR, TODAY_N4),
]


def resolve(base, n4_base, subdir):
    """n=4 lives outside the nodes sweep, so it gets its own base directory."""
    return (n4_base if subdir.startswith("n4_") else base) / subdir


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default="config_d_compare")
    ap.add_argument("--output-dir", default=str(TODAY_DIR))
    args = ap.parse_args()

    plt.rcParams.update({
        "font.family": "sans-serif", "font.size": 8,
        "axes.titlesize": 8, "axes.labelsize": 8,
        "xtick.labelsize": 7, "ytick.labelsize": 7, "legend.fontsize": 7,
        "lines.linewidth": 1.0, "axes.linewidth": 0.6,
        "grid.linewidth": 0.4, "grid.linestyle": "--",
        "grid.color": "lightgray", "grid.alpha": 0.8,
        "pdf.fonttype": 42, "ps.fonttype": 42,
    })

    fig, axes = plt.subplots(
        3, 2, figsize=(5.6, 4.6), sharex=True, sharey="row",
        gridspec_kw={"height_ratios": [3, 2, 2], "hspace": 0.16, "wspace": 0.07},
    )

    # One window every series covers in full, so no run shows its ramp-down.
    right = min(
        load_run(resolve(base, n4_base, subdir), "mean")[0][-1]
        for _t, base, n4_base in COLUMNS for subdir, _l in SERIES
    ) - TRIM_RIGHT_DUR - COMMON_MARGIN_S

    def clip(xs, ys, right):
        """Keep only samples inside the common plotted window."""
        out = [(x, y) for x, y in zip(xs, ys) if TRIM_LEFT_DUR <= x <= right]
        return ([x for x, _ in out], [y for _, y in out]) if out else ([], [])

    for c, (col_title, base, n4_base) in enumerate(COLUMNS):
        ax_tps, ax_lat, ax_mig = axes[0][c], axes[1][c], axes[2][c]

        for i, (subdir, label) in enumerate(SERIES):
            run_dir = resolve(base, n4_base, subdir)
            times, vals, lat_pairs, mig_times, mig_cum = load_run(run_dir, "mean")
            color = COLORS[i]
            pt, pv = _smoothed_series(*clip(times, vals, right))
            ax_tps.plot(pt, pv, color=color, linewidth=1.0)

            if lat_pairs:
                lt, lv = zip(*lat_pairs)
                pt, pv = _smoothed_series(*clip(list(lt), list(lv), right))
                ax_lat.plot(pt, pv, color=color, linewidth=1.0)

            mt, mc = clip(mig_times, list(mig_cum), right)
            if mt:
                ax_mig.step([TRIM_LEFT_DUR] + mt, [0] + mc,
                            where="post", color=color, linewidth=1.0)

        ax_tps.set_title(col_title, pad=4)
        for ax in (ax_tps, ax_lat, ax_mig):
            ax.axvline(WARMUP_S, color="black", linestyle="--", linewidth=0.8, alpha=0.7)
            ax.grid(True)
            ax.yaxis.set_major_locator(MaxNLocator(nbins=3))
            ax.xaxis.set_major_locator(MaxNLocator(nbins=4))
        ax_mig.set_xlabel("Time (s)")

    axes[0][0].set_xlim(left=TRIM_LEFT_DUR, right=right)

    axes[0][0].set_ylabel("Throughput [ktrans/s]", fontsize=7)
    axes[0][0].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}"))
    axes[1][0].set_ylabel("Mean Latency [sec]", fontsize=7)
    axes[1][0].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.1f}"))
    axes[2][0].set_ylabel("Cumul. Migs", fontsize=7)
    axes[2][0].yaxis.set_major_formatter(
        ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}k" if x >= 1000 else f"{x:.0f}"))

    handles = [mlines.Line2D([], [], color=COLORS[i], linewidth=1.0, label=lbl)
               for i, (_s, lbl) in enumerate(SERIES)]
    handles.append(mlines.Line2D([], [], color="black", linestyle="--", linewidth=0.8,
                                 alpha=0.7, label="warmup end"))
    fig.legend(handles=handles, loc="lower center", ncol=5, frameon=False,
               fontsize=6.5, handlelength=1.4, handletextpad=0.4,
               columnspacing=1.0, bbox_to_anchor=(0.5, -0.02))

    fig.suptitle("Config (d): Scalability with Committee Size (60% skew, 110k tx/s)",
                 fontsize=9, y=0.98)
    fig.subplots_adjust(left=0.10, right=0.985, top=0.90, bottom=0.12)

    out_dir = Path(args.output_dir)
    for ext in ("png", "pdf"):
        p = out_dir / f"{args.out}.{ext}"
        plt.savefig(p, dpi=200, bbox_inches="tight", pad_inches=0.02)
        print(f"Saved: {p.resolve()}")
    plt.close(fig)


if __name__ == "__main__":
    main()
