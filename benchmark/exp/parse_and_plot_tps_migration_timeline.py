#!/usr/bin/env python3
"""Plot TPS/migration timeline from pasted log blocks.

Paste the LB run log into CSV_BLOCK. Optionally paste a baseline (no-LB) run's
tps_timeline.py output into CSV_BLOCK_BASELINE and pass --baseline to overlay
it as a reference line on the same TPS panel.

Example:
    python benchmark/exp/parse_and_plot_tps_migration_timeline.py \
        --label n4_v_rate_imb60 -o timeline.png

    python benchmark/exp/parse_and_plot_tps_migration_timeline.py \
        --label n4_v_rate_imb60 --baseline -o timeline_vs_baseline.png
"""
import argparse
import csv
import re
from pathlib import Path

import numpy as np
np.Inf = np.inf  # patch for matplotlib compatibility with NumPy 2.0
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from plot_lb_configs import (
    TPS_OUTLIER,
    SMOOTH_WINDOW,
    _smoothed_series,
    load_run,
    COLORS,
    TRIM_LEFT_DUR,
    TRIM_RIGHT_DUR,
)


CSV_BLOCK = """

"""


# Paste migration events CSV here (format: timestamp_s,round,n_migrations).
CSV_BLOCK_MIGRATION = """

"""


# Paste the raw tps_timeline.csv content from the baseline (no-LB) run here.
# Format: timestamp_s, tps, n_batches, n_txs, lat_mean, lat_p50, lat_p90, lat_p95
CSV_BLOCK_BASELINE = """

"""

# python /home/zpkhor/narwhal-validator/benchmark/exp/parse_and_plot_tps_migration_timeline.py --baseline --latency mean /home/zpkhor/narwhal-validator/benchmark/exp/results/tps_timeline_lb_cloud_20260414_132149 /home/zpkhor/narwhal-validator/benchmark/exp/results/tps_timeline_baseline_cloud_20260414_132153 --o config_a.png
INCLUDED = [
    "n4_v_rate_imb90_r110000",
    "n4_v_rate_imb90_r80000",
    "n4_v_rate_imb90_r50000",
    "n4_v_rate_imb90_r20000",
]
# python /home/zpkhor/narwhal-validator/benchmark/exp/parse_and_plot_tps_migration_timeline.py --baseline --latency mean /home/zpkhor/narwhal-validator/benchmark/exp/results/tps_timeline_lb_cloud_20260414_132149 /home/zpkhor/narwhal-validator/benchmark/exp/results/tps_timeline_baseline_cloud_20260414_132153 --o config_b.png
# INCLUDED = [
#     "n4_v_rate_imb60_r110000",
#     "n4_v_rate_imb90_r110000",
#     "n4_v_rate_imb99_r110000",
# ]

# python /home/zpkhor/narwhal-validator/benchmark/exp/parse_and_plot_tps_migration_timeline.py --baseline --latency mean /home/zpkhor/narwhal-validator/benchmark/exp/results/tps_timeline_lb_cloud_20260414_132149 /home/zpkhor/narwhal-validator/benchmark/exp/results/tps_timeline_baseline_cloud_20260414_132153 --o config_c.png
# INCLUDED = [
#     "n4_v_rate_imb90_r110000",
#     "n4_bw_f_rate_imb90_r110000",
#     "n4_bw_f1_rate_imb90_r65000",
# ]
# python /home/zpkhor/narwhal-validator/benchmark/exp/parse_and_plot_tps_migration_timeline.py --baseline --latency mean /home/zpkhor/narwhal-validator/benchmark/exp/results/tps_timeline_lb_nodes_cloud_20260415_114941 /home/zpkhor/narwhal-validator/benchmark/exp/results/config_d_scalablit_baseline/  --o config_d.png
# INCLUDED = [
#     "n4_v_rate_imb60_r110000",
#     "n10_v_rate_imb60_r110000",
#     "n16_v_rate_imb60_r110000",
#     "n21_v_rate_imb60_r110000",
# ]

VALIDATOR_COLORS = ["tab:blue", "tab:orange", "tab:green", "tab:red",
                    "tab:purple", "tab:brown", "tab:pink", "tab:gray"]

_BALANCED_END_RE = re.compile(r'Balanced phase end spread: \d+ ms \((.+?)\)')
_REGION_OFFSET_RE = re.compile(r'R\d+:([\d.]+)s')


def _parse_balanced_phase_end(log_path):
    """Return the minimum balanced-phase-end offset (seconds after start), or None."""
    text = Path(log_path).read_text()
    m = _BALANCED_END_RE.search(text)
    if not m:
        return None
    offsets = _REGION_OFFSET_RE.findall(m.group(1))
    if not offsets:
        return None
    return min(float(v) for v in offsets)


def parse_raw_csv(text):
    """Parse raw CSV text (no block marker) into a list of dicts with valid timestamp_s."""
    lines = [line for line in text.strip().splitlines() if line.strip()]
    if len(lines) < 2:
        return None
    rows = []
    for row in csv.DictReader(lines):
        timestamp = (row.get("timestamp_s") or "").strip()
        if not timestamp:
            continue
        try:
            float(timestamp)
        except ValueError:
            continue
        rows.append(row)
    return rows or None


def plot_timeline(tps_rows, mig_rows, label, output, latency_col, baseline_rows=None, warmup_s=None):
    tps_rows = sorted(tps_rows, key=lambda row: float(row["timestamp_s"]))
    t0 = float(tps_rows[0]["timestamp_s"])

    assert "tps" in tps_rows[0], "CSV_BLOCK must have a 'tps' column"

    tps_rows = [row for row in tps_rows if float(row["tps"]) <= TPS_OUTLIER]
    assert tps_rows, f"All LB TPS rows filtered (threshold={TPS_OUTLIER})"
    tps_times = [float(row["timestamp_s"]) - t0 for row in tps_rows]

    # Pre-process baseline so baseline_times is available for the latency section
    baseline_times = []
    if baseline_rows:
        baseline_rows = sorted(baseline_rows, key=lambda row: float(row["timestamp_s"]))
        baseline_rows = [row for row in baseline_rows if float(row["tps"]) <= TPS_OUTLIER]
        assert baseline_rows, f"All baseline TPS rows filtered (threshold={TPS_OUTLIER})"
        baseline_t0 = float(baseline_rows[0]["timestamp_s"])
        baseline_times = [float(row["timestamp_s"]) - baseline_t0 for row in baseline_rows]

    # Determine latency data before building the figure so we know how many panels to create
    lat_data = []       # (times, values, label, linestyle, marker) for each series
    if latency_col:
        col = f"lat_{latency_col}"
        lat_pairs = [
            (t, float(row[col]))
            for t, row in zip(tps_times, tps_rows)
            if row.get(col, "").strip()
        ]
        if lat_pairs:
            lt, lv = zip(*lat_pairs)
            lat_data.append((list(lt), list(lv), f"Commit latency {latency_col} (ms)", "-", "o"))
        if baseline_rows:
            bl_lat_pairs = [
                (t, float(row[col]))
                for t, row in zip(baseline_times, baseline_rows)
                if row.get(col, "").strip()
            ]
            if bl_lat_pairs:
                blt, blv = zip(*bl_lat_pairs)
                lat_data.append((list(blt), list(blv), f"Baseline commit latency {latency_col} (ms)", "--", "x"))

    n_panels = 2 + (1 if lat_data else 0)
    height_ratios = [3, 2, 1] if lat_data else [3, 1]
    fig, axes = plt.subplots(
        n_panels, 1, figsize=(10, 4 + 2 * n_panels), sharex=True,
        gridspec_kw={"height_ratios": height_ratios},
    )
    ax_tps = axes[0]
    ax_lat = axes[1] if lat_data else None
    ax_mig = axes[-1]

    # --- TPS panel ---
    total_tps_vals = [float(row["tps"]) for row in tps_rows]
    plot_times, plot_vals = _smoothed_series(tps_times, total_tps_vals)
    ax_tps.plot(
        plot_times, plot_vals, color="tab:blue", label="Total TPS (LB)",
        linewidth=2.0, marker="o", markersize=3,
    )
    if baseline_rows:
        baseline_tps_vals = [float(row["tps"]) for row in baseline_rows]
        plot_times, plot_vals = _smoothed_series(baseline_times, baseline_tps_vals)
        ax_tps.plot(
            plot_times, plot_vals, color="tab:blue", linestyle="--",
            label="Baseline TPS (no LB)", linewidth=1.5, marker="x", markersize=3,
        )
    ax_tps.set_ylabel("Certified TPS (tx/s)")
    ax_tps.yaxis.set_major_formatter(ticker.FuncFormatter(
        lambda x, _: f"{x/1000:.0f}k" if x >= 1000 else f"{x:.0f}"
    ))
    ax_tps.grid(True, alpha=0.3)
    ax_tps.set_title(label)
    ax_tps.legend(loc="upper left", fontsize=8)

    # --- Latency panel (separate) ---
    if ax_lat is not None:
        for lt, lv, lbl, ls, mk in lat_data:
            plot_times, plot_vals = _smoothed_series(lt, lv)
            ax_lat.plot(plot_times, plot_vals, color="tab:gray", linestyle=ls,
                        linewidth=1.5, marker=mk, markersize=4, label=lbl)
        ax_lat.set_ylabel(f"Commit latency {latency_col} (ms)")
        ax_lat.grid(True, alpha=0.3)
        ax_lat.legend(loc="upper left", fontsize=8)

    # --- Migrations panel ---
    if mig_rows:
        mig_rows = sorted(mig_rows, key=lambda row: float(row["timestamp_s"]))
        mig_times = [float(row["timestamp_s"]) - t0 for row in mig_rows]
        mig_counts = [int(row["n_migrations"]) for row in mig_rows]
        cumulative = list(np.cumsum(mig_counts))
        ax_mig.plot(mig_times, cumulative, color="tab:red", linewidth=1.5,
                    drawstyle="steps-post")
    else:
        ax_mig.text(
            0.5, 0.5, "no migrations", transform=ax_mig.transAxes,
            ha="center", va="center", fontsize=9, color="gray",
        )
    ax_mig.set_ylabel("Cumulative accounts migrated")
    ax_mig.set_xlabel("Time (s)")
    ax_mig.grid(True, alpha=0.3)

    # --- Warmup line across all panels ---
    if warmup_s is not None:
        for ax in axes:
            ax.axvline(warmup_s, color="black", linestyle="--", linewidth=1.2, alpha=0.7)

    plt.tight_layout()
    plt.savefig(output, dpi=150, bbox_inches="tight")


def plot_from_dirs(lb_dir, baseline_dir, title, output, lat_col, warmup_s):
    assert INCLUDED, "INCLUDED is empty — add labels to plot"
    lb_dir = Path(lb_dir)
    baseline_dir = Path(baseline_dir)

    series = []  # list of (label, lb_data, bl_data)
    for label in INCLUDED:
        lb_run_dir   = lb_dir   / f"{label}_run_1"
        base_run_dir = baseline_dir / f"{label}_run_1"
        assert lb_run_dir.exists(),   f"LB run dir not found: {lb_run_dir.resolve()}"
        assert base_run_dir.exists(), f"Baseline run dir not found: {base_run_dir.resolve()}"
        effective_lat = lat_col or "p90"
        lb_data = load_run(lb_run_dir,   effective_lat)
        bl_data = load_run(base_run_dir, effective_lat)
        series.append((label, lb_data, bl_data))

    if warmup_s is None:
        candidates = []
        for label in INCLUDED:
            for run_dir in (lb_dir / f"{label}_run_1", baseline_dir / f"{label}_run_1"):
                v = _parse_balanced_phase_end(run_dir / "output.log")
                if v is not None:
                    candidates.append(v)
        if candidates:
            warmup_s = min(candidates)

    show_lat = lat_col is not None
    n_panels = 3 if show_lat else 2
    height_ratios = [3, 2, 2] if show_lat else [3, 2]
    fig, axes = plt.subplots(
        n_panels, 1, figsize=(10, 4 + 2 * n_panels), sharex=True,
        gridspec_kw={"height_ratios": height_ratios},
    )
    ax_tps = axes[0]
    ax_lat = axes[1] if show_lat else None
    ax_mig = axes[-1]

    max_t = 0.0
    any_mig = False

    for i, (label, lb_data, bl_data) in enumerate(series):
        color = COLORS[i % len(COLORS)]
        lb_tps_times, lb_tps_vals, lb_lat_pairs, mig_times, mig_cumulative = lb_data
        bl_tps_times, bl_tps_vals, bl_lat_pairs, _, _ = bl_data

        if lb_tps_times:
            max_t = max(max_t, lb_tps_times[-1])
        if bl_tps_times:
            max_t = max(max_t, bl_tps_times[-1])

        plot_times, plot_vals = _smoothed_series(lb_tps_times, lb_tps_vals)
        ax_tps.plot(plot_times, plot_vals, color=color, linewidth=1.5,
                    marker="o", markersize=2, label=f"{label} LB")
        plot_times, plot_vals = _smoothed_series(bl_tps_times, bl_tps_vals)
        ax_tps.plot(plot_times, plot_vals, color=color, linewidth=1.5,
                    linestyle="--", marker="x", markersize=2, label=f"{label} BL")

        if show_lat:
            if lb_lat_pairs:
                lt, lv = zip(*lb_lat_pairs)
                plot_times, plot_vals = _smoothed_series(lt, lv)
                ax_lat.plot(plot_times, plot_vals, color=color, linewidth=1.5,
                            marker="o", markersize=2, label=f"{label} LB")
            if bl_lat_pairs:
                bt, bv = zip(*bl_lat_pairs)
                plot_times, plot_vals = _smoothed_series(bt, bv)
                ax_lat.plot(plot_times, plot_vals, color=color, linewidth=1.5,
                            linestyle="--", marker="x", markersize=2, label=f"{label} BL")

        if mig_times:
            any_mig = True
            ax_mig.step([0.0] + mig_times, [0] + mig_cumulative, where="post",
                        color=color, linewidth=1.5, label=label)
        else:
            ax_mig.step([0.0], [0], where="post", color=color, linewidth=1.5, label=label)

    if not any_mig:
        ax_mig.text(0.5, 0.5, "no migrations", transform=ax_mig.transAxes,
                    ha="center", va="center", fontsize=9, color="gray")

    left = TRIM_LEFT_DUR
    right = max_t - TRIM_RIGHT_DUR
    if right > left:
        ax_tps.set_xlim(left=left, right=right)

    ax_tps.set_title(title)
    ax_tps.set_ylabel("Committed TPS (tx/s)")
    ax_tps.yaxis.set_major_formatter(ticker.FuncFormatter(
        lambda x, _: f"{x/1000:.0f}k" if x >= 1000 else f"{x:.0f}"
    ))
    ax_tps.grid(True, alpha=0.3)

    if show_lat and ax_lat is not None:
        ax_lat.set_ylabel(f"Latency {lat_col} (ms)")
        ax_lat.grid(True, alpha=0.3)

    ax_mig.set_ylabel("Cumul. migrations")
    ax_mig.yaxis.set_major_formatter(ticker.FuncFormatter(
        lambda x, _: f"{x/1000:.0f}k" if x >= 1000 else f"{x:.0f}"
    ))
    ax_mig.set_xlabel("Time (s)")
    ax_mig.grid(True, alpha=0.3)

    if warmup_s is not None:
        for ax in axes:
            ax.axvline(warmup_s, color="black", linestyle="--", linewidth=1.2, alpha=0.7)

    # Single legend at top: LB series in row 1, BL series in row 2
    handles, labels = ax_tps.get_legend_handles_labels()
    lb_handles = [h for h, l in zip(handles, labels) if l.endswith(" LB")]
    lb_labels  = [l for l in labels if l.endswith(" LB")]
    bl_handles = [h for h, l in zip(handles, labels) if l.endswith(" BL")]
    bl_labels  = [l for l in labels if l.endswith(" BL")]
    fig.legend(
        lb_handles + bl_handles, lb_labels + bl_labels,
        loc="upper center", bbox_to_anchor=(0.5, 1.0),
        ncol=len(INCLUDED), fontsize=8, frameon=True,
    )

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    plt.savefig(output, dpi=150, bbox_inches="tight")
    print(f"Saved to {Path(output).resolve()}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "lb_dir",
        nargs="?",
        default=None,
        help="(Dir mode) Results dir from fig_3_tps_timeline_lb.sh.",
    )
    parser.add_argument(
        "baseline_dir",
        nargs="?",
        default=None,
        help="(Dir mode) Results dir from fig_3_tps_timeline_baseline.sh.",
    )
    parser.add_argument(
        "--label",
        default="pasted_log",
        help="Label to show in the plot title.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Output PNG path.",
    )
    parser.add_argument(
        "--latency",
        choices=["mean", "p50", "p90", "p95"],
        default=None,
        help="Latency metric to overlay on TPS panel right axis.",
    )
    parser.add_argument(
        "--baseline",
        action="store_true",
        help="Overlay baseline (no-LB) TPS from CSV_BLOCK_BASELINE on the main LB TPS plot.",
    )
    parser.add_argument(
        "--warmup",
        type=float,
        default=None,
        help="Time (s) at which imbalance starts; draws a vertical dashed line. "
             "In dir mode, auto-detected from 'Balanced phase end spread' in output.log if omitted.",
    )
    args = parser.parse_args()

    output = args.output or f"tps_migration_{args.label}.png"

    use_dir_mode = not (CSV_BLOCK.strip() or CSV_BLOCK_MIGRATION.strip() or CSV_BLOCK_BASELINE.strip())
    if use_dir_mode:
        assert args.lb_dir,       "Dir mode: provide lb_dir as first positional argument"
        assert args.baseline_dir, "Dir mode: provide baseline_dir as second positional argument"
        plot_from_dirs(args.lb_dir, args.baseline_dir, args.label, output, args.latency, args.warmup)
        return

    assert CSV_BLOCK.strip(), "No log input provided in CSV_BLOCK"
    tps_rows = parse_raw_csv(CSV_BLOCK)
    mig_rows = parse_raw_csv(CSV_BLOCK_MIGRATION) or []
    assert tps_rows, "No valid TPS rows found in CSV_BLOCK"

    baseline_rows = None
    if args.baseline:
        assert CSV_BLOCK_BASELINE.strip(), "CSV_BLOCK_BASELINE is empty — paste baseline tps_timeline.py output into it"
        baseline_rows = parse_raw_csv(CSV_BLOCK_BASELINE)
        assert baseline_rows, "No valid rows found in CSV_BLOCK_BASELINE"

    plot_timeline(tps_rows, mig_rows, args.label, output, args.latency, baseline_rows, args.warmup)
    print(f"Saved to {Path(output).resolve()}")


if __name__ == "__main__":
    main()
