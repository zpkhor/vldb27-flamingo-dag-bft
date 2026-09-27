#!/usr/bin/env python3
"""SIGMOD-style TPS / latency / migration plots for configs a, b, c, d.

Produces one single-column PNG per config (4 total).

Usage:
    python benchmark/exp/plot_paper_configs.py [--output-dir DIR] [--configs a b c d]
"""
import argparse
import re
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
from plot_lb_configs import (
    load_run,
    _smoothed_series,
    TRIM_LEFT_DUR,
    TRIM_RIGHT_DUR,
)

# Paper palette — teal, coral, blue, orange (order matches reference image)
COLORS = ["#2A9D8F", "#E76F51", "#1f77b4", "#ff7f0e"]


# ── hardcoded run directories ──────────────────────────────────────────────────

_RESULTS = Path(__file__).parent / "results"

# LB_DIR_ABC/LB_DIR_D are the 2f+1 reroute-quorum reproduction (HEAD 2a73984) at
# DURATION=1650; panel (c) has its own pair below.
# BL_DIR_ABC is still the April baseline: it was not re-run, so its cells are 900s under
# older code and their dashed lines stop well before the LB lines.
# LB_DIR_ABC is symlinks: six cells from abc_2f1_cloud_20260814_182415 unchanged, plus the
# two whose balanced phase stalled re-run at DURATION=1000 (see its PROVENANCE.md).
LB_DIR_ABC = _RESULTS / "abc_2f1_shared_rerun_20260822"
BL_DIR_ABC = _RESULTS / "tps_timeline_baseline_cloud_20260414_132153"
LB_DIR_D   = _RESULTS / "tps_timeline_lb_nodes_cloud_20260814_162154"
BL_DIR_D   = _RESULTS / "config_d_baseline_90k_20260815_102235"

# Panel (c) has its own dirs at DURATION=2000: its cells take far longer to drain their
# backlog than (a)/(b)/(d), so at 1650s the f+1 cell is still draining at the right edge
# and reads as unrecovered. At 2000s the drain completes inside the run.
LB_DIR_C = _RESULTS / "config_c_full_d2000_20260822"
# Panel (c)'s own baselines, at DURATION=1450 - long enough to cover the 1400s the panel
# plots, where the 900s BL_DIR_ABC cells stopped at two thirds of the x-axis. Produced by
# config_c_baseline_repro.sh; cell parameters are its LB twin's, plus BASELINE=1.
BL_DIR_C = _RESULTS / "config_c_baseline_d1450_20260825_150911"


# ── config definitions ─────────────────────────────────────────────────────────
# series entries: (run_subdir_name, legend_label). Both curves are looked up by that same
# subdir name - the solid one under lb_dir, the dashed baseline under bl_dir - so the two
# dirs must agree on cell names or a series silently loses its baseline.
#
# "max_t" pins the x-axis right edge to max_t - TRIM_RIGHT_DUR rather than deriving it from
# the measured run length, so a plain `python plot_paper_configs.py --pdf` reproduces the
# shipped figures with no flags:
#   a/b:   883 keeps the x-axis of the 900s figures these replace.
#   c:     1570 - a wider window than a/b/d, see the panel's own note.
#   d:     883 as well, so all four configs share one window. Migrations in config (d)
#          keep arriving to the end of the 1650s run, so no right edge captures "all"
#          of them - matching a/b/c makes the four panels directly comparable instead.
#          --max-t on the CLI overrides this.

CONFIGS = {
    "a": {
        "title":  "Effect of Submission Rate (n=4, 90% skew)",
        "lb_dir": LB_DIR_ABC,
        "bl_dir": BL_DIR_ABC,
        "series": [
            ("n4_v_rate_imb90_r110000_run_1", "110k tx/s"),
            ("n4_v_rate_imb90_r80000_run_1",  "80k tx/s"),
            ("n4_v_rate_imb90_r50000_run_2",  "50k tx/s"),
            ("n4_v_rate_imb90_r20000_run_2",  "20k tx/s"),
        ],
        "output": "config_a.png",
        "max_t":  883.0,
    },
    "b": {
        "title":  "Effect of Load Skew (n=4, 110k tx/s)",
        "lb_dir": LB_DIR_ABC,
        "bl_dir": BL_DIR_ABC,
        "series": [
            ("n4_v_rate_imb60_r110000_run_1", "60% skew"),
            ("n4_v_rate_imb90_r110000_run_1", "90% skew"),
            ("n4_v_rate_imb99_r110000_run_1", "99% skew"),
        ],
        "output": "config_b.png",
        "max_t":  883.0,
    },
    "c": {
        "title":  "Effect of Bandwidth Constraints (n=4, 90% skew)",
        "lb_dir": LB_DIR_C,
        "bl_dir": BL_DIR_C,
        "series": [
            ("n4_v_rate_imb90_r110000_run_1",   "Balanced BW"),
            ("n4_bw_f_rate_imb90_r110000_run_1", "f nodes BW-limited"),
            ("n4_bw_f1_rate_imb90_r65000_run_1", "f+1 nodes BW-limited"),
        ],
        "output": "config_c.png",
        # Right edge = 1570 - TRIM_RIGHT_DUR = 1400s, wider than the other panels because
        # this config drains its backlog so much later: the f+1 latency only reaches ~2s at
        # ~1290s. Anything below ~1460 cuts that drop off and the cell reads as unrecovered.
        "max_t":  1570.0,
    },
    "d": {
        "title":  "Scalability with Committee Size (60% skew, 90k tx/s)",
        "lb_dir": LB_DIR_D,
        "bl_dir": BL_DIR_D,
        "series": [
            ("n4_v_rate_imb60_r90000_run_1",  "n=4"),
            ("n10_v_rate_imb60_r90000_run_1", "n=10"),
            ("n16_v_rate_imb60_r90000_run_3", "n=16"),
            ("n21_v_rate_imb60_r90000_run_1", "n=21"),
        ],
        "output": "config_d.png",
        "max_t":  1030.0,
    },
}

CONFIG_ORDER = ["a", "b", "c", "d"]


# ── warmup detection ───────────────────────────────────────────────────────────

_BALANCED_END_RE = re.compile(r'Balanced phase end spread: \d+ ms \((.+?)\)')
_REGION_OFFSET_RE = re.compile(r'R\d+:([\d.]+)s')


def _detect_warmup(lb_dir, bl_dir, series):
    """Return min balanced-phase-end offset (seconds after start), or None."""
    candidates = []
    for subdir, _ in series:
        for run_dir in (lb_dir, bl_dir):
            log = run_dir / subdir / "output.log"
            if not log.exists():
                continue
            m = _BALANCED_END_RE.search(log.read_text())
            if not m:
                continue
            offsets = _REGION_OFFSET_RE.findall(m.group(1))
            if offsets:
                candidates.append(min(float(v) for v in offsets))
    return min(candidates) if candidates else None


# ── SIGMOD style ───────────────────────────────────────────────────────────────

def _apply_sigmod_style():
    plt.rcParams.update({
        "font.family":       "sans-serif",
        "font.size":         8,
        "axes.titlesize":    8,
        "axes.labelsize":    8,
        "xtick.labelsize":   7,
        "ytick.labelsize":   7,
        "legend.fontsize":   7,
        "lines.linewidth":   1.0,
        "axes.linewidth":    0.6,
        "grid.linewidth":    0.4,
        "xtick.major.width": 0.6,
        "ytick.major.width": 0.6,
        "pdf.fonttype":      42,
        "ps.fonttype":       42,
        "grid.linestyle":    "--",
        "grid.color":        "lightgray",
        "grid.alpha":        0.8,
    })


# ── per-config plot ────────────────────────────────────────────────────────────

def plot_config(cfg_key, output_dir, show_title, save_pdf, lb_dir_override, max_t_override):
    cfg      = CONFIGS[cfg_key]
    lb_dir   = Path(lb_dir_override) if lb_dir_override else Path(cfg["lb_dir"])
    bl_dir   = Path(cfg["bl_dir"])
    series   = cfg["series"]
    title    = cfg["title"]
    out_path = Path(output_dir) / cfg["output"]

    warmup_s = _detect_warmup(lb_dir, bl_dir, series)
    if warmup_s is None:
        print(f"  WARNING: no warmup line detected for config {cfg_key}")

    fig, axes = plt.subplots(
        3, 1, figsize=(2.7, 4.2), sharex=True,
        gridspec_kw={"height_ratios": [3, 2, 2]},
    )
    ax_tps, ax_lat, ax_mig = axes

    any_mig  = False
    color_handles = []

    for i, (subdir, label) in enumerate(series):
        color = COLORS[i % len(COLORS)]
        lb_tps_times, lb_tps_vals, lb_lat_pairs, mig_times, mig_cumulative = \
            load_run(lb_dir / subdir, "mean")
        bl_tps_times, bl_tps_vals, bl_lat_pairs, _, _ = load_run(bl_dir / subdir, "mean")

        # TPS
        pt, pv = _smoothed_series(lb_tps_times, lb_tps_vals)
        ax_tps.plot(pt, pv, color=color, linestyle="-",  linewidth=1.0)
        pt, pv = _smoothed_series(bl_tps_times, bl_tps_vals)
        ax_tps.plot(pt, pv, color=color, linestyle="--", linewidth=0.8)

        # Latency
        if lb_lat_pairs:
            lt, lv = zip(*lb_lat_pairs)
            pt, pv = _smoothed_series(list(lt), list(lv))
            ax_lat.plot(pt, pv, color=color, linestyle="-",  linewidth=1.0)
        if bl_lat_pairs:
            bt, bv = zip(*bl_lat_pairs)
            pt, pv = _smoothed_series(list(bt), list(bv))
            ax_lat.plot(pt, pv, color=color, linestyle="--", linewidth=0.8)

        # Migrations — LB only (baseline has zero migrations)
        if mig_times:
            any_mig = True
            ax_mig.step([0.0] + mig_times, [0] + mig_cumulative,
                        where="post", color=color, linewidth=1.0)

        color_handles.append(
            mlines.Line2D([], [], color=color, linewidth=1.0, label=label)
        )

    if not any_mig:
        ax_mig.text(0.5, 0.5, "no migrations", transform=ax_mig.transAxes,
                    ha="center", va="center", fontsize=7, color="gray")

    # x limits. The right edge is pinned per config, or by --max-t, rather than taken from
    # the measured run length, so a re-run at a longer DURATION keeps the x-axis of the
    # figure it replaces. Trim widths stay TRIM_LEFT_DUR/TRIM_RIGHT_DUR either way.
    max_t = max_t_override if max_t_override > 0 else cfg["max_t"]
    left  = TRIM_LEFT_DUR
    right = max_t - TRIM_RIGHT_DUR
    assert right > left, (
        f"config {cfg_key}: max_t={max_t} leaves nothing between the trims "
        f"(left {TRIM_LEFT_DUR}s, right {TRIM_RIGHT_DUR}s)"
    )
    ax_tps.set_xlim(left=left, right=right)

    # Warmup line across all panels
    if warmup_s is not None:
        for ax in axes:
            ax.axvline(warmup_s, color="black", linestyle="--", linewidth=0.8, alpha=0.7)

    # Panel labels / formatting
    if show_title:
        ax_tps.set_title(title)
    ax_tps.set_ylabel("Throughput [ktrans/s]", fontsize=6)
    ax_tps.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}"))
    ax_tps.grid(True)

    ax_lat.set_ylabel("Mean Latency [sec]", fontsize=6)
    ax_lat.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.1f}"))
    ax_lat.grid(True)

    ax_mig.set_ylabel("Cumul. Migs", fontsize=6)
    ax_mig.yaxis.set_major_formatter(
        ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}k" if x >= 1000 else f"{x:.0f}"))
    ax_mig.set_xlabel("Time (s)")
    ax_mig.grid(True)

    for ax in axes:
        ax.yaxis.set_major_locator(MaxNLocator(nbins=3))
        ax.xaxis.set_major_locator(MaxNLocator(nbins=4))

    # Legend: above the panels at figure level so it can use full figure width
    baseline_proxy = mlines.Line2D(
        [], [], color="gray", linestyle="--", linewidth=0.8, label="Without load bal."
    )
    n_entries = len(color_handles) + 1
    ncol = (n_entries + 1) // 2  # two rows max
    fig.legend(
        handles=color_handles + [baseline_proxy],
        loc="upper center",
        bbox_to_anchor=(0.57, 0.95),
        ncol=ncol,
        frameon=False,
        fontsize=6,
        handlelength=1.4,
        handletextpad=0.4,
        columnspacing=0.8,
        borderpad=0.2,
        labelspacing=0.25,
    )

    fig.subplots_adjust(left=0.20, right=0.97, top=0.88, bottom=0.10, hspace=0.18)
    plt.savefig(out_path, dpi=200, bbox_inches="tight", pad_inches=0.02)
    print(f"Saved: {out_path.resolve()}")
    if save_pdf:
        pdf_path = out_path.with_suffix(".pdf")
        plt.savefig(pdf_path, bbox_inches="tight", pad_inches=0.02)
        print(f"Saved: {pdf_path.resolve()}")
    plt.close(fig)


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="SIGMOD-style TPS/latency/migration timeline plots for configs a–d."
    )
    ap.add_argument("--output-dir", default=".", help="Directory to write PNGs into.")
    ap.add_argument(
        "--configs", nargs="+", choices=CONFIG_ORDER, default=CONFIG_ORDER,
        metavar="CONFIG",
        help="Which configs to plot (default: all). Choices: a b c d",
    )
    ap.add_argument("--title", action="store_true", help="Show title on each plot.")
    ap.add_argument("--pdf",   action="store_true", help="Also save a PDF alongside the PNG.")
    ap.add_argument(
        "--lb-dir", default="",
        help="Override the hardcoded LB results dir for the selected configs "
             "(baseline dir is unchanged). Empty means use the hardcoded dir.",
    )
    ap.add_argument(
        "--max-t", type=float, default=0.0,
        help="Pin the x-axis right edge to <MAX_T> - TRIM_RIGHT_DUR for every selected "
             "config. 0 means use each config's own pinned max_t.",
    )
    args = ap.parse_args()

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    _apply_sigmod_style()

    for cfg_key in args.configs:
        print(f"Plotting config {cfg_key} ...")
        plot_config(cfg_key, args.output_dir, args.title, args.pdf, args.lb_dir, args.max_t)


if __name__ == "__main__":
    main()
