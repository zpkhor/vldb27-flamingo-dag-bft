#!/usr/bin/env python3
"""SIGMOD-style TPS / latency / migration plots for fig_ablation.sh results.

Produces one single-column PNG per load shape:
  - ablation_bal.png: balanced-load baseline plus four malicious cases
  - ablation_imb.png: imbalanced-load baseline plus four malicious cases

Expected inputs:
  baseline_dir: fig_ablation.sh results with BASELINE=1
                run dirs n4_bal_r<rate>_run_<run>, n4_imb_r<rate>_run_<run>
  ablation_dir: fig_ablation.sh results with BASELINE=0
                run dirs n4_{bal,imb}_{inflqd,deflqd,deflcap,inflcap}_r<rate>_run_<run>

Run benchmark/exp/tps_timeline_batch.sh on both dirs first so each run dir has
`tps_timeline.csv`. Migration events are read from each run's output.log.

Usage:
    python benchmark/exp/plot_paper_ablation.py BASELINE_DIR ABLATION_DIR [--output-dir DIR]
    python exp/plot_paper_ablation.py   /home/zpkhor/narwhal-validator/benchmark/exp/results/ablation_cloud_baseline_20260825_162809   /home/zpkhor/narwhal-validator/benchmark/exp/results/ablation_cloud_20260825_181540 --output-dir figs_ablation_repro --pdf
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
from plot_lb_configs import (  # noqa: E402
    load_run,
    _smoothed_series,
    TRIM_LEFT_DUR,
    TRIM_RIGHT_DUR,
)

# Paper palette: teal, coral, blue, orange (same order as plot_paper_configs.py)
COLORS = ["#2A9D8F", "#E76F51", "#1f77b4", "#ff7f0e"]

LOAD_ORDER = ["bal", "imb"]
LOAD_TITLES = {
    "bal": "Balanced Load Ablation (n=4, 110k tx/s)",
    "imb": "Imbalanced Load Ablation (n=4, 110k tx/s)",
}
LOAD_OUTPUTS = {
    "bal": "ablation_bal.png",
    "imb": "ablation_imb.png",
}
CASE_ORDER = ["inflqd", "deflqd", "deflcap", "inflcap"]
CASE_LABELS = {
    "inflqd": "Inflated QD",
    "deflqd": "Deflated QD",
    "deflcap": "Deflated cap.",
    "inflcap": "Inflated cap.",
}

DIR_RE = re.compile(r"^n(?P<n>\d+)_(?P<label>.+)_r(?P<rate>\d+)_run_(?P<run>\d+)$")
_BALANCED_END_RE = re.compile(r"Balanced phase end spread: \d+ ms \((.+?)\)")
_REGION_OFFSET_RE = re.compile(r"R\d+:([\d.]+)s")


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


def _k_fmt():
    return ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}k" if x >= 1000 else f"{x:.0f}")


def discover_runs(results_dir):
    """Return {(label, rate, run): run_dir} for fig_ablation-style run dirs."""
    runs = {}
    for subdir in sorted(Path(results_dir).iterdir()):
        if not subdir.is_dir():
            continue
        m = DIR_RE.match(subdir.name)
        if not m:
            continue
        label = m.group("label")
        rate = int(m.group("rate"))
        run = int(m.group("run"))
        runs[(label, rate, run)] = subdir
    return runs


def choose_rate(ablation_runs, requested_rate):
    if requested_rate is not None:
        return requested_rate
    rates = sorted({rate for label, rate, _ in ablation_runs if any(label.startswith(f"{load}_") for load in LOAD_ORDER)})
    assert rates, "No ablation run dirs found; expected labels like n4_bal_inflqd_r110000_run_1"
    assert len(rates) == 1, f"Multiple rates found {rates}; pass --rate"
    return rates[0]


def _detect_warmup(run_dirs):
    """Return min balanced-phase-end offset (seconds after start), or None."""
    candidates = []
    for run_dir in run_dirs:
        log = Path(run_dir) / "output.log"
        if not log.exists():
            continue
        text = log.read_text()
        m = _BALANCED_END_RE.search(text)
        if not m:
            continue
        offsets = _REGION_OFFSET_RE.findall(m.group(1))
        if offsets:
            candidates.append(min(float(v) for v in offsets))
    return min(candidates) if candidates else None


def build_load_config(load, baseline_runs, ablation_runs, rate, run):
    baseline_key = (load, rate, run)
    assert baseline_key in baseline_runs, (
        f"Missing BASELINE=1 run {baseline_key}; expected "
        f"n4_{load}_r{rate}_run_{run} under baseline_dir"
    )

    cases = []
    missing = []
    for case in CASE_ORDER:
        label = f"{load}_{case}"
        key = (label, rate, run)
        if key not in ablation_runs:
            missing.append(f"n4_{label}_r{rate}_run_{run}")
            continue
        cases.append((case, CASE_LABELS[case], ablation_runs[key]))
    assert not missing, "Missing BASELINE=0 ablation run dirs: " + ", ".join(missing)

    return {
        "load": load,
        "title": LOAD_TITLES[load],
        "output": LOAD_OUTPUTS[load],
        "baseline": baseline_runs[baseline_key],
        "cases": cases,
    }


def plot_load(cfg, output_dir, lat_col, show_title, save_pdf, trim_left, trim_right):
    out_path = Path(output_dir) / cfg["output"]
    baseline_dir = cfg["baseline"]
    cases = cfg["cases"]
    warmup_s = _detect_warmup([baseline_dir] + [run_dir for _, _, run_dir in cases])
    if warmup_s is None:
        print(f"  WARNING: no warmup line detected for {cfg['load']}")

    fig, axes = plt.subplots(
        3, 1, figsize=(2.7, 4.2), sharex=True,
        gridspec_kw={"height_ratios": [3, 2, 2]},
    )
    ax_tps, ax_lat, ax_mig = axes

    max_t = 0.0
    any_mig = False
    case_handles = []

    bl_tps_times, bl_tps_vals, bl_lat_pairs, _, _ = load_run(baseline_dir, lat_col)
    if bl_tps_times:
        max_t = max(max_t, bl_tps_times[-1])
    pt, pv = _smoothed_series(bl_tps_times, bl_tps_vals)
    ax_tps.plot(pt, pv, color="gray", linestyle="--", linewidth=0.8)
    if bl_lat_pairs:
        bt, bv = zip(*bl_lat_pairs)
        pt, pv = _smoothed_series(list(bt), list(bv))
        ax_lat.plot(pt, pv, color="gray", linestyle="--", linewidth=0.8)

    for i, (_case, label, run_dir) in enumerate(cases):
        color = COLORS[i % len(COLORS)]
        tps_times, tps_vals, lat_pairs, mig_times, mig_cumulative = load_run(run_dir, lat_col)
        if tps_times:
            max_t = max(max_t, tps_times[-1])

        pt, pv = _smoothed_series(tps_times, tps_vals)
        ax_tps.plot(pt, pv, color=color, linestyle="-", linewidth=1.0)

        if lat_pairs:
            lt, lv = zip(*lat_pairs)
            pt, pv = _smoothed_series(list(lt), list(lv))
            ax_lat.plot(pt, pv, color=color, linestyle="-", linewidth=1.0)

        if mig_times:
            any_mig = True
            ax_mig.step([0.0] + mig_times, [0] + mig_cumulative,
                        where="post", color=color, linewidth=1.0)

        case_handles.append(mlines.Line2D([], [], color=color, linewidth=1.0, label=label))

    if not any_mig:
        ax_mig.text(0.5, 0.5, "no migrations", transform=ax_mig.transAxes,
                    ha="center", va="center", fontsize=7, color="gray")

    right = max_t - trim_right
    if right > trim_left:
        ax_tps.set_xlim(left=trim_left, right=right)

    if warmup_s is not None:
        for ax in axes:
            ax.axvline(warmup_s, color="black", linestyle="--", linewidth=0.8, alpha=0.7)

    if show_title:
        ax_tps.set_title(cfg["title"])
    ax_tps.set_ylabel("Throughput [ktrans/s]", fontsize=6)
    ax_tps.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}"))
    ax_tps.grid(True)

    ax_lat.set_ylabel(f"{lat_col.upper()} Latency [sec]", fontsize=6)
    ax_lat.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.1f}"))
    ax_lat.grid(True)

    ax_mig.set_ylabel("Cumul. Migs", fontsize=6)
    ax_mig.yaxis.set_major_formatter(_k_fmt())
    ax_mig.set_xlabel("Time (s)")
    ax_mig.grid(True)

    for ax in axes:
        ax.yaxis.set_major_locator(MaxNLocator(nbins=3))
        ax.xaxis.set_major_locator(MaxNLocator(nbins=4))

    baseline_proxy = mlines.Line2D([], [], color="gray", linestyle="--", linewidth=0.8, label="Baseline")
    handles = case_handles + [baseline_proxy]
    fig.legend(
        handles=handles,
        loc="upper center",
        bbox_to_anchor=(0.57, 0.95),
        ncol=3,
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


def main():
    ap = argparse.ArgumentParser(
        description="SIGMOD-style TPS/latency/migration timeline plots for fig_ablation.sh."
    )
    ap.add_argument("baseline_dir", help="fig_ablation.sh BASELINE=1 results dir")
    ap.add_argument("ablation_dir", help="fig_ablation.sh BASELINE=0 results dir")
    ap.add_argument("--output-dir", default=".", help="Directory to write PNGs into.")
    ap.add_argument("--loads", nargs="+", choices=LOAD_ORDER, default=LOAD_ORDER,
                    help="Which load shapes to plot (default: bal imb).")
    ap.add_argument("--rate", type=int, default=None,
                    help="Input rate to plot. Auto-detected when there is only one rate.")
    ap.add_argument("--run", type=int, default=1, help="Retry/run number to plot (default: 1).")
    ap.add_argument("--latency", choices=["mean", "p50", "p90", "p95"], default="mean",
                    help="Latency statistic to plot (default: mean).")
    ap.add_argument("--trim-left", type=float, default=TRIM_LEFT_DUR,
                    help=f"Seconds to trim from left of x-axis (default: {TRIM_LEFT_DUR}).")
    ap.add_argument("--trim-right", type=float, default=TRIM_RIGHT_DUR,
                    help=f"Seconds to trim from right of x-axis (default: {TRIM_RIGHT_DUR}).")
    ap.add_argument("--title", action="store_true", help="Show title on each plot.")
    ap.add_argument("--pdf", action="store_true", help="Also save a PDF alongside the PNG.")
    args = ap.parse_args()

    baseline_dir = Path(args.baseline_dir)
    ablation_dir = Path(args.ablation_dir)
    assert baseline_dir.exists(), f"Path not found: {baseline_dir}"
    assert ablation_dir.exists(), f"Path not found: {ablation_dir}"

    baseline_runs = discover_runs(baseline_dir)
    ablation_runs = discover_runs(ablation_dir)
    rate = choose_rate(ablation_runs, args.rate)

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    _apply_sigmod_style()

    for load in args.loads:
        print(f"Plotting {load} ablation (rate={rate}, run={args.run}) ...")
        cfg = build_load_config(load, baseline_runs, ablation_runs, rate, args.run)
        plot_load(cfg, args.output_dir, args.latency, args.title, args.pdf,
                  args.trim_left, args.trim_right)


if __name__ == "__main__":
    main()
