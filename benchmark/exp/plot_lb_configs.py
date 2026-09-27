#!/usr/bin/env python3
"""Plot TPS / latency / migration timelines for configs a, b, c from a lb sweep results dir.

Configs are auto-detected from the run directory names:
  a) varying load   — n4_v_rate_imb90, all rates
  b) varying imb %  — n4_v_rate_imb{60,90,99}, rate 120k
  c) varying BW     — n4_{v_rate_imb90,bw_f_rate_imb90,bw_f1_rate_imb90}, rate 120k

Each figure: 3 stacked panels (TPS / latency / cumulative migrations), one line per series.

Usage:
    python benchmark/exp/plot_lb_configs.py <results_dir>
        [--latency {mean,p50,p90,p95}]
        [--output-dir <dir>]
"""
import argparse
import csv
import re
from pathlib import Path

import numpy as np
np.Inf = np.inf  # patch for matplotlib compatibility with NumPy 2.0
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


TPS_BLK_RE = re.compile(r"\+ TPS_TIMELINE_CSV:\n(.*?)(?=\n\s*\+|\n-{5}|\Z)", re.DOTALL)
MIG_BLK_RE = re.compile(r"\+ MIGRATION_EVENTS_CSV:\n(.*?)(?=\n\s*\+|\n-{5}|\Z)", re.DOTALL)

# ── config membership ──────────────────────────────────────────────────────────
# Labels are the part after stripping the "n<N>_" prefix from the dir name.
CONFIG_A_LABEL = "v_rate_imb90"   # any rate; series key = rate int
CONFIG_B_LABELS = {"v_rate_imb60", "v_rate_imb90", "v_rate_imb99"}
CONFIG_C_LABELS = {"v_rate_imb90", "bw_f_rate_imb90", "bw_f1_rate_imb90"}

CONFIG_B_DISPLAY = {
    "v_rate_imb60": "imb 60%",
    "v_rate_imb90": "imb 90%",
    "v_rate_imb99": "imb 99%",
}
CONFIG_B_ORDER = ["v_rate_imb60", "v_rate_imb90", "v_rate_imb99"]

CONFIG_C_DISPLAY = {
    "v_rate_imb90":     "Balanced BW",
    "bw_f_rate_imb90":  "BW limit f",
    "bw_f1_rate_imb90": "BW limit f+1",
}
CONFIG_C_ORDER = ["v_rate_imb90", "bw_f_rate_imb90", "bw_f1_rate_imb90"]

COLORS = ["tab:blue", "tab:orange", "tab:green", "tab:red"]
TPS_OUTLIER = 200_000
SMOOTH_WINDOW = 3   # rolling-average window (bins); 1 = no smoothing
# TRIM_LEFT_DUR = 20   # seconds to trim from the start of each plot
# TRIM_RIGHT_DUR = 520  # seconds to trim from the end of each plot
# For config d
TRIM_LEFT_DUR = 100   # seconds to trim from the start of each plot
TRIM_RIGHT_DUR = 170  # seconds to trim from the end of each plot
DIR_RE = re.compile(r"^n\d+_(.+)_r(\d+)_run_\d+$")


# ── data loading ───────────────────────────────────────────────────────────────

def _t0_abs_from_log(log_path):
    """First absolute timestamp from the TPS or migration CSV block in output.log."""
    text = log_path.read_text()
    for blk_re in (TPS_BLK_RE, MIG_BLK_RE):
        m = blk_re.search(text)
        if not m:
            continue
        for row in csv.DictReader(m.group(1).strip().splitlines()):
            return float(row["timestamp_s"])
    assert False, f"No absolute timestamp found in {log_path}"


def _parse_migrations(log_path, t0_abs):
    text = log_path.read_text()
    m = MIG_BLK_RE.search(text)
    if not m:
        return [], []
    rows = []
    for r in csv.DictReader(m.group(1).strip().splitlines()):
        try:
            float(r["timestamp_s"])
        except (ValueError, KeyError):
            continue
        rows.append(r)
    times = [float(r["timestamp_s"]) - t0_abs for r in rows]
    counts = [int(r["n_migrations"]) for r in rows]
    return times, counts


def load_run(run_dir, lat_col):
    """Return (tps_times, tps_vals, lat_pairs, mig_times, mig_cumulative)."""
    run_dir = Path(run_dir)
    tps_path = run_dir / "tps_timeline.csv"
    log_path = run_dir / "output.log"

    assert tps_path.exists(), (
        f"Missing tps_timeline.csv in {run_dir.resolve()}\n"
        "  -> run: bash benchmark/exp/tps_timeline_batch.sh <results_dir>"
    )
    assert log_path.exists(), f"Missing output.log in {run_dir.resolve()}"

    with open(tps_path) as f:
        rows = list(csv.DictReader(f))
    assert rows, f"Empty tps_timeline.csv in {run_dir}"

    # Support old header 'time_s' (relative, starts at 0.0) and new 'timestamp_s' (absolute epoch)
    time_col = "timestamp_s" if "timestamp_s" in rows[0] else "time_s"
    first_val = float(rows[0][time_col])
    is_absolute = first_val > 1e9

    if is_absolute:
        t0_abs = first_val
        tps_times = [float(r[time_col]) - t0_abs for r in rows]
    else:
        tps_times = [float(r[time_col]) for r in rows]
        t0_abs = _t0_abs_from_log(log_path)

    # Drop outlier bins
    pairs = [(t, r) for t, r in zip(tps_times, rows) if float(r["tps"]) <= TPS_OUTLIER]
    assert pairs, f"All TPS rows filtered (threshold={TPS_OUTLIER}) in {run_dir}"
    tps_times, rows = map(list, zip(*pairs))

    tps_vals = [float(r["tps"]) for r in rows]

    lat_field = f"lat_{lat_col}"
    lat_pairs = [
        (t, float(r[lat_field]))
        for t, r in zip(tps_times, rows)
        if r.get(lat_field, "").strip()
    ]

    mig_times, mig_counts = _parse_migrations(log_path, t0_abs)
    mig_cumulative = list(np.cumsum(mig_counts)) if mig_counts else []

    return tps_times, tps_vals, lat_pairs, mig_times, mig_cumulative


# ── config discovery ───────────────────────────────────────────────────────────

def discover_configs(results_dir):
    """Return (config_a, config_b, config_c), each a list of (display_label, run_dir)."""
    results_dir = Path(results_dir)
    config_a, config_b, config_c = [], [], []

    for subdir in sorted(results_dir.iterdir()):
        if not subdir.is_dir():
            continue
        m = DIR_RE.match(subdir.name)
        if not m:
            continue
        label, rate = m.group(1), int(m.group(2))

        if label == CONFIG_A_LABEL:
            rate_k = rate // 1000
            config_a.append((rate, f"{rate_k}k tx/s", subdir))

        if rate == 120000 and label in CONFIG_B_LABELS:
            sort_key = CONFIG_B_ORDER.index(label)
            config_b.append((sort_key, CONFIG_B_DISPLAY[label], subdir))

        if rate == 120000 and label in CONFIG_C_LABELS:
            sort_key = CONFIG_C_ORDER.index(label)
            config_c.append((sort_key, CONFIG_C_DISPLAY[label], subdir))

    config_a.sort(key=lambda x: x[0])
    config_b.sort(key=lambda x: x[0])
    config_c.sort(key=lambda x: x[0])

    return (
        [(disp, d) for _, disp, d in config_a],
        [(disp, d) for _, disp, d in config_b],
        [(disp, d) for _, disp, d in config_c],
    )


# ── plotting ───────────────────────────────────────────────────────────────────

def _smoothed_series(times, vals, window=SMOOTH_WINDOW):
    """Return a moving-average series with only the unsupported right edge trimmed."""
    if window <= 1 or len(vals) < window:
        return list(times), list(vals)

    kernel = np.ones(window) / window
    smoothed = list(np.convolve(vals, kernel, mode="valid"))
    return list(times[:len(smoothed)]), smoothed


def _k_fmt():
    return ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}k" if x >= 1000 else f"{x:.0f}")


def plot_config(series_list, title, lat_col, output_path):
    assert series_list, f"No runs for: {title}"

    fig, (ax_tps, ax_lat, ax_mig) = plt.subplots(
        3, 1, figsize=(10, 8), sharex=True,
        gridspec_kw={"height_ratios": [3, 2, 2]},
    )

    any_mig = False
    max_t = 0.0

    for i, (series_label, run_dir) in enumerate(series_list):
        color = COLORS[i % len(COLORS)]
        tps_times, tps_vals, lat_pairs, mig_times, mig_cumulative = load_run(run_dir, lat_col)

        max_t = max(max_t, tps_times[-1])

        plot_times, plot_vals = _smoothed_series(tps_times, tps_vals)
        ax_tps.plot(plot_times, plot_vals, color=color, linewidth=1.5,
                    marker="o", markersize=2, label=series_label)

        if lat_pairs:
            lt, lv = zip(*lat_pairs)
            plot_times, plot_vals = _smoothed_series(lt, lv)
            ax_lat.plot(plot_times, plot_vals, color=color, linewidth=1.5,
                        marker="o", markersize=2, label=series_label)

        if mig_times:
            any_mig = True
            st = [0.0] + mig_times
            sv = [0] + mig_cumulative
            ax_mig.step(st, sv, where="post", color=color, linewidth=1.5, label=series_label)
        else:
            ax_mig.step([0.0], [0], where="post", color=color, linewidth=1.5, label=series_label)

    if not any_mig:
        ax_mig.text(0.5, 0.5, "no migrations", transform=ax_mig.transAxes,
                    ha="center", va="center", fontsize=9, color="gray")

    left = TRIM_LEFT_DUR
    right = max_t - TRIM_RIGHT_DUR
    if right > left:
        ax_tps.set_xlim(left=left, right=right)

    ax_tps.set_title(title)
    ax_tps.set_ylabel("Committed TPS (tx/s)")
    ax_tps.yaxis.set_major_formatter(_k_fmt())
    ax_tps.legend(loc="lower right", fontsize=8)
    ax_tps.grid(True, alpha=0.3)

    ax_lat.set_ylabel(f"Latency {lat_col} (ms)")
    ax_lat.grid(True, alpha=0.3)

    ax_mig.set_ylabel("Cumul. migrations")
    ax_mig.yaxis.set_major_formatter(_k_fmt())
    ax_mig.set_xlabel("Time (s)")
    ax_mig.grid(True, alpha=0.3)

    plt.tight_layout()
    output_path = Path(output_path)
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {output_path.resolve()}")


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Plot TPS/latency/migration timelines for configs a/b/c."
    )
    ap.add_argument("results_dir")
    ap.add_argument("--latency", choices=["mean", "p50", "p90", "p95"], default="p90")
    ap.add_argument("--output-dir")
    args = ap.parse_args()

    results_dir = Path(args.results_dir)
    assert results_dir.is_dir(), f"Not a directory: {results_dir.resolve()}"
    output_dir = Path(args.output_dir) if args.output_dir else results_dir

    config_a, config_b, config_c = discover_configs(results_dir)

    print(f"Config a ({len(config_a)} runs): {[s for s, _ in config_a]}")
    print(f"Config b ({len(config_b)} runs): {[s for s, _ in config_b]}")
    print(f"Config c ({len(config_c)} runs): {[s for s, _ in config_c]}")

    configs = [
        (config_a, "Config a — varying load (n4, imb 90%, balanced BW)",       "config_a.png"),
        (config_b, "Config b — varying imbalance (n4, 120k tx/s, balanced BW)", "config_b.png"),
        (config_c, "Config c — varying BW scenario (n4, imb 90%, 120k tx/s)",   "config_c.png"),
    ]
    for series, title, fname in configs:
        if not series:
            print(f"WARNING: no runs found for {fname}, skipping")
            continue
        plot_config(series, title, args.latency, output_dir / fname)


if __name__ == "__main__":
    main()
