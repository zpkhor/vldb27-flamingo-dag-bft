#!/usr/bin/env python3
"""Plot 4-bar grouped bar chart: balanced baseline + 3 LB migration phases.

Bars per input rate:
  1. Balanced   (from baseline dir, n4_balanced label)
  2. Imbalanced (from baseline dir, matching imbalanced label)
  3. Migration  (from LB dir) — between first and last migration event
  4. Post-migration (from LB dir) — after last migration event

Requires both dirs to have been processed by parse_tps_migration_timeline.py
(producing tps_timeline.csv and migration_events.csv).

Usage:
    python plot_3phase_tps_latency.py <baseline_dir> <lb_dir> [-o output_prefix] [--latency p90]
"""
import argparse
import csv
from collections import defaultdict
from pathlib import Path
from statistics import mean

import numpy as np
np.Inf = np.inf  # patch for matplotlib compatibility with NumPy 2.0
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


LABEL_MAP = {
    "n4_balanced":      "Balanced",
    "n4_v_rate_imb60":  "Validator rate imb. 60%",
    "n4_v_rate_imb90":  "Validator rate imb. 90%",
    "n4_e_rate_imb60":  "Executor rate imb. 60%",
    "n4_e_rate_imb90":  "Executor rate imb. 90%",
    "n4_ve_rate_imb60": "V+E rate imb. 60%",
    "n4_ve_rate_imb90": "V+E rate imb. 90%",
    "n4_bw_f":          "BW limit (f)",
    "n4_bw_f1":         "BW limit (f+1)",
}

BALANCED_LABEL = "n4_balanced"

PHASE_COLORS = {
    "Balanced":       "tab:blue",
    "Imbalanced":     "tab:red",
    "Migration":      "tab:orange",
    "Post-migration": "tab:green",
}
PHASE_NAMES = list(PHASE_COLORS.keys())


@ticker.FuncFormatter
def tps_formatter(x, pos):
    if x >= 1_000:
        return f'{x/1000:.0f}k'
    return f'{x:.0f}'


@ticker.FuncFormatter
def sec_formatter(x, pos):
    return f'{float(x)/1000:.1f}'


def load_csv(path):
    assert path.exists(), f"File not found: {path}"
    with open(path, newline="") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        reader.fieldnames = [name.strip() for name in reader.fieldnames]
        return list(reader)


def get_validator_cols(fieldnames):
    return [c for c in fieldnames if c.startswith('v') and c[1:].isdigit()]


def total_tps(row, v_cols):
    return sum(float(row[c]) for c in v_cols)


def group_by_key(rows, key_fn):
    groups = defaultdict(list)
    for row in rows:
        groups[key_fn(row)].append(row)
    return groups


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("baseline_dir", help="Baseline (imbalanced, no LB) results dir")
    parser.add_argument("lb_dir", help="LB results dir")
    parser.add_argument("-o", "--output", help="Output path prefix (default: <lb_dir>/3phase_<label>)")
    parser.add_argument("--latency", default="p90", choices=["p50", "p90", "p95"],
                        help="E2E latency percentile to plot (default: p90)")
    args = parser.parse_args()

    baseline_dir = Path(args.baseline_dir)
    lb_dir = Path(args.lb_dir)
    lat_col = f"e2e_{args.latency}_ms"

    # Load CSVs
    baseline_tps_rows = load_csv(baseline_dir / "tps_timeline.csv")
    lb_tps_rows = load_csv(lb_dir / "tps_timeline.csv")
    lb_mig_rows = load_csv(lb_dir / "migration_events.csv")

    # Detect validator columns from LB tps CSV
    with open(lb_dir / "tps_timeline.csv", newline="") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        reader.fieldnames = [name.strip() for name in reader.fieldnames]
        v_cols = get_validator_cols(reader.fieldnames)
    assert v_cols, "No validator columns (v0, v1, ...) found in tps_timeline.csv"
    assert lat_col in lb_tps_rows[0], f"Column {lat_col} not found in tps_timeline.csv"

    # Validate: baseline dir must contain n4_balanced plus the imbalanced labels in LB dir
    baseline_labels = {r["label"].strip() for r in baseline_tps_rows}
    lb_labels = {r["label"].strip() for r in lb_tps_rows}
    assert BALANCED_LABEL in baseline_labels, (
        f"Baseline dir missing '{BALANCED_LABEL}' label"
    )
    imbalanced_baseline_labels = baseline_labels - {BALANCED_LABEL}
    assert imbalanced_baseline_labels == lb_labels, (
        f"Imbalanced label mismatch between dirs.\n"
        f"  Baseline only: {imbalanced_baseline_labels - lb_labels}\n"
        f"  LB only: {lb_labels - imbalanced_baseline_labels}"
    )

    # Group by (label, rate, run)
    run_key = lambda r: (r["label"].strip(), int(r["rate"].strip()), int(r["run"].strip()))
    baseline_by_run = group_by_key(baseline_tps_rows, run_key)
    lb_tps_by_run = group_by_key(lb_tps_rows, run_key)
    lb_mig_by_run = group_by_key(lb_mig_rows, run_key)

    # Pre-compute balanced baseline: {rate: (mean_tps, mean_lat)} averaged across runs
    balanced_by_rate = defaultdict(lambda: ([], []))  # rate -> ([tps...], [lat...])
    for (label, rate, run), rows in baseline_by_run.items():
        if label != BALANCED_LABEL:
            continue
        tps_vals = [total_tps(r, v_cols) for r in rows]
        lat_vals = [float(r[lat_col]) for r in rows if r[lat_col]]
        assert tps_vals, f"No balanced TPS rows for rate={rate} run={run}"
        assert lat_vals, f"No balanced latency rows for rate={rate} run={run}"
        balanced_by_rate[rate][0].append(mean(tps_vals))
        balanced_by_rate[rate][1].append(mean(lat_vals))
    balanced_metrics = {}  # rate -> (mean_tps, mean_lat)
    for rate, (tps_list, lat_list) in balanced_by_rate.items():
        balanced_metrics[rate] = (mean(tps_list), mean(lat_list))

    # Compute per-(label, rate) phase metrics
    # result: {label: {rate: {phase_name: (mean_tps, mean_lat)}}}
    results = defaultdict(lambda: defaultdict(dict))

    label_rate_runs = defaultdict(list)  # (label, rate) -> [run, ...]
    for (label, rate, run) in lb_tps_by_run:
        label_rate_runs[(label, rate)].append(run)

    for (label, rate), runs in sorted(label_rate_runs.items()):
        imb_tps_vals, imb_lat_vals = [], []
        phase2_tps_vals, phase2_lat_vals = [], []
        phase3_tps_vals, phase3_lat_vals = [], []

        for run in runs:
            # Imbalanced baseline
            bl_key = (label, rate, run)
            assert bl_key in baseline_by_run, f"Baseline missing run {bl_key}"
            bl_rows = baseline_by_run[bl_key]
            bl_tps = [total_tps(r, v_cols) for r in bl_rows]
            bl_lat = [float(r[lat_col]) for r in bl_rows if r[lat_col]]
            assert bl_tps, f"No baseline TPS rows for {bl_key}"
            assert bl_lat, f"No baseline latency rows for {bl_key}"
            imb_tps_vals.append(mean(bl_tps))
            imb_lat_vals.append(mean(bl_lat))

            # Migration boundaries
            mig_key = (label, rate, run)
            assert mig_key in lb_mig_by_run, (
                f"No migration events for {mig_key}. "
                f"LB dir may not have migration data for this run."
            )
            mig_rows = lb_mig_by_run[mig_key]
            mig_ts = [float(r["timestamp_s"]) for r in mig_rows]
            t_start = min(mig_ts)
            t_end = max(mig_ts)

            # Phase 2 & 3: from LB tps timeline
            lb_rows = lb_tps_by_run[(label, rate, run)]
            p2_tps, p2_lat = [], []
            p3_tps, p3_lat = [], []
            for r in lb_rows:
                ts = float(r["timestamp_s"])
                if t_start <= ts <= t_end:
                    p2_tps.append(total_tps(r, v_cols))
                    if r[lat_col]:
                        p2_lat.append(float(r[lat_col]))
                elif ts > t_end:
                    p3_tps.append(total_tps(r, v_cols))
                    if r[lat_col]:
                        p3_lat.append(float(r[lat_col]))

            assert p2_tps, f"No Phase 2 (migration) TPS rows for {mig_key}"
            assert p3_tps, f"No Phase 3 (post-migration) TPS rows for {mig_key}"
            assert p2_lat, f"No Phase 2 latency rows for {mig_key}"
            assert p3_lat, f"No Phase 3 latency rows for {mig_key}"
            phase2_tps_vals.append(mean(p2_tps))
            phase2_lat_vals.append(mean(p2_lat))
            phase3_tps_vals.append(mean(p3_tps))
            phase3_lat_vals.append(mean(p3_lat))

        phase_data = {
            "Imbalanced":     (mean(imb_tps_vals), mean(imb_lat_vals)),
            "Migration":      (mean(phase2_tps_vals), mean(phase2_lat_vals)),
            "Post-migration": (mean(phase3_tps_vals), mean(phase3_lat_vals)),
        }
        # Add balanced bar only if this rate exists in balanced runs
        if rate in balanced_metrics:
            phase_data["Balanced"] = balanced_metrics[rate]
        results[label][rate] = phase_data

    # Plot one figure per label (skip n4_balanced itself)
    for label in sorted(results.keys()):
        if label == BALANCED_LABEL:
            continue
        rates_data = results[label]
        rates = sorted(rates_data.keys())
        n_rates = len(rates)

        # Determine which phases are present (balanced may be missing for some rates)
        has_balanced = any("Balanced" in rates_data[r] for r in rates)
        phases = PHASE_NAMES if has_balanced else [p for p in PHASE_NAMES if p != "Balanced"]
        n_phases = len(phases)
        bar_width = 0.8 / n_phases

        fig, (ax_tps, ax_lat) = plt.subplots(2, 1, figsize=(max(6.4, n_rates * 2.0), 8),
                                              sharex=True)
        x = np.arange(n_rates)

        for i, phase in enumerate(phases):
            tps_vals = [rates_data[r].get(phase, (0.0, 0.0))[0] for r in rates]
            lat_vals = [rates_data[r].get(phase, (0.0, 0.0))[1] for r in rates]
            offset = (i - (n_phases - 1) / 2) * bar_width
            ax_tps.bar(x + offset, tps_vals, bar_width,
                       label=phase, color=PHASE_COLORS[phase])
            ax_lat.bar(x + offset, lat_vals, bar_width,
                       label=phase, color=PHASE_COLORS[phase])

        display_label = LABEL_MAP.get(label, label)
        ax_tps.set_title(f"{display_label} — 3-Phase Comparison", fontweight='bold')
        ax_tps.set_ylabel("Committed TPS (tx/s)", fontweight='bold')
        ax_tps.yaxis.set_major_formatter(tps_formatter)
        ax_tps.legend()
        ax_tps.grid(True, alpha=0.3)
        ax_tps.set_ylim(bottom=0)
        plt.setp(ax_tps.get_yticklabels(), fontweight='bold')

        ax_lat.set_ylabel(f"E2E Latency {args.latency} (s)", fontweight='bold')
        ax_lat.set_xlabel("Input Rate (tx/s)", fontweight='bold')
        ax_lat.yaxis.set_major_formatter(sec_formatter)
        ax_lat.set_xticks(x)
        ax_lat.set_xticklabels([f'{r/1000:.0f}k' if r >= 1000 else str(r) for r in rates],
                               fontweight='bold')
        ax_lat.legend()
        ax_lat.grid(True, alpha=0.3)
        ax_lat.set_ylim(bottom=0)
        plt.setp(ax_lat.get_yticklabels(), fontweight='bold')

        fig.tight_layout()

        if args.output:
            outfile = Path(args.output)
        else:
            outfile = lb_dir / f"3phase_{label}.png"
        fig.savefig(str(outfile), dpi=200, bbox_inches='tight')
        fig.savefig(str(outfile.with_suffix(".pdf")), bbox_inches='tight')
        plt.close(fig)
        print(f"{outfile.resolve()}")


if __name__ == "__main__":
    main()
