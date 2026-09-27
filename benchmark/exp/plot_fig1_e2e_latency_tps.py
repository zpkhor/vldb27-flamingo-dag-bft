#!/usr/bin/env python3
"""Plot f+1 Commit latency (workers) vs Committed TPS from a CSV produced by parse_latency_tps.py.

Usage:
    python plot_latency_tps.py <csv_file> [-o output.png]
"""
import argparse
import csv
from collections import defaultdict
from pathlib import Path
from statistics import mean, stdev

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# EXCLUDED = ["n4_v_rate_imb60", "n4_v_rate_imb90", "n4_e_rate_imb60", "n4_e_rate_imb90",]  # labels to omit from the plot
# EXCLUDED = ["n4_bw_f", "n4_bw_f1", "n4_v_rate_imb90", "n4_e_rate_imb90"]  # labels to omit from the plot
EXCLUDED = ["n4_v_rate_imb60", "n4_e_rate_imb60", "n4_bw_f1", "n4_ve_rate_imb60"]  # labels to omit from the plot
# EXCLUDED = ["n4_v_rate_imb90", "n4_e_rate_imb90", "n4_bw_f1", "n4_ve_rate_imb90"]  # labels to omit from the plot
# Color encodes scenario family; marker encodes variant (60% vs 90%, f vs f+1).
STYLE_MAP = {
    "n4_balanced":      ("tab:green",  "o"),
    "n4_v_rate_imb60":  ("tab:blue",   "o"),
    "n4_v_rate_imb90":  ("tab:blue",   "s"),
    "n4_e_rate_imb60":  ("tab:orange", "o"),
    "n4_e_rate_imb90":  ("tab:orange", "s"),
    "n4_ve_rate_imb60": ("tab:purple", "o"),
    "n4_ve_rate_imb90": ("tab:purple", "s"),
    "n4_bw_f":          ("tab:red",    "o"),
    "n4_bw_f1":         ("tab:red",    "s"),
}

CONFIG_ORDER = [
    "n4_balanced",
    "n4_v_rate_imb60", "n4_v_rate_imb90",
    "n4_e_rate_imb60", "n4_e_rate_imb90",
    "n4_ve_rate_imb60", "n4_ve_rate_imb90",
    "n4_bw_f", "n4_bw_f1",
]

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


@ticker.FuncFormatter
def default_major_formatter(x, pos):
    if x >= 1_000:
        return f'{x/1000:.0f}k'
    return f'{x:.0f}'


@ticker.FuncFormatter
def sec_major_formatter(x, pos):
    return f'{float(x)/1000:.1f}'


def label_sort_key(label):
    try:
        return (CONFIG_ORDER.index(label), label)
    except ValueError:
        return (len(CONFIG_ORDER), label)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file")
    parser.add_argument("-o", "--output", help="Output path (default: alongside csv)")
    parser.add_argument("--top-lim", type=int, default=None,
                        help="Y-axis upper limit in ms (e.g. 10000 for 10s)")
    args = parser.parse_args()

    csv_path = Path(args.csv_file)
    if csv_path.is_dir():
        matches = list(csv_path.glob("*.csv"))
        assert len(matches) == 1, f"Expected 1 CSV in {csv_path}, found {len(matches)}: {matches}"
        csv_path = matches[0]
    assert csv_path.exists(), f"Path not found: {csv_path}"

    outfile = Path(args.output) if args.output else csv_path.with_suffix(".png")

    # Group runs by (label, rate) to compute mean/stdev
    raw = defaultdict(list)  # (label, rate) -> [(latency_ms, tps), ...]
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        reader.fieldnames = [name.strip() for name in reader.fieldnames]
        lat_col = next(f for f in reader.fieldnames if f.endswith("_ms"))
        lat_label = lat_col.removesuffix("_ms")
        for row in reader:
            key = (row["label"].strip(), int(row["rate"].strip()))
            raw[key].append((int(row[lat_col].strip()), int(row["tps"].strip())))

    assert raw, f"No data found in {csv_path}"

    # Aggregate: per label, sorted by input rate (not TPS) to avoid backward lines
    data = defaultdict(list)  # label -> [(rate, mean_tps, std_tps, mean_lat, std_lat), ...]
    for (label, rate), points in raw.items():
        lats = [p[0] for p in points]
        tpss = [p[1] for p in points]
        mean_lat = mean(lats)
        mean_tps = mean(tpss)
        std_lat = stdev(lats) if len(lats) > 1 else 0
        std_tps = stdev(tpss) if len(tpss) > 1 else 0
        data[label].append((rate, mean_tps, std_tps, mean_lat, std_lat))

    for label in data:
        data[label].sort(key=lambda p: p[0])

    # Append sentinel point: same TPS as last measured point, latency = 100s
    for label in data:
        last = data[label][-1]  # (rate, mean_tps, std_tps, mean_lat, std_lat)
        data[label].append((last[0] + 1, last[1], 0, 100_000, 0))

    # Plot
    fig, ax = plt.subplots(figsize=(6.4, 4.8))

    for label in sorted((k for k in data if k not in EXCLUDED), key=label_sort_key):
        points = data[label]
        xs = [p[1] for p in points]
        ys = [p[3] for p in points]
        display = LABEL_MAP.get(label, label)
        color, marker = STYLE_MAP.get(label, ("tab:gray", "x"))
        ax.errorbar(
            xs, ys,
            label=display, marker=marker, color=color,
            capsize=3, linewidth=1, markersize=6,
        )

    ax.set_xlabel("E2E Throughput (tx/s)", fontweight='bold')
    ax.set_ylabel(f"{lat_label.title()} Latency (s)", fontweight='bold')
    ax.xaxis.set_major_formatter(default_major_formatter)
    ax.yaxis.set_major_formatter(sec_major_formatter)
    plt.xticks(weight='bold')
    plt.yticks(weight='bold')
    ax.set_xlim(xmin=0)
    ax.set_ylim(bottom=0, top=args.top_lim)
    ax.legend(loc='upper right')
    ax.grid(True)

    fig.tight_layout()
    fig.savefig(str(outfile), dpi=200, bbox_inches='tight')
    fig.savefig(str(outfile.with_suffix(".pdf")), bbox_inches='tight')
    print(outfile.resolve())


if __name__ == "__main__":
    main()
