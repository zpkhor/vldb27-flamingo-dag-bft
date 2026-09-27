#!/usr/bin/env python3
"""Plot f+1 Commit latency (workers) vs Committed TPS from a CSV produced by parse_latency_tps.py.

Usage:
    python plot_latency_tps.py <csv_file> [-o output.png]
"""
import argparse
import csv
from collections import defaultdict
from itertools import cycle
from pathlib import Path
from statistics import mean, stdev

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


CONFIG_ORDER = ["balanced", "rate_imb", "bw_f", "bw_f1"]

LABEL_MAP = {
    "balanced": "Balanced",
    "rate_imb": "Rate imbalance",
    "bw_f": "BW limit (f)",
    "bw_f1": "BW limit (f+1)",
}


@ticker.FuncFormatter
def default_major_formatter(x, pos):
    if x >= 1_000:
        return f'{x/1000:.0f}k'
    return f'{x:.0f}'


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
    assert csv_path.exists(), f"Path not found: {csv_path}"

    outfile = Path(args.output) if args.output else csv_path.with_suffix(".png")

    # Group runs by (label, rate) to compute mean/stdev
    raw = defaultdict(list)  # (label, rate) -> [(latency_ms, tps), ...]
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        reader.fieldnames = [name.strip() for name in reader.fieldnames]
        for row in reader:
            key = (row["label"].strip(), int(row["rate"].strip()))
            raw[key].append((int(row["latency_ms"].strip()), int(row["tps"].strip())))

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

    # Plot
    colors = cycle(['tab:green', 'tab:blue', 'tab:orange', 'tab:red'])
    markers = cycle(['o', 'v', 's', 'd'])

    fig, ax = plt.subplots(figsize=(6.4, 4.8))

    for label in sorted(data.keys(), key=label_sort_key):
        points = data[label]
        xs = [p[1] for p in points]
        x_err = [p[2] for p in points]
        ys = [p[3] for p in points]
        y_err = [p[4] for p in points]
        display = LABEL_MAP.get(label, label)
        ax.errorbar(
            xs, ys, yerr=y_err, xerr=x_err,
            label=display, marker=next(markers), color=next(colors),
            capsize=3, linewidth=2,
        )

    ax.set_xlabel("Throughput (tx/s)", fontweight='bold')
    ax.set_ylabel("p95 Latency (s)", fontweight='bold')
    ax.xaxis.set_major_formatter(default_major_formatter)
    ax.yaxis.set_major_formatter(sec_major_formatter)
    plt.xticks(weight='bold')
    plt.yticks(weight='bold')
    ax.set_xlim(xmin=0)
    ax.set_ylim(bottom=0, top=args.top_lim)
    ax.legend()
    ax.grid(True)

    fig.tight_layout()
    fig.savefig(str(outfile), dpi=200, bbox_inches='tight')
    fig.savefig(str(outfile.with_suffix(".pdf")), bbox_inches='tight')
    print(outfile.resolve())


if __name__ == "__main__":
    main()
