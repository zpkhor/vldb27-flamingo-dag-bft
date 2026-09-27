#!/usr/bin/env python3
"""
Plot E2E throughput vs executors/workers for a benchmark CSV.

Usage:
  python3 plot_benchmark.py <csv_file>           # plot single CSV with custom labels
  python3 plot_benchmark.py --scheduler <csv1> <csv2>  # plot scheduler comparison across CSVs

One line per unique (WRITEBACK_EXECUTOR, DISTRIBUTED_TX_RATE, NO_SEND_PAYMENT).
x-axis: NUM_E / NUM_W
y-axis: E2E TPS
Saves PNG next to the CSV.
"""
import csv
import sys
import os
from collections import defaultdict
import matplotlib.pyplot as plt


def read_series(csv_path):
    """Read CSV and return dict of (wb, dtx, nsp) → [(num, e2e)]."""
    with open(csv_path, 'r', newline='') as f:
        rows = list(csv.reader(f))

    header = rows[0]
    num_idx = header.index('NUM_E / NUM_W')
    wb_idx = header.index('WRITEBACK_EXECUTOR')
    dtx_idx = header.index('DISTRIBUTED_TX_RATE')
    nsp_idx = header.index('NO_SEND_PAYMENT')
    e2e_idx = header.index('E2E TPS')

    series = defaultdict(list)
    for row in rows[1:]:
        if not row or len(row) <= e2e_idx:
            continue
        if not row[e2e_idx].strip():
            continue
        try:
            num = int(row[num_idx])
            e2e = int(row[e2e_idx].replace(',', ''))
        except ValueError:
            continue
        key = (row[wb_idx], row[dtx_idx], row[nsp_idx])
        series[key].append((num, e2e))

    return series


# Renamed labels for 50%.csv and 90%.csv
LABEL_MAP = {
    ('0', '0', '1'): 'Flamingo, 0% distributed transactions',
    ('0', '0.2', '0'): 'Flamingo, 20% distributed transactions',
    ('1', '0', '1'): 'PilotFish, 0% distributed transactions',
    ('1', '0.2', '0'): 'PilotFish, 20% distributed transactions',
}

# Color encodes executor mode, marker encodes workload
MODE_COLOR = {'0': '#1f77b4', '1': '#d62728'}  # Flamingo=blue, PilotFish=red
WORKLOAD_MARKER = {
    ('0', '1'): 'o',    # dist_tx=0, no_payment
    ('0.2', '0'): 's',  # dist_tx=0.2, with_payment
}


def plot_single(csv_path):
    """Plot a single CSV with Flamingo/PilotFish labels, no title."""
    series = read_series(csv_path)
    if not series:
        print('No data to plot.')
        sys.exit(1)

    fig, ax = plt.subplots(figsize=(10, 6))

    for key in sorted(series.keys()):
        wb, dtx, nsp = key
        points = sorted(series[key])
        xs = [p[0] for p in points]
        ys = [p[1] for p in points]
        color = MODE_COLOR.get(wb, 'gray')
        marker = WORKLOAD_MARKER.get((dtx, nsp), 'x')
        linestyle = DIST_TX_LINESTYLE.get(dtx, '-')
        label = LABEL_MAP.get(key, f'wb={wb}, dtx={dtx}, nsp={nsp}')
        ax.plot(xs, ys, color=color, marker=marker, markersize=9,
                linewidth=2, linestyle=linestyle, label=label)

    ax.set_xlabel('Number of workers/executors')
    ax.set_ylabel('Throughput (tx/s)')
    ax.grid(True, alpha=0.3)
    ax.legend(loc='best', fontsize=9)
    ax.ticklabel_format(axis='y', style='plain')

    out_path = os.path.splitext(csv_path)[0] + '.png'
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f'Saved {out_path}')


# Labels for the scheduler comparison graph
SCHEDULER_LABELS = {
    'balanced': {
        ('0', '0', '1'): 'Balanced, 0% distributed transactions',
        ('0', '0.2', '0'): 'Balanced, 20% distributed transactions',
    },
    '50%': {
        ('0', '0', '1'): '50% imbalanced, 0% distributed transactions',
        ('0', '0.2', '0'): '50% imbalanced, 20% distributed transactions',
    },
    '90%': {
        ('0', '0', '1'): '90% imbalanced, 0% distributed transactions',
        ('0', '0.2', '0'): '90% imbalanced, 20% distributed transactions',
    },
}

SCENARIO_COLOR = {
    'balanced': '#1f77b4',  # blue
    '50%': '#d62728',       # red
    '90%': '#2ca02c',       # green
}

DIST_TX_MARKER = {
    '0': 'o',    # 0% distributed
    '0.2': 's',  # 20% distributed
}

DIST_TX_LINESTYLE = {
    '0': '-',
    '0.2': '--',
}


def plot_scheduler(csv_paths):
    """Plot only WB=0 (our scheduler) data from multiple CSVs."""
    fig, ax = plt.subplots(figsize=(10, 6))

    for csv_path in csv_paths:
        basename = os.path.splitext(os.path.basename(csv_path))[0]
        series = read_series(csv_path)
        labels = SCHEDULER_LABELS.get(basename, {})
        color = SCENARIO_COLOR.get(basename, 'gray')

        for key in sorted(series.keys()):
            wb, dtx, nsp = key
            if wb != '0':
                continue
            points = sorted(series[key])
            xs = [p[0] for p in points]
            ys = [p[1] for p in points]
            marker = DIST_TX_MARKER.get(dtx, 'x')
            linestyle = DIST_TX_LINESTYLE.get(dtx, '-')
            label = labels.get(key, f'{basename}, dtx={dtx}')
            ax.plot(xs, ys, color=color, marker=marker, markersize=9,
                    linewidth=2, linestyle=linestyle, label=label)

    ax.set_xlabel('Number of workers/executors')
    ax.set_ylabel('Throughput (tx/s)')
    ax.grid(True, alpha=0.3)
    ax.legend(loc='best', fontsize=9)
    ax.ticklabel_format(axis='y', style='plain')

    out_dir = os.path.dirname(csv_paths[0])
    out_path = os.path.join(out_dir, 'scheduler.png')
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f'Saved {out_path}')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage:')
        print('  python3 plot_benchmark.py <csv_file>')
        print('  python3 plot_benchmark.py --scheduler <csv1> <csv2> ...')
        sys.exit(1)

    if sys.argv[1] == '--scheduler':
        assert len(sys.argv) >= 3, 'Need at least one CSV for --scheduler'
        plot_scheduler(sys.argv[2:])
    else:
        assert len(sys.argv) == 2, 'Expected exactly one CSV file'
        plot_single(sys.argv[1])
