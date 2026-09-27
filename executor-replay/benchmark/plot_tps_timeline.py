#!/usr/bin/env python3
import argparse
import csv
import os
import sys
from glob import glob
from os.path import join
from pathlib import Path
from re import findall, search

import matplotlib.pyplot as plt
import matplotlib.ticker as tick

sys.path.insert(0, str(Path(__file__).resolve().parent))
from benchmark.logs import to_posix


@tick.FuncFormatter
def _major_formatter(x, pos):
    if pos is None:
        return
    if x >= 1_000:
        return f'{x/1000:.0f}k'
    else:
        return f'{x:.0f}'


def parse_clients(log_dir):
    tx_sizes = []
    starts = []
    for filename in sorted(glob(join(log_dir, 'client-*.log'))):
        with open(filename, 'r') as f:
            log = f.read()
        tx_sizes.append(int(search(r'Transactions size: (\d+)', log).group(1)))
        starts.append(to_posix(search(r'\[(.*Z) .* Start ', log).group(1)))
    assert tx_sizes, f'No client logs found in {log_dir}'
    assert len(set(tx_sizes)) == 1, 'Clients disagree on transaction size'
    return tx_sizes[0], starts


def parse_commits(log_dir):
    all_commits = []
    for filename in sorted(glob(join(log_dir, 'primary-*.log'))):
        with open(filename, 'r') as f:
            log = f.read()
        tmp = findall(r'\[(.*Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        all_commits.append([(d, to_posix(t)) for t, d in tmp])
    assert all_commits, f'No primary logs found in {log_dir}'
    return merge_fplus1(all_commits)


def merge_fplus1(all_commits):
    collected = {}
    for commits in all_commits:
        for k, v in commits:
            collected.setdefault(k, []).append(v)
    merged = {}
    for k, timestamps in collected.items():
        timestamps.sort()
        merged[k] = timestamps[min(1, len(timestamps) - 1)]
    return merged


def parse_sizes(log_dir, commits):
    sizes = {}
    for filename in sorted(glob(join(log_dir, 'worker-*.log'))):
        with open(filename, 'r') as f:
            log = f.read()
        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        for d, s in tmp:
            if d in commits:
                sizes[d] = int(s)
    return sizes


def apply_warmup(commits, starts, warmup, duration):
    if not warmup:
        return commits, min(starts)
    cutoff = min(starts) + warmup
    end_cutoff = min(starts) + duration - warmup / 2
    trimmed = {d: t for d, t in commits.items() if cutoff <= t < end_cutoff}
    return trimmed, cutoff


def compute_tps_over_time(commits, sizes, tx_size, effective_start, window):
    if not commits:
        return []
    end = max(commits.values())
    num_windows = int((end - effective_start) // window) + 1
    window_bytes = [0.0] * num_windows
    for digest, timestamp in commits.items():
        bucket = min(int((timestamp - effective_start) // window), num_windows - 1)
        window_bytes[bucket] += sizes.get(digest, 0)
    return [(i * window, window_bytes[i] / window / tx_size) for i in range(num_windows)]


def print_table(tps_series, window):
    print(f'COMMITTED TPS OVER TIME ({window}s windows):')
    print(f'  {"Time (s)":>10}  {"TPS":>10}')
    for t, tps in tps_series:
        print(f'  {t:>10.1f}  {round(tps):>10,}')
    avg = sum(tps for _, tps in tps_series) / len(tps_series)
    print(f'  {"average":>10}  {round(avg):>10,}')


def save_data(tps_series, output_dir):
    filename = os.path.abspath(join(output_dir, 'tps-over-time.csv'))
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['time_s', 'tps'])
        writer.writerows(tps_series)
    print(f'Data saved to {filename}')


def plot_tps_over_time(tps_series, output_dir, window):
    if not tps_series:
        return
    times, tps_values = zip(*tps_series)
    plt.figure(figsize=(10, 5))
    plt.plot(times, tps_values, marker='o', linestyle='-', linewidth=1.5, markersize=3)
    plt.xlabel('Time (s)', fontweight='bold')
    plt.ylabel('TPS (tx/s)', fontweight='bold')
    plt.title(f'Committed TPS Over Time ({window}s windows)', fontweight='bold')
    plt.grid(True)
    plt.xlim(left=0)
    plt.ylim(bottom=0)
    plt.gca().xaxis.set_major_formatter(_major_formatter)
    plt.gca().yaxis.set_major_formatter(_major_formatter)
    plt.tight_layout()
    filename = os.path.abspath(join(output_dir, 'tps-over-time.png'))
    plt.savefig(filename, bbox_inches='tight')
    plt.close()
    print(f'Plot saved to {filename}')


def main():
    parser = argparse.ArgumentParser(description='Plot committed TPS over time from benchmark logs.')
    parser.add_argument('log_dir', help='Log directory')
    parser.add_argument('--faults', type=int, default=0)
    parser.add_argument('--warmup', type=float, default=0)
    parser.add_argument('--duration', type=float, default=None)
    parser.add_argument('--window', type=float, default=5, help='Time window in seconds (default: 5)')
    args = parser.parse_args()

    assert args.duration is not None or args.warmup == 0, \
        '--duration is required when --warmup is set'

    tx_size, starts = parse_clients(args.log_dir)
    commits = parse_commits(args.log_dir)
    sizes = parse_sizes(args.log_dir, commits)

    commits, effective_start = apply_warmup(commits, starts, args.warmup, args.duration)
    assert commits, 'No commits found after warmup trimming'

    tps_series = compute_tps_over_time(commits, sizes, tx_size, effective_start, args.window)
    print_table(tps_series, args.window)
    save_data(tps_series, args.log_dir)
    plot_tps_over_time(tps_series, args.log_dir, args.window)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
