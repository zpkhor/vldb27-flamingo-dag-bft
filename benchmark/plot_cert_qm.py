#!/usr/bin/env python3
"""
Plot per-validator quorum metrics (queue delay and quorum latency) over rounds
from primary log files.

Parses log lines of the form:
  cert_qm (round=<r>) validator <pk>: created_at=<ts> n=<n> qd_sum=<s> qd_avg=<a> ql_sum=<s> ql_avg=<a>

Single run:  python plot_cert_qm.py <logs_dir> [-o output.png]
Batch mode:  python plot_cert_qm.py <results_dir> [-o output.png]  (discovers *_run_* subdirs)
"""
import argparse
import re
import sys
from pathlib import Path
from statistics import median

import matplotlib.pyplot as plt

from plot_common import CONFIG_ORDER

SKIP_NUM_ROUNDS = 10

CERT_QM_PATTERN = re.compile(
    r'cert_qm \(round=(\d+)\) validator (\S+): '
    r'created_at=(\d+) n=(\d+) qd_sum=(\d+) qd_avg=([\d.]+) ql_sum=(\d+) ql_avg=([\d.]+)'
)

BOOT_PATTERN = re.compile(r'Primary (\S+) successfully booted')

RUN_DIR_RE = re.compile(r'^(.+)_run_(\d+)$')
LABEL_RATE_RE = re.compile(r'^(.+)_r(\d+)$')


ROW_LABELS = [
    'Num batches (n)',
    'Avg queue delay (ms)',
    'Sum queue delay (ms)',
    'Avg quorum latency (ms)',
    'Sum quorum latency (ms)',
]
N_ROWS = len(ROW_LABELS)
ROW_KEYS = ['n', 'qd_avg', 'qd_sum', 'ql_avg', 'ql_sum']


def label_sort_key(label):
    m = LABEL_RATE_RE.match(label)
    if m:
        config, rate = m.group(1), int(m.group(2))
    else:
        config, rate = label, 0
    try:
        config_idx = CONFIG_ORDER.index(config)
    except ValueError:
        config_idx = len(CONFIG_ORDER)
    return (rate, -config_idx, label)


def parse_primary_log(log_path):
    """Return list of (round, validator_id, created_at, n, qd_sum, qd_avg, ql_sum, ql_avg)."""
    key_to_id = {}
    records = []
    with open(log_path, 'r') as f:
        content = f.read()

    for i, m in enumerate(BOOT_PATTERN.finditer(content)):
        key_to_id[m.group(1)] = i

    next_id = len(key_to_id)
    for m in CERT_QM_PATTERN.finditer(content):
        round_num = int(m.group(1))
        pk = m.group(2)
        created_at = int(m.group(3))
        n = int(m.group(4))
        qd_sum = int(m.group(5))
        qd_avg = float(m.group(6))
        ql_sum = int(m.group(7))
        ql_avg = float(m.group(8))
        if pk not in key_to_id:
            key_to_id[pk] = next_id
            next_id += 1
        records.append((round_num, key_to_id[pk], created_at, n, qd_sum, qd_avg, ql_sum, ql_avg))

    return records


def find_primary_logs(logs_dir):
    p = Path(logs_dir)
    assert p.is_dir(), f'Not a directory: {logs_dir}'
    logs = sorted(p.glob('primary-*.log'))
    assert logs, f'No primary-*.log files found in {logs_dir}'
    return logs


def collect_data(logs_dir):
    """Merge cert_qm records across all primary logs, skipping the first SKIP_NUM_ROUNDS rounds.

    Returns (by_validator, round_ts) where round_ts is {round: median created_at ms}.
    """
    primary_logs = find_primary_logs(logs_dir)
    merged = {}  # (round, validator_id) -> (created_at, n, qd_sum, qd_avg, ql_sum, ql_avg)
    for log_path in primary_logs:
        for round_num, vid, created_at, n, qd_sum, qd_avg, ql_sum, ql_avg in parse_primary_log(log_path):
            key = (round_num, vid)
            if key not in merged:
                merged[key] = (created_at, n, qd_sum, qd_avg, ql_sum, ql_avg)

    assert merged, 'No cert_qm log lines found'

    all_rounds = sorted({r for r, _ in merged})
    assert len(all_rounds) > SKIP_NUM_ROUNDS, \
        f'Not enough rounds to skip ({len(all_rounds)} unique rounds found, SKIP_NUM_ROUNDS={SKIP_NUM_ROUNDS})'
    skip_before = all_rounds[SKIP_NUM_ROUNDS]

    validators = sorted({vid for _, vid in merged})
    by_validator = {vid: {k: [] for k in ['rounds'] + ROW_KEYS} for vid in validators}
    round_created_ats = {}  # round -> [created_at, ...]
    for (round_num, vid), (created_at, n, qd_sum, qd_avg, ql_sum, ql_avg) in sorted(merged.items()):
        if round_num < skip_before:
            continue
        d = by_validator[vid]
        d['rounds'].append(round_num)
        d['n'].append(n)
        d['qd_sum'].append(qd_sum)
        d['qd_avg'].append(qd_avg)
        d['ql_sum'].append(ql_sum)
        d['ql_avg'].append(ql_avg)
        round_created_ats.setdefault(round_num, []).append(created_at)

    round_ts = {r: int(median(ts_list)) for r, ts_list in round_created_ats.items()}
    return by_validator, round_ts


def _plot_column(axes, by_validator, alpha=1.0):
    """Plot all metrics for one run's data into axes[0..N_ROWS-1]."""
    markers = ['o', 'v', 's', 'p', 'D', 'P', '^', 'X']
    for i, vid in enumerate(sorted(by_validator)):
        data = by_validator[vid]
        rounds = data['rounds']
        marker = markers[i % len(markers)]
        kw = dict(marker=marker, markersize=2, linewidth=0.8, label=f'V{vid}', alpha=alpha)
        for row, key in enumerate(ROW_KEYS):
            axes[row].plot(rounds, data[key], **kw)


def discover_runs(results_dir):
    """Return {label: [(run_num, run_dir), ...]} sorted by run number."""
    groups = {}
    for subdir in sorted(results_dir.iterdir()):
        if not subdir.is_dir():
            continue
        m = RUN_DIR_RE.match(subdir.name)
        if not m:
            continue
        label = m.group(1)
        run_num = int(m.group(2))
        groups.setdefault(label, []).append((run_num, subdir))
    for key in groups:
        groups[key].sort(key=lambda x: x[0])
    return groups


def _plot_2x_interval(ax, round_ts, skip_before, alpha=1.0):
    """Plot 2 * round_interval (ms) vs round on ax."""
    pts = []
    for r in sorted(round_ts):
        if r < skip_before or r - 1 not in round_ts:
            continue
        pts.append((r, 2 * (round_ts[r] - round_ts[r - 1])))
    if pts:
        xs, ys = zip(*pts)
        ax.plot(xs, ys, color='black', linewidth=0.8, marker='o', markersize=2, alpha=alpha)


def plot(logs_dir, output_path):
    by_validator, round_ts = collect_data(logs_dir)
    skip_before = min(min(v['rounds']) for v in by_validator.values())

    fig, axes = plt.subplots(N_ROWS + 1, 1, figsize=(12, 3 * (N_ROWS + 1)), sharex=True, squeeze=True)
    _plot_column(list(axes[:N_ROWS]), by_validator)
    _plot_2x_interval(axes[N_ROWS], round_ts, skip_before)

    for ax, ylabel in zip(axes[:N_ROWS], ROW_LABELS):
        ax.set_ylabel(ylabel)
        ax.grid(True, alpha=0.3)
        ax.legend(loc='upper right')
    axes[N_ROWS].set_ylabel('2x round interval (ms)')
    axes[N_ROWS].grid(True, alpha=0.3)
    axes[-1].set_xlabel('Round')

    fig.suptitle(f'Per-validator cert_qm — {Path(logs_dir).name}')
    fig.tight_layout()
    _save(fig, output_path, Path(logs_dir).name)


def plot_batch(results_dir, output_path):
    groups = discover_runs(Path(results_dir))
    assert groups, f'No *_run_* subdirectories found in {results_dir}'

    labels = sorted(groups.keys(), key=label_sort_key)
    n_cols = len(labels)

    fig, axes_grid = plt.subplots(N_ROWS + 1, n_cols, figsize=(6 * n_cols, 3 * (N_ROWS + 1)),
                                  sharex=False, squeeze=False)

    for col_idx, label in enumerate(labels):
        runs = groups[label]
        axes_col = [axes_grid[r][col_idx] for r in range(N_ROWS)]

        for run_idx, (_, run_dir) in enumerate(runs):
            if not list(run_dir.glob('primary-*.log')):
                continue
            try:
                by_validator, round_ts = collect_data(run_dir)
            except AssertionError as e:
                print(f'WARNING: {run_dir.name}: {e}', file=sys.stderr)
                continue
            alpha = 1.0 if run_idx == 0 else 0.5
            _plot_column(axes_col, by_validator, alpha=alpha)
            skip_before = min(min(v['rounds']) for v in by_validator.values())
            _plot_2x_interval(axes_grid[N_ROWS][col_idx], round_ts, skip_before, alpha=alpha)

        axes_col[0].set_title(label, fontsize=9, fontweight='bold')
        axes_grid[N_ROWS][col_idx].set_xlabel('Round')

    for r, ylabel in enumerate(ROW_LABELS):
        axes_grid[r][0].set_ylabel(ylabel)
    axes_grid[N_ROWS][0].set_ylabel('2x round interval (ms)')

    for r in range(N_ROWS + 1):
        for c in range(n_cols):
            ax = axes_grid[r][c]
            ax.grid(True, alpha=0.3)
            if ax.get_lines():
                ax.legend(fontsize=6, loc='upper right')

    fig.suptitle(Path(results_dir).name)
    fig.tight_layout()
    _save(fig, output_path, f'{Path(results_dir).name}-cert-qm')


def analyze_n1(logs_dir, target_n=1):
    """For each validator, find max qd_sum and max ql_sum (and their rounds) among n=target_n certificates."""
    by_validator, _ = collect_data(logs_dir)
    rows = []
    for vid in sorted(by_validator):
        data = by_validator[vid]
        n1_indices = [i for i, n in enumerate(data['n']) if n == target_n]
        if not n1_indices:
            rows.append((vid, None, None, None, None, 0))
            continue
        qd_i = max(n1_indices, key=lambda i: data['qd_sum'][i])
        ql_i = max(n1_indices, key=lambda i: data['ql_sum'][i])
        rows.append((
            vid,
            data['rounds'][qd_i], data['qd_sum'][qd_i],
            data['rounds'][ql_i], data['ql_sum'][ql_i],
            len(n1_indices),
        ))
    return rows


def print_analyze(logs_dir, label=None, target_n=1):
    try:
        rows = analyze_n1(logs_dir, target_n)
    except AssertionError as e:
        print(f'WARNING: {Path(logs_dir).name}: {e}', file=sys.stderr)
        return
    if not rows:
        print(f'{label or Path(logs_dir).name}: no n={target_n} records found')
        return
    no_data_msg = f'(no n={target_n})'
    header = f'  {"V":>3}  {"qd_round":>9}  {"max_qd":>8}  {"ql_round":>9}  {"max_ql":>8}  {f"n={target_n} count":>12}'
    print(f'\n{label or Path(logs_dir).name}')
    print(header)
    for vid, qd_round, max_qd, ql_round, max_ql, count in rows:
        if qd_round is None:
            print(f'  {"V"+str(vid):>3}  {no_data_msg:>9}  {"--":>8}  {"--":>9}  {"--":>8}  {count:>12}')
        else:
            print(f'  {"V"+str(vid):>3}  {qd_round:>9}  {max_qd:>8}  {ql_round:>9}  {max_ql:>8}  {count:>12}')


def _save(fig, output_path, default_stem):
    if output_path is None:
        benchmark_dir = Path(__file__).resolve().parent
        output_path = benchmark_dir / f'{default_stem}.png'
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(output_path), dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(output_path)


def main():
    parser = argparse.ArgumentParser(description='Plot per-validator cert_qm metrics from primary logs.')
    parser.add_argument('path', help='Logs dir (single run) or results dir containing *_run_* subdirs (batch)')
    parser.add_argument('-o', '--output', default=None, help='Output image path')
    parser.add_argument('--analyze', action='store_true',
                        help='Print per-validator max qd_sum and max ql_sum for n=--n rounds instead of plotting')
    parser.add_argument('--n', type=int, default=1, metavar='N',
                        help='Number of batches to filter on for --analyze (default: 1)')
    args = parser.parse_args()

    p = Path(args.path)
    assert p.exists(), f'Path not found: {p}'

    batch = p.is_dir() and any(
        d.is_dir() and RUN_DIR_RE.match(d.name) for d in p.iterdir()
    )

    try:
        if args.analyze:
            if batch:
                groups = discover_runs(p)
                assert groups, f'No *_run_* subdirectories found in {p}'
                for label in sorted(groups.keys(), key=label_sort_key):
                    for run_num, run_dir in groups[label]:
                        if list(run_dir.glob('primary-*.log')):
                            print_analyze(run_dir, label=f'{label} run={run_num}', target_n=args.n)
            else:
                print_analyze(p, target_n=args.n)
        elif batch:
            plot_batch(p, args.output)
        else:
            plot(p, args.output)
    except (AssertionError, FileNotFoundError) as e:
        print(f'Error: {e}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
