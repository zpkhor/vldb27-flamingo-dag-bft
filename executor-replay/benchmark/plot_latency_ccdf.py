import argparse
import re
import sys
from pathlib import Path

import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).resolve().parent))
from benchmark.logs import LogParser


RUN_DIR_RE = re.compile(r'^(.+)_run_(\d+)$')

CONFIG_ORDER = [
    'balanced',
    'imbalanced',
    'imbalanced_bw1',
    'imbalanced_bw2',
    'imbalanced_bw3',
]

LABEL_RATE_RE = re.compile(r'^(.+)_r(\d+)$')

BENCHMARK_DIR = Path(__file__).resolve().parent


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            'Plot end-to-end committed latency CDF. '
            'One path: single plot. Two paths: baseline vs lb comparison.'
        )
    )
    parser.add_argument(
        'path1',
        help='Log directory (single run) or results directory (sweep)',
    )
    parser.add_argument(
        'path2',
        nargs='?',
        default=None,
        help='Optional second path for comparison (baseline=path1, lb=path2)',
    )
    parser.add_argument(
        '-o', '--output',
        help='Output image path. Defaults to benchmark/<name>-latency-{cdf|ccdf}.png',
    )
    parser.add_argument(
        '--faults', type=int, default=0,
        help='Number of Byzantine faults (default: 0)',
    )
    parser.add_argument(
        '--warmup', type=float, default=0,
        help='Warmup duration in seconds to trim from start (default: 0)',
    )
    parser.add_argument(
        '--duration', type=float, default=None,
        help='Benchmark duration in seconds (default: auto)',
    )
    parser.add_argument(
        '--per-validator', action='store_true',
        help='(Single-run mode only) Overlay one CDF curve per validator',
    )
    parser.add_argument(
        '--cdf', action='store_true',
        help='Plot CDF instead of default tail-latency CCDF',
    )
    return parser.parse_args()


def extract_global_latencies_ms(parser):
    """Replicates _worker_committed_latency logic, returning raw latencies in ms."""
    assert isinstance(parser.faults, int), 'faults must be an int to compute threshold'
    threshold = parser.faults + 1

    global_sent = {}
    for sent in parser.sent_samples:
        global_sent.update(sent)

    latencies_ms = []
    for received in parser.sample_to_batch:
        for key, batch_id in received.items():
            if batch_id not in parser.commits:
                continue
            send_time = global_sent.get(key)
            if send_time is None or send_time < parser.effective_start:
                continue
            timestamps = parser.committed_times_by_batch.get(batch_id, [])
            if len(timestamps) < threshold:
                continue
            end_time = sorted(timestamps)[threshold - 1]
            latencies_ms.append((end_time - send_time) * 1000)
    return latencies_ms


def extract_per_validator_latencies_ms(parser):
    """Returns {validator_id: [latency_ms, ...]} using each validator's received samples."""
    assert isinstance(parser.faults, int), 'faults must be an int to compute threshold'
    threshold = parser.faults + 1

    global_sent = {}
    for sent in parser.sent_samples:
        global_sent.update(sent)

    result = {}
    for v in sorted(parser.sample_to_batch_by_validator.keys()):
        v_sent = {}
        for sent in parser.sent_samples_by_validator.get(v, []):
            v_sent.update(sent)

        latencies_ms = []
        for received in parser.sample_to_batch_by_validator[v]:
            for key, batch_id in received.items():
                if batch_id not in parser.commits:
                    continue
                send_time = v_sent.get(key)
                if send_time is None:
                    send_time = global_sent.get(key)
                if send_time is None or send_time < parser.effective_start:
                    continue
                timestamps = parser.committed_times_by_batch.get(batch_id, [])
                if len(timestamps) < threshold:
                    continue
                end_time = sorted(timestamps)[threshold - 1]
                latencies_ms.append((end_time - send_time) * 1000)
        result[v] = latencies_ms
    return result


def load_run_latencies(run_dir, faults, warmup, duration):
    parser = LogParser.process(
        str(run_dir),
        faults=faults,
        warmup=warmup,
        duration=duration,
    )
    return extract_global_latencies_ms(parser)


def discover_runs(results_dir):
    """Returns {label: [(run_number, run_dir), ...]} sorted by run number."""
    groups = {}
    for subdir in sorted(results_dir.iterdir()):
        if not subdir.is_dir():
            continue
        match = RUN_DIR_RE.match(subdir.name)
        if not match:
            continue
        label = match.group(1)
        run_num = int(match.group(2))
        groups.setdefault(label, []).append((run_num, subdir))
    for key in groups:
        groups[key].sort(key=lambda x: x[0])
    return groups


def group_by_config_rate(groups):
    """Splits {label: runs} into {config: {rate_str: runs}}.

    Labels matching <config>_r<rate> are split; others use the full label as
    config with rate=None.
    """
    result = {}
    for label, runs in groups.items():
        m = LABEL_RATE_RE.match(label)
        if m:
            config, rate = m.group(1), m.group(2)
        else:
            config, rate = label, None
        result.setdefault(config, {}).setdefault(rate, []).extend(runs)
    return result


def cdf_xy(latencies_ms):
    sorted_lat = sorted(latencies_ms)
    n = len(sorted_lat)
    y = [i / n for i in range(1, n + 1)]
    return sorted_lat, y


def ccdf_xy(latencies_ms):
    sorted_lat = sorted(latencies_ms)
    n = len(sorted_lat)
    y = [1 - (i / n) for i in range(1, n + 1)]
    return sorted_lat, y


def dist_xy(latencies_ms, use_cdf):
    return cdf_xy(latencies_ms) if use_cdf else ccdf_xy(latencies_ms)


def add_percentile_markers(ax, latencies_ms):
    sorted_lat = sorted(latencies_ms)
    n = len(sorted_lat)
    for pct, name in [(0.50, 'p50'), (0.95, 'p95'), (0.99, 'p99')]:
        idx = min(int(pct * n), n - 1)
        val = sorted_lat[idx]
        ax.axvline(val, linestyle='--', linewidth=0.8, alpha=0.6, label=f'{name}={val:.0f}ms')


def _pool_latencies(config_rate, config, rate, run_loader, faults, warmup, duration):
    lat = []
    for _, run_dir in config_rate[config][rate]:
        lat.extend(run_loader(run_dir, faults, warmup, duration))
    return lat


def plot_sweep(path1, groups1, path2, groups2, args):
    config_rate1 = group_by_config_rate(groups1)
    config_rate2 = group_by_config_rate(groups2) if groups2 else {}

    all_config_keys = set(config_rate1.keys()) | set(config_rate2.keys())
    configs = sorted(all_config_keys, key=lambda c: (
        CONFIG_ORDER.index(c) if c in CONFIG_ORDER else len(CONFIG_ORDER), c
    ))

    # Assign a distinct color per rate (same rate = same color across both paths)
    all_rates = sorted(
        {r for cr in config_rate1.values() for r in cr} |
        {r for cr in config_rate2.values() for r in cr},
        key=lambda r: int(r) if r is not None else float('inf')
    )
    color_cycle = plt.rcParams['axes.prop_cycle'].by_key()['color']
    rate_color = {r: color_cycle[i % len(color_cycle)] for i, r in enumerate(all_rates)}

    n_cols = len(configs)
    fig, axes = plt.subplots(1, n_cols, figsize=(5 * n_cols, 5), sharey=True)
    if n_cols == 1:
        axes = [axes]

    for ax, config in zip(axes, configs):
        rates_in_config = sorted(
            set(config_rate1.get(config, {}).keys()) | set(config_rate2.get(config, {}).keys()),
            key=lambda r: int(r) if r is not None else float('inf')
        )
        for rate in rates_in_config:
            color = rate_color[rate]
            rate_label = f'r{rate}' if rate is not None else 'all'

            if rate in config_rate1.get(config, {}):
                lat = _pool_latencies(config_rate1, config, rate, load_run_latencies, args.faults, args.warmup, args.duration)
                assert lat, f'No latencies for config={config!r} rate={rate!r} in path1'
                x, y = dist_xy(lat, args.cdf)
                lbl = f'{rate_label} base' if groups2 else f'{rate_label}  n={len(lat)}'
                ax.plot(x, y, color=color, linestyle='-', linewidth=1.5, label=lbl)

            if groups2 and rate in config_rate2.get(config, {}):
                lat = _pool_latencies(config_rate2, config, rate, load_run_latencies, args.faults, args.warmup, args.duration)
                assert lat, f'No latencies for config={config!r} rate={rate!r} in path2'
                x, y = dist_xy(lat, args.cdf)
                ax.plot(x, y, color=color, linestyle='--', linewidth=1.5, label=f'{rate_label} lb')

        ax.set_title(config)
        ax.set_ylim(0, 1.02)
        ax.set_xscale('log')
        ax.legend(loc='lower right')
        ax.grid(True, alpha=0.3)
        ax.set_xlabel('Latency (ms)')

    axes[0].set_ylabel('CDF' if args.cdf else 'CCDF')
    if groups2:
        fig.suptitle(f'{path1.name}  (solid)  vs  {path2.name}  (dashed)')
    else:
        fig.suptitle(path1.name)
    fig.tight_layout()
    return fig


def plot_single_run(path1, path2, args):
    lp1 = LogParser.process(str(path1), faults=args.faults, warmup=args.warmup, duration=args.duration)
    lat1 = extract_global_latencies_ms(lp1)
    assert lat1, 'No committed sample latencies found in path1'

    fig, ax = plt.subplots(figsize=(8, 5))

    if path2:
        lp2 = LogParser.process(str(path2), faults=args.faults, warmup=args.warmup, duration=args.duration)
        lat2 = extract_global_latencies_ms(lp2)
        assert lat2, 'No committed sample latencies found in path2'

        if args.per_validator:
            per_v1 = extract_per_validator_latencies_ms(lp1)
            per_v2 = extract_per_validator_latencies_ms(lp2)
            all_validators = sorted(set(per_v1.keys()) | set(per_v2.keys()))
            color_cycle = plt.rcParams['axes.prop_cycle'].by_key()['color']
            v_color = {v: color_cycle[i % len(color_cycle)] for i, v in enumerate(all_validators)}
            for v in all_validators:
                color = v_color[v]
                if v in per_v1 and per_v1[v]:
                    x, y = dist_xy(per_v1[v], args.cdf)
                    ax.plot(x, y, color=color, linestyle='-', linewidth=1.2, alpha=0.8, label=f'v{v} base')
                if v in per_v2 and per_v2[v]:
                    x, y = dist_xy(per_v2[v], args.cdf)
                    ax.plot(x, y, color=color, linestyle='--', linewidth=1.2, alpha=0.8, label=f'v{v} lb')
            x, y = dist_xy(lat1, args.cdf)
            ax.plot(x, y, color='black', linestyle='-', linewidth=2.5, alpha=0.9, label='global base')
            x, y = dist_xy(lat2, args.cdf)
            ax.plot(x, y, color='black', linestyle='--', linewidth=2.5, alpha=0.9, label='global lb')
        else:
            x, y = dist_xy(lat1, args.cdf)
            ax.plot(x, y, linestyle='-', linewidth=1.5, label=path1.name)
            x, y = dist_xy(lat2, args.cdf)
            ax.plot(x, y, linestyle='--', linewidth=1.5, label=path2.name)

        ax.set_title(f'{path1.name}  (solid)  vs  {path2.name}  (dashed)')
    else:
        if args.per_validator:
            per_v = extract_per_validator_latencies_ms(lp1)
            for v, lat in per_v.items():
                if not lat:
                    continue
                sorted_lat = sorted(lat)
                mean_ms = sum(lat) / len(lat)
                p95 = sorted_lat[min(int(0.95 * len(sorted_lat)), len(sorted_lat) - 1)]
                x, y = dist_xy(lat, args.cdf)
                ax.plot(x, y, linewidth=1.5, alpha=0.8, label=f'v{v}  mean={mean_ms:.0f}ms  p95={p95:.0f}ms')
            x, y = dist_xy(lat1, args.cdf)
            ax.plot(x, y, linewidth=2.5, linestyle='--', alpha=0.9, label='global')
        else:
            x, y = dist_xy(lat1, args.cdf)
            ax.plot(x, y, linewidth=1.5, label='global')
            add_percentile_markers(ax, lat1)

        ax.set_title(path1.name)

    ax.set_xlabel('Latency (ms)')
    ax.set_ylabel('CDF' if args.cdf else 'CCDF')
    ax.set_ylim(0, 1.02)
    ax.set_xscale('log')
    ax.grid(True, alpha=0.3)
    ax.legend(loc='lower right')
    fig.tight_layout()
    return fig


def main():
    args = parse_args()
    path1 = Path(args.path1)
    assert path1.exists() and path1.is_dir(), f'Path not found: {path1}'

    path2 = Path(args.path2) if args.path2 else None
    if path2:
        assert path2.exists() and path2.is_dir(), f'Path not found: {path2}'

    groups1 = discover_runs(path1)
    sweep_mode = bool(groups1)

    groups2 = None
    if path2:
        groups2 = discover_runs(path2)
        assert bool(groups2) == sweep_mode, 'Both paths must be the same mode (single-run or sweep)'

    if sweep_mode:
        if args.per_validator:
            print('Warning: --per-validator is ignored in sweep mode', file=sys.stderr)
        fig = plot_sweep(path1, groups1, path2, groups2, args)
    else:
        fig = plot_single_run(path1, path2, args)

    dist_name = 'cdf' if args.cdf else 'ccdf'

    if args.output:
        output_path = Path(args.output)
    elif path2:
        output_path = BENCHMARK_DIR / f'{path1.name}_vs_{path2.name}-latency-{dist_name}.png'
    else:
        output_path = BENCHMARK_DIR / f'{path1.name}-latency-{dist_name}.png'

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(output_path), dpi=200, bbox_inches='tight')
    print(output_path.resolve())
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
