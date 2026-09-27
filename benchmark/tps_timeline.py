#!/usr/bin/env python3
"""Output committed TPS timeline binned by a fixed time interval.

Usage:
    python benchmark/tps_timeline.py <log_dir> [--bin 5] [--warmup SECONDS]

Reads duration/faults from <log_dir>/bench-params.json.
Warmup trimming is applied only when --warmup is given.

Output CSV columns:
  timestamp_s : Unix timestamp (start of bin)
  tps       : transactions/sec committed in this bin
  n_batches : committed batches in this bin
  n_txs     : total transactions in this bin
  lat_mean  : mean f+1 commit latency (ms), empty if no samples
  lat_p50   : p50 latency (ms)
  lat_p90   : p90 latency (ms)
  lat_p95   : p95 latency (ms)
"""
import argparse
import csv
import json
import sys
from pathlib import Path
from statistics import mean

sys.path.insert(0, str(Path(__file__).resolve().parent))
from benchmark.logs import LogParser


def percentile(sorted_vals, pct):
    n = len(sorted_vals)
    idx = max(0, n * pct // 100 - 1)
    return sorted_vals[idx]


def main():
    parser = argparse.ArgumentParser(description='Committed TPS timeline binned by time interval.')
    parser.add_argument('log_dir')
    parser.add_argument('--bin', type=float, default=10.0, dest='bin_s', metavar='SECONDS')
    parser.add_argument('--warmup', type=float, default=None, metavar='SECONDS')
    args = parser.parse_args()

    params_path = Path(args.log_dir) / 'bench-params.json'
    assert params_path.exists(), f'bench-params.json not found in {args.log_dir}'
    with open(params_path) as f:
        params = json.load(f)
    duration = params['duration']
    faults = params['faults']
    warmup = args.warmup if args.warmup is not None else 0

    lp = LogParser.process(args.log_dir, faults=faults, warmup=warmup, duration=duration)

    assert lp.commits, 'No commits found'
    assert lp.effective_start, 'No effective_start (no client log?)'

    start = lp.effective_start
    bin_s = args.bin_s

    # Bin commits by commit timestamp
    tps_bins = {}
    for digest, t in lp.commits.items():
        b = int((t - start) / bin_s)
        tps_bins.setdefault(b, []).append(digest)

    # Bin latency samples by commit timestamp (mirrors _latency_by_round logic in logs.py)
    lat_bins = {}
    for received in lp.sample_to_batch:
        for key, batch_id in received.items():
            if batch_id not in lp.commits:
                continue
            send_time = lp.sent_samples.get(key)
            if send_time is None or send_time < start:
                continue
            commit_time = lp.commits[batch_id]
            latency_ms = (commit_time - send_time) * 1000
            assert latency_ms > 0, f'Negative latency for sample {key}'
            b = int((commit_time - start) / bin_s)
            lat_bins.setdefault(b, []).append(latency_ms)

    all_bins = sorted(set(tps_bins) | set(lat_bins))

    writer = csv.writer(sys.stdout)
    writer.writerow(['timestamp_s', 'tps', 'n_batches', 'n_txs', 'lat_mean', 'lat_p50', 'lat_p90', 'lat_p95'])
    for b in all_bins:
        digests = tps_bins.get(b, [])
        total_bytes = sum(lp.sizes.get(d, 0) for d in digests)
        n_txs = total_bytes // lp.size
        tps = n_txs / bin_s

        lats = lat_bins.get(b, [])
        if lats:
            lats_s = sorted(lats)
            lat_str = [
                f'{mean(lats_s):.1f}',
                f'{percentile(lats_s, 50):.1f}',
                f'{percentile(lats_s, 90):.1f}',
                f'{percentile(lats_s, 95):.1f}',
            ]
        else:
            lat_str = ['', '', '', '']

        writer.writerow([f'{start + b * bin_s:.3f}', f'{tps:.1f}', len(digests), n_txs] + lat_str)


if __name__ == '__main__':
    main()
