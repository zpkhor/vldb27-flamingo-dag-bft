#!/usr/bin/env python3
import argparse
import csv
import re
import sys


def parse_block(block):
    # First line of block is everything after the split on "CMD: ", i.e. the rest of the CMD line
    lines = block.split('\n')
    cmd_line = lines[0].strip()

    def find(pattern, text):
        m = re.search(pattern, text)
        if not m:
            return ''
        return m.group(1).replace(',', '')

    label = find(r'\bLABEL=(\S+)', cmd_line)
    m = re.search(r'^n(\d+)_', label)
    n = m.group(1) if m else ''
    committed_tps = find(r'Committed TPS:\s*([\d,]+)\s*tx/s', block)
    commit_lat_mean_ms = find(r'f\+1 Commit latency \(workers\) \(mean\):\s*([\d,]+)\s*ms', block)
    commit_lat_p50_ms = find(r'f\+1 Commit latency \(workers\) \(p50\):\s*([\d,]+)\s*ms', block)
    commit_lat_p90_ms = find(r'f\+1 Commit latency \(workers\) \(p90\):\s*([\d,]+)\s*ms', block)
    commit_lat_p95_ms = find(r'f\+1 Commit latency \(workers\) \(p95\):\s*([\d,]+)\s*ms', block)

    return [label, n, committed_tps, commit_lat_mean_ms, commit_lat_p50_ms, commit_lat_p90_ms, commit_lat_p95_ms, cmd_line]


HEADER = ['label', 'n', 'committed_tps', 'commit_lat_mean_ms', 'commit_lat_p50_ms', 'commit_lat_p90_ms', 'commit_lat_p95_ms', 'cmd']
EXCLUDED_HEADER = ['commit_lat_mean_ms', 'commit_lat_p90_ms', 'cmd']


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("log_file")
    parser.add_argument("--align", action="store_true")
    args = parser.parse_args()

    with open(args.log_file) as f:
        content = f.read()

    parts = content.split('CMD: ')
    blocks = [p for p in parts[1:] if p.strip()]

    output_header = [h for h in HEADER if h not in EXCLUDED_HEADER]
    output_indices = [HEADER.index(h) for h in output_header]

    rows = []
    for block in blocks:
        row = parse_block(block)
        out = [row[i] for i in output_indices]
        if any(v == '' for v in out):
            print(f"WARNING: skipping block with label={row[0]!r} — missing fields", file=sys.stderr)
            continue
        rows.append(out)

    if args.align:
        all_rows = [output_header] + rows
        widths = [max(len(r[i]) for r in all_rows) for i in range(len(output_header))]
        for row in all_rows:
            print('  '.join(v.ljust(widths[i]) for i, v in enumerate(row)))
    else:
        writer = csv.writer(sys.stdout)
        writer.writerow(output_header)
        for row in rows:
            writer.writerow(row)


if __name__ == '__main__':
    main()
