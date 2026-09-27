#!/usr/bin/env python3
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
    committed_tps = find(r'Committed TPS:\s*([\d,]+)\s*tx/s', block)
    commit_lat_mean_ms = find(r'f\+1 Commit latency \(workers\) \(mean\):\s*([\d,]+)\s*ms', block)
    commit_lat_p50_ms = find(r'f\+1 Commit latency \(workers\) \(p50\):\s*([\d,]+)\s*ms', block)
    commit_lat_p90_ms = find(r'f\+1 Commit latency \(workers\) \(p90\):\s*([\d,]+)\s*ms', block)
    commit_lat_p95_ms = find(r'f\+1 Commit latency \(workers\) \(p95\):\s*([\d,]+)\s*ms', block)

    return [label, committed_tps, commit_lat_mean_ms, commit_lat_p50_ms, commit_lat_p90_ms, commit_lat_p95_ms, cmd_line]


HEADER = ['label', 'committed_tps', 'commit_lat_mean_ms', 'commit_lat_p50_ms', 'commit_lat_p90_ms', 'commit_lat_p95_ms', 'cmd']


def main():
    if len(sys.argv) < 2:
        print(f'Usage: {sys.argv[0]} <merged_output.log>', file=sys.stderr)
        sys.exit(1)

    with open(sys.argv[1]) as f:
        content = f.read()

    parts = content.split('CMD: ')
    blocks = [p for p in parts[1:] if p.strip()]

    writer = csv.writer(sys.stdout)
    writer.writerow(HEADER)
    for block in blocks:
        writer.writerow(parse_block(block))


if __name__ == '__main__':
    main()
