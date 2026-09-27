#!/usr/bin/env python3
"""
Run all benchmark commands from 50%.csv and balanced.csv sequentially on control-host,
parse Committed TPS and E2E TPS, and update the CSVs after each run.

Uses default manifest.xml (21-node) by stripping --manifest flag from commands.
"""
import csv
import subprocess
import re
import sys

SSH_HOST = 'anonuser@control-host'
REMOTE_PREFIX = (
    'cd ~/Projects/narwhal/benchmark && '
    'source ~/venvs/narwhal/bin/activate && '
    'export PATH="$HOME/.cargo/bin:$PATH" && '
)

CSV_FILES = [
    '/home/anonuser/narwhal/Experiment/50%.csv',
    '/home/anonuser/narwhal/Experiment/balanced.csv',
    '/home/anonuser/narwhal/Experiment/90%.csv',
]


def strip_manifest(cmd):
    return re.sub(r'\s*--manifest\s+\S+', '', cmd).strip()


def run_remote(cmd):
    clean_cmd = strip_manifest(cmd)
    remote_cmd = REMOTE_PREFIX + clean_cmd + ' 2>&1'
    result = subprocess.run(
        ['ssh', '-A', SSH_HOST, remote_cmd],
        capture_output=True, text=True, timeout=900
    )
    return result.stdout + result.stderr


def parse_tps(output):
    committed = re.search(r'Committed TPS:\s+([\d,]+)\s+tx/s', output)
    e2e = re.search(r'E2E TPS:\s+([\d,]+)\s+tx/s', output)
    if committed and e2e:
        return (
            int(committed.group(1).replace(',', '')),
            int(e2e.group(1).replace(',', '')),
        )
    return None, None


def read_csv(path):
    with open(path, 'r', newline='') as f:
        return list(csv.reader(f))


def write_csv(path, rows):
    with open(path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)


# Run all commands and fill TPS (skips rows already filled — safe to resume)
for csv_file in CSV_FILES:
    print(f'\n{"="*60}')
    print(f'Processing: {csv_file}')
    print('='*60)

    rows = read_csv(csv_file)
    header = rows[0]
    committed_idx = header.index('Committed TPS')
    e2e_idx = header.index('E2E TPS')
    cmd_idx = header.index('Command')

    for i, row in enumerate(rows[1:], 1):
        if not row or len(row) <= cmd_idx or not row[cmd_idx].strip():
            continue

        cmd = row[cmd_idx].strip()

        while len(row) <= max(committed_idx, e2e_idx):
            row.append('')

        # Skip if already filled
        if row[committed_idx].strip() and row[e2e_idx].strip():
            print(f'Row {i}: already filled ({row[committed_idx]}, {row[e2e_idx]}), skipping')
            continue

        print(f'\nRow {i}: {strip_manifest(cmd)[:100]}')
        sys.stdout.flush()

        committed, e2e = None, None
        for attempt in range(1, 4):
            output = run_remote(cmd)
            committed, e2e = parse_tps(output)
            if committed is not None:
                break
            print(f'Attempt {attempt} failed. Output tail:\n{output[-500:]}')
            if attempt < 3:
                print('Retrying...')
                # Kill any leftover processes before retrying
                run_remote('fab cloudlab-kill --username anonuser')

        if committed is None:
            print(f'FAILED after 3 attempts for row {i}, giving up.')
            sys.exit(1)

        print(f'Row {i}: Committed TPS={committed}, E2E TPS={e2e}')
        sys.stdout.flush()

        row[committed_idx] = str(committed)
        row[e2e_idx] = str(e2e)
        write_csv(csv_file, rows)

print('\nAll done.')
