#!/usr/bin/env python3
"""Check that TPS increases monotonically with rate for each label in latency_tps.csv.

Warns when a higher input rate yields lower TPS than the previous rate point for
the same label, which usually indicates a bad/flaky run rather than saturation.

Usage:
    python check_tps_monotone.py <results_dir_or_csv> [--auto-purge]

Exit code: 0 if clean, 1 if any violations found.
"""
import argparse
import csv
import importlib.util
import shutil
import sys
from collections import defaultdict
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "purge_runs", Path(__file__).parent / "purge_runs.py"
)
_purge_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_purge_mod)

RUN_SUFFIX_RE = None  # resolved at runtime from label+rate+run


def load_csv(csv_path):
    raw = defaultdict(list)  # label -> [(rate, tps, run)]
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        reader.fieldnames = [name.strip() for name in reader.fieldnames]
        for row in reader:
            label = row["label"].strip()
            rate = int(row["rate"].strip())
            tps = int(row["tps"].strip())
            run = int(row["run"].strip())
            raw[label].append((rate, tps, run))
    return raw


def main():
    parser = argparse.ArgumentParser(
        description="Check TPS monotonicity across rate points."
    )
    parser.add_argument("target", metavar="results_dir_or_csv")
    parser.add_argument(
        "--auto-purge",
        action="store_true",
        help="Automatically purge runs with TPS drop >5%%",
    )
    args = parser.parse_args()
    target = Path(args.target)

    if target.is_dir():
        matches = list(target.glob("*.csv"))
        assert len(matches) == 1, f"Expected 1 CSV in {target}, found {len(matches)}: {matches}"
        csv_path = matches[0]
    else:
        csv_path = target
    assert csv_path.exists(), f"Not found: {csv_path}"
    results_dir = csv_path.parent

    raw = load_csv(csv_path)
    assert raw, f"No data in {csv_path}"

    violations = 0
    for label in sorted(raw):
        points = sorted(raw[label], key=lambda p: p[0])  # sort by rate
        for i in range(1, len(points)):
            prev_rate, prev_tps, _ = points[i - 1]
            curr_rate, curr_tps, curr_run = points[i]
            if curr_tps < prev_tps:
                drop_pct = (curr_tps - prev_tps) / prev_tps * 100
                run_dir_name = f"{label}_r{curr_rate}_run_{curr_run}"
                print(
                    f"WARNING [{label}] rate {prev_rate}->{curr_rate}: "
                    f"tps {prev_tps}->{curr_tps} (drop {drop_pct:.1f}%)"
                )
                print(f"  purge: {run_dir_name}")
                violations += 1

                if args.auto_purge and drop_pct < -5:
                    run_dir = results_dir / run_dir_name
                    assert run_dir.exists(), f"Run dir not found: {run_dir.resolve()}"
                    print(f"  AUTO-PURGING: {run_dir.resolve()}")
                    log_path = results_dir / "merged_output.log"
                    if log_path.exists():
                        _purge_mod.scrub_log(log_path, str(run_dir.resolve()), False)
                    _purge_mod.scrub_csv(csv_path, label, curr_rate, curr_run, False)
                    shutil.rmtree(run_dir)
                    print(f"  removed: {run_dir.resolve()}")

    if violations:
        print(f"\n{violations} violation(s) found.")
        sys.exit(1)
    else:
        print("OK: TPS is monotonically non-decreasing for all labels.")


if __name__ == "__main__":
    main()
