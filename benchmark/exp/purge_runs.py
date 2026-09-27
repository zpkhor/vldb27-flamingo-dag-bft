#!/usr/bin/env python3
"""Remove bad run directories and scrub their entries from merged_output.log and latency_tps.csv.

Accepts run dir names (bare or absolute paths). If bare names are given,
--results-dir must point to the parent directory.

Usage:
    python purge_runs.py --results-dir <dir> <run_dir_name> [...]
    python purge_runs.py /abs/path/to/n4_ve_rate_imb60_r72000_run_1 [...]
    python purge_runs.py --dry-run --results-dir <dir> <run_dir_name>
"""
import argparse
import csv
import re
import shutil
import sys
from pathlib import Path

RUN_DIR_RE = re.compile(r"^n(\d+)_(.+)_r(\d+)_run_(\d+)$")

BASE_FIELDS = ["label", "n", "rate", "run"]
TAIL_FIELDS = ["tps"]


def resolve_run_dirs(names, results_dir):
    resolved = []
    for name in names:
        p = Path(name)
        if p.is_absolute() or p.parent != Path("."):
            resolved.append(p.resolve())
        else:
            assert results_dir is not None, (
                f"--results-dir required when passing bare run dir names (got: {name!r})"
            )
            resolved.append(Path(results_dir).resolve() / p.name)
    return resolved


def parse_run_dir_name(name):
    m = RUN_DIR_RE.match(name)
    assert m, f"Run dir name does not match expected pattern n<N>_<label>_r<rate>_run_<run>: {name!r}"
    n = int(m.group(1))
    label = f"n{n}_{m.group(2)}"
    rate = int(m.group(3))
    run = int(m.group(4))
    return label, n, rate, run


def scrub_log(log_path, run_dir_name, dry_run):
    """Remove the CMD block for run_dir_name from merged_output.log."""
    lines = log_path.read_text(encoding="utf-8").splitlines(keepends=True)

    # Find indices of lines starting with "CMD: "
    cmd_indices = [i for i, l in enumerate(lines) if l.startswith("CMD: ")]
    cmd_indices.append(len(lines))  # sentinel

    # Match --log-dir=<any-path>/<run_dir_name> regardless of symlink resolution
    log_dir_re = re.compile(r"--log-dir=\S*/" + re.escape(run_dir_name) + r"(?:\s|$)")
    removed_blocks = 0
    keep = [True] * len(lines)

    for k, start in enumerate(cmd_indices[:-1]):
        end = cmd_indices[k + 1]
        if log_dir_re.search(lines[start]):
            for i in range(start, end):
                keep[i] = False
            removed_blocks += 1

    if removed_blocks == 0:
        print(
            f"  WARNING: no CMD block found for {run_dir_name} in {log_path}\n"
            f"           (CMD may have been logged without --log-dir by an older script version — remove manually)",
            file=sys.stderr,
        )
        return 0

    kept_lines = [l for i, l in enumerate(lines) if keep[i]]
    removed_line_count = len(lines) - len(kept_lines)
    print(f"  log: removing {removed_line_count} lines ({removed_blocks} block(s)) from {log_path.resolve()}")
    if not dry_run:
        log_path.write_text("".join(kept_lines), encoding="utf-8")
    return removed_line_count


def scrub_csv(csv_path, label, rate, run, dry_run):
    """Remove matching rows from latency_tps.csv."""
    rows = []
    removed = 0
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        fieldnames = [name.strip() for name in reader.fieldnames]
        assert set(BASE_FIELDS + TAIL_FIELDS).issubset(set(fieldnames)), (
            f"CSV is missing expected columns. Got: {fieldnames}"
        )
        for row in reader:
            if (
                row["label"].strip() == label
                and int(row["rate"].strip()) == rate
                and int(row["run"].strip()) == run
            ):
                removed += 1
            else:
                rows.append({k: row[k].strip() for k in fieldnames})

    print(f"  csv: removing {removed} row(s) from {csv_path.resolve()}")
    if removed == 0:
        print(f"  WARNING: no CSV row matched label={label!r} rate={rate} run={run}", file=sys.stderr)
    if not dry_run and removed > 0:
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    return removed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dirs", nargs="+", metavar="run_dir")
    parser.add_argument("--results-dir", metavar="DIR")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    run_dirs = resolve_run_dirs(args.run_dirs, args.results_dir)

    for run_dir in run_dirs:
        print(f"\nPurging: {run_dir.resolve()}" + (" [DRY RUN]" if args.dry_run else ""))
        assert run_dir.exists(), f"Run dir not found: {run_dir.resolve()}"

        results_dir = run_dir.parent
        log_path = results_dir / "merged_output.log"
        csv_path = results_dir / "latency_tps.csv"

        label, n, rate, run = parse_run_dir_name(run_dir.name)
        print(f"  parsed: label={label!r} n={n} rate={rate} run={run}")

        if log_path.exists():
            scrub_log(log_path, run_dir.name, args.dry_run)
        else:
            print(f"  log: {log_path} not found, skipping")

        if csv_path.exists():
            scrub_csv(csv_path, label, rate, run, args.dry_run)
        else:
            print(f"  csv: {csv_path} not found, skipping")

        if not args.dry_run:
            shutil.rmtree(run_dir)
            print(f"  removed: {run_dir.resolve()}")
        else:
            print(f"  would remove: {run_dir.resolve()}")

    print("\nDone." if not args.dry_run else "\nDry run complete. No changes made.")


if __name__ == "__main__":
    main()
