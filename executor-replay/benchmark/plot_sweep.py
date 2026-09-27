#!/usr/bin/env python3
"""Generic sweep plotter. Auto-discovers <label>_run_<N> subdirectories."""
import argparse
import re
from pathlib import Path
from typing import Dict, List, Tuple

from plot_worker_batches import WINDOW_SIZE
from plot_common import plot_experiment, CONFIG_ORDER


# <label>_run_<N>
RUN_DIR_RE = re.compile(r"^(.+)_run_(\d+)$")

# <config>_r<rate> within a label
LABEL_RATE_RE = re.compile(r"^(.+)_r(\d+)$")


def label_sort_key(label: str) -> tuple:
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot all sweep runs in a single combined figure."
    )
    parser.add_argument(
        "results_dir",
        help="Path to a results directory containing *_run_* subdirectories",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output image path. Defaults to benchmark/<results_dir_name>-sweep.png",
    )
    parser.add_argument(
        "--no-smooth",
        action="store_true",
        help=f"Disable moving average smoothing (window={WINDOW_SIZE}).",
    )
    return parser.parse_args()


def discover_runs(results_dir: Path) -> Dict[str, List[Tuple[int, Path]]]:
    """Returns {label: [(run_number, run_dir), ...]} sorted by run number."""
    groups: Dict[str, List[Tuple[int, Path]]] = {}
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


def main() -> int:
    args = parse_args()
    results_dir = Path(args.results_dir)

    assert results_dir.exists(), f"Path not found: {results_dir}"

    groups = discover_runs(results_dir)
    assert groups, f"No *_run_* subdirectories found in {results_dir}"

    labels = sorted(groups.keys(), key=label_sort_key)
    columns = [(label, groups[label]) for label in labels]
    # print(labels)
    # return 0

    output_path = Path(args.output) if args.output else (
        Path(__file__).resolve().parent / f"{results_dir.name}-sweep.png"
    )

    plot_experiment(results_dir, output_path, not args.no_smooth, columns, lambda d: d)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
