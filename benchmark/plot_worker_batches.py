#!/usr/bin/env python3
import argparse
import re
import statistics
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

# Compatibility for older Matplotlib releases that still reference np.Inf.
if not hasattr(np, "Inf"):
    np.Inf = np.inf

import matplotlib.pyplot as plt


WINDOW_SIZE = 1


def moving_average(values: List[float], window: int) -> List[float]:
    result = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        result.append(sum(values[start : i + 1]) / (i - start + 1))
    return result


CERTIFIED_LINE_PATTERN = re.compile(
    r"Certified B(\d+)\([^)]+\) -> (\S+)"
)

BATCH_LINE_PATTERN = re.compile(
    r"^\[(?P<timestamp>[^\]]+)\s+INFO\s+worker::batch_maker\]\s+"
    r"Batch\s+(?P<digest>\S+)\s+contains\s+(?P<size_bytes>\d+)\s+B$"
)

QUORUM_LINE_PATTERN = re.compile(
    r"^\[(?P<timestamp>[^\]]+)\s+INFO\s+worker::quorum_waiter\]\s+"
    r"Quorum\s+for\s+batch\s+(?P<digest>\S+)\s+"
    r"queue_delay\s+(?P<queue_delay_ms>\d+)ms\s+quorum_latency\s+(?P<quorum_latency_ms>\d+)ms$"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Plot batch size, inter-quorum time, and quorum latency from worker logs."
        )
    )
    parser.add_argument(
        "path",
        help="Path to a worker log file or to a directory containing worker-*.log files",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Optional output image path. If omitted, the plot is saved in the benchmark directory.",
    )
    parser.add_argument(
        "--no-smooth",
        action="store_true",
        help=f"Disable moving average smoothing (window={WINDOW_SIZE}).",
    )
    parser.add_argument(
        "--skip",
        type=int,
        default=15,
        help="Number of initial batches to skip (default: 15).",
    )
    return parser.parse_args()


def group_by_round(rounds: List[int], values: List[float]) -> Tuple[List[int], List[float]]:
    bins: Dict[int, List[float]] = {}
    for r, v in zip(rounds, values):
        bins.setdefault(r, []).append(v)
    sorted_bins = sorted(bins.items())
    return [r for r, _ in sorted_bins], [sum(vals) / len(vals) for _, vals in sorted_bins]


def _make_stats_label(label: str, values: List[float]) -> str:
    if len(values) >= 2:
        p95 = statistics.quantiles(values, n=20)[18]
        std = statistics.stdev(values)
        mean = statistics.mean(values)
        return f"{label}  mean={mean:.0f}  p95={p95:.0f}  σ={std:.0f}"
    return label


def parse_timestamp(raw_timestamp: str) -> datetime:
    if raw_timestamp.endswith("Z"):
        raw_timestamp = raw_timestamp[:-1] + "+00:00"
    return datetime.fromisoformat(raw_timestamp)


def extract_batches(log_path: Path) -> List[Tuple[datetime, str, int]]:
    batches = []
    with log_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            match = BATCH_LINE_PATTERN.match(line.strip())
            if not match:
                continue

            timestamp = parse_timestamp(match.group("timestamp"))
            digest = match.group("digest")
            size_bytes = int(match.group("size_bytes"))
            batches.append((timestamp, digest, size_bytes))

    return batches


def extract_quorum_metrics_by_digest(log_path: Path) -> Dict[str, Tuple[int, int]]:
    metrics = {}
    with log_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            match = QUORUM_LINE_PATTERN.match(line.strip())
            if not match:
                continue
            metrics[match.group("digest")] = (
                int(match.group("queue_delay_ms")),
                int(match.group("quorum_latency_ms")),
            )
    return metrics


def extract_digest_to_round(primary_log_path: Path) -> Dict[str, int]:
    mapping: Dict[str, int] = {}
    with primary_log_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            m = CERTIFIED_LINE_PATTERN.search(line)
            if m:
                mapping[m.group(2)] = int(m.group(1))
    return mapping


def collect_digest_to_round(logs_dir: Path) -> Dict[str, int]:
    merged: Dict[str, int] = {}
    for primary_log in sorted(logs_dir.glob("primary-*.log")):
        merged.update(extract_digest_to_round(primary_log))
    return merged


LEGEND_FONT_SIZE = 4


def _apply_compact_legend_style() -> None:
    # Keep full labels, but render legend text/box more compact.
    plt.rcParams["legend.fontsize"] = LEGEND_FONT_SIZE
    plt.rcParams["legend.borderpad"] = 0.25
    plt.rcParams["legend.labelspacing"] = 0.25
    plt.rcParams["legend.handlelength"] = 1.2
    plt.rcParams["legend.handletextpad"] = 0.4
    plt.rcParams["legend.borderaxespad"] = 0.25


def plot_worker_log(
    worker_log: Path,
    batch_ax,
    latency_ax,
    queue_ax,
    digest_to_round: Dict[str, int],
    label: str,
    smooth: bool,
    linewidth: float = 1.2,
    alpha: float = 1.0,
    skip: int = 0,
) -> Tuple[bool, bool]:
    """Plot batch sizes, quorum latency, and queue delay from a single worker log onto the given axes.

    Returns (plotted_batches, plotted_quorum).
    """
    _apply_compact_legend_style()

    batches = extract_batches(worker_log)[skip:]
    if not batches:
        return False, False

    batch_rounds = []
    filtered_batches = []
    for ts, digest, size in batches:
        r = digest_to_round.get(digest)
        if r is not None:
            batch_rounds.append(r)
            filtered_batches.append((ts, digest, size))

    if not batch_rounds:
        return False, False

    sizes = [s for _, _, s in filtered_batches]
    grp_rounds, grp_sizes = group_by_round(batch_rounds, sizes)
    batch_label = _make_stats_label(label, grp_sizes)
    y_sizes = moving_average(grp_sizes, WINDOW_SIZE) if smooth else grp_sizes
    batch_ax.plot(grp_rounds, y_sizes, linewidth=linewidth, alpha=alpha, label=batch_label)

    quorum_by_digest = extract_quorum_metrics_by_digest(worker_log)
    latency_rounds, latency_ms, queue_ms = [], [], []
    for r, (_, digest, _) in zip(batch_rounds, filtered_batches):
        m = quorum_by_digest.get(digest)
        if m is not None:
            latency_rounds.append(r)
            queue_ms.append(m[0])
            latency_ms.append(m[1])

    if latency_ms:
        grp_r, grp_lat = group_by_round(latency_rounds, latency_ms)
        _, grp_q = group_by_round(latency_rounds, queue_ms)

        lat_label = _make_stats_label(label, grp_lat)
        q_label = _make_stats_label(label, grp_q)
        y_latency = moving_average(grp_lat, WINDOW_SIZE) if smooth else grp_lat
        y_queue = moving_average(grp_q, WINDOW_SIZE) if smooth else grp_q
        latency_ax.plot(grp_r, y_latency, linewidth=linewidth, alpha=alpha, label=lat_label)
        queue_ax.plot(grp_r, y_queue, linewidth=linewidth, alpha=alpha, label=q_label)
        return True, True

    return True, False


def find_worker_logs(path: Path) -> List[Path]:
    if path.is_file():
        return [path]

    if not path.is_dir():
        return []

    return sorted(path.glob("worker-*.log"))


def default_output_path(path: Path) -> Path:
    benchmark_dir = Path(__file__).resolve().parent
    if path.is_dir():
        return benchmark_dir / f"{path.name}-worker-batches.png"
    return benchmark_dir / f"{path.stem}-batches.png"


def plot_batches(path: Path, output_path: Optional[Path], smooth: bool, skip: int = 0) -> None:
    worker_logs = find_worker_logs(path)
    if not worker_logs:
        raise ValueError("No worker log files were found")

    logs_dir = path if path.is_dir() else path.parent
    digest_to_round = collect_digest_to_round(logs_dir)

    fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=False)

    plotted_logs = 0
    plotted_quorum_latency_logs = 0
    for worker_log in worker_logs:
        label = worker_log.stem
        plotted_batches, plotted_quorum = plot_worker_log(worker_log, axes[0], axes[1], axes[2], digest_to_round, label, smooth, skip=skip)
        if plotted_batches:
            plotted_logs += 1
        if plotted_quorum:
            plotted_quorum_latency_logs += 1

    if plotted_logs == 0:
        raise ValueError(
            "No batches could be mapped to a round — check that primary-*.log files are present in the same directory"
        )

    axes[0].set_ylabel("Batch size (B)")
    axes[0].grid(True, alpha=0.3)
    axes[0].legend(loc="upper right")

    axes[1].set_ylabel("Quorum latency (ms)")
    axes[1].grid(True, alpha=0.3)
    if plotted_quorum_latency_logs > 0:
        axes[1].legend(loc="upper right")

    axes[2].set_xlabel("Round")
    axes[2].set_ylabel("Queue delay (ms)")
    axes[2].grid(True, alpha=0.3)
    if plotted_quorum_latency_logs > 0:
        axes[2].legend(loc="upper right")

    fig.text(0.5, 0.01, path.name, ha="center", fontsize=10)
    fig.tight_layout(rect=[0, 0.03, 1, 1])

    if output_path is None:
        output_path = default_output_path(path)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(output_path), dpi=200, bbox_inches="tight")
    print(output_path)


def main() -> int:
    args = parse_args()
    path = Path(args.path)

    if not path.exists():
        print(f"Path not found: {path}", file=sys.stderr)
        return 1

    output_path = Path(args.output) if args.output else None

    try:
        plot_batches(path, output_path, not args.no_smooth, args.skip)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())