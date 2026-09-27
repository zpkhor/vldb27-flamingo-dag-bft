#!/usr/bin/env python3
"""Shared plotting utilities for all phase plot scripts."""
import re
from pathlib import Path
from typing import Callable, List, Tuple

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

from plot_worker_batches import find_worker_logs, plot_worker_log, collect_digest_to_round


CONFIG_ORDER = [
    "n7",
    "n7_rate_imb",
    "n7_rate_imb_rr",
    "n7_bw_f",
    "n7_bw_f_rr",
    "n7_bw_f1",
    "n7_bw_f1_rr",
    "n7_rr",
    "n4",
    "n4_rate_imb",
    "n4_rate_imb_rr",
    "n4_bw_f",
    "n4_bw_f_rr",
    "n4_bw_f1",
    "n4_bw_f1_rr",
    "n4_rr",
]

LATENCY_RE = re.compile(r"f\+1 Commit latency \(workers\) \(mean\): ([\d,]+) ms")
TPS_RE = re.compile(r"Committed TPS: ([\d,]+) tx/s")
PER_VALIDATOR_SECTION_RE = re.compile(
    r"\+ PER-VALIDATOR COMMIT METRICS:.*?(?=\n\s*\+|\Z)", re.DOTALL
)
PER_VALIDATOR_ROW_RE = re.compile(r"^\s+(\d+)\s+([\d,]+)\s+([\d,]+)", re.MULTILINE)


def parse_metrics(run_dir: Path) -> Tuple[str, str]:
    output_log = run_dir / "output.log"
    if not output_log.exists():
        return "?", "?"
    text = output_log.read_text(encoding="utf-8")
    latency_match = LATENCY_RE.search(text)
    tps_match = TPS_RE.search(text)
    latency = latency_match.group(1) + "ms" if latency_match else "?"
    tps = tps_match.group(1) + "tx/s" if tps_match else "?"
    return latency, tps


def parse_per_validator_metrics(run_dir: Path) -> List[Tuple[int, str, str]]:
    output_log = run_dir / "output.log"
    if not output_log.exists():
        return []
    text = output_log.read_text(encoding="utf-8")
    section_match = PER_VALIDATOR_SECTION_RE.search(text)
    if not section_match:
        return []
    results = []
    for m in PER_VALIDATOR_ROW_RE.finditer(section_match.group(0)):
        v_id = int(m.group(1))
        tps = m.group(2).replace(",", "")
        mean_ms = m.group(3).replace(",", "")
        results.append((v_id, tps, mean_ms))
    return results


# ColumnSpec: [(col_title, [(run_num, run_dir), ...])]
ColumnSpec = List[Tuple[str, List[Tuple[int, Path]]]]


def plot_experiment(
    results_dir: Path,
    output_path: Path,
    smooth: bool,
    columns: ColumnSpec,
    worker_logs_dir: Callable[[Path], Path],
) -> None:
    """Generic multi-column experiment plotter.

    Args:
        results_dir: Used for the bottom-center label.
        output_path: Where to save the figure.
        smooth: Whether to apply moving-average smoothing.
        columns: [(col_title, [(run_num, run_dir), ...])], one entry per column.
        worker_logs_dir: Maps run_dir to the directory containing worker-*.log files.
    """
    n_cols = len(columns)
    assert n_cols > 0, f"No columns to plot for {results_dir}"

    fig = plt.figure(figsize=(6 * n_cols, 20))
    gs = gridspec.GridSpec(
        4, n_cols,
        figure=fig,
        height_ratios=[3, 3, 3, 1],
        hspace=0.45,
        wspace=0.35,
    )
    axes = [[fig.add_subplot(gs[row, col]) for col in range(n_cols)] for row in range(4)]

    for col_idx, (col_title, runs) in enumerate(columns):
        metrics_lines = []

        for run_num, run_dir in runs:
            alpha = 1.0 if run_num == runs[0][0] else 0.5
            logs_dir = worker_logs_dir(run_dir)
            worker_logs = find_worker_logs(logs_dir)
            digest_to_round = collect_digest_to_round(logs_dir)

            for worker_log in worker_logs:
                worker_label = f"{worker_log.stem} (run {run_num})"
                plot_worker_log(
                    worker_log,
                    axes[0][col_idx], axes[1][col_idx], axes[2][col_idx],
                    digest_to_round, worker_label, smooth, 1.0, alpha,
                )

            latency, tps = parse_metrics(run_dir)
            metrics_lines.append(f"Run {run_num}: Latency={latency}  TPS={tps}")
            for v_id, v_tps, v_mean in parse_per_validator_metrics(run_dir):
                metrics_lines.append(f"  V{v_id}: {v_tps}tx/s  {v_mean}ms")

        axes[2][col_idx].set_xlabel(col_title, fontsize=10, fontweight="bold")

        ax_text = axes[3][col_idx]
        ax_text.axis("off")
        ax_text.text(
            0.5, 0.5,
            "\n".join(metrics_lines),
            transform=ax_text.transAxes,
            ha="center", va="center",
            fontsize=9, family="monospace",
        )

    axes[0][0].set_ylabel("Batch size (B)")
    axes[1][0].set_ylabel("Quorum latency (ms)")
    axes[2][0].set_ylabel("Queue delay (ms)")

    for col_idx in range(n_cols):
        for row_idx in range(3):
            axes[row_idx][col_idx].grid(True, alpha=0.3)
            if axes[row_idx][col_idx].get_lines():
                axes[row_idx][col_idx].legend(fontsize=6, loc="upper right")

    fig.text(0.5, 0.01, results_dir.name, ha="center", fontsize=12)
    fig.tight_layout(rect=(0, 0.03, 1, 1))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    print(output_path)
