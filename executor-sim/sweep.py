"""
Sweep comparison of Flamingo vs paper/paper.py schedulers across all
combinations of tx_size_dist, cross_shard_prob, overlap_prob, and zipf_alpha.

Outputs sweep_results.csv with avg remote reads and scheduler runtime per combo.
"""

import sys
import os
import csv
import time
import collections
import itertools
import concurrent.futures

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'paper'))

from config import Configuration
from simulator import generate_batches_parallel, Simulator as SimStandard
from evaluator_paper import Simulator as SimPaper
from flamingo import Scheduler as BestScheduler
from paper import scheduler as paper_scheduler

NUM_BATCHES = 5
SEEDS = [0, 1000, 2000]
EPSILON = Configuration.EPSILON
PAPER_BATCH_TIMEOUT_S = 30  # paper's O(n²) loop can diverge; abort batch if exceeded

TX_SIZE_DISTS = {
    'light':  Configuration.TX_SIZE_LIGHT,
    'medium': Configuration.TX_SIZE_MEDIUM,
    'heavy':  Configuration.TX_SIZE_HEAVY,
}

CROSS_SHARD_PROBS = [0.25, 0.5, 0.75]
OVERLAP_PROBS     = [0.0, 0.3, 0.6, 0.9]
ZIPF_ALPHAS       = [0.8, 1.2]


def run_best_program(batches):
    """Run best_program scheduler batch-by-batch, timing only the scheduler call."""
    scheduler = BestScheduler()
    sim = SimStandard(Configuration.NUM_EXECUTORS, Configuration.NUM_ACCOUNTS)
    sim.reset_partition()
    executed_txs = [0.0] * Configuration.NUM_EXECUTORS
    total_remote_reads = 0
    total_duration = 0.0

    for batch in batches:
        partition_copy = sim.partition.copy()
        t0 = time.perf_counter()
        schedule = scheduler(partition_copy, batch, Configuration.NUM_EXECUTORS, executed_txs)
        total_duration += time.perf_counter() - t0

        valid, rr = sim.evaluate_batch_schedule(batch, schedule)
        if not valid:
            raise RuntimeError("best_program produced invalid schedule")
        total_remote_reads += rr

        counts = collections.Counter(schedule)
        for e in range(Configuration.NUM_EXECUTORS):
            executed_txs[e] += counts.get(e, 0)

        if hasattr(scheduler, 'update_history'):
            scheduler.update_history(batch)

    return total_remote_reads, total_duration


def run_paper(batches):
    """Run paper scheduler batch-by-batch, timing only the scheduler call.

    Returns (total_remote_reads, total_duration) or raises TimeoutError if any
    single batch exceeds PAPER_BATCH_TIMEOUT_S (paper's rebalancing loop can diverge).
    """
    sim = SimPaper(Configuration.NUM_EXECUTORS, Configuration.NUM_ACCOUNTS)
    sim.reset_partition()
    total_remote_reads = 0
    total_duration = 0.0

    for batch in batches:
        partition_copy = sim.partition.copy()

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                paper_scheduler, partition_copy, batch, Configuration.NUM_EXECUTORS, EPSILON
            )
            t0 = time.perf_counter()
            try:
                schedule, ordered_batch = future.result(timeout=PAPER_BATCH_TIMEOUT_S)
            except concurrent.futures.TimeoutError:
                future.cancel()
                raise TimeoutError(
                    f"paper scheduler exceeded {PAPER_BATCH_TIMEOUT_S}s on a batch"
                )
            total_duration += time.perf_counter() - t0

        # Pass duration_seconds=0.0 so cost formula doesn't mix timing into remote reads
        _, rr = sim.evaluate_batch_schedule(
            ordered_batch, schedule, batch,
            duration_seconds=0.0,
            time_penalty_weight=Configuration.TIME_PENALTY_WEIGHT,
            epsilon=EPSILON,
        )
        total_remote_reads += rr

    return total_remote_reads, total_duration


def main():
    output_path = os.path.join(os.path.dirname(__file__), 'sweep_results.csv')
    fieldnames = [
        'seed', 'scheduler', 'tx_size_dist', 'cross_shard_prob', 'overlap_prob', 'zipf_alpha',
        'avg_remote_reads_per_batch', 'avg_runtime_per_batch_s',
    ]

    original_tx_dist = Configuration.TX_SIZE_DIST
    combos = list(itertools.product(
        TX_SIZE_DISTS.keys(), CROSS_SHARD_PROBS, OVERLAP_PROBS, ZIPF_ALPHAS
    ))
    total = len(combos) * len(SEEDS)

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        n = 0
        for seed_offset in SEEDS:
            for tx_size_name, cross_shard_prob, overlap_prob, zipf_alpha in combos:
                n += 1
                print(f"[{n}/{total}] seed={seed_offset} tx={tx_size_name} csp={cross_shard_prob} "
                      f"op={overlap_prob} alpha={zipf_alpha}", flush=True)

                # Patch TX_SIZE_DIST before generating batches
                Configuration.TX_SIZE_DIST = TX_SIZE_DISTS[tx_size_name]
                batches = generate_batches_parallel(
                    Configuration.NUM_ACCOUNTS,
                    Configuration.BATCH_SIZE,
                    Configuration.NUM_EXECUTORS,
                    cross_shard_prob,
                    NUM_BATCHES,
                    zipf_alpha,
                    overlap_prob=overlap_prob,
                    history_window=Configuration.HISTORY_WINDOW,
                    reuse_fraction=Configuration.REUSE_FRACTION,
                    seed_offset=seed_offset,
                )
                Configuration.TX_SIZE_DIST = original_tx_dist  # restore

                common = {
                    'seed': seed_offset,
                    'tx_size_dist': tx_size_name,
                    'cross_shard_prob': cross_shard_prob,
                    'overlap_prob': overlap_prob,
                    'zipf_alpha': zipf_alpha,
                }

                # best_program
                rr_bp, dur_bp = run_best_program(batches)
                writer.writerow({
                    **common,
                    'scheduler': 'best_program',
                    'avg_remote_reads_per_batch': round(rr_bp / NUM_BATCHES, 2),
                    'avg_runtime_per_batch_s': round(dur_bp / NUM_BATCHES, 6),
                })

                # paper
                try:
                    rr_p, dur_p = run_paper(batches)
                    paper_row = {
                        **common,
                        'scheduler': 'paper',
                        'avg_remote_reads_per_batch': round(rr_p / NUM_BATCHES, 2),
                        'avg_runtime_per_batch_s': round(dur_p / NUM_BATCHES, 6),
                    }
                except TimeoutError as e:
                    print(f"  [TIMEOUT] {e}", flush=True)
                    paper_row = {
                        **common,
                        'scheduler': 'paper',
                        'avg_remote_reads_per_batch': 'timeout',
                        'avg_runtime_per_batch_s': 'timeout',
                    }
                writer.writerow(paper_row)

                f.flush()

    print(f"\nDone. Results written to {output_path}")


if __name__ == '__main__':
    main()
