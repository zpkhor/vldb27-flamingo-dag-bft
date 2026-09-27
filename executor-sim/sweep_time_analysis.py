"""
Sweep of Flamingo vs paper/paper.py schedulers across
batch_size × tx_size_dist × num_executors combinations.

Fixed: overlap_prob=0.6, cross_shard_prob=0.5, zipf_alpha=1.2,
       history_window=16, reuse_fraction=0.5

Varying: batch_size in [128, 256, 512, 1000]
         tx_size_dist in [medium, heavy]
         num_executors in [4, 8, 16, 20]

Outputs sweep_results_time_analysis.csv (64 rows: 32 per scheduler).
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
PAPER_BATCH_TIMEOUT_S = 30

# Fixed parameters
OVERLAP_PROB = 0.6
CROSS_SHARD_PROB = 0.5
ZIPF_ALPHA = 1.2
HISTORY_WINDOW = 16
REUSE_FRACTION = 0.5

# Varying parameters
BATCH_SIZES = [128, 256, 512, 1000]
TX_SIZE_DISTS = {
    'medium': [(2, 0.5), (4, 0.3), (8, 0.2)],
    'heavy':  [(4, 0.5), (8, 0.3), (16, 0.2)],
}
NUM_EXECUTORS_LIST = [4, 8, 16, 20]


def run_best_program(batches, num_executors):
    scheduler = BestScheduler()
    sim = SimStandard(num_executors, Configuration.NUM_ACCOUNTS)
    sim.reset_partition()
    executed_txs = [0.0] * num_executors
    total_remote_reads = 0
    total_duration = 0.0

    for batch in batches:
        partition_copy = sim.partition.copy()
        t0 = time.perf_counter()
        schedule = scheduler(partition_copy, batch, num_executors, executed_txs)
        total_duration += time.perf_counter() - t0

        valid, rr = sim.evaluate_batch_schedule(batch, schedule)
        if not valid:
            raise RuntimeError("best_program produced invalid schedule")
        total_remote_reads += rr

        counts = collections.Counter(schedule)
        for e in range(num_executors):
            executed_txs[e] += counts.get(e, 0)

        if hasattr(scheduler, 'update_history'):
            scheduler.update_history(batch)

    return total_remote_reads, total_duration


def run_paper(batches, num_executors):
    sim = SimPaper(num_executors, Configuration.NUM_ACCOUNTS)
    sim.reset_partition()
    total_remote_reads = 0
    total_duration = 0.0

    for batch in batches:
        partition_copy = sim.partition.copy()

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                paper_scheduler, partition_copy, batch, num_executors, EPSILON
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

        _, rr = sim.evaluate_batch_schedule(
            ordered_batch, schedule, batch,
            duration_seconds=0.0,
            time_penalty_weight=Configuration.TIME_PENALTY_WEIGHT,
            epsilon=EPSILON,
        )
        total_remote_reads += rr

    return total_remote_reads, total_duration


def main():
    output_path = os.path.join(os.path.dirname(__file__), 'sweep_results_time_analysis.csv')
    fieldnames = [
        'seed', 'scheduler', 'tx_size_dist', 'batch_size', 'num_executors',
        'avg_remote_reads_per_batch', 'avg_runtime_per_batch_s',
    ]

    original_tx_dist = Configuration.TX_SIZE_DIST
    combos = list(itertools.product(
        TX_SIZE_DISTS.keys(), BATCH_SIZES, NUM_EXECUTORS_LIST
    ))
    total = len(combos) * len(SEEDS)

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        n = 0
        for seed_offset in SEEDS:
            for tx_size_name, batch_size, num_executors in combos:
                n += 1
                print(f"[{n}/{total}] seed={seed_offset} tx={tx_size_name} batch={batch_size} "
                      f"executors={num_executors}", flush=True)

                Configuration.TX_SIZE_DIST = TX_SIZE_DISTS[tx_size_name]
                batches = generate_batches_parallel(
                    num_accounts=Configuration.NUM_ACCOUNTS,
                    batch_size=batch_size,
                    num_executors=num_executors,
                    cross_shard_prob=CROSS_SHARD_PROB,
                    num_batches=NUM_BATCHES,
                    zipf_alpha=ZIPF_ALPHA,
                    overlap_prob=OVERLAP_PROB,
                    history_window=HISTORY_WINDOW,
                    reuse_fraction=REUSE_FRACTION,
                    seed_offset=seed_offset,
                )
                Configuration.TX_SIZE_DIST = original_tx_dist

                common = {
                    'seed': seed_offset,
                    'tx_size_dist': tx_size_name,
                    'batch_size': batch_size,
                    'num_executors': num_executors,
                }

                # best_program
                rr_bp, dur_bp = run_best_program(batches, num_executors)
                writer.writerow({
                    **common,
                    'scheduler': 'best_program',
                    'avg_remote_reads_per_batch': round(rr_bp / NUM_BATCHES, 2),
                    'avg_runtime_per_batch_s': round(dur_bp / NUM_BATCHES, 6),
                })

                # paper
                try:
                    rr_p, dur_p = run_paper(batches, num_executors)
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
