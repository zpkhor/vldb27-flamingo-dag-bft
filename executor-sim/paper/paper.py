import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from typing import List, Dict, Tuple, Optional
from config import Transaction
import collections
import math


# Bounded to keep the simulator from blowing up.
# This is still Hermes-like, but not an unbounded implementation.
MAX_LOOKAHEAD = 128
MAX_DELTA = 8


def remote_read_cost(tx: Transaction, executor: int, partition: Dict[int, int]) -> int:
    """Hermes Step 1 cost: remote reads over read_set only."""
    return sum(1 for acc in tx.read_set if partition.get(acc) != executor)


def apply_write_fusion(tx: Transaction, executor: int, partition: Dict[int, int]) -> None:
    """Hermes simplified update: only write_set changes ownership."""
    for acc in tx.write_set:
        partition[acc] = executor


def compute_load_sets(schedule: List[int], num_executors: int, theta: int):
    counts = collections.Counter(schedule)
    for eid in range(num_executors):
        counts[eid] += 0
    overloaded = {eid for eid in range(num_executors) if counts[eid] > theta}
    underloaded = {eid for eid in range(num_executors) if counts[eid] < theta}
    return counts, overloaded, underloaded


def find_new_route(
    tx: Transaction,
    underloaded: set[int],
    partition_final: Dict[int, int],
    delta: int,
    future_txs: List[Transaction],
    future_routes: List[int],
) -> Optional[int]:
    """
    Hermes Step 3 approximation aligned to the paper's logic:
      extra_edges =
        1) remote reads of Ti if routed to x'
        2) later reads to Ti.write_set by transactions not routed to x'
    """
    if not underloaded:
        return None

    best_executor = None
    best_extra = float('inf')
    tx_write_keys = set(tx.write_set)

    limited_future = list(zip(future_txs[:MAX_LOOKAHEAD], future_routes[:MAX_LOOKAHEAD]))

    for executor in underloaded:
        extra_edges = 0

        # Incoming remote reads for Ti at the candidate executor.
        for acc in tx.read_set:
            if partition_final.get(acc) != executor:
                extra_edges += 1

        # Future reads of Ti.write_set by later txs not routed to executor.
        for sub_tx, sub_route in limited_future:
            if sub_route == executor:
                continue

            # Count one edge if this later tx reads anything written by tx.
            if any(acc in tx_write_keys for acc in sub_tx.read_set):
                extra_edges += 1

        if extra_edges <= delta and extra_edges < best_extra:
            best_extra = extra_edges
            best_executor = executor

    return best_executor


def scheduler(
    partition: Dict[int, int],
    batch: List[Transaction],
    num_executors: int,
    epsilon: float
) -> Tuple[List[int], List[Transaction]]:
    """
    Hermes-inspired prescient transaction routing with explicit read/write sets.

    Returns:
        schedule: executor assignments for reordered batch
        ordered_batch: reordered batch B'
    """
    if not batch:
        return [], []

    # --- Step 1: reorder + route greedily minimizing remote reads ---
    current_partition = partition.copy()
    remaining = list(batch)
    ordered_batch: List[Transaction] = []
    schedule: List[int] = []

    for _ in range(len(batch)):
        best_tx_idx = -1
        best_tx = None
        best_executor = -1
        best_cost = float('inf')
        best_partial_load = float('inf')

        partial_counts = collections.Counter(schedule)

        for idx, tx in enumerate(remaining):
            for executor in range(num_executors):
                cost = remote_read_cost(tx, executor, current_partition)
                partial_load = partial_counts[executor]

                # Prefer lower cost, then lower partial load, then lower executor id
                if (
                    cost < best_cost
                    or (cost == best_cost and partial_load < best_partial_load)
                    or (cost == best_cost and partial_load == best_partial_load and executor < best_executor)
                ):
                    best_cost = cost
                    best_partial_load = partial_load
                    best_tx_idx = idx
                    best_tx = tx
                    best_executor = executor

        ordered_batch.append(best_tx)
        schedule.append(best_executor)
        apply_write_fusion(best_tx, best_executor, current_partition)
        remaining.pop(best_tx_idx)

    # Snapshot P_b after Step 1.
    partition_final = current_partition.copy()

    # --- Step 2: identify overloaded / underloaded nodes ---
    theta = math.ceil((len(batch) / num_executors) * (1 + epsilon))
    counts, overloaded, underloaded = compute_load_sets(schedule, num_executors, theta)

    # --- Step 3: backward rerouting with bounded delta ---
    delta = 1
    while overloaded and delta <= MAX_DELTA:
        moved_any = False

        for i in range(len(ordered_batch) - 1, -1, -1):
            src = schedule[i]
            if src not in overloaded:
                continue

            dst = find_new_route(
                ordered_batch[i],
                underloaded,
                partition_final,
                delta,
                ordered_batch[i + 1:],
                schedule[i + 1:]
            )
            if dst is None or dst == src:
                continue

            schedule[i] = dst
            counts[src] -= 1
            counts[dst] += 1
            moved_any = True

            if counts[src] <= theta:
                overloaded.discard(src)
            if counts[src] < theta:
                underloaded.add(src)

            if counts[dst] >= theta:
                underloaded.discard(dst)
            if counts[dst] > theta:
                overloaded.add(dst)

            # Update final partition only for write ownership that still points to src.
            for acc in ordered_batch[i].write_set:
                if partition_final.get(acc) == src:
                    partition_final[acc] = dst

            if not overloaded:
                break

        if not moved_any:
            delta += 1

    # Deterministic fallback rebalance if bounded Step 3 could not finish.
    if overloaded:
        for i in range(len(schedule) - 1, -1, -1):
            src = schedule[i]
            if counts[src] <= theta:
                continue

            candidates = [eid for eid in range(num_executors) if counts[eid] < theta]
            if not candidates:
                break

            dst = min(
                candidates,
                key=lambda eid: (counts[eid], remote_read_cost(ordered_batch[i], eid, partition_final), eid)
            )

            schedule[i] = dst
            counts[src] -= 1
            counts[dst] += 1

            if counts[src] <= theta and src in overloaded:
                overloaded.discard(src)

            if not overloaded:
                break

    return schedule, ordered_batch


if __name__ == "__main__":
    from evaluator_paper import Simulator, generate_batches_parallel
    from config import Configuration
    import numpy as np

    all_scores = []
    all_remote_reads = []
    all_durations = []

    print(f"Running simulations for ZIPF_ALPHAS: {Configuration.ZIPF_ALPHAS}\n")

    for zipf_alpha in Configuration.ZIPF_ALPHAS:
        print(f"Testing with ZIPF_ALPHA = {zipf_alpha}")
        sim = Simulator(
            Configuration.NUM_EXECUTORS,
            Configuration.NUM_ACCOUNTS,
            Configuration.PARTITION_STRATEGY
        )

        batches = generate_batches_parallel(
            Configuration.NUM_ACCOUNTS,
            Configuration.BATCH_SIZE,
            Configuration.NUM_EXECUTORS,
            Configuration.CROSS_SHARD_PROBS[0],
            Configuration.NUM_BATCHES,
            zipf_alpha
        )

        total_score, total_remote_reads, total_duration = sim.run_simulation(
            batches,
            scheduler,
            time_penalty_weight=Configuration.TIME_PENALTY_WEIGHT,
            epsilon=Configuration.EPSILON
        )

        all_scores.append(total_score)
        all_remote_reads.append(total_remote_reads)
        all_durations.append(total_duration)

        print(
            f"  Score: {total_score}, Remote Reads: {total_remote_reads}, "
            f"Duration: {total_duration:.2f}s\n"
        )

    avg_score = np.mean(all_scores)
    avg_remote_reads = np.mean(all_remote_reads)
    avg_duration = np.mean(all_durations)

    print("=" * 60)
    print("AGGREGATED RESULTS:")
    print(f"Average Score: {avg_score:.2f}")
    print(f"Average Remote Reads: {avg_remote_reads:.2f}")
    print(f"Average Duration: {avg_duration:.2f} seconds")
    print("=" * 60)