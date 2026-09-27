"""Simulator: batch generation and scheduling evaluation harness."""

import numpy as np
import time
import concurrent.futures
import traceback
import random
import collections
from typing import List, Tuple, Callable
from config import Configuration, Transaction, ZipfGenerator
from partitioner import get_partitioner


def choose_num_accs(rng: random.Random, dist=None) -> int:
    """Sample a transaction size from a distribution of (num_accounts, probability) pairs."""
    if dist is None:
        dist = Configuration.TX_SIZE_DIST
    p = rng.random()
    cumulative = 0.0
    for num_accs, prob in dist:
        cumulative += prob
        if p < cumulative:
            return num_accs
    return dist[-1][0]  # fallback for floating-point edge cases


def generate_batch(
    num_accounts: int,
    batch_size: int,
    num_executors: int,
    cross_shard_prob: float = 0.5,
    zipf_alpha: float = 1.0,
    seed: int = None,
    precomputed_zeta: np.ndarray = None,
    overlap_prob: float = 0.0,
    history_window: int = 16,
    reuse_fraction: float = 0.5,
) -> List[Transaction]:
    """Generates a batch of transactions with Zipfian skew and optional temporal overlap.

    Args:
        precomputed_zeta: Optional pre-computed zeta array for ZipfGenerator to avoid
                         redundant computation across multiple batches.
        overlap_prob: Probability that a multi-account tx reuses accounts from a recent tx.
        history_window: Number of recent transactions to remember for overlap injection.
        reuse_fraction: Fraction of secondary slots filled by reused accounts when overlapping.
    """
    if seed is not None:
        generator = ZipfGenerator(num_accounts, zipf_alpha, seed, precomputed_zeta)
        rng = random.Random(seed + 1)
    else:
        generator = ZipfGenerator(num_accounts, zipf_alpha, precomputed_zeta=precomputed_zeta)
        rng = random.Random()

    partitioner = get_partitioner(
        Configuration.PARTITION_STRATEGY,
        num_executors,
        num_accounts
    )

    def get_executor(acc_id):
        return partitioner.get_executor(acc_id)

    secondary_zipf_mix = Configuration.SECONDARY_ZIPF_MIX
    recent_accounts: collections.deque = collections.deque(maxlen=history_window)
    batch = []

    for i in range(batch_size):
        num_accs = choose_num_accs(rng)
        accounts = set()

        # Step 1: pick primary hot account using Zipf
        primary_acc = generator.next()
        accounts.add(primary_acc)

        # Step 2: decide whether this tx should be cross-shard
        is_cross_shard = rng.random() < cross_shard_prob

        if num_accs > 1:
            # Step 3: inject temporal overlap by reusing accounts from a recent tx
            if recent_accounts and rng.random() < overlap_prob:
                source = rng.choice(list(recent_accounts))
                n_reuse = max(1, int(reuse_fraction * (num_accs - 1)))
                candidates = [a for a in source if a not in accounts]
                rng.shuffle(candidates)
                for acc in candidates[:n_reuse]:
                    if len(accounts) < num_accs:
                        accounts.add(acc)

            # Step 4: fill remaining accounts — mixture of Zipf and uniform so hot
            # keys still show up, but not everything collapses to one shard
            while len(accounts) < num_accs:
                if rng.random() < secondary_zipf_mix:
                    candidate = generator.next()
                else:
                    candidate = rng.randint(0, num_accounts - 1)
                accounts.add(candidate)

            # Enforce cross-shard: ensure at least one secondary account lives on a
            # different executor than the primary
            if is_cross_shard:
                primary_exec = get_executor(primary_acc)
                secondaries = [a for a in accounts if a != primary_acc]
                if all(get_executor(a) == primary_exec for a in secondaries):
                    victim = secondaries[0]
                    accounts.discard(victim)
                    while True:
                        candidate = rng.randint(0, num_accounts - 1)
                        if candidate not in accounts and get_executor(candidate) != primary_exec:
                            accounts.add(candidate)
                            break


        acc_list = list(accounts)
        rng.shuffle(acc_list)

        num_writes = min(Configuration.NUM_WRITE_KEYS, len(acc_list))
        write_set = acc_list[:num_writes]
        read_set = acc_list

        recent_accounts.append(list(accounts))
        batch.append(Transaction(id=i, read_set=read_set, write_set=write_set))
    return batch


def generate_batches_parallel(
    num_accounts: int,
    batch_size: int,
    num_executors: int,
    cross_shard_prob: float,
    num_batches: int,
    zipf_alpha: float,
    overlap_prob: float = 0.0,
    history_window: int = 16,
    reuse_fraction: float = 0.5,
    seed_offset: int = 0,
) -> List[List[Transaction]]:
    """Generate all batches for a given zipf_alpha in parallel.

    Optimization: Pre-compute the zeta array once for all batches with the same
    (num_accounts, zipf_alpha) parameters to avoid O(num_accounts) computation
    for each batch.

    Args:
        seed_offset: Added to each batch's seed (seed_offset + i) so different
                     runs produce independent batches.
    """
    # Pre-compute zeta array once for this (num_accounts, alpha) pair
    tmp = np.power(np.arange(1, num_accounts + 1, dtype=float), -zipf_alpha)
    precomputed_zeta = np.cumsum(tmp) / np.sum(tmp)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                generate_batch, num_accounts, batch_size, num_executors,
                cross_shard_prob, zipf_alpha, seed=seed_offset + i,
                precomputed_zeta=precomputed_zeta,
                overlap_prob=overlap_prob,
                history_window=history_window,
                reuse_fraction=reuse_fraction,
            )
            for i in range(num_batches)
        ]
        return [f.result() for f in futures]


class Simulator:
    def __init__(self, num_executors: int, num_accounts: int, partition_strategy: str = None):
        self.num_executors = num_executors
        self.num_accounts = num_accounts
        self.partitioner = get_partitioner(
            partition_strategy or Configuration.PARTITION_STRATEGY,
            num_executors,
            num_accounts
        )
        self.initial_partition = self.partitioner.get_all_partitions()
        self.partition = self.initial_partition.copy()

    def reset_partition(self):
        self.partition = self.initial_partition.copy()

    def print_partition_distribution(self):
        """Print the number of accounts owned by each executor."""
        assert set(self.partition.values()) == set(range(self.num_executors))
        executor_counts = collections.Counter(self.partition.values())
        running_sum = 0
        print("Partition Distribution:")
        for executor in range(self.num_executors):
            count = executor_counts.get(executor, 0)
            running_sum += count
            print(f"  Executor {executor}: {count} accounts")
        assert running_sum == self.num_accounts

    def run_simulation(self,
                      batches: List[List[Transaction]],
                      scheduler_instance: Callable,
                      time_penalty_weight: float = 1000.0,
                      remote_reads_weight: float = 0.1) -> Tuple[float, int, float]:
        """
        Runs simulation over a sequence of batches.

        Returns (total_score, total_remote_reads, total_duration, total_completed) where:
        - total_score = total_completed_txs - remote_reads_weight * total_remote_reads
                        - time_penalty_weight * total_duration
        - Higher is better.

        executed_txs (cumulative assignments per executor) is passed to the scheduler
        every batch so the scheduler can adapt load balancing.
        """
        self.reset_partition()
        total_remote_reads = 0
        total_duration = 0.0
        executed_txs = [0.0] * self.num_executors

        for i, batch in enumerate(batches):
            current_partition = self.partition.copy()  # prevent scheduler from modifying partition
            start_time = time.time()
            try:
                schedule = scheduler_instance(current_partition, batch, self.num_executors, executed_txs)
            except Exception as e:
                print(f"Batch {i}: Scheduler raised exception: {e}")
                traceback.print_exc()
                return float('-inf'), 0, 0.0, 0.0

            end_time = time.time()
            total_duration += end_time - start_time

            valid, remote_reads = self.evaluate_batch_schedule(batch, schedule)

            if not valid:
                return float('-inf'), 0, 0.0, 0.0

            total_remote_reads += remote_reads

            counts = collections.Counter(schedule)
            for e in range(self.num_executors):
                executed_txs[e] += counts.get(e, 0)

            # Update scheduler history for next batch
            if hasattr(scheduler_instance, 'update_history'):
                scheduler_instance.update_history(batch)
            if hasattr(scheduler_instance, 'batch_history') and len(scheduler_instance.batch_history) > scheduler_instance.WINDOW_SIZE:
                return float('-inf'), 0, 0.0, 0.0  # too many batches in history

        total_completed = sum(executed_txs)
        final_score = total_completed - remote_reads_weight * total_remote_reads - time_penalty_weight * total_duration
        return final_score, total_remote_reads, total_duration, total_completed

    def evaluate_batch_schedule(self,
                batch: List[Transaction],
                schedule: List[int]) -> Tuple[bool, int]:
        """
        Evaluates a schedule for a single batch.
        Returns (valid, remote_reads).
        """
        if not isinstance(schedule, list):
            print(f"Error: Schedule is not a list, got {type(schedule)}")
            return False, 0

        if len(batch) != len(schedule):
            print(f"Error: Schedule length {len(schedule)} != Batch length {len(batch)}")
            return False, 0

        total_remote_reads = 0

        for tx, executor_id in zip(batch, schedule):
            assert isinstance(executor_id, int), f"Executor {executor_id} is not an integer, but {type(executor_id)}"
            assert executor_id < self.num_executors, f"Executor {executor_id} is not less than {self.num_executors}"

            # 1. Count remote reads using only read_set
            for acc in tx.read_set:
                owner = self.partition[acc]
                if owner != executor_id:
                    total_remote_reads += 1

            # 2. Simplified ownership update: writes become local / fused
            for acc in tx.write_set:
                assert isinstance(acc, int), f"Account {acc} is not an integer, but {type(acc)}"
                self.partition[acc] = executor_id

        return True, total_remote_reads


def run_with_timeout(func, args=(), kwargs={}, timeout_seconds=10):
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func, *args, **kwargs)
        try:
            return future.result(timeout=timeout_seconds)
        except concurrent.futures.TimeoutError:
            raise TimeoutError(f"Function timed out after {timeout_seconds} seconds")
