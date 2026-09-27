"""
Evaluator for the simulator example (paper implementation variant).
"""

import math
import time
import traceback
import collections
from typing import List, Tuple, Callable
from config import Configuration, Transaction
from partitioner import get_partitioner
from simulator import generate_batch, generate_batches_parallel


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
                      epsilon: float = 0.1) -> Tuple[float, int, float]:
        """
        Runs simulation over a sequence of batches.
        Returns (total_score, total_remote_reads, total_duration)
        where total_score is (theoretical_max_remote_reads - accumulated_cost).
        Higher is better.
        """
        self.reset_partition()
        total_cost = 0.0
        total_remote_reads = 0
        total_duration = 0.0

        # Calculate theoretical max remote reads (if every access was remote)
        theoretical_max_remote_reads = sum(len(tx.accounts) for batch in batches for tx in batch)

        for i, batch in enumerate(batches):
            current_partition = self.partition.copy() # prevent scheduler from modifying partition
            start_time = time.time()
            try:
                schedule, ordered_batch = scheduler_instance(current_partition, batch, self.num_executors, epsilon)
            except Exception as e:
                print(f"Batch {i}: Scheduler raised exception: {e}")
                traceback.print_exc()
                return float('-inf'), 0, 0.0

            end_time = time.time()

            duration = end_time - start_time
            total_duration += duration

            # This 'score' is actually the cost for this batch
            batch_cost, remote_reads = self.evaluate_batch_schedule(
                ordered_batch, schedule, batch, duration, time_penalty_weight, epsilon
            )

            total_cost += batch_cost
            total_remote_reads += remote_reads

        final_score = theoretical_max_remote_reads - total_cost
        self.print_partition_distribution()
        return final_score, total_remote_reads, total_duration

    def evaluate_batch_schedule(
        self,
        reordered_batch: List[Transaction],
        schedule: List[int],
        original_batch: List[Transaction],
        duration_seconds: float,
        time_penalty_weight: float = 1000.0,
        epsilon: float = 0.1
    ) -> Tuple[float, int]:
        """
        Evaluates a schedule for a single batch.
        Returns (cost, remote_reads).
        Returns (float('inf'), ...) if load balancing constraint is violated.
        """
        if not isinstance(schedule, list):
            print(f"Error: Schedule is not a list, got {type(schedule)}")
            return float('inf'), 0

        if len(reordered_batch) != len(schedule):
            print(f"Error: Schedule length {len(schedule)} != Batch length {len(reordered_batch)}")
            return float('inf'), 0

        if len(reordered_batch) != len(original_batch):
            print(f"Error: Batch length {len(reordered_batch)} != Original batch length {len(original_batch)}")
            return float('inf'), 0

        counts = collections.Counter(schedule)
        avg_load = len(reordered_batch) / self.num_executors
        max_allowed = math.ceil(avg_load * (1 + epsilon))

        total_remote_reads = 0

        for tx, executor_id in zip(reordered_batch, schedule):
            assert isinstance(executor_id, int), f"Executor {executor_id} is not an integer, but {type(executor_id)}"
            assert executor_id < self.num_executors, f"Executor {executor_id} is not less than {self.num_executors}"

            # Count only remote reads
            for acc in tx.read_set:
                owner = self.partition[acc]
                if owner != executor_id:
                    total_remote_reads += 1

            # Ownership update only on writes
            for acc in tx.write_set:
                assert isinstance(acc, int), f"Account {acc} is not an integer, but {type(acc)}"
                self.partition[acc] = executor_id

        cost = total_remote_reads + (duration_seconds * time_penalty_weight)

        max_load = max(counts.values()) if counts else 0
        if max_load > max_allowed:
            cost = float('inf')

        return cost, total_remote_reads
