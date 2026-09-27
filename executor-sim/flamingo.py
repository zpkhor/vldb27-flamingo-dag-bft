from typing import List, Dict
from config import Transaction
import time
import collections
import math


class Scheduler:
    """Stateful scheduler that tracks account access frequencies with sliding window."""

    WINDOW_SIZE = 10

    def __init__(self):
        """Initialize scheduler state"""
        self.access_frequency = collections.defaultdict(int)
        self.batch_history = []  # List of Counters tracking account access counts per batch
        self.initial_imbalance_factor = 0.5  # starting epsilon for load balancing

    def __call__(self, partition: Dict[int, int], batch: List[Transaction],
                 num_executors: int, executed_txs: List[float]) -> List[int]:
        """Greedy assignment with intra-batch ownership tracking and load balancing."""
        # Initialize schedule and tracking structures
        schedule = []  # Final executor assignment for each transaction
        local_updates = {}  # Tracks most recent executor assigned to each account within this batch

        # Calculate maximum transactions allowed per executor (with self.initial_imbalance_factor tolerance)
        max_allowed = math.ceil((len(batch) / num_executors) * (1 + self.initial_imbalance_factor))

        # Track current load (assigned transactions) for each executor
        current_loads = [0] * num_executors

        # Process each transaction in the batch
        for tx in batch:
            # Build voting scores: each executor holding accounts gets weighted votes
            votes = {}  # Maps executor_id -> total vote score

            for account in tx.accounts:
                # Determine current owner: use intra-batch assignment if available, otherwise partition
                owner = local_updates.get(account, partition[account])
                votes[owner] = votes.get(owner, 0) + 1

            # Select best executor for this transaction
            best_executor = -1

            if votes:
                # Fast path: if all accounts belong to a single executor
                if len(votes) == 1:
                    executor_id = next(iter(votes))
                    if current_loads[executor_id] < max_allowed:
                        best_executor = executor_id

                # Otherwise, find executor with highest vote score (and capacity)
                if best_executor == -1:
                    best_vote_score = -1

                    for executor_id, vote_score in votes.items():
                        # Only consider executors that haven't exceeded capacity
                        if current_loads[executor_id] < max_allowed:
                            # Select if: higher vote score, or same score but lower load
                            if vote_score > best_vote_score or (
                                vote_score == best_vote_score and current_loads[executor_id] < current_loads[best_executor]
                            ):
                                best_vote_score, best_executor = vote_score, executor_id

            # Fallback: if no valid executor found, assign to least loaded executor
            if best_executor == -1:
                best_executor = current_loads.index(min(current_loads))

            # Record assignment and update tracking
            schedule.append(best_executor)
            current_loads[best_executor] += 1

            # Update local ownership for all accounts in this transaction
            for account in tx.accounts:
                local_updates[account] = best_executor
        return schedule

    def update_history(self, batch: List[Transaction]):
        """
        Update sliding window with accounts from the current batch.
        """
        # Count all account accesses in current batch (including duplicates)
        current_batch_counts = collections.Counter()
        for tx in batch:
            for acc in tx.accounts:
                current_batch_counts[acc] += 1

        # Update total frequency with current batch
        for acc, count in current_batch_counts.items():
            self.access_frequency[acc] += count

        # Add to history
        self.batch_history.append(current_batch_counts)

        # Remove old batch if window size exceeded (sliding window decay)
        if len(self.batch_history) > self.WINDOW_SIZE:
            old_batch = self.batch_history.pop(0)
            for acc, count in old_batch.items():
                self.access_frequency[acc] -= count
                if self.access_frequency[acc] <= 0:
                    del self.access_frequency[acc]
