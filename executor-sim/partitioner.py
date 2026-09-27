"""Partitioning strategies for distributing accounts across executors.

This module provides a centralized implementation of partitioning logic,
eliminating duplication and enabling configuration-driven strategy selection.
"""

from abc import ABC, abstractmethod
from typing import Dict


class Partitioner(ABC):
    """Abstract base class for partitioning strategies."""

    def __init__(self, num_executors: int, num_accounts: int):
        """Initialize partitioner.

        Args:
            num_executors: Number of executors to partition across
            num_accounts: Total number of accounts to partition
        """
        self.num_executors = num_executors
        self.num_accounts = num_accounts

    @abstractmethod
    def get_executor(self, acc_id: int) -> int:
        """Get the executor ID for a given account ID.

        Args:
            acc_id: Account ID to partition

        Returns:
            Executor ID (0 to num_executors-1)
        """
        pass

    @abstractmethod
    def get_all_partitions(self) -> Dict[int, int]:
        """Get partition mapping for all accounts.

        Returns:
            Dictionary mapping account_id -> executor_id
        """
        pass


class HashPartitioner(Partitioner):
    """Hash-based partitioning strategy.

    Uses hash(acc_id) % num_executors to distribute accounts.
    Provides good distribution but accounts are scattered across executors.
    """

    def get_executor(self, acc_id: int) -> int:
        """Get executor using hash-based partitioning."""
        return hash(acc_id) % self.num_executors

    def get_all_partitions(self) -> Dict[int, int]:
        """Generate hash-based partition mapping for all accounts."""
        return {
            acc_id: hash(acc_id) % self.num_executors
            for acc_id in range(self.num_accounts)
        }


class RangePartitioner(Partitioner):
    """Range-based partitioning strategy.

    Divides account ID space into contiguous ranges, one per executor.
    Provides locality but may have imbalanced distribution if num_accounts
    is not evenly divisible by num_executors.
    """

    def get_executor(self, acc_id: int) -> int:
        """Get executor using range-based partitioning."""
        accounts_per_executor = self.num_accounts // self.num_executors
        return min(acc_id // accounts_per_executor, self.num_executors - 1)

    def get_all_partitions(self) -> Dict[int, int]:
        """Generate range-based partition mapping for all accounts."""
        accounts_per_executor = self.num_accounts // self.num_executors
        return {
            acc_id: min(acc_id // accounts_per_executor, self.num_executors - 1)
            for acc_id in range(self.num_accounts)
        }


def get_partitioner(
    strategy: str, num_executors: int, num_accounts: int
) -> Partitioner:
    """Factory function to create appropriate partitioner.

    Args:
        strategy: Partitioning strategy name ("hash" or "range")
        num_executors: Number of executors to partition across
        num_accounts: Total number of accounts to partition

    Returns:
        Partitioner instance for the specified strategy

    Raises:
        ValueError: If strategy is not recognized
    """
    strategies = {
        "hash": HashPartitioner,
        "range": RangePartitioner,
    }

    if strategy not in strategies:
        raise ValueError(
            f"Unknown partition strategy: {strategy}. "
            f"Available strategies: {', '.join(strategies.keys())}"
        )

    return strategies[strategy](num_executors, num_accounts)
