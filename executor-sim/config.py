import random
import numpy as np
from dataclasses import dataclass
from typing import List


@dataclass
class Transaction:
    """Represents a transaction with explicit read/write sets."""
    id: int
    read_set: List[int]
    write_set: List[int]

    @property
    def accounts(self) -> List[int]:
        # Backward-compatible union view for older code paths.
        return list(dict.fromkeys(self.read_set + self.write_set))


class ZipfGenerator:
    """
    Generates random samples following a Zipfian distribution.
    """

    def __init__(self, n: int, alpha: float, seed: int = 42, precomputed_zeta: np.ndarray = None):
        self.n = n
        self.alpha = alpha
        self.seed = seed
        self.rng = random.Random(seed)

        if precomputed_zeta is not None:
            self.zeta = precomputed_zeta
        else:
            tmp = np.power(np.arange(1, n + 1, dtype=float), -alpha)
            self.zeta = np.cumsum(tmp) / np.sum(tmp)

    def next(self) -> int:
        u = self.rng.random()
        return int(np.searchsorted(self.zeta, u))


class Configuration:
    NUM_EXECUTORS = 8
    NUM_ACCOUNTS = 1_000_000
    BATCH_SIZE = 1000
    NUM_BATCHES = 100
    ZIPF_ALPHAS = [0.8, 1.2]
    CROSS_SHARD_PROBS = [0.25, 0.5, 0.75]
    EPSILON = 0.5
    TIME_PENALTY_WEIGHT = 5000.0
    REMOTE_READS_WEIGHT = 1
    PARTITION_STRATEGY = "range"

    OVERLAP_PROBS = [0.0, 0.3, 0.6, 0.9]
    HISTORY_WINDOW = 16
    REUSE_FRACTION = 0.5

    SECONDARY_ZIPF_MIX = 0.7

    TX_SIZE_LIGHT  = [(2, 0.8), (4, 0.2)]
    TX_SIZE_MEDIUM = [(2, 0.5), (4, 0.3), (8, 0.2)]
    TX_SIZE_HEAVY  = [(4, 0.5), (8, 0.3), (16, 0.2)]
    TX_SIZE_DIST = TX_SIZE_MEDIUM

    # Hermes-style simplified txn shape for the simulator:
    # one write key, all selected keys are read.
    NUM_WRITE_KEYS = 1