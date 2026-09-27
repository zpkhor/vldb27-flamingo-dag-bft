# Executor Simulator Artifact

Reproduction bundle for the scheduling experiments reported in the paper.

## Contents

| File | Purpose |
|---|---|
| `config.py` | Simulation parameters (executors, accounts, tx-size distributions, etc.) |
| `partitioner.py` | Range / hash partitioning strategies |
| `simulator.py` | Batch generation + simulator harness |
| `flamingo.py` | Flamingo scheduler implementation |
| `paper/paper.py` | Hermes-inspired reference scheduler |
| `paper/evaluator_paper.py` | Simulator variant used by the reference scheduler |
| `sweep.py` | Main sweep (tx size × cross-shard prob × overlap prob × zipf alpha) |
| `sweep_time_analysis.py` | Runtime sweep (batch size × tx size × num executors) |
| `sweep_results.csv` | Pre-computed output of `sweep.py` |
| `sweep_results_time_analysis.csv` | Pre-computed output of `sweep_time_analysis.py` |
| `sweep_analysis.ipynb` | Generates the routing-quality figures |
| `sweep_time_analysis.ipynb` | Generates the scheduling-time figures |

## Installation

```
pip install -r requirements.txt
```

All other imports are Python standard library.

## Quick reproduction (figures only)

The two notebooks read the shipped CSVs and regenerate every figure in the paper:

```
jupyter notebook sweep_analysis.ipynb
jupyter notebook sweep_time_analysis.ipynb
```

Or execute them headlessly:

```
jupyter nbconvert --to notebook --execute sweep_analysis.ipynb      --output sweep_analysis.ipynb
jupyter nbconvert --to notebook --execute sweep_time_analysis.ipynb --output sweep_time_analysis.ipynb
```

## Full reproduction (rerun sweeps from scratch)

```
python sweep.py                  # writes sweep_results.csv
python sweep_time_analysis.py    # writes sweep_results_time_analysis.csv
```

Then rerun the notebooks as above.

The sweeps use three seeds (0, 1000, 2000) and are deterministic; rerunning
should reproduce the shipped CSVs bit-for-bit.

## Configuration

Defaults live in `config.py` (`Configuration` class). The sweep scripts
override the sweep axes directly; keep `config.py` unchanged for bit-exact
reproduction.

## Attribution

The `paper/` directory contains a reference scheduler implementation inspired
by Hermes [CITATION]. The `simulator/` module provides a simplified
single-host harness suitable for measuring routing quality and scheduling
time on commodity hardware.
