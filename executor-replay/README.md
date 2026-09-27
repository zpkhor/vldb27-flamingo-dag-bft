# Narwhal + Executor Tier

This repository contains the code accompanying our VLDB submission on executor-tier scheduling
for DAG-based BFT systems. It extends [Narwhal](https://arxiv.org/abs/2105.11827) with a third
tier of *executor* processes that receive committed batches from workers, partition
application state across shards, and can be scaled independently of the consensus tier.

Author-identifying strings (usernames, emails, institutions, CloudLab project/experiment IDs)
have been replaced with generic placeholders such as `anonuser`, `control-host`, and `AnonProject`.

## Repository layout

| Path                | Purpose                                                              |
| ------------------- | -------------------------------------------------------------------- |
| `primary/`          | Primary process (consensus tier)                                     |
| `worker/`           | Worker process (batching + dissemination tier)                       |
| `node/src/executor` | Executor process (data-fusion `BatchExecutor` + `DistributedTxExecutor`) |
| `consensus/`        | Tusk consensus implementation                                        |
| `network/`          | TCP transport layer                                                  |
| `config/`           | Committee / parameter types shared by all binaries                   |
| `crypto/` `store/`  | Ed25519 crypto primitives and RocksDB storage wrapper                |
| `benchmark/`        | Python driver scripts (Fabric tasks) for local, Docker, and CloudLab runs |
| `Experiment/`       | Recorded CSV result sets from our paper's experiments                |

## Dependencies

* Rust stable (any recent toolchain supporting edition 2021)
* Clang (required by the RocksDB build)
* Python 3.9 with the packages in `benchmark/requirements.txt`
* `tmux` for local runs
* Docker + Docker Compose for the `fab docker` driver
* `sudo`-less SSH access to a set of CloudLab nodes (or equivalent Linux hosts) for the
  `fab cloudlab*` drivers

Install the Python dependencies once:

```bash
cd benchmark
python3 -m venv ~/venvs/narwhal
source ~/venvs/narwhal/bin/activate
pip install -r requirements.txt
```

## Build

```bash
cargo build --release --features benchmark
```

The `benchmark` feature enables the structured log lines that the Python parsers consume.
The driver scripts below also invoke this build automatically.

## Running benchmarks

All experiments are driven by [Fabric](https://www.fabfile.org/) tasks defined in
`benchmark/fabfile.py`. From `benchmark/`, list available tasks with:

```bash
fab --list
```

### 1. Replay benchmark (single machine)

`fab replay` runs a primary + N workers + N executors on the local machine, replaying a
pre-recorded batch arrival trace (`benchmark/record_rate*.csv`). The executors process
SmallBank transactions against a sharded account state.

```bash
REPLAY_CSV=record_rate100k.csv \
NUM_WORKERS=4 NUM_EXECUTORS=4 \
DURATION=80 \
NO_SEND_PAYMENT=1 NEW_SCHEDULER=1 \
fab replay
```

Relevant environment variables (defaults in parentheses):

| Variable                | Meaning                                                        |
| ----------------------- | -------------------------------------------------------------- |
| `REPLAY_CSV`            | Trace file under `benchmark/` (`record_rate25k.csv`)           |
| `REPLAY_TX_SIZE`        | Transaction size in bytes (`512`)                              |
| `WORKERS`               | Number of worker processes (`1`)                               |
| `NUM_EXECUTORS`         | Number of executor processes (`1`)                             |
| `NUM_ACCOUNTS`          | Total SmallBank accounts (`1_000_000`)                         |
| `DURATION`              | Benchmark wall-clock duration in seconds (`120`)               |
| `EXECUTOR_SKEW_WEIGHTS` | Comma-separated account-range weights, e.g. `3,1,1,1`          |
| `DISTRIBUTED_TX_RATE`   | Fraction of cross-executor SendPayment transactions, `0.0..1.0`|
| `NO_SEND_PAYMENT`       | Disable cross-executor SendPayment (`0`)                       |
| `IN_MEMORY_STORE`       | Use the in-memory store to isolate the executor path (`1`)     |
| `WRITEBACK_EXECUTOR`    | Use the writeback executor instead of data fusion (`0`)        |

### 2. Replay benchmark (CloudLab / distributed)

`fab cloudlab-replay` places the primary, each worker, and each executor on its own node
from a CloudLab RSpec manifest. Worker → executor and executor ↔ executor links are shaped
with `tc` to the configured bandwidth cap.

```bash
REPLAY_CSV=record_rate100k.csv \
NUM_WORKERS=4 NUM_EXECUTORS=4 \
EXECUTOR_BW_MBPS=10000 DURATION=80 \
NO_SEND_PAYMENT=1 NEW_SCHEDULER=1 \
EXECUTOR_SKEW_WEIGHTS=3,1,1,1 \
fab cloudlab-replay --username <your-ssh-user> --manifest /path/to/manifest.xml
```

The manifest must describe at least `1 + NUM_WORKERS + NUM_EXECUTORS` nodes. An example
manifest (downloaded from the CloudLab portal and anonymized) is provided at
`benchmark/manifest.xml`. Replace `--username` with the SSH user configured on your own
CloudLab slice.

Additional variables (see `benchmark/fabfile.py::cloudlab_replay` for the full list):

| Variable                | Meaning                                                        |
| ----------------------- | -------------------------------------------------------------- |
| `EXECUTOR_BW_MBPS`      | LAN bandwidth cap on executor-bound traffic (Mbit/s)           |
| `NODE_OFFSET`           | Skip the first N nodes of the manifest (useful when sharing a slice) |
| `NEW_SCHEDULER`         | Enable the load-aware scheduler evaluated in the paper         |

### 3. Consensus-only benchmarks

For the consensus-tier sensitivity experiments (bandwidth asymmetry, validator-rate imbalance,
routing modes) we use the non-replay drivers:

* `fab docker` — runs a full 4+ validator testbed in Docker with `tc` shaping per container.
* `fab cloudlab` — runs the same experiment on CloudLab machines.

See `benchmark/saturation_sweep.sh` and `benchmark/scalability_baseline_sweep.sh` for the
parameter sweeps used in the paper's saturation and scalability plots.

## Reproducing the paper figures

The `Experiment/` directory holds the result CSVs used for the executor-tier figures:

| File                    | Scenario                                                     |
| ----------------------- | ------------------------------------------------------------ |
| `Experiment/balanced.csv` | Uniform account-access distribution                        |
| `Experiment/50%.csv`      | One shard receives 50% of the workload (`EXECUTOR_SKEW_WEIGHTS` column) |
| `Experiment/90%.csv`      | One shard receives 90% of the workload                     |

Each row records the rate, executor/worker count, skew weights, and the committed /
end-to-end TPS that our run produced, along with the exact `fab cloudlab-replay` command
line. Re-running those commands on a suitably sized CloudLab slice reproduces the measured
point.

`benchmark/run_benchmarks.py` is a helper that iterates over the three CSVs, SSHes into a
control host, and fills the `Committed TPS` / `E2E TPS` columns in place. Edit the
`SSH_HOST`, `REMOTE_PREFIX`, and `CSV_FILES` constants before use.

## License

Apache 2.0 — see [LICENSE](LICENSE).
