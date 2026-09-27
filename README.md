# Flamingo: DAG-BFT with Validator- and Executor-Level Load Balancing

Artifact accompanying the VLDB 2027 submission. It extends a
Narwhal/Tusk-based DAG-BFT mempool with load balancing at two tiers:

- **Ordering tier** — clients are migrated across validators via `MigrationNotice`
  messages emitted by the worker's `Synchronizer`, adapting to skewed submission and
  heterogeneous validator capacity.
- **Execution tier** — committed transactions are redistributed across executor workers by
  a deterministic, order-preserving scheduler that balances load while minimizing
  cross-worker data movement.

## Artifact layout

The artifact is three components, each independently runnable:

| Component | Tier | What it produces |
|---|---|---|
| this repository (root) | ordering (+ end-to-end) | Figures 1, 5, 6, 7, 11 |
| `executor-replay/` | execution, real deployment | Figure 8 |
| `executor-sim/` | execution, single-host simulator | Figures 9, 10 |

`executor-replay/` is a self-contained fork of the same Narwhal codebase with a third tier
of *executor* processes, driven by recorded batch-arrival traces. `executor-sim/` is a pure
Python simulator for measuring scheduling quality and runtime without a cluster.

## Figure map

Which committed file corresponds to which figure in the paper:

| Paper figure | File | How to reproduce |
|---|---|---|
| Fig. 1 — load imbalance / validator heterogeneity motivation | `motivation_latency_tps.png` | [Figure 1](#figure-1--motivation) |
| Figs. 2–4 — schematics (DAG construction, Flamingo overview, local DAG view) | — | hand-drawn, not generated here |
| Fig. 5a — impact of submission rate | `benchmark/config_a.pdf` | [Figure 5](#figure-5--validator-level-load-balancing) |
| Fig. 5b — impact of load skewness | `benchmark/config_b.pdf` | ditto |
| Fig. 5c — impact of bandwidth | `benchmark/config_c.pdf` | ditto |
| Fig. 5d — impact of scalability | `benchmark/config_d.pdf` | ditto |
| Fig. 6 — adaptivity under a shifting hotspot | `benchmark/hotspot_shift_repro.pdf` | [Figure 6](#figure-6--shifting-hotspot) |
| Fig. 7a — robustness to misreporting, balanced | `benchmark/figs_ablation_repro/ablation_bal.pdf` | [Figure 7](#figure-7--robustness-to-misreporting) |
| Fig. 7b — robustness to misreporting, skewed | `benchmark/figs_ablation_repro/ablation_imb.pdf` | ditto |
| Fig. 8 — executor throughput under 50% skew | `executor-replay/Experiment/50%.csv` | [Part 2](#part-2--executor-tier-replay-executor-replay) |
| Fig. 9 — scheduler runtime vs. batch size | `executor-sim/sweep_time_batchsize.{pdf,png}` | [Part 3](#part-3--executor-scheduler-simulator-executor-sim) |
| Fig. 10 — remote reads vs. overlap probability | `executor-sim/sweep_routing_quality_overlap.{pdf,png}` | ditto |
| Fig. 11 — end-to-end under validator/executor/combined skew | `benchmark/fig4_plots/fig4_combined.pdf` | [Figure 11](#figure-11--end-to-end-evaluation) |

Note on script names: several sweep scripts keep historical `fig_3_*` / `fig_4_*` names from
an earlier draft's numbering. `fig_3_tps_timeline_*.sh` produces **Figure 5**, and
`fig_4_e2e_tps_timeline.sh` produces **Figure 11**.

## Prerequisites

- Rust toolchain (1.70+) and `clang` (required by rocksdb).
- Python 3.9 and `tmux`.
- One of:
  - **Docker** (for `MODE=docker`, the default), with `docker-compose`.
  - **CloudLab** allocation plus a `manifest.xml` (for `MODE=cloudlab`).

Setup:

```bash
# Rust build
cargo build --release --features benchmark

# Python deps (conda env 'narwhal39' used during development)
pip install -r benchmark/requirements.txt
pip install numpy          # required by the plotting scripts, not in requirements.txt
```

The `benchmark` feature enables the structured log lines that the Python parsers consume.
The `fab` drivers invoke this build themselves, so `cargo` must be on `PATH` alongside `fab`.

---

# Part 1 — Validator tier (this repository)

All experiments here drive `fab cloudlab` (or `fab docker`) through `benchmark/fabfile.py`.
Results land in `benchmark/exp/results/`, in run dirs named `<label>_r<rate>_run_<n>`.

`benchmark/exp/README.md` is the deep reference for this tier: exact provenance of every
committed figure, which results dirs the plots read, and the known gotchas. This section is
the command summary.

## Figure 5 — validator-level load balancing

Four panels, each varying one axis. `imbN` = the first `ceil(n/4)` validators receive `N%`
of the total client load; `bw_f` / `bw_f1` = `f` / `f+1` validators given 200 Mbps instead
of 600 Mbps.

| panel | varies | held fixed | cells | duration |
|---|---|---|---|---|
| (a) | offered load | n=4, imb90, balanced BW | 110k, 80k, 50k, 20k tx/s | 1650s |
| (b) | imbalance % | n=4, 110k, balanced BW | imb99, imb90, imb60 | 1650s |
| (c) | bandwidth scenario | n=4, imb90 | balanced @110k, `bw_f` @110k, `bw_f1` @65k | 2000s |
| (d) | committee size | imb60, balanced BW, 90k | n=21, 16, 10, 4 | 1650s |

Panels (a) and (b) share one `n4_v_rate_imb90_r110000` run; (c) has its own cells at a
longer duration, because its backlog drains far later than the other panels'.

Load-balancing side — panels (a) and (b):

```bash
MODE=cloudlab bash benchmark/exp/fig_3_tps_timeline_lb.sh
```

Panel (d) has its own script, because n=21 and n=16 need the whole 23-machine manifest and
cannot share the cluster:

```bash
MODE=cloudlab bash benchmark/exp/fig_3_tps_timeline_nodes_lb.sh
```

Baseline side (no load balancing; `BASELINE=1` is set by the script), giving the dashed
reference lines:

```bash
MODE=cloudlab bash benchmark/exp/fig_3_tps_timeline_baseline.sh   # panels (a), (b)
DURATION=1450 bash benchmark/exp/config_c_baseline_repro.sh       # panel (c)
```

Baselines are paired to LB runs by identical run-subdir name, so a baseline cell must use
the same label *and* rate as its LB counterpart, or the panel silently loses that dashed
line. Panel (c)'s LB side has no committed generator script — see `benchmark/exp/README.md`
for its three cells' parameters.

Then plot:

```bash
python benchmark/exp/plot_paper_configs.py --configs a b c d --pdf --output-dir benchmark
```

The results dirs are **hardcoded** near the top of `plot_paper_configs.py`; a fresh sweep is
not picked up until you edit those constants or pass `--lb-dir <dir>`.

### Common environment variables

| Variable | Default | Description |
|---|---|---|
| `MODE` | `docker` | `docker` or `cloudlab`. |
| `DURATION` | `1650` (LB), `900` (baseline) | Run length in seconds. |
| `WARMUP` | `240` | Warm-up seconds excluded from metrics. |
| `RETRIES` | `1` | Runs per (label, rate) pair. |
| `CPUS_PER_VALIDATOR` | `8` | Pinned CPUs per validator (docker). |
| `LATENCY` | `100ms` | Inter-validator link latency (docker). |
| `PRIMARY_BW` | `300mbit` | Primary-network bandwidth (docker). |
| `MANIFEST` | `manifest.xml` | CloudLab manifest path (cloudlab). |
| `LOCAL_ORCH` | `0` | Orchestrate from local host if `1` (cloudlab). |
| `NODE_OFFSET` | `0` | Skip the first N machines of the manifest (cloudlab). |

Existing `RUN_DIR`s are skipped, so a sweep can be resumed by re-running the same script.

## Figure 6 — shifting hotspot

The hot region moves from validator 0 to validator 1 at `2.5 × WARMUP` seconds (600s at the
default `WARMUP=240`) and the algorithm has to re-converge, giving the figure its two dips.

The client-side shift lives in a patch rather than on the branch, because the swap is
hardcoded as "region 0 ↔ region 1" for this figure only. One command applies the patch,
runs, unapplies it (on a `trap`, so Ctrl-C still restores the tree), plots, and scores:

```bash
bash benchmark/exp/run_hotspot_shift.sh
```

Defaults are n=4, imb90, 110k tx/s, `DURATION=1400`, `WARMUP=240`; `RETRIES=N` runs N
attempts. Output goes to `benchmark/hotspot_shift_repro.pdf`.

Run-to-run spread is real here — a run can match on throughput and migrations while its
latency tail makes the plotted plateaus wobble. `benchmark/exp/check_hotspot_shift.py
<run_dir> ...` scores the five windows the figure is about, so attempts can be compared on
identical windows instead of by eye.

## Figure 7 — robustness to misreporting

A 2×4 grid of {balanced, imbalanced} load × {`inflated_queue_delay`, `deflated_qd`,
`deflated_capacity`, `inflated_cap`}, against a non-malicious baseline. The malicious node
is the last validator (v3 at n=4), so its case annotation is logged in `primary-3.log` as
`ablation_case`. Defaults are n=4, 110k tx/s, `DURATION=900`, `WARMUP=240`.

The malicious grid needs the two `[ABLATION]` blocks in `consensus/src/lib.rs` to be
uncommented. While they are commented out, `MALICIOUS_MODE` still reaches the primary but
changes nothing, so all four cases silently return honest runs.

```bash
BASELINE=1 RETRIES=5 MODE=cloudlab bash benchmark/exp/fig_ablation.sh   # 2 cells x 5 repeats
BASELINE=0 RETRIES=1 MODE=cloudlab bash benchmark/exp/fig_ablation.sh   # 2x4 malicious grid
```

At the default 900s duration the 18 runs take roughly 5 hours. Then plot both dirs
together, baseline first:

```bash
python benchmark/exp/plot_paper_ablation.py <BASELINE_DIR> <GRID_DIR> \
    --output-dir benchmark/figs_ablation_repro --pdf
```

`ablation_bal.pdf` is Figure 7a, `ablation_imb.pdf` is Figure 7b. Use `MODE=docker` for a
local run; `CASES` takes comma-separated label substrings to run a subset (e.g.
`CASES=imb_inflqd`).

## Figure 11 — end-to-end evaluation

Compares four load-balancing configurations under validator-level, executor-level, and
combined 90% skew. Rows are the scenarios, columns are [throughput, latency]; dashed lines
are committed (consensus) performance, solid lines are end-to-end. CloudLab only, n=4 with
3 executors, 900s per run.

`LB_MODE` selects which tiers are enabled, and each mode writes its own timestamped dir:

| `LB_MODE` | validator LB (`BASELINE`) | executor LB (`NEW_SCHEDULER`) | results dir tag |
|---|---|---|---|
| `both` | yes (0) | yes (1) | `lb` |
| `validator` | yes (0) | no (0) | `vlb` |
| `executor` | no (1) | yes (1) | `elb` |
| `none` | no (1) | no (0) | `baseline` |

```bash
LB_MODE=both,validator,executor,none bash benchmark/exp/fig_4_e2e_tps_timeline.sh
```

`NO_SEND_PAYMENT=1` is the default and must stay on. SendPayment picks its destination
account uniformly over the global range, and under data fusion each cross-executor
transaction reassigns ownership, so the account→executor map fully decorrelates within
~50s — that erases `E_SKEW_WEIGHTS` before the measurement window starts.

To resume, preset `RESULTS_DIR` to an existing dir and pass that mode alone. `RESULTS_DIR`
pins one dir, so it rejects a multi-mode `LB_MODE`:

```bash
RESULTS_DIR=<existing_dir> LB_MODE=executor bash benchmark/exp/fig_4_e2e_tps_timeline.sh
```

Then combine the four dirs into the figure:

```bash
python benchmark/exp/plot_fig4_e2e_commit.py \
    --baseline0-dir <lb_dir> --baseline1-dir <baseline_dir> \
    --vlb-dir <vlb_dir> --elb-dir <elb_dir> \
    --output-dir benchmark/fig4_plots --combined --pdf
```

Each `--*-dir` defaults to the newest `results/e2e_tps_timeline_<tag>_cloud_*`, so with a
clean results dir the flags can be omitted. Drop `--combined` to emit one figure per run
label instead.

## Figure 1 — motivation

The motivating measurement (impact of load imbalance and of a single bandwidth-limited
validator) is produced by:

```bash
MODE=cloudlab bash benchmark/exp/fig1_motivation_run.sh
python benchmark/exp/parse_e2e_latency_tps.py <results_dir>            # -> <results_dir>/latency_tps.csv
python benchmark/exp/plot_fig1_e2e_latency_tps.py <results_dir>/latency_tps.csv
```

The parser takes the results dir; the plotter takes the CSV the parser writes, not the dir.

`benchmark/exp/fig_1.2_motivation_lb_run.sh` is the load-balanced companion sweep.

## Checking run health

A stalled run still shows a plausible plateau, and a truncated migration curve reads as
convergence. Before trusting any run:

- `output.log` must contain a `SUMMARY:` block. A cell that dies in setup has no summary, so
  it also has no `WARNINGS:` — absence of warnings is not health.
- `python benchmark/exp/commit_span_check.py <RESULTS_DIR>` catches truncation that the
  0.9-ratio warning misses.
- Mean/E2E TPS averages over the migration dip. Judge a run by the tail — the recovered
  throughput after migrations settle — and by whether migrations converge at all.

---

# Part 2 — Executor-tier replay (`executor-replay/`)

`executor-replay/` holds the executor-tier scheduling experiments (**Figure 8**). It extends
[Narwhal](https://arxiv.org/abs/2105.11827) with a third tier of *executor* processes that
receive committed batches from workers, partition application state across shards, and can
be scaled independently of the consensus tier.

All paths in this part are relative to `executor-replay/` unless stated otherwise.

## Repository layout

| Path | Purpose |
|---|---|
| `primary/` | Primary process (consensus tier) |
| `worker/` | Worker process (batching + dissemination tier) |
| `worker/src/executor.rs` | Executor process entry point |
| `worker/src/batch_executor.rs` | Data-fusion executor |
| `worker/src/writeback_batch_executor.rs` | Writeback (distributed-tx) executor |
| `consensus/` | Tusk consensus implementation |
| `network/` | TCP transport layer |
| `config/` | Committee / parameter types shared by all binaries |
| `crypto/` `store/` | Ed25519 crypto primitives and RocksDB storage wrapper |
| `benchmark/` | Python driver scripts (Fabric tasks) for local, Docker, and CloudLab runs |
| `Experiment/` | Recorded CSV result sets from the paper's experiments |

## Dependencies

- Rust stable (any recent toolchain supporting edition 2021)
- Clang (required by the RocksDB build)
- Python 3.9 with the packages in `executor-replay/benchmark/requirements.txt`
- `tmux` for local runs
- Docker + Docker Compose for the `fab docker` driver
- SSH access to a set of CloudLab nodes (or equivalent Linux hosts) for the `fab cloudlab*`
  drivers

```bash
cd executor-replay/benchmark
python3 -m venv ~/venvs/narwhal
source ~/venvs/narwhal/bin/activate
pip install -r requirements.txt
```

## Build

```bash
cd executor-replay
cargo build --release --features benchmark
```

The driver scripts below also invoke this build automatically.

## Running benchmarks

Experiments are driven by [Fabric](https://www.fabfile.org/) tasks in
`executor-replay/benchmark/fabfile.py`. From `executor-replay/benchmark/`, list tasks with
`fab --list`.

> **Env-var naming differs between the two replay tasks.** `fab replay` reads `WORKERS`;
> `fab cloudlab-replay` reads `NUM_WORKERS`. Passing the wrong one is silently ignored and
> you get the default of 1 worker. `fab replay` also ignores `NEW_SCHEDULER` and
> `NO_SEND_PAYMENT` entirely — those are only read by `fab cloudlab-replay`.

### 1. Replay benchmark (single machine)

`fab replay` runs a primary + N workers + N executors on the local machine, replaying a
pre-recorded batch-arrival trace (`benchmark/record_rate*.csv`). The executors process
SmallBank transactions against a sharded account state.

```bash
cd executor-replay/benchmark
REPLAY_CSV=record_rate100k.csv \
WORKERS=4 NUM_EXECUTORS=4 \
DURATION=80 \
fab replay
```

`REPLAY_CSV` is resolved relative to the current directory. Its default
(`benchmark/record_rate25k.csv`) assumes a working directory of `executor-replay/`, so when
running from `executor-replay/benchmark/` you must pass it explicitly — otherwise the task
hard-fails on its existence assert.

| Variable | Default | Meaning |
|---|---|---|
| `REPLAY_CSV` | `benchmark/record_rate25k.csv` | Trace file, path relative to CWD |
| `REPLAY_TX_SIZE` | `512` | Transaction size in bytes |
| `WORKERS` | `1` | Number of worker processes |
| `NUM_EXECUTORS` | `1` | Number of executor processes |
| `NUM_ACCOUNTS` | `1000000` | Total SmallBank accounts |
| `DURATION` | `120` | Benchmark wall-clock duration in seconds |
| `EXECUTOR_SKEW_WEIGHTS` | `''` | Comma-separated account-range weights, e.g. `3,1,1,1` |
| `DISTRIBUTED_TX_RATE` | `0.0` | Fraction of cross-executor SendPayment transactions, `0.0..1.0` |
| `IN_MEMORY_STORE` | `1` | Use the in-memory store to isolate the executor path |
| `WRITEBACK_EXECUTOR` | `false` | Use the writeback executor instead of data fusion |

### 2. Replay benchmark (CloudLab / distributed)

`fab cloudlab-replay` places the primary, each worker, and each executor on its own node
from a CloudLab RSpec manifest. Worker → executor and executor ↔ executor links are shaped
with `tc` to the configured bandwidth cap.

```bash
cd executor-replay/benchmark
REPLAY_CSV=record_rate100k.csv \
NUM_WORKERS=4 NUM_EXECUTORS=4 \
EXECUTOR_BW_MBPS=10000 DURATION=80 \
NO_SEND_PAYMENT=1 NEW_SCHEDULER=1 \
EXECUTOR_SKEW_WEIGHTS=3,1,1,1 \
fab cloudlab-replay --username <your-ssh-user> --manifest /path/to/manifest.xml
```

The manifest must describe at least `1 + NUM_WORKERS + NUM_EXECUTORS` nodes. An example
manifest is provided at `executor-replay/benchmark/manifest.xml`.

Additional variables beyond the table above (see `fabfile.py::cloudlab_replay` for the full
list):

| Variable | Default | Meaning |
|---|---|---|
| `NUM_WORKERS` | `1` | Number of worker processes (note: not `WORKERS`) |
| `EXECUTOR_BW_MBPS` | `10000` | LAN bandwidth cap on executor-bound traffic (Mbit/s) |
| `NEW_SCHEDULER` | `0` | Enable the load-aware scheduler evaluated in the paper |
| `NO_SEND_PAYMENT` | `0` | Disable cross-executor SendPayment |
| `NODE_OFFSET` | `0` | Skip the first N nodes of the manifest (useful when sharing a slice) |
| `WRITEBACK_EXECUTOR` | `0` | Writeback executor (note: `0`/`1` here, `false`/`true` in `fab replay`) |

### 3. Consensus-only benchmarks

For the consensus-tier sensitivity experiments (bandwidth asymmetry, validator-rate
imbalance, routing modes) use the non-replay drivers: `fab docker` runs a full 4+ validator
testbed in Docker with `tc` shaping per container, `fab cloudlab` runs the same on CloudLab.
See `benchmark/saturation_sweep.sh` and `benchmark/scalability_baseline_sweep.sh` for the
parameter sweeps behind the saturation and scalability plots.

## Reproducing Figure 8

`executor-replay/Experiment/` holds the result CSVs for the executor tier:

| File | Scenario |
|---|---|
| `Experiment/50%.csv` | One shard receives 50% of the workload — **this is Figure 8** |
| `Experiment/balanced.csv` | Uniform account-access distribution |
| `Experiment/90%.csv` | One shard receives 90% of the workload |

Figure 8's caption fixes the skew at 50%, so it is plotted from `50%.csv`; the other two are
the same sweep at the other skew settings.

Each row records `Tag` (commit), `BUSY_SPINS`, `Rate`, `NUM_E / NUM_W`, `EXECUTOR_BW_MBPS`,
`WRITEBACK_EXECUTOR`, `EXECUTOR_SKEW_WEIGHTS`, `DISTRIBUTED_TX_RATE`, `NO_SEND_PAYMENT`, the
measured `Committed TPS` / `E2E TPS`, and the exact `fab cloudlab-replay` command line that
produced it. Re-running a row's `Command` on a suitably sized CloudLab slice reproduces that
point.

`executor-replay/benchmark/run_benchmarks.py` iterates over the three CSVs, SSHes into a
control host, and fills the `Committed TPS` / `E2E TPS` columns in place. Edit the
`SSH_HOST`, `REMOTE_PREFIX`, and `CSV_FILES` constants before use.

---

# Part 3 — Executor scheduler simulator (`executor-sim/`)

A single-host Python simulator for measuring routing quality and scheduling time of the
executor-tier scheduler, without a cluster. Produces **Figures 9 and 10**.

All paths in this part are relative to `executor-sim/`.

| File | Purpose |
|---|---|
| `config.py` | Simulation parameters (executors, accounts, tx-size distributions, …) |
| `partitioner.py` | Range / hash partitioning strategies |
| `simulator.py` | Batch generation + simulator harness |
| `flamingo.py` | Flamingo scheduler implementation |
| `paper/paper.py` | Hermes-inspired reference scheduler |
| `paper/evaluator_paper.py` | Simulator variant used by the reference scheduler |
| `sweep.py` | Main sweep (tx size × cross-shard prob × overlap prob × zipf alpha) |
| `sweep_time_analysis.py` | Runtime sweep (batch size × tx size × num executors) |
| `sweep_results.csv` | Pre-computed output of `sweep.py` |
| `sweep_results_time_analysis.csv` | Pre-computed output of `sweep_time_analysis.py` |
| `sweep_analysis.ipynb` | Generates the routing-quality figures (Fig. 10) |
| `sweep_time_analysis.ipynb` | Generates the scheduling-time figures (Fig. 9) |

## Installation

```bash
cd executor-sim
pip install -r requirements.txt   # numpy, pandas, matplotlib, jupyter
```

All other imports are Python standard library.

## Quick reproduction (figures only)

The two notebooks read the shipped CSVs and regenerate the figures without re-running any
sweep:

```bash
jupyter nbconvert --to notebook --execute sweep_analysis.ipynb      --output sweep_analysis.ipynb
jupyter nbconvert --to notebook --execute sweep_time_analysis.ipynb --output sweep_time_analysis.ipynb
```

`sweep_analysis.ipynb` writes `sweep_routing_quality_overlap.{pdf,png}` (**Figure 10**) and
`sweep_routing_quality_crossshard.{pdf,png}`; `sweep_time_analysis.ipynb` writes
`sweep_time_batchsize.{pdf,png}` (**Figure 9**) and `sweep_time_executors.{pdf,png}`. Only
the two figures used in the paper are committed; the other two are generated fresh.

## Full reproduction (rerun sweeps from scratch)

```bash
python sweep.py                  # writes sweep_results.csv
python sweep_time_analysis.py    # writes sweep_results_time_analysis.csv
```

Then rerun the notebooks as above. The sweeps use three seeds (0, 1000, 2000) and are
deterministic, so rerunning should reproduce the shipped CSVs bit-for-bit.

Defaults live in `config.py` (`Configuration` class). The sweep scripts override the sweep
axes directly; keep `config.py` unchanged for bit-exact reproduction.

## Attribution

`paper/` contains a reference scheduler implementation inspired by Hermes. The simulator
harness is a simplified single-host model suitable for measuring routing quality and
scheduling time on commodity hardware.

---

## License

Apache-2.0 (see `LICENSE`).
