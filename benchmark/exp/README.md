# Paper experiments

Deep reference for the **validator-tier** figures: configs (a)–(d), the hot-spot shift, the
end-to-end figure, and the ablation. It covers exact provenance, which results dirs each
plot reads, and the known gotchas. The root `README.md` is the artifact entry point and also
covers the executor tier (`executor-replay/`, `executor-sim/`).

| committed figure | section |
|---|---|
| `benchmark/config_{a,b,c,d}.pdf` | [Configs (a)–(d)](#configs-ad) |
| `benchmark/hotspot_shift_repro.pdf` | [Hot-spot shift](#hot-spot-shift) |
| `benchmark/fig4_plots/fig4_combined.pdf` | [End-to-end figure](#end-to-end-figure-fig4) |
| `benchmark/figs_ablation_repro/ablation_{bal,imb}.pdf` | [Ablation figures](#ablation-figures) |

Everything here drives `fab cloudlab` (or `fab docker`) through `benchmark/fabfile.py`.
`fab` only exists in the `narwhal39` conda env, and `fab` compiles the binaries itself, so
`cargo` must be on `PATH` too:

```bash
source activate narwhal39
cargo build --release --features benchmark
```

Results land in `benchmark/exp/results/` (a symlink to `/mnt/data/results`). Run dirs are
named `<label>_r<rate>_run_<n>`; see the label grammar in `CLAUDE.md`.

## Where the committed figures came from

Re-running the plot commands in this file against these dirs reproduces the committed PDFs
pixel-for-pixel. The sweep commands below regenerate the *data*, which is a fresh
measurement — see the per-section caveats for where a re-sweep will not land on the same
dir contents.

| figure | primary dirs (under `benchmark/exp/results/`) | baseline dirs |
|---|---|---|
| config (a), (b) | `abc_2f1_shared_rerun_20260822` | `tps_timeline_baseline_cloud_20260414_132153` |
| config (c) | `config_c_full_d2000_20260822` | `config_c_baseline_d1450_20260825_150911` |
| config (d) | `tps_timeline_lb_nodes_cloud_20260814_162154` | `config_d_baseline_90k_20260815_102235` |
| fig4 | `e2e_tps_timeline_lb_cloud_20260809_164311` (both), `..._vlb_cloud_20260809_115216`, `..._elb_cloud_20260809_153835` | `e2e_tps_timeline_baseline_cloud_20260809_223706` |
| ablation | `ablation_cloud_20260825_181540` (grid) | `ablation_cloud_baseline_20260825_162809` |

## Configs (a)–(d)

Four panels, each varying one axis. `imbN` = the first `ceil(n/4)` validators receive `N%`
of the total client load; `bw_f` / `bw_f1` = `f` / `f+1` validators given 200 Mbps instead
of 600 Mbps.

| config | varies | held fixed | cells | duration |
|---|---|---|---|---|
| (a) | offered load | n=4, imb90, balanced BW | 110k, 80k, 50k, 20k tx/s | 1650s |
| (b) | imbalance % | n=4, 110k, balanced BW | imb99, imb90\*, imb60 | 1650s |
| (c) | bandwidth scenario | n=4, imb90 | balanced @110k, `bw_f` @110k, `bw_f1` @65k | 2000s |
| (d) | committee size | imb60, balanced BW, 90k | n=21, 16, 10, 4 | 1650s |

\* (a) and (b) share the single `n4_v_rate_imb90_r110000` run. (c) does **not** share it —
it has its own balanced cell in its own dir, see below. So (a)+(b) are 6 unique LB cells and
(c) is a further 3.

Config (d) runs at 90k rather than the 110k of (a)–(c): under the 2f+1 reroute quorum
(`cffe261`) the largest cell that still stops migrating is n=21 at 90k. Panel (d) compares
node counts at a single fixed rate, so every cell uses 90k — meaning (b)'s `n4_v_rate_imb60`
at 110k can no longer double as (d)'s n=4 point.

### Why (c) has its own dirs

Panel (c)'s cells take far longer to drain their backlog than (a)/(b)/(d): the `bw_f1`
cell's latency only falls back to ~2s at ~1290s. At the 1650s of the other panels it is
still draining at the right edge and reads as unrecovered, so (c) was re-measured at
`DURATION=2000` and plots to 1400s (`max_t = 1570`) rather than 883s.

Its baselines needed the same treatment. The April `BL_DIR_ABC` cells are 900s, which on
(c)'s wider x-axis stopped at two thirds of the panel, so (c) has its own baselines at
`DURATION=1450` — the smallest run covering the plotted window with a 10s bin to spare.

### Run the LB side

The 6 cells of configs (a)–(b), run serially:

```bash
MODE=cloudlab bash benchmark/exp/fig_3_tps_timeline_lb.sh
```

`MODE` defaults to `docker`, so `MODE=cloudlab` is required for the cluster. The defaults
already match the committed figures — `DURATION=1650`, `WARMUP=240`, `RETRIES=1`.
`NODE_OFFSET` (default 0) selects which machines of the manifest the sweep occupies.

Config (d) has its own script, because n=21 and n=16 need the whole 23-machine manifest and
cannot share the cluster:

```bash
MODE=cloudlab bash benchmark/exp/fig_3_tps_timeline_nodes_lb.sh
```

Config (c)'s LB side has **no committed generator script**. `config_c_full_d2000_20260822`
was produced by a `config_c_full_repro.sh` that is not in the repo and not in git history —
`config_c_baseline_repro.sh`'s header still refers to it. To re-measure (c), run its three
cells at `DURATION=2000 WARMUP=240` with the cell parameters read back out of
`config_c_baseline_repro.sh`'s `CELLS` (they are that script's own source, minus `BASELINE=1`):

All three use `RATE_WEIGHTS=0.9,0.0333333,0.0333333,0.0333333`:

| cell | `WORKER_BANDWIDTHS_MBPS` | `RATE` |
|---|---|---|
| `n4_v_rate_imb90_r110000` | `600,600,600,600` | 110000 |
| `n4_bw_f_rate_imb90_r110000` | `600,600,600,200` | 110000 |
| `n4_bw_f1_rate_imb90_r65000` | `600,600,200,200` | 65000 |

### Run the baseline side

The dashed reference lines come from separate `BASELINE=1` runs. For (a) and (b):

```bash
MODE=cloudlab bash benchmark/exp/fig_3_tps_timeline_baseline.sh
```

Two things to know. Its (a)–(b) cells default to `DURATION=900`, which is what the committed
baseline was measured at — that is why the dashed lines stop well before the LB lines on
those two panels. And its (d) cells are **commented out** in the script; they need
`DURATION=1650` to match their LB side, so they cannot come from the same invocation.

For (c), use its own baseline script instead — it runs two cells concurrently on disjoint
machines (`NODE_OFFSET` 0 and 4 of the 10-node manifest):

```bash
DURATION=1450 bash benchmark/exp/config_c_baseline_repro.sh [OUT_DIR]
```

Baselines are paired to LB runs by identical run-subdir name, so a baseline cell must use the
same label *and* rate as its LB counterpart or the panel silently loses that dashed line.

### Plot

```bash
python benchmark/exp/plot_paper_configs.py --configs a b c d --pdf --output-dir benchmark
```

Writes `config_{a,b,c,d}.png` (and `.pdf` with `--pdf`).

**The results dirs are hardcoded** near the top of `plot_paper_configs.py`: `LB_DIR_ABC` /
`BL_DIR_ABC` for (a) and (b), `LB_DIR_C` / `BL_DIR_C` for (c), `LB_DIR_D` / `BL_DIR_D` for
(d). A fresh sweep is not picked up until you either edit those constants or pass
`--lb-dir <dir>`, which overrides the LB dir for the selected configs only — the baseline
dir stays hardcoded. `--max-t` pins the x-axis right edge for every selected config,
overriding each config's own pinned value.

### `LB_DIR_ABC` is hand-curated

`abc_2f1_shared_rerun_20260822` is a symlink-only dir, not the output of one sweep. Its 8
symlinks point into three different source dirs, so a fresh `fig_3_tps_timeline_lb.sh` sweep
will **not** reconstruct it:

| cells | source dir | code | duration |
|---|---|---|---|
| `imb60_r110000`, `imb99_r110000` | `abc_2f1_cloud_20260814_182415` | clean `38f9ab7` | 1650s |
| all four config-(a) rates | `config_a_repro_20260822` | `38f9ab7` + dirty `consensus/src/lib.rs` | 1000s |
| `bw_f_r110000`, `bw_f1_r65000` | `config_c_bw_repro_20260822` | — | 1000s |

Consequences worth knowing:

- Panels (a) and (b) mix code versions and run lengths: four cells are on a patched tree
  (per-key `structural_excess` donor test, patch at
  `results/config_a_repro_20260822/consensus_lib_rs.patch`) at 1000s, two are on clean
  `38f9ab7` at 1650s. Nothing past ~713s is plotted, so the panels are unaffected, but tail
  length is not comparable across cells.
- The symlink names remap attempt numbers (`..._r20000_run_2` → the source's `run_3`,
  `..._r50000_run_2` → `run_1`, `..._r80000_run_1` → `run_2`), because
  `plot_paper_configs.py` looks cells up by subdir name and the baseline dir pairs against
  those names. The chosen attempt is the one that passed the balanced-phase check, not
  necessarily the first.
- The two `bw_*` cells are dead weight: (c) now reads `LB_DIR_C`, so nothing plots them.

The dir's own `PROVENANCE.md` is stale — it describes an earlier composition ("6 cells
unchanged, 2 re-run" into `abc_shared_cell_20260822_rerun`). The symlinks were later
repointed at `config_a_repro_20260822`, so it is now the other way round: 2 unchanged, 6
re-run. Its before/after balanced-phase numbers, and the reason for the re-run (a stalled
pre-skew phase showing as a dip ahead of the skew), still apply.

## Hot-spot shift

The hot region moves from validator 0 to validator 1 at `2.5 × WARMUP` seconds (600s at the
default `WARMUP=240`), and the LB algorithm has to re-converge — giving the figure its two
dips.

This one needs a patch. The client-side shift lives in `benchmark/exp/hotspot_shift.patch`
rather than on the branch, because `cloudlab_bench.py` hardcodes the swap as
"region 0 ↔ region 1" for this figure only. Nothing else needs patching: `consensus/src/lib.rs`
is already identical to `zp/hotspot-shift-v2`, and this branch carries `cffe261`, so the run
uses the 2f+1 reroute quorum.

```bash
bash benchmark/exp/run_hotspot_shift.sh
```

That applies the patch, runs, unapplies it, plots, and scores the run — one command. The
unapply is on a `trap`, so Ctrl-C or a kill still restores the tree; on a signal the sweep
aborts rather than continuing with the patch already reverted. Defaults are n=4, imb90,
110k tx/s, `DURATION=1400`, `WARMUP=240`; `RETRIES=N` runs N attempts.

Output goes to `benchmark/hotspot_shift_repro.pdf`, which is the committed figure. The older
reference `benchmark/hotspot_shift.pdf` was removed in `0c6debb` ("delete old plots"), so the
"Reference:" path the script prints on exit no longer resolves; recover it from git history if
you want the side-by-side comparison.

To re-plot an existing run dir without re-running:

```bash
python benchmark/tps_timeline.py <run_dir> > <run_dir>/tps_timeline.csv
python benchmark/exp/hotspot.py --run-dir <run_dir> -o out.pdf
```

`benchmark/exp/check_hotspot_shift.py <run_dir> [<run_dir> ...]` scores the five windows the
figure is about (warmup plateau, imbalance dip, recovered plateau, shift dip, post-shift
plateau) plus total migrations, so attempts can be compared on identical windows instead of
by eye. Pass the April reference dir alongside a new run to compare directly.

Run-to-run spread is real here: the figure's own provenance
(`benchmark/exp/hotspot_reruns_summary.tsv` on `zp/hotspot-shift-v2`) records three attempts
before one scored well. A run can match on throughput and migrations while its latency tail
makes the plotted plateaus wobble, since the timeline is built from f+1 commit-reply samples.

## End-to-end figure (fig4)

`benchmark/fig4_plots/fig4_combined.pdf` compares four load-balancing configurations under
validator- and executor-level skew. Rows are the scenarios, columns are [TPS, latency];
within a cell colour encodes the configuration and linestyle separates committed (dashed)
from end-to-end (solid). Cloudlab only, n=4 with 3 executors, 900s per run.

`LB_MODE` selects which layers are enabled, and each mode writes its own timestamped dir:

| `LB_MODE` | validator LB (`BASELINE`) | executor LB (`NEW_SCHEDULER`) | results dir tag |
|---|---|---|---|
| `both` | yes (0) | yes (1) | `lb` |
| `validator` | yes (0) | no (0) | `vlb` |
| `executor` | no (1) | yes (1) | `elb` |
| `none` | no (1) | no (0) | `baseline` |

The three plotted rows are `n4_v_rate_imb90`, `n4_e_rate_imb90` and `n4_ve_rate_imb90`, all
at 100k tx/s (validator skew `27,1,1,1`; executor skew `18,1,1`). The script also runs a
`n4_bw_f` cell at 72k, which `--combined` does not use. All modes use the data-fusion
`batch_executor` (no `WRITEBACK_EXECUTOR`).

```bash
LB_MODE=both,validator,executor,none bash benchmark/exp/fig_4_e2e_tps_timeline.sh
```

`NO_SEND_PAYMENT=1` is the default and must stay on. SendPayment picks its destination
account uniformly over the global range, and under data fusion each cross-executor tx
reassigns ownership, so the account→executor map fully decorrelates within ~50s — that
erases `E_SKEW_WEIGHTS` before the measurement window starts. Result dirs predating
2026-08-09 used that workload and must not be mixed in.

To resume, preset `RESULTS_DIR` to an existing dir and pass that mode alone — runs
accumulate there, cells with a `SUMMARY:` block are kept, and cells that died mid-run are
moved to `<results_dir>/failed/` and redone. `RESULTS_DIR` pins one dir, so it rejects a
multi-mode `LB_MODE`.

```bash
RESULTS_DIR=<existing_dir> LB_MODE=executor bash benchmark/exp/fig_4_e2e_tps_timeline.sh
```

Each sweep ends by generating `tps_timeline.csv` and running `commit_span_check.py`, exiting
non-zero if a run stopped early. Then combine the four dirs:

```bash
python benchmark/exp/plot_fig4_e2e_commit.py \
    --baseline0-dir <lb_dir> --baseline1-dir <baseline_dir> \
    --vlb-dir <vlb_dir> --elb-dir <elb_dir> \
    --output-dir benchmark/fig4_plots --combined --pdf
```

Each `--*-dir` defaults to the newest `results/e2e_tps_timeline_<tag>_cloud_*`, so with a
clean results dir the flags can be omitted entirely. Drop `--combined` to emit one figure per
run label instead. The plot rebuilds the timeline from the raw logs when `tps_timeline.csv`
is absent, so it works on older result dirs too.

## Ablation figures

`benchmark/figs_ablation_repro/ablation_{bal,imb}.pdf` show how one malicious validator
misreporting its quorum metrics affects load balancing, over a 2×4 grid of
{balanced, imbalanced} load × {`inflated_queue_delay`, `deflated_qd`, `deflated_capacity`,
`inflated_cap`}, against a non-malicious baseline. The malicious node is the last validator
(v3 at n=4), so its case annotation is logged in `primary-3.log` as `ablation_case`.
Imbalance overloads the first validator (v0) via `RATE_WEIGHTS`. Defaults are n=4,
110k tx/s, `DURATION=900`, `WARMUP=240`, `IN_MEMORY_STORE=1`.

`BASELINE=0` requires the `[ABLATION]` blocks in `consensus/src/lib.rs` to be uncommented;
while commented out `MALICIOUS_MODE` reaches the primary but changes nothing. They are live
code on this branch (`consensus/src/lib.rs:203`).

Each sweep prints its absolute results dir; at the default 900s duration the 18 runs take
roughly 5 hours.

```bash
BASELINE=1 RETRIES=5 MODE=cloudlab bash benchmark/exp/fig_ablation.sh   # 2 cells x 5 repeats
BASELINE=0 RETRIES=1 MODE=cloudlab bash benchmark/exp/fig_ablation.sh   # 2x4 malicious grid
```

Both sweeps are resumable: re-invoking the same command keeps every cell whose `output.log`
has a `SUMMARY:` block, and redoes the ones that died mid-run (the previous attempt is kept
under `<results_dir>/failed/`). Each sweep ends by generating the per-run `tps_timeline.csv`
and running `commit_span_check.py`; a cell that stopped early is reported there and exits
non-zero. Such a cell still carries a `SUMMARY:`, so delete its run dir before re-invoking.

Then plot both dirs together, baseline first:

```bash
python benchmark/exp/plot_paper_ablation.py <BASELINE_DIR> <GRID_DIR> \
    --output-dir figs_ablation_repro --pdf
```

The plot reads `run_1` of each cell by default (`--run N` picks another repeat), so the
`RETRIES=5` on the baseline sweep buys spare attempts rather than an average.

Use `MODE=docker` for a local run. `NODES`, `RATE`, `DURATION` and `WARMUP` are overridable;
`CASES` takes comma-separated label substrings to run a subset (e.g. `CASES=imb_inflqd`).

## Before trusting any run

A stalled run still shows a plausible plateau, and a truncated migration curve reads as
convergence.

- `output.log` must contain a `SUMMARY:` block. A cell that dies in setup has no summary, so
  it also has no `WARNINGS:` — absence of warnings is not health.
- `python benchmark/exp/commit_span_check.py <RESULTS_DIR>` catches truncation that the
  0.9-ratio warning misses.
- Mean/E2E TPS averages over the migration dip. Judge a run by the tail — the recovered TPS
  after migrations settle — and by whether migrations converge at all.
