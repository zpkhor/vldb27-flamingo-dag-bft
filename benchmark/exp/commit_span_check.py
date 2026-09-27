#!/usr/bin/env python3
"""Did the system commit for the whole run, or did committed output stop early?

The failure this catches: `fab` returns normally, the configured duration elapses and the
node processes log to the end, but committed output stopped partway through. Read from the
summary alone that looks like a throughput plateau, and - for the migration curve - like
convergence.

Source of truth is `tps_timeline.csv`, whose bins come from `LogParser.commits` - the f+1
commit time per batch merged across primaries (`_merge_results_fplus1`). A bin exists only
where the system committed something, so the last bin is the last moment the system
committed at all. A healthy run's last bin starts at `duration - bin_s`.

Two measures were tried and rejected:
  - `migration_activity.py`'s MAX_GAP_S guard: load_run trims TRIM_RIGHT_DUR off the tail,
    so a run whose commits die inside that trim window ends its analysis window exactly at
    its last sample and reports no trailing hole.
  - per-worker "Committed sample tx" lines: workers go quiet staggered (n21: 503s, 599s,
    642s ...) in the April reference too, so it does not separate good runs from bad. It
    reflects local batch availability, not whether the system committed.

Usage:
    python benchmark/exp/commit_span_check.py <RESULTS_DIR> [RUN_SUBDIR ...]
"""
import argparse
import csv
import json
import sys
from pathlib import Path
from statistics import median

# One bin of slack: a healthy run's last bin starts at duration - bin_s, and the client
# stops sending a moment before the configured duration elapses.
SHORTFALL_TOL_S = 20.0

# A hole this long means the system stopped committing and resumed.
MAX_HOLE_S = 30.0


def analyze(run_dir):
    run_dir = Path(run_dir)
    csv_path = run_dir / "tps_timeline.csv"
    assert csv_path.exists(), (
        f"No tps_timeline.csv in {run_dir.resolve()} - run "
        f"benchmark/exp/parse_tps_migration_timeline.py on the results dir first."
    )

    params_path = run_dir / "bench-params.json"
    assert params_path.exists(), (
        f"No bench-params.json in {run_dir.resolve()} - the run produced no logs at all. "
        f"Check output.log: a cell that dies during setup (e.g. 'Parallel SSH failed on') "
        f"leaves an empty run dir, and grepping it for WARNINGS finds none, which reads as "
        f"a pass."
    )
    params = json.loads(params_path.read_text())
    duration = float(params["duration"])

    rows = list(csv.DictReader(open(csv_path)))
    assert rows, f"Empty tps_timeline.csv in {run_dir.resolve()}"

    ts = [float(r["timestamp_s"]) for r in rows]
    assert len(ts) >= 2, f"Only {len(ts)} bin(s) in {csv_path.resolve()}"

    diffs = [ts[i + 1] - ts[i] for i in range(len(ts) - 1)]
    bin_s = median(diffs)
    assert bin_s > 0, f"Non-positive bin width in {csv_path.resolve()}"

    t0 = ts[0]
    covered_end = ts[-1] - t0
    expected_end = duration - bin_s
    shortfall = expected_end - covered_end

    # Missing bins between the first and last row: the system committed, went silent, and
    # came back. `diffs` is already the per-row spacing, so any diff over one bin is a hole.
    worst_hole, hole_at = 0.0, 0.0
    for i, d in enumerate(diffs):
        if d - bin_s > worst_hole:
            worst_hole, hole_at = d - bin_s, ts[i] - t0

    ok = shortfall <= SHORTFALL_TOL_S and worst_hole <= MAX_HOLE_S

    return {
        "run": run_dir.name,
        "duration": duration,
        "bin_s": bin_s,
        "bins": len(rows),
        "covered_end": covered_end,
        "shortfall": shortfall,
        "worst_hole": worst_hole,
        "hole_at": hole_at,
        "ok": ok,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("results_dir")
    ap.add_argument("runs", nargs="*", help="Run subdir names (default: all with a timeline).")
    args = ap.parse_args()

    results_dir = Path(args.results_dir)
    if args.runs:
        run_dirs = [results_dir / r for r in args.runs]
    else:
        run_dirs = sorted(d for d in results_dir.iterdir()
                          if d.is_dir() and (d / "tps_timeline.csv").exists())
    assert run_dirs, f"No run dirs with tps_timeline.csv in {results_dir.resolve()}"

    print(f"{'RUN':<34} {'DUR':>5} {'BINS':>5} {'COMMITS_TO':>10} {'SHORT':>6} "
          f"{'HOLE':>5} {'AT':>6}  VERDICT")
    failed = []
    for d in run_dirs:
        i = analyze(d)
        verdict = "PASS" if i["ok"] else "STOPPED EARLY"
        if not i["ok"]:
            failed.append(i["run"])
        print(f"{i['run']:<34} {i['duration']:>4.0f}s {i['bins']:>5} {i['covered_end']:>9.0f}s "
              f"{i['shortfall']:>5.0f}s {i['worst_hole']:>4.0f}s {i['hole_at']:>5.0f}s  {verdict}")

    print()
    print(f"PASS = last commit bin within {SHORTFALL_TOL_S:.0f}s of the run end, "
          f"no hole > {MAX_HOLE_S:.0f}s.")
    print(f"Checked: {results_dir.resolve()}")
    if failed:
        print(f"\nSTOPPED EARLY: {', '.join(failed)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
