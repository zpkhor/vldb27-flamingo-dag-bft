#!/usr/bin/env python3
"""Plot Fig. 4 committed/E2E TPS and latency timelines for the LB ablation modes.

This is self-contained for fig_4_e2e_tps_timeline.sh result directories:
  * committed TPS/latency is loaded from tps_timeline.csv when present, or
    regenerated from raw logs using benchmark.logs.LogParser;
  * E2E TPS/latency is derived directly from client send/completion logs;
  * one paper-style timeline figure is written per run label.

Up to four variants (fig_4_e2e_tps_timeline.sh LB_MODE) are plotted together:
  lb       (LB_MODE=both)      -- validator + executor LB
  vlb      (LB_MODE=validator) -- validator LB only
  elb      (LB_MODE=executor)  -- executor LB only
  baseline (LB_MODE=none)      -- neither
Each variant dir may be passed explicitly; otherwise the newest matching
results/e2e_tps_timeline_<tag>_cloud_* directory is used when one exists.

Usage:
    python benchmark/exp/plot_fig4_e2e_commit.py \
        --baseline0-dir benchmark/exp/results/e2e_tps_timeline_lb_cloud_... \
        --baseline1-dir benchmark/exp/results/e2e_tps_timeline_baseline_cloud_... \
        --vlb-dir benchmark/exp/results/e2e_tps_timeline_vlb_cloud_... \
        --elb-dir benchmark/exp/results/e2e_tps_timeline_elb_cloud_... \
        --output-dir benchmark/fig4_plots --pdf
    zpkhor@dedos12: python3 benchmark/exp/plot_fig4_e2e_commit.py \
        --baseline0-dir /home/zpkhor/narwhal-validator/benchmark/exp/results/e2e_tps_timeline_lb_cloud_20260809_164311 \
        --baseline1-dir /home/zpkhor/narwhal-validator/benchmark/exp/results/e2e_tps_timeline_baseline_cloud_20260809_223706 \
        --vlb-dir /home/zpkhor/narwhal-validator/benchmark/exp/results/e2e_tps_timeline_vlb_cloud_20260809_115216 \
        --elb-dir /home/zpkhor/narwhal-validator/benchmark/exp/results/e2e_tps_timeline_elb_cloud_20260809_153835 \
        --output-dir benchmark/fig4_plots \
        --combined --pdf

    All four dirs above are the 2026-08-09 NO_SEND_PAYMENT=1 runs. Earlier result dirs used
    the SendPayment workload, where cross-executor migration dissolved E_SKEW_WEIGHTS within
    ~50s, so their executor-imbalance rows measure nothing and must not be mixed in.

Add --combined to emit a single figure instead of one per run: rows are the
scenarios (v / e / ve), columns are [TPS, latency]. Each cell carries
{commit, E2E} x {available variants}, where color encodes the variant and
linestyle encodes commit (dashed) vs E2E (solid).
"""
import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean

plt = None
mlines = None
ticker = None
MaxNLocator = None

RESULTS_DIR = Path(__file__).parent / "results"
BENCH_DIR = Path(__file__).resolve().parents[1]

RUN_DIR_RE = re.compile(r"^n(\d+)_(.+)_r(\d+)_run_(\d+)$")
CMD_WARMUP_RE = re.compile(r"\bWARMUP=(\d+(?:\.\d+)?)\b")
BALANCED_END_RE = re.compile(r"Balanced phase end spread: \d+ ms \((.+?)\)")
REGION_OFFSET_RE = re.compile(r"R\d+:([\d.]+)s")

CLIENT_START_RE = re.compile(r"\[(.*Z) .* Start sending transactions")
CLIENT_RATE_RE = re.compile(r"Transactions rate: (\d+) tx/s")
REGION_RATE_RE = re.compile(r"Region (\d+): accounts .* rate_weight=([\d.]+)")
SEND_RE = re.compile(
    r"\[(.*Z) .* Sending sample transaction (\d+) account (\d+) from region (\d+) to region (\d+)"
)
COMPLETE_RE = re.compile(
    r"\[(.*Z) .* Sample transaction (\d+) account (\d+) completed with (\d+) confirmations"
)

PRECISION = 20.0  # benchmark_client sends one sample per region per 1/PRECISION second.
SMOOTH_WINDOW = 3

# Ablation variants, in legend/plot order. Keys match the results-dir tag
# written by fig_4_e2e_tps_timeline.sh (e2e_tps_timeline_<tag>_cloud_*).
VARIANT_ORDER = ["lb", "vlb", "elb", "baseline"]
VARIANT_LABELS = {
    "lb":       "Flamingo",
    "vlb":      "Validator LB only",
    "elb":      "Executor LB only",
    "baseline": "Baseline",
}
COLORS = {
    "lb":       "#2A9D8F",
    "vlb":      "#264653",
    "elb":      "#C42A8E",
    "baseline": "#E9A020",
}
LINESTYLES = {
    "lb":       "-",
    "vlb":      "-.",
    "elb":      ":",
    "baseline": "--",
}
VARIANT_GLOBS = {v: f"e2e_tps_timeline_{v}_cloud_*" for v in VARIANT_ORDER}

SCENARIO_ORDER = [
    "n4_v_rate_imb90",
    "n4_e_rate_imb90",
    "n4_ve_rate_imb90",
]

# Rows of the --combined grid (cols = [E2E TPS, E2E latency]).
COMBINED_SCENARIO_ORDER = [
    "n4_v_rate_imb90",
    "n4_e_rate_imb90",
    "n4_ve_rate_imb90",
]
# The ve row prefers the 90% run but falls back to 60% when only that was run.
COMBINED_SCENARIO_FALLBACK = {"n4_ve_rate_imb90": "n4_ve_rate_imb60"}
LABEL_MAP = {
    "n4_balanced":      "Balanced",
    "n4_v_rate_imb60":  "Validator rate imb. 60%",
    "n4_v_rate_imb90":  "Validator rate imb. 90%",
    "n4_e_rate_imb60":  "Executor rate imb. 60%",
    "n4_e_rate_imb90":  "Executor rate imb. 90%",
    "n4_ve_rate_imb60": "V+E rate imb. 60%",
    "n4_ve_rate_imb90": "V+E rate imb. 90%",
    "n4_bw_f":          "BW limit (f)",
    "n4_bw_f1":         "BW limit (f+1)",
}


def _load_matplotlib():
    global plt, mlines, ticker, MaxNLocator
    if plt is not None:
        return
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as _plt
    import matplotlib.lines as _mlines
    import matplotlib.ticker as _ticker
    from matplotlib.ticker import MaxNLocator as _MaxNLocator

    plt = _plt
    mlines = _mlines
    ticker = _ticker
    MaxNLocator = _MaxNLocator


def _apply_sigmod_style():
    plt.rcParams.update({
        "font.family":       "sans-serif",
        "font.size":         8,
        "axes.titlesize":    8,
        "axes.labelsize":    8,
        "xtick.labelsize":   7,
        "ytick.labelsize":   7,
        "legend.fontsize":   7,
        "lines.linewidth":   1.0,
        "axes.linewidth":    0.6,
        "grid.linewidth":    0.4,
        "xtick.major.width": 0.6,
        "ytick.major.width": 0.6,
        "pdf.fonttype":      42,
        "ps.fonttype":       42,
        "grid.linestyle":    "--",
        "grid.color":        "lightgray",
        "grid.alpha":        0.8,
    })


def _to_posix(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()


def _percentile(sorted_vals, pct):
    if not sorted_vals:
        return None
    idx = max(0, len(sorted_vals) * pct // 100 - 1)
    return sorted_vals[idx]


def _fmt_float(value, digits=1):
    return "" if value is None else f"{value:.{digits}f}"


def _k_fmt():
    return ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}k" if x >= 1000 else f"{x:.0f}")


def _s_fmt():
    return ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}" if x >= 1000 else f"{x/1000:.1f}")


def _smoothed_series(times, vals, window=SMOOTH_WINDOW):
    if window <= 1 or len(vals) < window:
        return list(times), list(vals)
    out_t, out_v = [], []
    for i in range(0, len(vals) - window + 1):
        out_t.append(times[i])
        out_v.append(sum(vals[i:i + window]) / window)
    return out_t, out_v


def _latest_result_dir(pattern):
    matches = sorted(p for p in RESULTS_DIR.glob(pattern) if p.is_dir())
    return matches[-1] if matches else None


def _scenario_sort_key(run_name):
    m = RUN_DIR_RE.match(run_name)
    label = f"n{m.group(1)}_{m.group(2)}" if m else run_name
    try:
        return (SCENARIO_ORDER.index(label), run_name)
    except ValueError:
        return (len(SCENARIO_ORDER), run_name)


def _display_label(run_name):
    m = RUN_DIR_RE.match(run_name)
    if not m:
        return run_name
    label = f"n{m.group(1)}_{m.group(2)}"
    rate_k = int(m.group(3)) // 1000
    return f"{LABEL_MAP.get(label, label)} ({rate_k}k tx/s)"


def _detect_warmup(run_dirs):
    candidates = []
    for run_dir in run_dirs:
        log_path = Path(run_dir) / "output.log"
        if not log_path.exists():
            continue
        text = log_path.read_text(encoding="utf-8", errors="replace")
        m = BALANCED_END_RE.search(text)
        if m:
            offsets = REGION_OFFSET_RE.findall(m.group(1))
            if offsets:
                candidates.append(min(float(v) for v in offsets))
                continue
        m = CMD_WARMUP_RE.search(text)
        if m:
            candidates.append(float(m.group(1)))
    return min(candidates) if candidates else None


def _read_csv_rows(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _write_csv_rows(path, fields, rows):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _load_commit_timeline_csv(path):
    rows = _read_csv_rows(path)
    if not rows:
        return {"times": [], "tps": [], "lat": {}}

    time_col = "timestamp_s" if "timestamp_s" in rows[0] else "time_s"
    first = float(rows[0][time_col])
    is_absolute = first > 1e9
    times = [(float(r[time_col]) - first) if is_absolute else float(r[time_col]) for r in rows]
    tps_vals = [float(r["tps"]) for r in rows]

    lat = {}
    for name in ("mean", "p50", "p90", "p95"):
        col = f"lat_{name}"
        pairs = []
        for t, row in zip(times, rows):
            raw = row.get(col, "").strip()
            if raw:
                pairs.append((t, float(raw)))
        lat[name] = pairs
    return {"times": times, "tps": tps_vals, "lat": lat}


def _generate_commit_timeline(run_dir, bin_s):
    sys.path.insert(0, str(BENCH_DIR))
    from benchmark.logs import LogParser

    params_path = Path(run_dir) / "bench-params.json"
    if not params_path.exists():
        raise FileNotFoundError(f"Missing bench-params.json in {run_dir}")
    params = json.loads(params_path.read_text(encoding="utf-8"))

    lp = LogParser.process(
        str(run_dir),
        faults=params["faults"],
        duration=params["duration"],
        warmup=0,
    )
    if not lp.commits or not lp.effective_start:
        return {"times": [], "tps": [], "lat": {}}

    start = lp.effective_start
    tps_bins = defaultdict(list)
    for digest, ts in lp.commits.items():
        if ts < start:
            continue
        tps_bins[int((ts - start) / bin_s)].append(digest)

    lat_bins = defaultdict(list)
    for received in lp.sample_to_batch:
        for key, batch_id in received.items():
            if batch_id not in lp.commits:
                continue
            send_time = lp.sent_samples.get(key)
            if send_time is None or send_time < start:
                continue
            commit_time = lp.commits[batch_id]
            latency_ms = (commit_time - send_time) * 1000
            if latency_ms > 0:
                lat_bins[int((commit_time - start) / bin_s)].append(latency_ms)

    bins = sorted(set(tps_bins) | set(lat_bins))
    rows = []
    for b in bins:
        digests = tps_bins.get(b, [])
        total_bytes = sum(lp.sizes.get(d, 0) for d in digests)
        n_txs = total_bytes // lp.size
        lats = sorted(lat_bins.get(b, []))
        rows.append({
            "timestamp_s": f"{start + b * bin_s:.3f}",
            "tps": f"{n_txs / bin_s:.1f}",
            "n_batches": str(len(digests)),
            "n_txs": str(n_txs),
            "lat_mean": _fmt_float(mean(lats) if lats else None),
            "lat_p50": _fmt_float(_percentile(lats, 50)),
            "lat_p90": _fmt_float(_percentile(lats, 90)),
            "lat_p95": _fmt_float(_percentile(lats, 95)),
        })

    return _load_commit_timeline_from_rows(rows)


def _load_commit_timeline_from_rows(rows):
    if not rows:
        return {"times": [], "tps": [], "lat": {}}
    first = float(rows[0]["timestamp_s"])
    times = [float(r["timestamp_s"]) - first for r in rows]
    tps_vals = [float(r["tps"]) for r in rows]
    lat = {}
    for name in ("mean", "p50", "p90", "p95"):
        col = f"lat_{name}"
        lat[name] = [(t, float(r[col])) for t, r in zip(times, rows) if r.get(col, "").strip()]
    return {"times": times, "tps": tps_vals, "lat": lat, "rows": rows}


def load_commit_timeline(run_dir, bin_s, derived_dir=None):
    csv_path = Path(run_dir) / "tps_timeline.csv"
    if csv_path.exists():
        return _load_commit_timeline_csv(csv_path)

    if derived_dir is not None:
        cached = Path(derived_dir) / f"{Path(run_dir).name}_commit_tps_timeline.csv"
        if cached.exists():
            return _load_commit_timeline_csv(cached)

    data = _generate_commit_timeline(run_dir, bin_s)
    if derived_dir is not None and data.get("rows"):
        out = Path(derived_dir) / f"{Path(run_dir).name}_commit_tps_timeline.csv"
        _write_csv_rows(out, [
            "timestamp_s", "tps", "n_batches", "n_txs",
            "lat_mean", "lat_p50", "lat_p90", "lat_p95",
        ], data["rows"])
    return data


def parse_e2e_timeline(run_dir, bin_s):
    sent = {}
    completed = {}
    rates_by_region = {}
    start_times = []
    fallback_rates = {}

    for path in sorted(Path(run_dir).glob("client-*.log")):
        client_region = None
        client_rate = None
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                m = CLIENT_START_RE.search(line)
                if m:
                    start_times.append(_to_posix(m.group(1)))
                    continue
                m = CLIENT_RATE_RE.search(line)
                if m:
                    client_rate = float(m.group(1))
                    continue
                m = REGION_RATE_RE.search(line)
                if m:
                    client_region = int(m.group(1))
                    rates_by_region[client_region] = float(m.group(2))
                    if client_rate is not None:
                        fallback_rates[client_region] = client_rate
                    continue
                m = SEND_RE.search(line)
                if m:
                    key = (int(m.group(2)), int(m.group(3)))
                    sent[key] = {
                        "send_ts": _to_posix(m.group(1)),
                        "region": int(m.group(4)),
                    }
                    continue
                m = COMPLETE_RE.search(line)
                if m:
                    key = (int(m.group(2)), int(m.group(3)))
                    completed[key] = _to_posix(m.group(1))

        if client_region is not None and client_rate is not None and client_region not in rates_by_region:
            rates_by_region[client_region] = client_rate

    if not start_times:
        return {"times": [], "tps": [], "lat": {}, "rows": []}

    for region, rate in fallback_rates.items():
        rates_by_region.setdefault(region, rate)

    start = min(start_times)
    bins = defaultdict(lambda: {"tx": 0.0, "samples": 0, "lats": []})
    for key, complete_ts in completed.items():
        info = sent.get(key)
        if not info:
            continue
        send_ts = info["send_ts"]
        if send_ts < start or complete_ts < start:
            continue
        region = info["region"]
        multiplier = rates_by_region.get(region)
        if multiplier is None:
            if rates_by_region:
                multiplier = sum(rates_by_region.values()) / len(rates_by_region)
            else:
                multiplier = 0.0
        represented_txs = multiplier / PRECISION
        b = int((complete_ts - start) / bin_s)
        bins[b]["tx"] += represented_txs
        bins[b]["samples"] += 1
        bins[b]["lats"].append((complete_ts - send_ts) * 1000)

    rows = []
    for b in sorted(bins):
        item = bins[b]
        lats = sorted(item["lats"])
        rows.append({
            "timestamp_s": f"{start + b * bin_s:.3f}",
            "tps": f"{item['tx'] / bin_s:.1f}",
            "n_samples": str(item["samples"]),
            "n_txs_represented": f"{item['tx']:.1f}",
            "lat_mean": _fmt_float(mean(lats) if lats else None),
            "lat_p50": _fmt_float(_percentile(lats, 50)),
            "lat_p90": _fmt_float(_percentile(lats, 90)),
            "lat_p95": _fmt_float(_percentile(lats, 95)),
        })

    data = _load_commit_timeline_from_rows(rows)
    data["rows"] = rows
    return data


def load_e2e_timeline(run_dir, bin_s, derived_dir=None):
    if derived_dir is not None:
        cached = Path(derived_dir) / f"{Path(run_dir).name}_e2e_tps_timeline.csv"
        if cached.exists():
            return _load_commit_timeline_csv(cached)

    data = parse_e2e_timeline(run_dir, bin_s)
    if derived_dir is not None and data.get("rows"):
        out = Path(derived_dir) / f"{Path(run_dir).name}_e2e_tps_timeline.csv"
        _write_csv_rows(out, [
            "timestamp_s", "tps", "n_samples", "n_txs_represented",
            "lat_mean", "lat_p50", "lat_p90", "lat_p95",
        ], data["rows"])
    return data


def discover_run_names(variant_dirs, requested):
    if requested:
        return requested
    per_variant = [
        {p.name for p in Path(d).iterdir() if p.is_dir() and RUN_DIR_RE.match(p.name)}
        for d in variant_dirs.values()
    ]
    common = set.intersection(*per_variant)
    names = sorted(common, key=_scenario_sort_key)
    if not names:
        raise ValueError("No run subdirectories common to all variant dirs: "
                         + ", ".join(f"{k}={v}" for k, v in variant_dirs.items()))
    return names


def _load_series(run_dir, args):
    """{'commit': ..., 'e2e': ...} timelines for one run dir, with CSV caching."""
    derived = Path(run_dir).parent / "derived_csv"
    return {
        "commit": load_commit_timeline(run_dir, args.bin, derived),
        "e2e": load_e2e_timeline(run_dir, args.bin, derived),
    }


def _plot_one(ax, data, metric, latency_col, color, linestyle, label):
    if metric == "tps":
        times, vals = data["times"], data["tps"]
    else:
        pairs = data["lat"].get(latency_col, [])
        if not pairs:
            return False
        times, vals = zip(*pairs)
    if not times:
        return False
    pt, pv = _smoothed_series(list(times), list(vals))
    ax.plot(pt, pv, color=color, linestyle=linestyle, linewidth=1.0, label=label)
    return True


def _resolve_combined_runs(run_names):
    """Map each COMBINED_SCENARIO_ORDER label to a concrete run_name (or None)."""
    by_label = {}
    for name in run_names:
        m = RUN_DIR_RE.match(name)
        if not m:
            continue
        by_label.setdefault(f"n{m.group(1)}_{m.group(2)}", name)
    resolved = []
    for label in COMBINED_SCENARIO_ORDER:
        chosen = by_label.get(label) or by_label.get(COMBINED_SCENARIO_FALLBACK.get(label, ""))
        resolved.append((label, chosen))
    return resolved


def plot_combined(resolved, variant_dirs, output_dir, args):
    nrows = len(resolved)
    fig, axes = plt.subplots(nrows, 2, figsize=(5.4, 1.00 * nrows + 0.5),
                             squeeze=False, sharex="col")

    tps_fmt = ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}")
    lat_fmt = ticker.FuncFormatter(
        lambda x, _: f"{x/1000:.0f}" if x >= 1000 else f"{x/1000:.1f}")

    # Each cell carries {commit, e2e} x {variants}.
    # Color encodes the variant; linestyle encodes the metric kind.
    kind_styles = {"commit": "--", "e2e": "-"}
    kind_labels = {"commit": "Committed", "e2e": "End-to-end"}
    variants = list(variant_dirs)

    for row, (label, run_name) in enumerate(resolved):
        ax_tps, ax_lat = axes[row]

        scenario_label = label
        if run_name is not None:
            m = RUN_DIR_RE.match(run_name)
            if m:
                scenario_label = f"n{m.group(1)}_{m.group(2)}"
        ax_tps.set_ylabel(f"{LABEL_MAP.get(scenario_label, scenario_label)}\nThroughput [ktrans/s]",
                          fontsize=6)
        ax_lat.set_ylabel(f"{args.latency.capitalize()} Latency [sec]", fontsize=6)

        if run_name is None:
            for ax in (ax_tps, ax_lat):
                ax.text(0.5, 0.5, "n/a", transform=ax.transAxes,
                        ha="center", va="center", fontsize=7, color="gray")
                ax.grid(True)
            continue

        run_dirs = {v: variant_dirs[v] / run_name for v in variants}
        series = {v: _load_series(d, args) for v, d in run_dirs.items()}

        max_t = 0.0
        for v in variants:
            for kind in ("commit", "e2e"):
                data = series[v][kind]
                if data["times"]:
                    max_t = max(max_t, max(data["times"]))

        for ax, metric, lat_col in ((ax_tps, "tps", None), (ax_lat, "lat", args.latency)):
            any_line = False
            for v in variants:
                for kind in ("commit", "e2e"):
                    any_line |= _plot_one(
                        ax, series[v][kind], metric, lat_col,
                        COLORS[v], kind_styles[kind],
                        f"{VARIANT_LABELS[v]} ({kind_labels[kind]})",
                    )
            if not any_line:
                ax.text(0.5, 0.5, "n/a", transform=ax.transAxes,
                        ha="center", va="center", fontsize=7, color="gray")
            ax.grid(True)
            ax.yaxis.set_major_locator(MaxNLocator(nbins=3))
            ax.yaxis.set_major_formatter(tps_fmt if metric == "tps" else lat_fmt)
            ax.xaxis.set_major_locator(MaxNLocator(nbins=4))

        warmup_s = _detect_warmup(list(run_dirs.values()))
        if warmup_s is not None:
            for ax in (ax_tps, ax_lat):
                ax.axvline(warmup_s, color="black", linestyle=":", linewidth=0.8, alpha=0.75)

        left = args.trim_left
        right = max_t - args.trim_right
        if right > left:
            for ax in (ax_tps, ax_lat):
                ax.set_xlim(left=left, right=right)

    axes[0][0].set_title("Throughput")
    axes[0][1].set_title("Latency")
    for ax in axes[-1]:
        ax.set_xlabel("Time (s)")

    # Factored legend: color names the variant, linestyle names the metric, so
    # 4 variants x 2 metrics needs 6 entries instead of 8.
    handles = [
        mlines.Line2D([], [], color=COLORS[v], linestyle="-", label=VARIANT_LABELS[v])
        for v in variants
    ] + [
        mlines.Line2D([], [], color="black", linestyle=kind_styles[k], label=kind_labels[k])
        for k in ("commit", "e2e")
    ]
    ncol = min(len(handles), 6)
    fig.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.5, 1.0),
               ncol=ncol, frameon=False, fontsize=6.5, handlelength=1.4,
               columnspacing=0.9, handletextpad=0.35, borderaxespad=0.0,
               borderpad=0.0)

    legend_rows = -(-len(handles) // ncol)
    fig.subplots_adjust(left=0.16, right=0.97, top=0.92 - 0.035 * (legend_rows - 1),
                        bottom=0.07, hspace=0.10, wspace=0.30)
    out = output_dir / "fig4_combined.png"
    plt.savefig(out, dpi=200, bbox_inches="tight", pad_inches=0.02)
    print(f"Saved: {out.resolve()}")
    if args.pdf:
        pdf = out.with_suffix(".pdf")
        plt.savefig(pdf, bbox_inches="tight", pad_inches=0.02)
        print(f"Saved: {pdf.resolve()}")
    plt.close(fig)


def plot_run(run_name, variant_dirs, output_dir, args):
    variants = list(variant_dirs)
    run_dirs = {v: variant_dirs[v] / run_name for v in variants}
    series = {v: _load_series(d, args) for v, d in run_dirs.items()}

    fig, axes = plt.subplots(
        4, 1, figsize=(2.9, 5.2), sharex=True,
        gridspec_kw={"height_ratios": [3, 3, 2, 2]},
    )
    ax_commit_tps, ax_e2e_tps, ax_commit_lat, ax_e2e_lat = axes

    panels = [
        (ax_commit_tps, "commit", "tps", None, "Commit TPS [k tx/s]"),
        (ax_e2e_tps, "e2e", "tps", None, "E2E TPS [k tx/s]"),
        (ax_commit_lat, "commit", "lat", args.latency, f"Commit {args.latency} [s]"),
        (ax_e2e_lat, "e2e", "lat", args.latency, f"E2E {args.latency} [s]"),
    ]

    max_t = 0.0
    for v in variants:
        for kind in ("commit", "e2e"):
            data = series[v][kind]
            if data["times"]:
                max_t = max(max_t, max(data["times"]))

    for ax, kind, metric, lat_col, ylabel in panels:
        any_line = False
        for v in variants:
            any_line |= _plot_one(
                ax,
                series[v][kind],
                metric,
                lat_col,
                COLORS[v],
                LINESTYLES[v],
                VARIANT_LABELS[v],
            )
        if not any_line:
            ax.text(0.5, 0.5, "n/a", transform=ax.transAxes,
                    ha="center", va="center", fontsize=7, color="gray")
        ax.set_ylabel(ylabel, fontsize=6)
        ax.grid(True)
        ax.yaxis.set_major_locator(MaxNLocator(nbins=3))
        if metric == "tps":
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}"))
        else:
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}" if x >= 1000 else f"{x/1000:.1f}"))

    warmup_s = _detect_warmup(list(run_dirs.values()))
    if warmup_s is not None:
        for ax in axes:
            ax.axvline(warmup_s, color="black", linestyle=":", linewidth=0.8, alpha=0.75)

    left = args.trim_left
    right = max_t - args.trim_right
    if right > left:
        ax_commit_tps.set_xlim(left=left, right=right)

    axes[-1].set_xlabel("Time (s)")
    axes[-1].xaxis.set_major_locator(MaxNLocator(nbins=4))
    ax_commit_tps.set_title(_display_label(run_name))

    handles = [
        mlines.Line2D([], [], color=COLORS[v], linestyle=LINESTYLES[v], label=VARIANT_LABELS[v])
        for v in variants
    ]
    fig.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.58, 0.985),
               ncol=2, frameon=False, fontsize=6, handlelength=1.6,
               columnspacing=1.0, handletextpad=0.4)

    legend_rows = -(-len(handles) // 2)
    fig.subplots_adjust(left=0.20, right=0.97, top=0.92 - 0.03 * (legend_rows - 1),
                        bottom=0.08, hspace=0.18)
    out = output_dir / f"fig4_{run_name}.png"
    plt.savefig(out, dpi=200, bbox_inches="tight", pad_inches=0.02)
    print(f"Saved: {out.resolve()}")
    if args.pdf:
        pdf = out.with_suffix(".pdf")
        plt.savefig(pdf, bbox_inches="tight", pad_inches=0.02)
        print(f"Saved: {pdf.resolve()}")
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser(
        description="Plot committed/E2E TPS and latency timelines for fig_4_e2e_tps_timeline.sh outputs."
    )
    ap.add_argument("--baseline0-dir", type=Path,
                    help="LB_MODE=both results dir. Defaults to newest e2e_tps_timeline_lb_cloud_*.")
    ap.add_argument("--baseline1-dir", type=Path,
                    help="LB_MODE=none results dir. Defaults to newest e2e_tps_timeline_baseline_cloud_*.")
    ap.add_argument("--vlb-dir", type=Path,
                    help="LB_MODE=validator (validator LB only) results dir. "
                         "Defaults to newest e2e_tps_timeline_vlb_cloud_* if one exists.")
    ap.add_argument("--elb-dir", type=Path,
                    help="LB_MODE=executor (executor LB only) results dir. "
                         "Defaults to newest e2e_tps_timeline_elb_cloud_* if one exists.")
    ap.add_argument("--output-dir", type=Path, default=Path("."), help="Directory to write plots.")
    ap.add_argument("--runs", nargs="+", help="Specific run subdirectory names to plot.")
    ap.add_argument("--latency", choices=["mean", "p50", "p90", "p95"], default="mean")
    ap.add_argument("--bin", type=float, default=10.0, help="Timeline bin width in seconds.")
    ap.add_argument("--trim-left", type=float, default=None,
                    help="Seconds to trim from left of x-axis (default: plot_lb_configs.TRIM_LEFT_DUR).")
    ap.add_argument("--trim-right", type=float, default=None,
                    help="Seconds to trim from right of x-axis (default: plot_lb_configs.TRIM_RIGHT_DUR).")
    ap.add_argument("--combined", action="store_true",
                    help="Emit one combined figure: rows = scenarios "
                         "(v/e/ve), cols = [TPS, latency], "
                         "{commit,E2E} x {variants} lines per cell.")
    ap.add_argument("--csv-only", action="store_true", help="Write derived CSVs and skip plotting.")
    ap.add_argument("--pdf", action="store_true", help="Also save a PDF alongside each PNG.")
    args = ap.parse_args()

    requested_dirs = {
        "lb": args.baseline0_dir,
        "vlb": args.vlb_dir,
        "elb": args.elb_dir,
        "baseline": args.baseline1_dir,
    }
    # lb/baseline are mandatory; vlb/elb join the plot only when their runs exist.
    variant_dirs = {}
    for variant in VARIANT_ORDER:
        d = requested_dirs[variant] or _latest_result_dir(VARIANT_GLOBS[variant])
        if d is None:
            assert variant in ("vlb", "elb"), (
                f"No results dir for variant '{variant}': pass it explicitly or run "
                f"fig_4_e2e_tps_timeline.sh (no match for {RESULTS_DIR / VARIANT_GLOBS[variant]})"
            )
            continue
        assert d.is_dir(), f"Not a directory: {d}"
        variant_dirs[variant] = d

    args.output_dir.mkdir(parents=True, exist_ok=True)
    run_names = discover_run_names(variant_dirs, args.runs)

    for variant, d in variant_dirs.items():
        print(f"{VARIANT_LABELS[variant]} ({variant}): {d.resolve()}")
    print(f"Runs: {', '.join(run_names)}")

    if args.csv_only:
        for run_name in run_names:
            for base in variant_dirs.values():
                run_dir = base / run_name
                derived = base / "derived_csv"
                load_commit_timeline(run_dir, args.bin, derived)
                load_e2e_timeline(run_dir, args.bin, derived)
        return

    sys.path.insert(0, str(Path(__file__).parent))
    from plot_lb_configs import TRIM_LEFT_DUR, TRIM_RIGHT_DUR
    if args.trim_left is None:
        args.trim_left = TRIM_LEFT_DUR
    if args.trim_right is None:
        args.trim_right = TRIM_RIGHT_DUR

    _load_matplotlib()
    _apply_sigmod_style()
    if args.combined:
        resolved = _resolve_combined_runs(run_names)
        print("Combined rows: " + ", ".join(
            f"{lbl}->{rn or 'MISSING'}" for lbl, rn in resolved))
        plot_combined(resolved, variant_dirs, args.output_dir, args)
        return
    for run_name in run_names:
        plot_run(run_name, variant_dirs, args.output_dir, args)


if __name__ == "__main__":
    main()
