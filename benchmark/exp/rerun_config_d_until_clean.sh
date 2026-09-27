#!/usr/bin/env bash
# Config (d) sweep that retries a cell until it commits for the whole run.
#
# Why this exists: the plain sweep (fig_3_tps_timeline_nodes_lb.sh) accepts whatever a cell
# produces, and a cell that stops committing partway through still yields a plausible
# plateau - it reads as convergence, not as failure. This
# script applies both acceptance checks after every attempt and re-runs the cell if it
# fails:
#   1. the run's own WARNINGS: block in output.log (logs.py::_format_warnings_section)
#   2. exp/commit_span_check.py - last commit bin within 20s of the run end, no hole >30s
#
# n21 at 110k sits at ~97% of Sigma-cap, so even good code needs more than one attempt
# sometimes: at 254742e it produced +910s then +990s on consecutive runs.
#
# Usage:
#   MODE=cloudlab bash benchmark/exp/rerun_config_d_until_clean.sh
#   CODE_REF=b1f416a MAX_ATTEMPTS=5 bash benchmark/exp/rerun_config_d_until_clean.sh
set -eo pipefail

CODE_REF=${CODE_REF:-254742e}
MAX_ATTEMPTS=${MAX_ATTEMPTS:-3}
DURATION=${DURATION:-1000}
WARMUP=${WARMUP:-240}
RATE=${RATE:-110000}
MANIFEST=${MANIFEST:-manifest.xml}
LATENCY=${LATENCY:-100ms}
PRIMARY_BW=${PRIMARY_BW:-300mbit}
LOCAL_ORCH=${LOCAL_ORCH:-0}
SKEW=${SKEW:-0.6}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_DIR="$(cd "$BENCH_DIR/.." && pwd)"

# The binary is rsynced from target/release; it is not rebuilt per cell. Refuse to run
# against a tree that is not the code under test rather than silently benchmark whatever
# was built last (that mistake cost a full sweep on 2026-08-12).
HEAD_SHA=$(cd "$REPO_DIR" && git rev-parse --short HEAD)
WANT_SHA=$(cd "$REPO_DIR" && git rev-parse --short "$CODE_REF")
if [[ "$HEAD_SHA" != "$WANT_SHA" ]]; then
    echo "ERROR: HEAD is $HEAD_SHA, expected $WANT_SHA ($CODE_REF)." >&2
    echo "  cd $REPO_DIR && git checkout $CODE_REF && source ~/.cargo/env && cargo build --release --features benchmark" >&2
    exit 1
fi
if ! (cd "$REPO_DIR" && git diff --quiet); then
    echo "ERROR: working tree has uncommitted changes; the binary would not match $CODE_REF." >&2
    echo "  Stash them first: cd $REPO_DIR && git stash push" >&2
    exit 1
fi

RESULTS_DIR="$SCRIPT_DIR/results/config_d_clean_${WANT_SHA}_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

echo "Config (d) sweep, retry-until-clean"
echo "  code:     $WANT_SHA ($CODE_REF)"
echo "  attempts: up to $MAX_ATTEMPTS per cell"
echo "  results:  $RESULTS_DIR"
echo "==========================================="

cd "$BENCH_DIR"

# Largest committee first: it is the cell that fails, so a bad build is caught early.
FAILED_CELLS=()
for N in 21 16 10; do
    LABEL="n${N}_v_rate_imb60"
    RUN_DIR="$RESULTS_DIR/${LABEL}_r${RATE}_run_1"

    BWS=$(python -c "print(','.join(['600']*$N))")
    WEIGHTS=$(python -c "
n=$N; load=$SKEW; x=(n+3)//4
print(','.join(['%g'%(load/x)]*x + ['%g'%((1-load)/(n-x))]*(n-x)))")

    PASSED=0
    for ATTEMPT in $(seq 1 "$MAX_ATTEMPTS"); do
        ATT_DIR="$RESULTS_DIR/attempts/${LABEL}_attempt_${ATTEMPT}"
        mkdir -p "$ATT_DIR"
        echo ""
        echo "--- $LABEL attempt $ATTEMPT/$MAX_ATTEMPTS -> $ATT_DIR ---"

        FAB_CMD="LOCAL_ORCH=$LOCAL_ORCH NODE_OFFSET=${NODE_OFFSET:-0} NODES=$N WORKER_BANDWIDTHS_MBPS=$BWS RATE_WEIGHTS=$WEIGHTS RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP IN_MEMORY_STORE=1 fab cloudlab --manifest=$MANIFEST --latency=$LATENCY --primary-bw=$PRIMARY_BW --log-dir=$ATT_DIR"
        echo "CMD: $FAB_CMD"
        OUTPUT=$(eval "$FAB_CMD" 2>&1) || true
        { echo "CMD: $FAB_CMD"; echo "$OUTPUT"; } > "$ATT_DIR/output.log"

        # A cell that dies in setup leaves no logs, and grepping an empty run for WARNINGS
        # finds none - which would read as a pass. Check for the params file first.
        if [[ ! -f "$ATT_DIR/bench-params.json" ]]; then
            echo "  FAILED: no bench-params.json (run produced no logs; check output.log)"
            continue
        fi

        # `fab` runs logs.py at the end; logs.py hard-fails on any ' ERROR ' line in a
        # worker log (reported as "Worker(s) panicked", which it usually is not). Such a
        # run has no summary at all - and an absent summary has no WARNINGS: block, which
        # would otherwise read as a pass. Require the summary before trusting anything.
        if ! grep -q "SUMMARY:" "$ATT_DIR/output.log"; then
            echo "  FAILED: no SUMMARY in output.log (logs.py refused to parse the run)"
            grep -m1 -E "ParseError|Error:" "$ATT_DIR/output.log" | sed 's/^/    /'
            continue
        fi

        if grep -q "WARNINGS:" "$ATT_DIR/output.log"; then
            echo "  FAILED: $(grep -A4 'WARNINGS:' "$ATT_DIR/output.log" | grep -E '^\s+- ' | sed 's/^ *- //' | tr '\n' ';')"
            continue
        fi

        # Not fatal to the sweep: a cell that cannot be parsed is a failed attempt, not a
        # reason to abandon the remaining attempts and the remaining cells (`set -e` did
        # exactly that on 2026-08-13 and cost the run).
        if ! python "$BENCH_DIR/tps_timeline.py" "$ATT_DIR" --bin 10 > "$ATT_DIR/tps_timeline.csv" 2> "$ATT_DIR/tps_timeline.err"; then
            echo "  FAILED: tps_timeline.py could not parse the run"
            tail -1 "$ATT_DIR/tps_timeline.err" | sed 's/^/    /'
            continue
        fi
        if python "$SCRIPT_DIR/commit_span_check.py" "$RESULTS_DIR/attempts" "$(basename "$ATT_DIR")"; then
            echo "  PASSED"
            cp -r "$ATT_DIR" "$RUN_DIR"
            PASSED=1
            break
        fi
        echo "  FAILED: commit span check"
    done

    if [[ "$PASSED" -eq 0 ]]; then
        echo "!!! $LABEL did not produce a clean run in $MAX_ATTEMPTS attempts"
        FAILED_CELLS+=("$LABEL")
    fi
done

echo ""
echo "==========================================="
echo "Results: $RESULTS_DIR"
if [[ ${#FAILED_CELLS[@]} -gt 0 ]]; then
    echo "UNCLEAN CELLS: ${FAILED_CELLS[*]}"
    echo "Attempts kept under $RESULTS_DIR/attempts for diagnosis."
    exit 1
fi
echo "All cells committed for the full run."
python "$SCRIPT_DIR/commit_span_check.py" "$RESULTS_DIR"
