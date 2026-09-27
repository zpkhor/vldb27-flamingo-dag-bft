#!/usr/bin/env bash
# Reproduce benchmark/hotspot_shift.pdf: apply the client patch, run, unapply, plot.
#
# The hot region moves from validator 0 to validator 1 at 2.5*WARMUP seconds (600s
# with WARMUP=240) and the LB algorithm has to re-converge, giving the figure its two
# dips. The client-side shift lives in benchmark/exp/hotspot_shift.patch instead of on
# the branch because cloudlab_bench.py hardcodes "region 0 <-> region 1" for this
# figure only.
#
# The patch is applied only around the fab run (fab compiles locally and distributes
# the binaries) and reverted immediately after, including on abort. Nothing else needs
# patching: consensus/src/lib.rs is already identical to zp/hotspot-shift-v2, and this
# branch carries cffe261, so the run uses the 2f+1 migration confirmation threshold.
#
# The reference figure benchmark/hotspot_shift.pdf is never overwritten; this writes
# to $OUT_PDF (default benchmark/hotspot_shift_repro.pdf) so the two can be compared.
#
# Usage:
#   source activate narwhal39
#   bash benchmark/exp/run_hotspot_shift.sh
#
# Env: RETRIES, DURATION, WARMUP, RATE, NODES, RATE_WEIGHTS, BANDWIDTHS_MBPS,
#      LATENCY, PRIMARY_BW, MANIFEST, LOCAL_ORCH, NODE_OFFSET, LABEL,
#      RESULTS_DIR, OUT_PDF, REF_RUN_DIR
set -euo pipefail

RETRIES=${RETRIES:-1}
DURATION=${DURATION:-1400}
WARMUP=${WARMUP:-240}
RATE=${RATE:-110000}
NODES=${NODES:-4}

LATENCY=${LATENCY:-100ms}
PRIMARY_BW=${PRIMARY_BW:-300mbit}
MANIFEST=${MANIFEST:-manifest.xml}
LOCAL_ORCH=${LOCAL_ORCH:-0}
NODE_OFFSET=${NODE_OFFSET:-0}

# imb90 on 4 nodes: validator 0 takes 90% of the load, the other three split 10%.
BANDWIDTHS_MBPS=${BANDWIDTHS_MBPS:-600,600,600,600}
RATE_WEIGHTS=${RATE_WEIGHTS:-0.9,0.0333333,0.0333333,0.0333333}
LABEL=${LABEL:-n4_v_rate_imb90}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_DIR="$(cd "$BENCH_DIR/.." && pwd)"
PATCH="$SCRIPT_DIR/hotspot_shift.patch"

OUT_PDF=${OUT_PDF:-$BENCH_DIR/hotspot_shift_repro.pdf}
RESULTS_DIR="${RESULTS_DIR:-$SCRIPT_DIR/results/tps_timeline_lb_cloud_hotspot_$(date +%Y%m%d_%H%M%S)}"
# April run behind the committed reference figure; scored alongside the new run.
REF_RUN_DIR="${REF_RUN_DIR:-$SCRIPT_DIR/results/tps_timeline_lb_cloud_hotspot_20260417_171708/n4_v_rate_imb90_r110000_run_1}"

fail() { echo "ERROR: $*" >&2; exit 1; }

command -v fab >/dev/null || fail "fab not found on PATH. Run: source activate narwhal39"
# fab compiles the node/client itself; non-interactive shells miss rustup's PATH entry.
if ! command -v cargo >/dev/null && [[ -f "$HOME/.cargo/env" ]]; then
    # shellcheck disable=SC1091
    source "$HOME/.cargo/env"
fi
command -v cargo >/dev/null || fail "cargo not found on PATH (fab compiles locally)"
[[ -f "$PATCH" ]] || fail "missing patch: $PATCH"

cd "$REPO_DIR"

if git apply --reverse --check "$PATCH" 2>/dev/null; then
    fail "patch is already applied (a previous run aborted?). Revert it with:
    git -C $REPO_DIR apply --reverse $PATCH"
fi

# The unapply is `git apply --reverse`, so refuse to start if a patched file already
# carries unrelated edits that the reverse would silently fold into.
PATCHED_FILES=$(git apply --numstat "$PATCH" | awk '{print $3}')
DIRTY=$(git status --porcelain -- $PATCHED_FILES)
[[ -z "$DIRTY" ]] || fail $'the files this patch touches have uncommitted changes:\n'"$DIRTY"

git apply --check "$PATCH" || fail "patch does not apply to the current tree: $PATCH"

PATCH_APPLIED=0
unapply() {
    if [[ $PATCH_APPLIED -eq 1 ]]; then
        git -C "$REPO_DIR" apply --reverse "$PATCH"
        PATCH_APPLIED=0
        echo "Patch reverted: $PATCH"
    fi
}
# INT/TERM must exit, not just unapply: bash resumes the script after a signal
# handler returns, which would run the remaining retries with the patch already
# reverted -- i.e. silently produce runs with no hot-spot shift at all.
on_signal() {
    echo "Interrupted; aborting sweep." >&2
    unapply
    exit 130
}
trap unapply EXIT
trap on_signal INT TERM

mkdir -p "$RESULTS_DIR"
OUTPUT_LOG="$RESULTS_DIR/merged_output.log"

echo "Hot-spot shift sweep (cloudlab)"
echo "Nodes: $NODES, rate: $RATE, weights: $RATE_WEIGHTS"
echo "Duration: ${DURATION}s, Warmup: ${WARMUP}s, shift at $((WARMUP * 5 / 2))s, Retries: ${RETRIES}"
echo "Patch:   $PATCH"
echo "Results: $RESULTS_DIR"
echo "Figure:  $OUT_PDF"
echo "==========================================="

git apply "$PATCH"
PATCH_APPLIED=1
echo "Patch applied: $PATCH"

cd "$BENCH_DIR"

for RETRY in $(seq 1 "$RETRIES"); do
    RUN_DIR="$RESULTS_DIR/${LABEL}_r${RATE}_run_${RETRY}"

    if [[ -d "$RUN_DIR" ]]; then
        echo "Skipping $RUN_DIR (already exists)"
        continue
    fi
    mkdir -p "$RUN_DIR"

    echo ""
    echo "--- $LABEL | bw=$BANDWIDTHS_MBPS rate_w=$RATE_WEIGHTS rate=$RATE Run: $RETRY/$RETRIES ---"

    FAB_CMD="HOTSPOT_SHIFT=1 LOCAL_ORCH=$LOCAL_ORCH NODE_OFFSET=$NODE_OFFSET NODES=$NODES WORKER_BANDWIDTHS_MBPS=$BANDWIDTHS_MBPS RATE_WEIGHTS=$RATE_WEIGHTS RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP IN_MEMORY_STORE=1 fab cloudlab --manifest=$MANIFEST --latency=$LATENCY --primary-bw=$PRIMARY_BW --log-dir=$RUN_DIR"

    echo "CMD: LABEL=${LABEL}_r${RATE}_run_${RETRY} $FAB_CMD" | tee -a "$OUTPUT_LOG"
    OUTPUT=$(eval "$FAB_CMD" 2>&1) || true
    { echo "CMD: LABEL=${LABEL}_r${RATE}_run_${RETRY} $FAB_CMD"; echo "$OUTPUT"; } > "$RUN_DIR/output.log"
    echo "$OUTPUT" >> "$OUTPUT_LOG"

    if echo "$OUTPUT" | grep -qiE 'panic|error|failed'; then
        echo "WARNING: Errors detected in $LABEL rate=$RATE run=$RETRY"
        echo "$OUTPUT" | grep -iE 'panic|error|failed' > "$RUN_DIR/errors.log"
    fi

    sleep 2
done

# The patch is only needed for compile+run; plotting reads the run dirs.
unapply
trap - EXIT INT TERM

echo ""
echo "==========================================="
echo "Runs complete. Plotting from $RESULTS_DIR"

for RETRY in $(seq 1 "$RETRIES"); do
    RUN_DIR="$RESULTS_DIR/${LABEL}_r${RATE}_run_${RETRY}"
    grep -q 'SUMMARY:' "$RUN_DIR/output.log" \
        || fail "no SUMMARY: block in $RUN_DIR/output.log — the run died before finishing"

    python "$BENCH_DIR/tps_timeline.py" "$RUN_DIR" > "$RUN_DIR/tps_timeline.csv"
    RUN_PDF="$RESULTS_DIR/hotspot_shift_run${RETRY}.pdf"
    python "$SCRIPT_DIR/hotspot.py" --run-dir "$RUN_DIR" -o "$RUN_PDF"
    # With RETRIES>1 every run gets its own PDF; run 1 is the one published to OUT_PDF.
    if [[ $RETRY -eq 1 ]]; then
        cp "$RUN_PDF" "$OUT_PDF"
    fi
done

echo ""
echo "=== window scores (new run vs April reference) ==========="
SCORE_DIRS=()
for RETRY in $(seq 1 "$RETRIES"); do
    SCORE_DIRS+=("$RESULTS_DIR/${LABEL}_r${RATE}_run_${RETRY}")
done
if [[ -d "$REF_RUN_DIR" ]]; then
    SCORE_DIRS+=("$REF_RUN_DIR")
else
    echo "NOTE: reference run dir not found, scoring the new run only: $REF_RUN_DIR"
fi
python "$SCRIPT_DIR/check_hotspot_shift.py" "${SCORE_DIRS[@]}"

echo ""
echo "==========================================="
echo "Figure:    $OUT_PDF"
echo "Reference: $BENCH_DIR/hotspot_shift.pdf"
echo "Results:   $RESULTS_DIR"
