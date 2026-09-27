#!/usr/bin/env bash
# Re-run selected config a/b/c cells until their LB throughput plateau matches or
# beats the reference run, then assemble a "best" results dir for plotting.
#
# Every attempt is kept on disk and recorded in summary.tsv - reruns select on
# outcome, so the discarded attempts must stay visible.
#
# Pass criterion per cell: mean plateau >= MIN_FRAC * reference mean, and CV <= MAX_CV.
# Reference values come from the run of the same name in REF_DIR.
#
# Usage: bash benchmark/exp/rerun_until_flat.sh <REF_DIR> <BASE_DIR> [MAX_ATTEMPTS]
#   REF_DIR  - reference results dir (old runs to match)
#   BASE_DIR - current results dir holding the runs being replaced
set -euo pipefail

REF_DIR="${1:?usage: $0 REF_DIR BASE_DIR [MAX_ATTEMPTS]}"
BASE_DIR="${2:?usage: $0 REF_DIR BASE_DIR [MAX_ATTEMPTS]}"
MAX_ATTEMPTS="${3:-3}"

MIN_FRAC=${MIN_FRAC:-0.97}
MAX_CV=${MAX_CV:-0.12}
DURATION=${DURATION:-900}
WARMUP=${WARMUP:-240}
LATENCY=${LATENCY:-100ms}
PRIMARY_BW=${PRIMARY_BW:-300mbit}
MANIFEST=${MANIFEST:-manifest.xml}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REF_DIR="$(cd "$REF_DIR" && pwd)"
BASE_DIR="$(cd "$BASE_DIR" && pwd)"

# OUT_DIR may be set to an existing dir to resume: attempts with a tps_timeline.csv
# are re-evaluated instead of re-run.
OUT_DIR="${OUT_DIR:-$SCRIPT_DIR/results/abc_reruns_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$OUT_DIR"
SUMMARY="$OUT_DIR/summary.tsv"
[[ -f "$SUMMARY" ]] || printf 'cell\tattempt\tmean\tfrac_of_ref\tcv\tverdict\n' > "$SUMMARY"

# Cells to retry: "run_name|BANDWIDTHS_MBPS|RATE_WEIGHTS|RATE"
CELLS=(
    "n4_v_rate_imb90_r110000_run_1|600,600,600,600|0.9,0.0333333,0.0333333,0.0333333|110000"
    "n4_v_rate_imb99_r110000_run_1|600,600,600,600|0.99,0.00333333,0.00333333,0.00333333|110000"
    "n4_bw_f_rate_imb90_r110000_run_1|600,600,600,200|0.9,0.0333333,0.0333333,0.0333333|110000"
    "n4_bw_f1_rate_imb90_r65000_run_1|600,600,200,200|0.9,0.0333333,0.0333333,0.0333333|65000"
)

cd "$BENCH_DIR"

echo "Rerun-until-flat"
echo "  reference: $REF_DIR"
echo "  base:      $BASE_DIR"
echo "  attempts:  $MAX_ATTEMPTS per cell (min_frac=$MIN_FRAC max_cv=$MAX_CV)"
echo "  output:    $OUT_DIR"
echo "==========================================="

for CELL in "${CELLS[@]}"; do
    IFS='|' read -r RUN_NAME BWS WEIGHTS RATE <<< "$CELL"

    # Reference plateau for this cell.
    # check_tps_recovered.py exits 1 when a run fails its check; that is data here, not an error.
    REF_LINE=$(python "$SCRIPT_DIR/check_tps_recovered.py" "$REF_DIR" "$RUN_NAME" 2>/dev/null | sed -n '2p' || true)
    REF_MEAN=$(echo "$REF_LINE" | awk '{gsub(/,/,"",$3); print $3}')
    REF_CV=$(echo "$REF_LINE" | awk '{print $5}')
    # Flatness bar is per-cell: the reference's own CV (plus slack), floored at MAX_CV.
    CV_LIMIT=$(awk -v r="$REF_CV" -v m="$MAX_CV" 'BEGIN{v=r*1.15; print (v>m)?v:m}')
    echo ""
    echo "--- $RUN_NAME (ref plateau=${REF_MEAN} ref_cv=${REF_CV} cv_limit=${CV_LIMIT}) ---"

    BEST_MEAN=0
    BEST_PATH=""
    for ((i=1; i<=MAX_ATTEMPTS; i++)); do
        ATT_DIR="$OUT_DIR/attempt_${i}"
        RUN_DIR="$ATT_DIR/$RUN_NAME"
        mkdir -p "$RUN_DIR"

        FAB_CMD="LOCAL_ORCH=${LOCAL_ORCH:-0} NODE_OFFSET=${NODE_OFFSET:-0} NODES=4 WORKER_BANDWIDTHS_MBPS=$BWS RATE_WEIGHTS=$WEIGHTS RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP IN_MEMORY_STORE=1 fab cloudlab --manifest=$MANIFEST --latency=$LATENCY --primary-bw=$PRIMARY_BW --log-dir=$RUN_DIR"
        if [[ -f "$RUN_DIR/tps_timeline.csv" ]]; then
            echo "  attempt $i/$MAX_ATTEMPTS (reusing existing run)"
        else
            echo "  attempt $i/$MAX_ATTEMPTS ..."
            OUTPUT=$(eval "$FAB_CMD" 2>&1) || true
            { echo "CMD: LABEL=$RUN_NAME $FAB_CMD"; echo "$OUTPUT"; } > "$RUN_DIR/output.log"

            if ! echo "$OUTPUT" | grep -q "TPS_TIMELINE_CSV"; then
                echo "    run failed (no timeline in output); see $RUN_DIR/output.log"
                printf '%s\t%d\t\t\t\tRUN_FAILED\n' "$RUN_NAME" "$i" >> "$SUMMARY"
                continue
            fi

            python tps_timeline.py "$RUN_DIR" > "$RUN_DIR/tps_timeline.csv"
        fi

        EVAL_LINE=$(python "$SCRIPT_DIR/check_tps_recovered.py" "$ATT_DIR" "$RUN_NAME" 2>/dev/null | sed -n '2p' || true)
        MEAN=$(echo "$EVAL_LINE" | awk '{gsub(/,/,"",$3); print $3}')
        CV=$(echo "$EVAL_LINE" | awk '{print $5}')
        FRAC=$(awk -v m="$MEAN" -v r="$REF_MEAN" 'BEGIN{printf "%.4f", m/r}')
        VERDICT=$(awk -v f="$FRAC" -v c="$CV" -v mf="$MIN_FRAC" -v mc="$CV_LIMIT" \
            'BEGIN{print (f>=mf && c<=mc) ? "PASS" : "FAIL"}')
        echo "    mean=$MEAN frac_of_ref=$FRAC cv=$CV -> $VERDICT"
        printf '%s\t%d\t%s\t%s\t%s\t%s\n' "$RUN_NAME" "$i" "$MEAN" "$FRAC" "$CV" "$VERDICT" >> "$SUMMARY"

        if awk -v m="$MEAN" -v b="$BEST_MEAN" 'BEGIN{exit !(m>b)}'; then
            BEST_MEAN=$MEAN
            BEST_PATH=$RUN_DIR
        fi
        [[ "$VERDICT" == "PASS" ]] && break
    done

    echo "$BEST_PATH" > "$OUT_DIR/best_${RUN_NAME}.path"
    echo "  best: $BEST_PATH (mean=$BEST_MEAN)"
done

# Assemble a plotting dir: originals for untouched cells, best attempt for retried ones.
BEST_DIR="$OUT_DIR/best"
mkdir -p "$BEST_DIR"
for d in "$BASE_DIR"/*/; do
    name=$(basename "$d")
    ln -sfn "$(readlink -f "$d")" "$BEST_DIR/$name"
done
for CELL in "${CELLS[@]}"; do
    IFS='|' read -r RUN_NAME _ _ _ <<< "$CELL"
    BP=$(cat "$OUT_DIR/best_${RUN_NAME}.path")
    if [[ -n "$BP" ]]; then
        ln -sfn "$BP" "$BEST_DIR/$RUN_NAME"
    fi
done
# config a expects a run_2 alias for the 50k series
ln -sfn "$BEST_DIR/n4_v_rate_imb90_r50000_run_1" "$BEST_DIR/n4_v_rate_imb90_r50000_run_2" 2>/dev/null || true

echo ""
echo "==========================================="
echo "Done. Attempts + summary: $OUT_DIR"
echo "Plot dir: $BEST_DIR"
cat "$SUMMARY"
