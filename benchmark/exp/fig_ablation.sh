#!/usr/bin/env bash
# Ablation sweep. Default mode runs a 2x4 grid:
#   {balanced, imbalanced} x {inflated_queue_delay, deflated_qd, deflated_capacity, inflated_cap}
# BASELINE=1 runs only the two non-malicious load-shape cases: balanced and imbalanced.
# Default mode investigates the impact of a single malicious validator misreporting its quorum metrics.
#
# Malicious node: the LAST validator (v_{n-1}). With NODES=4 this is v3, so the case
# annotation is logged in primary-3.log (look for a line tagged "ablation_case").
# Imbalance overloads the FIRST validator (v0) via RATE_WEIGHTS.
#
# MALICIOUS_MODE (new env var, consumed downstream once wired into fabfile.py ->
# consensus/src/lib.rs compute_rerouting; this script only sets it):
#   inflated_queue_delay - last node reports an inflated avg_queue_delay to fake overload
#   deflated_qd          - last node reports a deflated (zero) avg_queue_delay to look unloaded
#   deflated_capacity    - last node reports a deflated (zero) capacity to look slow
#   inflated_cap         - last node reports an inflated capacity to look fast
#
# Supports both docker and cloudlab via MODE env var.
# Fixed: IN_MEMORY_STORE=1
#
# Rerunning: results accumulate in RESULTS_DIR. A cell whose output.log has a SUMMARY:
# block is left alone; a cell that died mid-run is quarantined under failed/ and redone,
# so re-invoking the same command repairs a partial sweep.
#
# Reproduce the figures in commit 4b75808 (n=4, 110k tx/s, cloudlab):
#   BASELINE=1 RETRIES=5 MODE=cloudlab bash benchmark/exp/fig_ablation.sh   # 2 cells x 5
#   BASELINE=0 RETRIES=1 MODE=cloudlab bash benchmark/exp/fig_ablation.sh   # 2x4 grid
#   python benchmark/exp/plot_paper_ablation.py <BASELINE_DIR> <GRID_DIR> \
#       --output-dir figs_ablation_repro --pdf
# BASELINE=0 requires the [ABLATION] blocks in consensus/src/lib.rs to be uncommented;
# while commented out MALICIOUS_MODE reaches the primary but changes nothing.
set -euo pipefail

MODE=${MODE:-docker}

# Common defaults
RETRIES=${RETRIES:-1}
DURATION=${DURATION:-900}
WARMUP=${WARMUP:-240}
BASELINE=${BASELINE:-0}
# Run dirs are numbered RUN_ID_BASE+1 .. RUN_ID_BASE+RETRIES, so shards driven by
# fig_ablation_parallel.sh can write distinct repeats of the same cell into one dir.
RUN_ID_BASE=${RUN_ID_BASE:-0}

# Docker-specific defaults
CPUS_PER_VALIDATOR=${CPUS_PER_VALIDATOR:-8}
LATENCY=${LATENCY:-100ms}
PRIMARY_BW=${PRIMARY_BW:-300mbit}

# CloudLab-specific defaults
MANIFEST=${MANIFEST:-manifest.xml}
LOCAL_ORCH=${LOCAL_ORCH:-0}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# A driver may preset RESULTS_DIR so several concurrent shards accumulate into one dir
# (see fig_ablation_parallel.sh); otherwise derive a fresh timestamped one.
RESULTS_DIR_PRESET=1
if [[ -z "${RESULTS_DIR:-}" ]]; then
    RESULTS_DIR_PRESET=0
    RESULTS_DIR="$SCRIPT_DIR/results/ablation"
    if [[ "$MODE" == "docker" ]]; then
        RESULTS_DIR="${RESULTS_DIR}_docker"
    elif [[ "$MODE" == "cloudlab" ]]; then
        RESULTS_DIR="${RESULTS_DIR}_cloud"
    fi
    if [[ "$MODE" == "docker" && "${CPU_ISOLATE:-1}" == "1" && "$CPUS_PER_VALIDATOR" -gt 0 ]]; then
        RESULTS_DIR="${RESULTS_DIR}_isolate"
    fi
    RESULTS_DIR="${RESULTS_DIR}_$(date +%Y%m%d_%H%M%S)"
fi
mkdir -p "$RESULTS_DIR"
RESULTS_DIR="$(cd "$RESULTS_DIR" && pwd)"
OUTPUT_LOG="$RESULTS_DIR/merged_output.log"
# Concurrent shards must not interleave their output into one file.
if [[ "${NODE_OFFSET:-0}" != "0" ]]; then
    OUTPUT_LOG="$RESULTS_DIR/merged_output_off${NODE_OFFSET}.log"
fi

# plot_paper_ablation.py reads tps_timeline.csv, so a standalone sweep generates it here.
# A driver that preset RESULTS_DIR fans several shards into that one dir, where per-shard
# post-processing would race on the same CSVs; there it stays the driver's job.
if [[ "$RESULTS_DIR_PRESET" == "1" ]]; then
    POSTPROCESS=${POSTPROCESS:-0}
else
    POSTPROCESS=${POSTPROCESS:-1}
fi

# Helpers to generate per-validator comma-separated values (f = (n-1)/3 for BFT).
# make_bw n scenario [fast_bw [slow_bw]]: scenarios: '' (balanced), 'bw_f', 'bw_f1'
make_bw() {
    local n="$1" scenario="$2"
    local fast="${3:-${FAST_BW:-600}}" slow="${4:-${SLOW_BW:-200}}"
    local f=$(( (n - 1) / 3 ))
    local slow_count
    case "$scenario" in
        bw_f)  slow_count=$f ;;
        bw_f1) slow_count=$((f + 1)) ;;
        *)     slow_count=0 ;;
    esac
    local out="" i
    for ((i=0; i < n - slow_count; i++)); do out="${out:+$out,}$fast"; done
    for ((i=0; i < slow_count;     i++)); do out="${out:+$out,}$slow"; done
    echo "$out"
}

# make_weights n: all-ones weight vector (balanced load)
make_weights() {
    local n="$1" out="" i
    for ((i=0; i < n; i++)); do out="${out:+$out,}1"; done
    echo "$out"
}

# make_rate_imb_weights n [load=0.9]: first x=ceil(n/4) nodes share <load> fraction of total load,
# remaining n-x nodes share 1-load. Weights sum to 1: w_high=load/x, w_low=(1-load)/(n-x).
# For n=4 this puts <load> on v0 alone (overloads the first validator).
make_rate_imb_weights() {
    local n="$1"
    local load="${2:-0.9}"
    local x=$(( (n + 3) / 4 ))
    local high; high=$(awk "BEGIN { printf \"%g\", $load/$x }")
    local low; low=$(awk "BEGIN { printf \"%g\", (1-$load)/($n-$x) }")
    local out="" i
    for ((i=0; i < x; i++)); do out="${out:+$out,}$high"; done
    for ((i=x; i < n; i++)); do out="${out:+$out,}$low"; done
    echo "$out"
}

# Each entry: "label|BANDWIDTHS_MBPS|RATE_WEIGHTS|NODES|RATES|MALICIOUS_MODE".
# Balanced BW throughout. In baseline mode, consensus disables load-balancing and
# compute_rerouting, so MALICIOUS_MODE would have no effect.
NODES=${NODES:-4}
RATE=${RATE:-110000}
if [[ "$BASELINE" == "1" ]]; then
    CONFIGS=(
        "n4_bal|$(make_bw 4 '')|$(make_weights 4)|$NODES|$RATE|"
        "n4_imb|$(make_bw 4 '')|$(make_rate_imb_weights 4 0.9)|$NODES|$RATE|"
    )
else
    CONFIGS=(
        "n4_bal_inflqd|$(make_bw 4 '')|$(make_weights 4)|$NODES|$RATE|inflated_queue_delay"
        "n4_bal_deflqd|$(make_bw 4 '')|$(make_weights 4)|$NODES|$RATE|deflated_qd"
        "n4_bal_deflcap|$(make_bw 4 '')|$(make_weights 4)|$NODES|$RATE|deflated_capacity"
        "n4_bal_inflcap|$(make_bw 4 '')|$(make_weights 4)|$NODES|$RATE|inflated_cap"
        "n4_imb_inflqd|$(make_bw 4 '')|$(make_rate_imb_weights 4 0.9)|$NODES|$RATE|inflated_queue_delay"
        "n4_imb_deflqd|$(make_bw 4 '')|$(make_rate_imb_weights 4 0.9)|$NODES|$RATE|deflated_qd"
        "n4_imb_deflcap|$(make_bw 4 '')|$(make_rate_imb_weights 4 0.9)|$NODES|$RATE|deflated_capacity"
        "n4_imb_inflcap|$(make_bw 4 '')|$(make_rate_imb_weights 4 0.9)|$NODES|$RATE|inflated_cap"
    )
fi

# CASES: optional comma-separated substrings; keep only configs whose label matches one.
# e.g. CASES=imb_inflqd runs the single imbalanced inflated-queue-delay case.
CASES=${CASES:-}
if [[ -n "$CASES" ]]; then
    IFS=',' read -ra CASE_FILTERS <<< "$CASES"
    FILTERED=()
    for CONFIG in "${CONFIGS[@]}"; do
        IFS='|' read -r LABEL _ _ _ _ _ <<< "$CONFIG"
        for FILTER in "${CASE_FILTERS[@]}"; do
            if [[ "$LABEL" == *"$FILTER"* ]]; then
                FILTERED+=("$CONFIG")
                break
            fi
        done
    done
    if [[ ${#FILTERED[@]} -eq 0 ]]; then
        echo "ERROR: CASES='$CASES' matched no config labels" >&2
        exit 1
    fi
    CONFIGS=("${FILTERED[@]}")
fi

# Validate unique labels
declare -A seen_labels
for CONFIG in "${CONFIGS[@]}"; do
    IFS='|' read -r LABEL _ _ _ _ _ <<< "$CONFIG"
    if [[ -v seen_labels["$LABEL"] ]]; then
        echo "ERROR: Duplicate label '$LABEL' in CONFIGS" >&2
        exit 1
    fi
    seen_labels["$LABEL"]=1
done

echo "Ablation sweep ($MODE)"
if [[ "$BASELINE" == "1" ]]; then
    echo "Configs: ${#CONFIGS[@]} ({bal,imb}; no malicious mode in baseline)"
else
    echo "Configs: ${#CONFIGS[@]} (2x4: {bal,imb} x {inflated_queue_delay,deflated_qd,deflated_capacity,inflated_cap})"
fi
echo "Duration: ${DURATION}s, Warmup: ${WARMUP}s, Retries: ${RETRIES}, Baseline: ${BASELINE}"
[[ -n "$CASES" ]] && echo "CASES filter: $CASES -> $(printf '%s ' "${CONFIGS[@]%%|*}")"
echo "Results: $RESULTS_DIR"
echo "==========================================="

cd "$BENCH_DIR"

# CPU isolation (shared machine noise reduction, docker mode only)
if [[ "$MODE" == "docker" && "${CPU_ISOLATE:-1}" == "1" && "$CPUS_PER_VALIDATOR" -gt 0 ]]; then
    MAX_NODES=0
    for CONFIG in "${CONFIGS[@]}"; do
        IFS='|' read -r _ _ _ N _ _ <<< "$CONFIG"
        (( N > MAX_NODES )) && MAX_NODES=$N
    done
    sudo "$BENCH_DIR/cpu_isolate.sh" setup \
        --nodes="$MAX_NODES" \
        --cpus-per-validator="$CPUS_PER_VALIDATOR" \
        --numa-node-cpus="${NUMA_NODE_CPUS:-32}"
    trap 'sudo "$BENCH_DIR/cpu_isolate.sh" teardown' EXIT
fi

for CONFIG in "${CONFIGS[@]}"; do
    IFS='|' read -r LABEL BANDWIDTHS_MBPS RATE_WEIGHTS NODES RATES_STR MALICIOUS_MODE <<< "$CONFIG"
    IFS=',' read -ra RATES <<< "$RATES_STR"

    for RATE in "${RATES[@]}"; do
        for RETRY in $(seq 1 "$RETRIES"); do
            RUN_ID=$((RUN_ID_BASE + RETRY))
            RUN_DIR="$RESULTS_DIR/${LABEL}_r${RATE}_run_${RUN_ID}"

            # No SUMMARY: block means the cell died mid-run. Skipping it on the strength
            # of the directory alone would make a re-invocation preserve exactly the cells
            # that need redoing, so quarantine the corpse and run the cell again.
            if [[ -d "$RUN_DIR" ]]; then
                if grep -q 'SUMMARY:' "$RUN_DIR/output.log" 2>/dev/null; then
                    echo "Skipping $RUN_DIR (already complete)"
                    continue
                fi
                QUARANTINE="$RESULTS_DIR/failed/${LABEL}_r${RATE}_run_${RUN_ID}_$(date +%Y%m%d_%H%M%S)"
                mkdir -p "$RESULTS_DIR/failed"
                mv "$RUN_DIR" "$QUARANTINE"
                echo "Redoing $RUN_DIR (no SUMMARY); previous attempt kept at $QUARANTINE"
            fi
            mkdir -p "$RUN_DIR"

            echo ""
            CASE_DESC="baseline=$BASELINE"
            FAB_ENV="NODES=$NODES WORKER_BANDWIDTHS_MBPS=$BANDWIDTHS_MBPS RATE_WEIGHTS=$RATE_WEIGHTS RATE=$RATE BASELINE=$BASELINE DURATION=$DURATION WARMUP=$WARMUP IN_MEMORY_STORE=1"
            if [[ -n "$MALICIOUS_MODE" ]]; then
                CASE_DESC="malicious=$MALICIOUS_MODE $CASE_DESC"
                FAB_ENV="MALICIOUS_MODE=$MALICIOUS_MODE $FAB_ENV"
            fi

            echo "--- $LABEL | bw=$BANDWIDTHS_MBPS rate_w=$RATE_WEIGHTS rate=$RATE $CASE_DESC Run: $RETRY/$RETRIES ---"

            if [[ "$MODE" == "docker" ]]; then
                FAB_CMD="$FAB_ENV fab docker --cpus-per-validator=$CPUS_PER_VALIDATOR --latency=$LATENCY --primary-bw=$PRIMARY_BW --log-dir=$RUN_DIR"
            elif [[ "$MODE" == "cloudlab" ]]; then
                FAB_CMD="LOCAL_ORCH=$LOCAL_ORCH NODE_OFFSET=${NODE_OFFSET:-0} $FAB_ENV fab cloudlab --manifest=$MANIFEST --latency=$LATENCY --primary-bw=$PRIMARY_BW --log-dir=$RUN_DIR"
            else
                echo "ERROR: Unknown MODE=$MODE (expected docker or cloudlab)" >&2
                exit 1
            fi

            echo "CMD: LABEL=${LABEL}_r${RATE}_run_${RUN_ID} $FAB_CMD" | tee -a "$OUTPUT_LOG"
            OUTPUT=$(eval "$FAB_CMD" 2>&1) || true
            { echo "CMD: LABEL=${LABEL}_r${RATE}_run_${RUN_ID} $FAB_CMD"; echo "$OUTPUT"; } > "$RUN_DIR/output.log"
            echo "$OUTPUT" >> "$OUTPUT_LOG"

            if echo "$OUTPUT" | grep -qiE 'panic|error|failed'; then
                echo "WARNING: Errors detected in $LABEL rate=$RATE run=$RETRY"
                echo "$OUTPUT" | grep -iE 'panic|error|failed' > "$RUN_DIR/errors.log"
            fi

            sleep 2
        done
    done
done


SPAN_STATUS=0
if [[ "$POSTPROCESS" == "1" ]]; then
    echo ""
    echo "=== generating tps timelines ==="
    bash "$SCRIPT_DIR/tps_timeline_batch.sh" "$RESULTS_DIR" 2>&1 | tee -a "$OUTPUT_LOG"
    echo ""
    echo "=== commit span check ==="
    python "$SCRIPT_DIR/commit_span_check.py" "$RESULTS_DIR" 2>&1 | tee -a "$OUTPUT_LOG" \
        || SPAN_STATUS=$?
fi

echo ""
echo "==========================================="
echo "Ablation sweep complete. Results in $RESULTS_DIR"
if (( SPAN_STATUS != 0 )); then
    # A truncated run still carries a SUMMARY:, so the skip above cannot catch it.
    echo "ERROR: commit_span_check did not pass (see above). A run that stopped early still" >&2
    echo "       carries a SUMMARY:, so re-invoking skips it - delete those run dirs under" >&2
    echo "       $RESULTS_DIR first, then re-invoke to redo them." >&2
    exit "$SPAN_STATUS"
fi
