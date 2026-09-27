#!/usr/bin/env bash
# Load balancing figure sweep: validator imbalance scenarios only for tps timeline
# Supports both docker and cloudlab via MODE env var.
# Fixed: IN_MEMORY_STORE=1
# Results accumulate in a fixed dir; existing RUN_DIRs are skipped.
set -euo pipefail

MODE=${MODE:-docker}

# Common defaults
RETRIES=${RETRIES:-1}
# 1650s, matching fig_3_tps_timeline_lb.sh. At 90k the n=21 cell issues its last migration
# near t=760s, so a 1000s cell ends with the tail still inside the settling window and
# convergence_check.py's early/late windows overlap it.
DURATION=${DURATION:-1650}
WARMUP=${WARMUP:-240}

# Docker-specific defaults
CPUS_PER_VALIDATOR=${CPUS_PER_VALIDATOR:-8}
LATENCY=${LATENCY:-100ms}
PRIMARY_BW=${PRIMARY_BW:-300mbit}

# CloudLab-specific defaults
MANIFEST=${MANIFEST:-manifest.xml}
LOCAL_ORCH=${LOCAL_ORCH:-0}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results/tps_timeline_lb_nodes"
if [[ "$MODE" == "docker" ]]; then
    RESULTS_DIR="${RESULTS_DIR}_docker"
elif [[ "$MODE" == "cloudlab" ]]; then
    RESULTS_DIR="${RESULTS_DIR}_cloud"
fi
if [[ "$MODE" == "docker" && "${CPU_ISOLATE:-1}" == "1" && "$CPUS_PER_VALIDATOR" -gt 0 ]]; then
    RESULTS_DIR="${RESULTS_DIR}_isolate"
fi
RESULTS_DIR="${RESULTS_DIR}_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"
OUTPUT_LOG="$RESULTS_DIR/merged_output.log"

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

# make_weights n: all-ones weight vector
make_weights() {
    local n="$1" out="" i
    for ((i=0; i < n; i++)); do out="${out:+$out,}1"; done
    echo "$out"
}

# make_rate_imb_weights n [load=0.9]: first x=ceil(n/4) nodes share <load> fraction of total load,
# remaining n-x nodes share 1-load. Weights sum to 1: w_high=load/x, w_low=(1-load)/(n-x).
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

# Each entry: "label|BANDWIDTHS_MBPS|RATE_WEIGHTS|NODES|RATES"
# /home/zpkhor/narwhal-validator/benchmark/exp/results/tps_timeline_lb_nodes_cloud_20260415_114941
#
# Rate is 90k, not the 110k of configs (a)-(c): under the 2f+1 reroute quorum (cffe261) the
# largest cell that still stops migrating is n=21 at 90k (results/config_d_2f1_maxrate_
# 20260814_034603 - 90k converges, 92.5k does not). Panel (d) compares node counts at one
# fixed rate, so every cell here uses it, n=4 included - the (b) n4_v_rate_imb60 cell is at
# 110k and can no longer double as (d)'s n=4 point.
#
# n=21 and n=16 lead: they are the cells whose convergence is in question, and the manifest
# has 23 machines so they cannot share the cluster with anything else.
CONFIGS=(
    # (d) varying nodes: imb60, 90k, balanced BW  (use longer duration as different run)
    "n21_v_rate_imb60|$(make_bw 21 '')|$(make_rate_imb_weights 21 0.6)|21|90000"
    "n16_v_rate_imb60|$(make_bw 16 '')|$(make_rate_imb_weights 16 0.6)|16|90000"
    "n10_v_rate_imb60|$(make_bw 10 '')|$(make_rate_imb_weights 10 0.6)|10|90000"
    "n4_v_rate_imb60|$(make_bw 4 '')|$(make_rate_imb_weights 4 0.6)|4|90000"
)

# Validate unique labels
declare -A seen_labels
for CONFIG in "${CONFIGS[@]}"; do
    IFS='|' read -r LABEL _ _ _ _ <<< "$CONFIG"
    if [[ -v seen_labels["$LABEL"] ]]; then
        echo "ERROR: Duplicate label '$LABEL' in CONFIGS" >&2
        exit 1
    fi
    seen_labels["$LABEL"]=1
done

echo "Baseline sweep ($MODE)"
echo "Configs: ${#CONFIGS[@]} (per-config rates)"
echo "Duration: ${DURATION}s, Warmup: ${WARMUP}s, Retries: ${RETRIES}"
echo "Results: $RESULTS_DIR"
echo "==========================================="

cd "$BENCH_DIR"

# CPU isolation (shared machine noise reduction, docker mode only)
if [[ "$MODE" == "docker" && "${CPU_ISOLATE:-1}" == "1" && "$CPUS_PER_VALIDATOR" -gt 0 ]]; then
    MAX_NODES=0
    for CONFIG in "${CONFIGS[@]}"; do
        IFS='|' read -r _ _ _ N _ <<< "$CONFIG"
        (( N > MAX_NODES )) && MAX_NODES=$N
    done
    sudo "$BENCH_DIR/cpu_isolate.sh" setup \
        --nodes="$MAX_NODES" \
        --cpus-per-validator="$CPUS_PER_VALIDATOR" \
        --numa-node-cpus="${NUMA_NODE_CPUS:-32}"
    trap 'sudo "$BENCH_DIR/cpu_isolate.sh" teardown' EXIT
fi

for CONFIG in "${CONFIGS[@]}"; do
    IFS='|' read -r LABEL BANDWIDTHS_MBPS RATE_WEIGHTS NODES RATES_STR <<< "$CONFIG"
    IFS=',' read -ra RATES <<< "$RATES_STR"

    for RATE in "${RATES[@]}"; do
        for RETRY in $(seq 1 "$RETRIES"); do
            RUN_DIR="$RESULTS_DIR/${LABEL}_r${RATE}_run_${RETRY}"

            if [[ -d "$RUN_DIR" ]]; then
                echo "Skipping $RUN_DIR (already exists)"
                continue
            fi
            mkdir -p "$RUN_DIR"

            echo ""
            echo "--- $LABEL | bw=$BANDWIDTHS_MBPS rate_w=$RATE_WEIGHTS rate=$RATE Run: $RETRY/$RETRIES ---"

            if [[ "$MODE" == "docker" ]]; then
                FAB_CMD="NODES=$NODES WORKER_BANDWIDTHS_MBPS=$BANDWIDTHS_MBPS RATE_WEIGHTS=$RATE_WEIGHTS RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP IN_MEMORY_STORE=1 fab docker --cpus-per-validator=$CPUS_PER_VALIDATOR --latency=$LATENCY --primary-bw=$PRIMARY_BW --log-dir=$RUN_DIR"
            elif [[ "$MODE" == "cloudlab" ]]; then
                FAB_CMD="LOCAL_ORCH=$LOCAL_ORCH NODE_OFFSET=${NODE_OFFSET:-0} NODES=$NODES WORKER_BANDWIDTHS_MBPS=$BANDWIDTHS_MBPS RATE_WEIGHTS=$RATE_WEIGHTS RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP IN_MEMORY_STORE=1 fab cloudlab --manifest=$MANIFEST --latency=$LATENCY --primary-bw=$PRIMARY_BW --log-dir=$RUN_DIR"
            else
                echo "ERROR: Unknown MODE=$MODE (expected docker or cloudlab)" >&2
                exit 1
            fi

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
    done
done


echo ""
echo "==========================================="
echo "Baseline sweep complete. Results in $RESULTS_DIR"
