#!/usr/bin/env bash
# Motivation figure sweep: load-balancing mode (NEW_SCHEDULER=1, no BASELINE).
# Covers validator/executor imbalance and BW-limited scenarios (configs 2-9 of baseline script).
# Balanced config omitted — with LB active it mirrors the baseline balanced run.
# Supports both docker and cloudlab via MODE env var.
# Fixed: NEW_SCHEDULER=1 NUM_EXECUTORS=3 NO_SEND_PAYMENT=1 IN_MEMORY_STORE=1
# Results accumulate in a fixed dir; existing RUN_DIRs are skipped.
set -euo pipefail

MODE=${MODE:-docker}

# Common defaults
RETRIES=${RETRIES:-1}
DURATION=${DURATION:-180}
WARMUP=${WARMUP:-30}

# Docker-specific defaults
CPUS_PER_VALIDATOR=${CPUS_PER_VALIDATOR:-8}
LATENCY=${LATENCY:-100ms}
PRIMARY_BW=${PRIMARY_BW:-300mbit}

# CloudLab-specific defaults
MANIFEST=${MANIFEST:-manifest.xml}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results/motivation_lb"
if [[ "$MODE" == "docker" ]]; then
    RESULTS_DIR="${RESULTS_DIR}_docker"
elif [[ "$MODE" == "cloudlab" ]]; then
    RESULTS_DIR="${RESULTS_DIR}_cloud"
fi
if [[ "$MODE" == "docker" && "${CPU_ISOLATE:-1}" == "1" && "$CPUS_PER_VALIDATOR" -gt 0 ]]; then
    RESULTS_DIR="${RESULTS_DIR}_isolate"
fi
mkdir -p "$RESULTS_DIR"
OUTPUT_LOG="$RESULTS_DIR/merged_output.log"

# Each entry: "label|BANDWIDTHS_MBPS|RATE_WEIGHTS|E_SKEW_WEIGHTS|RATES"
# n=4, f=1, m=3 executors
# Configs 2-7: rate sweep = n4_balanced rates (50000,110000,125000)
# Config 8 (bw_f):  ~80% of balanced rates
# Config 9 (bw_f1): ~50% of balanced rates
CONFIGS=(
    # 2. 90% load on first validator
    "n4_v_rate_imb90|600,600,600,600|27,1,1,1|1,1,1|50000,110000,125000"

    # 3. 60% load on first validator
    "n4_v_rate_imb60|600,600,600,600|4.5,1,1,1|1,1,1|50000,110000,125000"

    # 4. 90% load on first executor
    "n4_e_rate_imb90|600,600,600,600|1,1,1,1|18,1,1|50000,110000,125000"

    # 5. 60% load on first executor
    "n4_e_rate_imb60|600,600,600,600|1,1,1,1|3,1,1|50000,110000,125000"

    # 6. 90% load on first validator + 90% on first executor
    "n4_ve_rate_imb90|600,600,600,600|27,1,1,1|18,1,1|50000,110000,125000"

    # 7. 60% load on first validator + 60% on first executor
    "n4_ve_rate_imb60|600,600,600,600|4.5,1,1,1|3,1,1|50000,110000,125000"

    # 8. f=1 low-bw validator (200mbit), ~80% of balanced rates
    "n4_bw_f|600,600,600,200|1,1,1,1|1,1,1|60000,88000,108000"

    # 9. f+1=2 low-bw validators (200mbit), ~50% of balanced rates
    "n4_bw_f1|600,600,200,200|1,1,1,1|1,1,1|38000,53000,68000"
)

check_certified_tps_consistency() {
    local run_dir="$1"
    local logs_dir="$2"
    local output_log="$3"
    local check_log="$run_dir/certified_tps_check.log"
    local errors_log="$run_dir/errors.log"

    { echo "=== certified_tps consistency check ==="; date -Iseconds; } > "$check_log"

    mapfile -t primary_logs < <(ls "$logs_dir"/primary-*.log 2>/dev/null | sort)
    if [[ ${#primary_logs[@]} -eq 0 ]]; then
        echo "ERROR: certified_tps check: no primary logs under $logs_dir" | tee -a "$errors_log" >> "$output_log"
        return 1
    fi

    # Normalize each primary's certified_tps lines to: "round validator rest_of_values"
    local tmpdir
    tmpdir=$(mktemp -d "$run_dir/.ctps_XXXXXX")
    local total_lines=0
    for f in "${primary_logs[@]}"; do
        local norm="$tmpdir/$(basename "$f" .log).norm"
        grep "certified_tps" "$f" \
            | sed -E 's/.*round=([0-9]+)\) validator ([^:]+): (.*)/\1 \2 \3/' \
            > "$norm" || true
        local c
        c=$(wc -l < "$norm")
        echo "$(basename "$f"): $c lines" >> "$check_log"
        total_lines=$((total_lines + c))
    done

    if [[ "$total_lines" -eq 0 ]]; then
        echo "certified_tps check: zero lines across ${#primary_logs[@]} primaries (skipping)" >> "$check_log"
        rm -rf "$tmpdir"
        return 0
    fi

    # For each (round, validator) appearing in multiple primaries, values must agree.
    local n_unique_lines n_unique_keys
    n_unique_lines=$(cat "$tmpdir"/*.norm | sort -u | wc -l)
    n_unique_keys=$(cat "$tmpdir"/*.norm | sort -k1,2 -u | wc -l)
    echo "unique lines: $n_unique_lines, unique (round,validator): $n_unique_keys" >> "$check_log"

    if [[ "$n_unique_lines" -ne "$n_unique_keys" ]]; then
        {
            echo "CONFLICT: $n_unique_lines unique value-lines vs $n_unique_keys unique (round,validator) keys"
            echo "--- entries by frequency ---"
            cat "$tmpdir"/*.norm | sort | uniq -c | sort -rn | head -40
        } >> "$check_log"
        echo "ERROR: certified_tps values differ across primaries for same (round,validator). Details: $check_log" \
            | tee -a "$errors_log" >> "$output_log"
        rm -rf "$tmpdir"
        return 1
    fi

    rm -rf "$tmpdir"
}

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

echo "Motivation LB sweep ($MODE)"
echo "Configs: ${#CONFIGS[@]} (per-config rates)"
echo "Duration: ${DURATION}s, Warmup: ${WARMUP}s, Retries: ${RETRIES}"
echo "Results: $RESULTS_DIR"
echo "==========================================="

cd "$BENCH_DIR"

# Derive max node count across all configs for CPU isolation
MAX_NODES=0
for CONFIG in "${CONFIGS[@]}"; do
    _label="${CONFIG%%|*}"
    _n=$(echo "$_label" | grep -oP '^n\K[0-9]+')
    (( _n > MAX_NODES )) && MAX_NODES=$_n
done

# CPU isolation (shared machine noise reduction, docker mode only)
if [[ "$MODE" == "docker" && "${CPU_ISOLATE:-1}" == "1" && "$CPUS_PER_VALIDATOR" -gt 0 ]]; then
    sudo "$BENCH_DIR/cpu_isolate.sh" setup \
        --nodes="$MAX_NODES" \
        --cpus-per-validator="$CPUS_PER_VALIDATOR" \
        --numa-node-cpus="${NUMA_NODE_CPUS:-32}" \
    trap 'sudo "$BENCH_DIR/cpu_isolate.sh" teardown' EXIT
fi

for RETRY in $(seq 1 "$RETRIES"); do
    for CONFIG in "${CONFIGS[@]}"; do
        IFS='|' read -r LABEL BANDWIDTHS_MBPS RATE_WEIGHTS E_SKEW_WEIGHTS RATES_STR <<< "$CONFIG"
        IFS=',' read -ra RATES <<< "$RATES_STR"
        NODES=$(echo "$LABEL" | grep -oP '^n\K[0-9]+')

        for RATE in "${RATES[@]}"; do
            RUN_DIR="$RESULTS_DIR/${LABEL}_r${RATE}_run_${RETRY}"

            if [[ -d "$RUN_DIR" ]]; then
                echo "Skipping $RUN_DIR (already exists)"
                continue
            fi
            mkdir -p "$RUN_DIR"

            echo ""
            echo "--- $LABEL | bw=$BANDWIDTHS_MBPS rate_w=$RATE_WEIGHTS e_skew_w=$E_SKEW_WEIGHTS rate=$RATE Run: $RETRY/$RETRIES ---"

            if [[ "$MODE" == "docker" ]]; then
                FAB_CMD="NODES=$NODES WORKER_BANDWIDTHS_MBPS=$BANDWIDTHS_MBPS RATE_WEIGHTS=$RATE_WEIGHTS RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP IN_MEMORY_STORE=1 fab docker --cpus-per-validator=$CPUS_PER_VALIDATOR --latency=$LATENCY --primary-bw=$PRIMARY_BW --log-dir=$RUN_DIR"
            elif [[ "$MODE" == "cloudlab" ]]; then
                FAB_CMD="LOCAL_ORCH=1 NODES=$NODES WORKER_BANDWIDTHS_MBPS=$BANDWIDTHS_MBPS RATE_WEIGHTS=$RATE_WEIGHTS RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP IN_MEMORY_STORE=1 fab cloudlab --manifest=$MANIFEST --latency=$LATENCY --primary-bw=$PRIMARY_BW"
            else
                echo "ERROR: Unknown MODE=$MODE (expected docker or cloudlab)" >&2
                exit 1
            fi

            echo "CMD: LABEL=${LABEL}_r${RATE}_run_${RETRY} $FAB_CMD" | tee -a "$OUTPUT_LOG"
            OUTPUT=$(eval "$FAB_CMD" 2>&1) || true
            { echo "CMD: LABEL=${LABEL}_r${RATE}_run_${RETRY} $FAB_CMD"; echo "$OUTPUT"; } > "$RUN_DIR/output.log"
            echo "$OUTPUT" >> "$OUTPUT_LOG"
            check_certified_tps_consistency "$RUN_DIR" "$RUN_DIR" "$OUTPUT_LOG" || true

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
echo "Plotting combined figure..."
python "$BENCH_DIR/plot_sweep.py" "$RESULTS_DIR" -o "$RESULTS_DIR/sweep.png" || \
    echo "WARNING: combined plot failed"

echo ""
echo "==========================================="
echo "Motivation LB sweep complete. Results in $RESULTS_DIR"
