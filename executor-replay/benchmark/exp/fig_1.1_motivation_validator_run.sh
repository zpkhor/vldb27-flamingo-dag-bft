#!/usr/bin/env bash
# Scenario sweep: full flexibility, each config specifies all params independently.
# Supports both docker and cloudlab via MODE env var.
set -euo pipefail

MODE=${MODE:-docker}

# Common defaults
RETRIES=${RETRIES:-2}
DURATION=${DURATION:-80}
WARMUP=${WARMUP:-10}

# Docker-specific defaults
CPUS_PER_VALIDATOR=${CPUS_PER_VALIDATOR:-8}
LATENCY=${LATENCY:-100ms}
PRIMARY_BW=${PRIMARY_BW:-250mbit}

# CloudLab-specific defaults
MANIFEST=${MANIFEST:-manifest.xml}
LATENCY_MS=${LATENCY_MS:-100}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results/scenario_${MODE}_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"
OUTPUT_LOG="$RESULTS_DIR/merged_output.log"

# Each entry: "label|BANDWIDTHS_MBPS|RATE_WEIGHTS|NODES|BASELINE|RATES"
# n=4, f=1; for ~60% rate to v0: weight_0 = 1.5*(n-1), others = 1
# Per-config RATES tuned to each config's saturation region:
#   balanced  saturates ~105k TPS
#   rate_imb  saturates ~70k  TPS
#   bw_f      saturates ~50k  TPS
#   bw_f1     saturates ~35k  TPS
CONFIGS=(
    "n4_balanced|500,500,500,500|1,1,1,1|4|1|30000,50000,70000,90000,100000,110000,120000,130000"
    "n4_rate_imb|500,500,500,500|4.5,1,1,1|4|1|20000,33000,45000,56000,65000,72000,80000,90000"
    "n4_bw_f|500,500,500,150|1,1,1,1|4|1|20000,30000,38000,45000,50000,56000,65000,78000"
    "n4_bw_f1|500,500,150,150|1,1,1,1|4|1|10000,18000,25000,30000,33000,36000,40000,45000"
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
    IFS='|' read -r LABEL _ _ _ _ _ <<< "$CONFIG"
    if [[ -v seen_labels["$LABEL"] ]]; then
        echo "ERROR: Duplicate label '$LABEL' in CONFIGS" >&2
        exit 1
    fi
    seen_labels["$LABEL"]=1
done

echo "Scenario sweep ($MODE)"
echo "Configs: ${#CONFIGS[@]} (per-config rates)"
echo "Duration: ${DURATION}s, Warmup: ${WARMUP}s, Retries: ${RETRIES}"
echo "Results: $RESULTS_DIR"
echo "==========================================="

cd "$BENCH_DIR"

for CONFIG in "${CONFIGS[@]}"; do
    IFS='|' read -r LABEL BANDWIDTHS_MBPS RATE_WEIGHTS NODES_VAL BASELINE RATES_STR <<< "$CONFIG"
    IFS=',' read -ra RATES <<< "$RATES_STR"

    for RATE in "${RATES[@]}"; do
        for RETRY in $(seq 1 "$RETRIES"); do
            RUN_DIR="$RESULTS_DIR/${LABEL}_r${RATE}_run_${RETRY}"
            mkdir -p "$RUN_DIR"

            echo ""
            echo "--- $LABEL | nodes=$NODES_VAL bw=$BANDWIDTHS_MBPS rate_w=$RATE_WEIGHTS baseline=$BASELINE rate=$RATE Run: $RETRY/$RETRIES ---"

            if [[ "$MODE" == "docker" ]]; then
                FAB_CMD="NODES=$NODES_VAL WORKER_BANDWIDTHS_MBPS=$BANDWIDTHS_MBPS RATE_WEIGHTS=$RATE_WEIGHTS BASELINE=$BASELINE RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP fab docker --cpus-per-validator=$CPUS_PER_VALIDATOR --latency=$LATENCY --primary-bw=$PRIMARY_BW"
            elif [[ "$MODE" == "cloudlab" ]]; then
                FAB_CMD="NODES=$NODES_VAL WORKER_BANDWIDTHS_MBPS=$BANDWIDTHS_MBPS RATE_WEIGHTS=$RATE_WEIGHTS BASELINE=$BASELINE RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP fab cloudlab --manifest=$MANIFEST --latency-ms=$LATENCY_MS"
            else
                echo "ERROR: Unknown MODE=$MODE (expected docker or cloudlab)" >&2
                exit 1
            fi

            echo "CMD: $FAB_CMD" | tee -a "$OUTPUT_LOG"
            OUTPUT=$(eval "$FAB_CMD" 2>&1) || true
            echo "$OUTPUT" | tee "$RUN_DIR/output.log" >> "$OUTPUT_LOG"

            cp -r logs/* "$RUN_DIR/" 2>/dev/null || true
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
echo "Scenario sweep complete. Results in $RESULTS_DIR"
