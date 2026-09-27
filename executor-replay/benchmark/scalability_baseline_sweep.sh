#!/usr/bin/env bash
# Scalability sweep: baseline routing, balanced load, vary node count.
# n = 4, 10, 19, 31, 43; primary-bw=200mbit, worker-bw=600mbit.
# CloudLab only.
set -euo pipefail

RATE=${RATE:-120000}
RETRIES=${RETRIES:-2}
DURATION=${DURATION:-240}
WARMUP=${WARMUP:-10}
LATENCY=${LATENCY:-100ms}
PRIMARY_BW=${PRIMARY_BW:-200mbit}
WORKER_BW=${WORKER_BW:-600mbit}
MANIFEST=${MANIFEST:-manifest.xml}
USERNAME=${USERNAME:-anonuser}

cd "$(dirname "$0")"
RESULTS_DIR="$(pwd)/results/scalability_baseline_cloudlab_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"
OUTPUT_LOG="$RESULTS_DIR/merged_output.log"

NODES_LIST=(4 10 19)
# NODES_LIST=(31 43)

# Build CONFIGS: "label|NODES"
CONFIGS=()
for N in "${NODES_LIST[@]}"; do
    CONFIGS+=("n${N}_bl_r${RATE}|${N}")
done

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

run_one_config() {
    local CONFIG="$1"
    local LABEL NODES_VAL
    IFS='|' read -r LABEL NODES_VAL <<< "$CONFIG"
    [[ $LABEL =~ _r([0-9]+)$ ]] || { echo "ERROR: label '$LABEL' missing _r<rate> suffix" >&2; exit 1; }
    local RATE_VAL="${BASH_REMATCH[1]}"

    for RETRY in $(seq 1 "$RETRIES"); do
        local RUN_DIR="$RESULTS_DIR/${LABEL}_run_${RETRY}"
        mkdir -p "$RUN_DIR"

        echo ""
        echo "--- $LABEL | nodes=$NODES_VAL worker-bw=$WORKER_BW primary-bw=$PRIMARY_BW rate=$RATE_VAL Run: $RETRY/$RETRIES ---"

        local FAB_CMD
        FAB_CMD="ROUTING_MODE=baseline NODES=$NODES_VAL RATE=$RATE_VAL DURATION=$DURATION WARMUP=$WARMUP IN_MEMORY_STORE=1 fab cloudlab --manifest=$MANIFEST --username=$USERNAME --latency=$LATENCY --primary-bw=$PRIMARY_BW --worker-bw=$WORKER_BW --log-dir=$RUN_DIR"

        echo "CMD: LABEL=$LABEL $FAB_CMD"
        local OUTPUT
        OUTPUT=$(eval "$FAB_CMD" 2>&1) || true
        { echo "CMD: LABEL=$LABEL $FAB_CMD"; printf '%s\n' "$OUTPUT"; } > "$RUN_DIR/output.log"

        check_certified_tps_consistency "$RUN_DIR" "$RUN_DIR" "$RUN_DIR/output.log" || true

        if echo "$OUTPUT" | grep -qiE 'panic|error|failed'; then
            echo "WARNING: Errors detected in $LABEL rate=$RATE_VAL run=$RETRY"
            echo "$OUTPUT" | grep -iE 'panic|error|failed' > "$RUN_DIR/errors.log"
        fi

        sleep 2
    done
}

# Validate unique labels
declare -A seen_labels
for CONFIG in "${CONFIGS[@]}"; do
    IFS='|' read -r LABEL _ <<< "$CONFIG"
    if [[ -v seen_labels["$LABEL"] ]]; then
        echo "ERROR: Duplicate label '$LABEL' in CONFIGS" >&2
        exit 1
    fi
    seen_labels["$LABEL"]=1
done

echo "Scalability baseline sweep (cloudlab)"
echo "Nodes: ${NODES_LIST[*]}, Rate: $RATE, Worker BW: $WORKER_BW, Primary BW: $PRIMARY_BW"
echo "Configs: ${#CONFIGS[@]}"
echo "Duration: ${DURATION}s, Warmup: ${WARMUP}s, Retries: $RETRIES"
echo "Results: $RESULTS_DIR"
echo "==========================================="

for CONFIG in "${CONFIGS[@]}"; do
    run_one_config "$CONFIG"
done
cat "$RESULTS_DIR"/*/output.log > "$OUTPUT_LOG" 2>/dev/null || true

echo ""
echo "==========================================="
echo "Plotting combined figure..."
python plot_sweep.py "$RESULTS_DIR" -o "$RESULTS_DIR/sweep.png" || \
    echo "WARNING: combined plot failed"

echo ""
echo "==========================================="
echo "Scalability baseline sweep complete. Results in $RESULTS_DIR"
