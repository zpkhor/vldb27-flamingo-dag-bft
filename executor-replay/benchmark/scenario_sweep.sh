#!/usr/bin/env bash
# Scenario sweep: full flexibility, each config specifies all params independently.
# Supports both docker and cloudlab via MODE env var.
# All configs run sequentially (single cluster).
set -euo pipefail

MODE=${MODE:-docker}

# Common defaults
RETRIES=${RETRIES:-2}
DURATION=${DURATION:-240}
WARMUP=${WARMUP:-10}

# Docker-specific defaults
CPUS_PER_VALIDATOR=${CPUS_PER_VALIDATOR:-8}
LATENCY=${LATENCY:-100ms}
PRIMARY_BW=${PRIMARY_BW:-250mbit}

# CloudLab-specific defaults
MANIFEST=${MANIFEST:-manifest.xml}

cd "$(dirname "$0")"
RESULTS_DIR="$(pwd)/results/scenario_${MODE}_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"
OUTPUT_LOG="$RESULTS_DIR/merged_output.log"

# Each entry: "label|BANDWIDTHS_MBPS|RATE_WEIGHTS|NODES|ROUTING_MODE"
# label must end with _r<rate> (e.g. n4_rate_imb_r100000); rate is extracted from there.
# ROUTING_MODE: empty for default routing, "round-robin" for round-robin, "baseline" for baseline.
CONFIGS=(
    # "n4_r90000|500,500,500,500|1,1,1,1|4|"
    "n7_r90000|500,500,500,500,500,500,500|1,1,1,1,1,1,1|7|"

    # n4 versions (for comparison with n7 configs above)
    "n4_rate_imb_r90000|500,500,500,500|4.5,1,1,1|4|"
    "n7_rate_imb_r90000|500,500,500,500,500,500,500|9,1,1,1,1,1,1|7|"
    # "n4_rate_imb_rr_r90000|500,500,500,500|4.5,1,1,1|4|round-robin"

    # bw_f
    "n4_bw_f_r90000|500,500,500,150|1,1,1,1|4|"
    "n7_bw_f_r90000|500,500,500,500,500,150,150|1,1,1,1,1,1,1|7|"
    # "n4_bw_f_rr_r90000|500,500,500,150|1,1,1,1|4|round-robin"

    # bw_f1
    "n4_bw_f1_r60000|500,500,150,150|1,1,1,1|4|"
    "n7_bw_f1_r60000|500,500,500,500,150,150,150|1,1,1,1,1,1,1|7|"
    # "n4_bw_f1_rr_r45000|500,500,150,150|1,1,1,1|4|round-robin"

    # rr (baseline has no certificate overhead)
    # "n4_rr_r95000|500,500,500,500|1,1,1,1|4|round-robin"

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

run_one_config() {
    local CONFIG="$1"
    local LABEL BANDWIDTHS_MBPS RATE_WEIGHTS NODES_VAL ROUTING_MODE_VAL
    IFS='|' read -r LABEL BANDWIDTHS_MBPS RATE_WEIGHTS NODES_VAL ROUTING_MODE_VAL <<< "$CONFIG"
    [[ $LABEL =~ _r([0-9]+)$ ]] || { echo "ERROR: label '$LABEL' missing _r<rate> suffix" >&2; exit 1; }
    local RATE="${BASH_REMATCH[1]}"
    local ROUTING_MODE_ENV=""
    [[ -n "$ROUTING_MODE_VAL" ]] && ROUTING_MODE_ENV="ROUTING_MODE=$ROUTING_MODE_VAL"

    for RETRY in $(seq 1 "$RETRIES"); do
        local RUN_DIR="$RESULTS_DIR/${LABEL}_run_${RETRY}"
        mkdir -p "$RUN_DIR"

        echo ""
        echo "--- $LABEL | nodes=$NODES_VAL bw=$BANDWIDTHS_MBPS rate_w=$RATE_WEIGHTS routing=${ROUTING_MODE_VAL:-default} rate=$RATE Run: $RETRY/$RETRIES ---"

        local FAB_CMD
        if [[ "$MODE" == "docker" ]]; then
            FAB_CMD="${ROUTING_MODE_ENV:+$ROUTING_MODE_ENV }NODES=$NODES_VAL WORKER_BANDWIDTHS_MBPS=$BANDWIDTHS_MBPS RATE_WEIGHTS=$RATE_WEIGHTS RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP IN_MEMORY_STORE=1 fab docker --cpus-per-validator=$CPUS_PER_VALIDATOR --latency=$LATENCY --primary-bw=$PRIMARY_BW"
        elif [[ "$MODE" == "cloudlab" ]]; then
            FAB_CMD="${ROUTING_MODE_ENV:+$ROUTING_MODE_ENV }NODES=$NODES_VAL WORKER_BANDWIDTHS_MBPS=$BANDWIDTHS_MBPS RATE_WEIGHTS=$RATE_WEIGHTS RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP IN_MEMORY_STORE=1 fab cloudlab --manifest=$MANIFEST --latency=$LATENCY --primary-bw=$PRIMARY_BW --log-dir=$RUN_DIR"
        else
            echo "ERROR: Unknown MODE=$MODE (expected docker or cloudlab)" >&2
            exit 1
        fi

        echo "CMD: LABEL=$LABEL $FAB_CMD"
        local OUTPUT
        OUTPUT=$(eval "$FAB_CMD" 2>&1) || true
        { echo "CMD: LABEL=$LABEL $FAB_CMD"; echo "$OUTPUT"; } > "$RUN_DIR/output.log"

        if [[ "$MODE" == "docker" ]]; then
            cp -r logs/* "$RUN_DIR/" 2>/dev/null || true
        fi
        check_certified_tps_consistency "$RUN_DIR" "$RUN_DIR" "$RUN_DIR/output.log" || true

        if echo "$OUTPUT" | grep -qiE 'panic|error|failed'; then
            echo "WARNING: Errors detected in $LABEL rate=$RATE run=$RETRY"
            echo "$OUTPUT" | grep -iE 'panic|error|failed' > "$RUN_DIR/errors.log"
        fi

        sleep 2
    done
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

echo "Scenario sweep ($MODE)"
echo "Configs: ${#CONFIGS[@]}"
echo "Duration: ${DURATION}s, Warmup: ${WARMUP}s, Retries: ${RETRIES}"
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
echo "Scenario sweep complete. Results in $RESULTS_DIR"
