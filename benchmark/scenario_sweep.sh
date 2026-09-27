#!/usr/bin/env bash
# Scenario sweep: full flexibility, each config specifies all params independently.
# Supports both docker and cloudlab via MODE env var.
# All configs run sequentially (single cluster).
set -euo pipefail

MODE=${MODE:-docker}

# Common defaults
RETRIES=${RETRIES:-2}
DURATION=${DURATION:-1200}
WARMUP=${WARMUP:-30}

# Docker-specific defaults
CPUS_PER_VALIDATOR=${CPUS_PER_VALIDATOR:-8}
LATENCY=${LATENCY:-100ms}
PRIMARY_BW=${PRIMARY_BW:-300mbit}

# CloudLab-specific defaults
MANIFEST=${MANIFEST:-manifest.xml}

cd "$(dirname "$0")"
RESULTS_DIR="$(pwd)/results/scenario_${MODE}_$(date +%Y%m%d_%H%M%S)"
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

# Each entry: "label|BANDWIDTHS_MBPS|RATE_WEIGHTS|NODES|ROUTING_MODE[|DURATION[|RETRIES]]"
# label must end with _r<rate> (e.g. n4_rate_imb_r100000); rate is extracted from there.
# ROUTING_MODE: empty for default routing, "round-robin" for round-robin, "baseline" for baseline.
# DURATION/RETRIES: per-config overrides; fall back to global DURATION/RETRIES if empty.
CONFIGS=(
    # balanced default (240s, 1 run)
    "n4_r135000|$(make_bw 4 '')|$(make_weights 4)|4||240|1"
    "n13_r135000|$(make_bw 13 '')|$(make_weights 13)|13||240|1"
    "n22_r135000|$(make_bw 22 '')|$(make_weights 22)|22||240|1"
    "n31_r135000|$(make_bw 31 '')|$(make_weights 31)|31||240|1"

    # balanced baseline routing (240s, 1 run)
    "n4_bl_r135000|$(make_bw 4 '')|$(make_weights 4)|4|baseline|240|1"
    "n13_bl_r135000|$(make_bw 13 '')|$(make_weights 13)|13|baseline|240|1"
    "n22_bl_r135000|$(make_bw 22 '')|$(make_weights 22)|22|baseline|240|1"
    "n31_bl_r135000|$(make_bw 31 '')|$(make_weights 31)|31|baseline|240|1"

    # rate_imb90: ceil(n/10) nodes share 90% of load
    "n4_rate_imb90_r135000|$(make_bw 4 '')|$(make_rate_imb_weights 4 0.9)|4|"
    "n13_rate_imb90_r135000|$(make_bw 13 '')|$(make_rate_imb_weights 13 0.9)|13|"
    "n22_rate_imb90_r135000|$(make_bw 22 '')|$(make_rate_imb_weights 22 0.9)|22|"
    "n31_rate_imb90_r135000|$(make_bw 31 '')|$(make_rate_imb_weights 31 0.9)|31|"

    # rate_imb60: ceil(n/10) nodes share 60% of load
    "n4_rate_imb60_r135000|$(make_bw 4 '')|$(make_rate_imb_weights 4 0.6)|4|"
    "n13_rate_imb60_r135000|$(make_bw 13 '')|$(make_rate_imb_weights 13 0.6)|13|"
    "n22_rate_imb60_r135000|$(make_bw 22 '')|$(make_rate_imb_weights 22 0.6)|22|"
    "n31_rate_imb60_r135000|$(make_bw 31 '')|$(make_rate_imb_weights 31 0.6)|31|"

    # bw_f
    # "n4_bw_f_r120000|$(make_bw 4 bw_f)|$(make_weights 4)|4|"
    # "n31_bw_f_r120000|$(make_bw 31 bw_f)|$(make_weights 31)|31|"
    # "n4_bw_f_rr_r120000|$(make_bw 4 bw_f)|$(make_weights 4)|4|round-robin"

    # bw_f1
    # "n4_bw_f1_r60000|$(make_bw 4 bw_f1)|$(make_weights 4)|4|"
    # "n31_bw_f1_r60000|$(make_bw 31 bw_f1)|$(make_weights 31)|31|"
    # "n4_bw_f1_rr_r45000|$(make_bw 4 bw_f1)|$(make_weights 4)|4|round-robin"

    # rr (baseline has no certificate overhead)
    # "n4_rr_r95000|$(make_bw 4 '')|$(make_weights 4)|4|round-robin"

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
    local LABEL BANDWIDTHS_MBPS RATE_WEIGHTS NODES_VAL ROUTING_MODE_VAL DURATION_OVR RETRIES_OVR
    IFS='|' read -r LABEL BANDWIDTHS_MBPS RATE_WEIGHTS NODES_VAL ROUTING_MODE_VAL DURATION_OVR RETRIES_OVR <<< "$CONFIG"
    [[ $LABEL =~ _r([0-9]+)$ ]] || { echo "ERROR: label '$LABEL' missing _r<rate> suffix" >&2; exit 1; }
    local RATE="${BASH_REMATCH[1]}"
    local ROUTING_MODE_ENV=""
    [[ -n "$ROUTING_MODE_VAL" ]] && ROUTING_MODE_ENV="ROUTING_MODE=$ROUTING_MODE_VAL"
    local run_duration="${DURATION_OVR:-$DURATION}"
    local run_retries="${RETRIES_OVR:-$RETRIES}"

    for RETRY in $(seq 1 "$run_retries"); do
        local RUN_DIR="$RESULTS_DIR/${LABEL}_run_${RETRY}"
        mkdir -p "$RUN_DIR"

        echo "$RUN_DIR"
        echo "--- $LABEL | nodes=$NODES_VAL bw=$BANDWIDTHS_MBPS rate_w=$RATE_WEIGHTS routing=${ROUTING_MODE_VAL:-default} rate=$RATE Run: $RETRY/$run_retries ---"

        local FAB_CMD
        if [[ "$MODE" == "docker" ]]; then
            FAB_CMD="${ROUTING_MODE_ENV:+$ROUTING_MODE_ENV }NODES=$NODES_VAL WORKER_BANDWIDTHS_MBPS=$BANDWIDTHS_MBPS RATE_WEIGHTS=$RATE_WEIGHTS RATE=$RATE DURATION=$run_duration WARMUP=$WARMUP IN_MEMORY_STORE=1 fab docker --cpus-per-validator=$CPUS_PER_VALIDATOR --latency=$LATENCY --primary-bw=$PRIMARY_BW --log-dir=$RUN_DIR"
        elif [[ "$MODE" == "cloudlab" ]]; then
            FAB_CMD="${LOCAL_ORCH:+LOCAL_ORCH=$LOCAL_ORCH }${ROUTING_MODE_ENV:+$ROUTING_MODE_ENV }NODES=$NODES_VAL WORKER_BANDWIDTHS_MBPS=$BANDWIDTHS_MBPS RATE_WEIGHTS=$RATE_WEIGHTS RATE=$RATE DURATION=$run_duration WARMUP=$WARMUP IN_MEMORY_STORE=1 fab cloudlab --manifest=$MANIFEST --latency=$LATENCY --primary-bw=$PRIMARY_BW --log-dir=$RUN_DIR"
        else
            echo "ERROR: Unknown MODE=$MODE (expected docker or cloudlab)" >&2
            exit 1
        fi

        echo "CMD: LABEL=$LABEL $FAB_CMD"
        local OUTPUT
        OUTPUT=$(eval "$FAB_CMD" 2>&1) || true
        { echo "CMD: LABEL=$LABEL $FAB_CMD"; echo "$OUTPUT"; echo "CMD: LABEL=$LABEL"; } > "$RUN_DIR/output.log"
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

# CPU isolation (shared machine noise reduction, docker mode only)
if [[ "$MODE" == "docker" && "${CPU_ISOLATE:-1}" == "1" && "$CPUS_PER_VALIDATOR" -gt 0 ]]; then
    MAX_NODES=0
    for CONFIG in "${CONFIGS[@]}"; do
        IFS='|' read -r _ _ _ N _ <<< "$CONFIG"
        (( N > MAX_NODES )) && MAX_NODES=$N
    done
    sudo "$(dirname "$0")/cpu_isolate.sh" setup \
        --nodes="$MAX_NODES" \
        --cpus-per-validator="$CPUS_PER_VALIDATOR" \
        --numa-node-cpus="${NUMA_NODE_CPUS:-32}" \
        --num-executors="${NUM_EXECUTORS:-0}"
    trap 'sudo "$(dirname "$0")/cpu_isolate.sh" teardown' EXIT
fi

if [[ "$MODE" == "cloudlab" ]]; then
    echo "Running CloudLab preflight: fab cloudlab-check"
    ${LOCAL_ORCH:+LOCAL_ORCH=$LOCAL_ORCH} fab cloudlab-check
fi

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
