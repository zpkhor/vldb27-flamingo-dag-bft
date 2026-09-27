#!/usr/bin/env bash
# Sweep over NODES and latency (cloudlab only).
# Fixed: BASELINE=1, primary-bw=300mbit
# Results accumulate in a fixed dir; existing RUN_DIRs are skipped.
set -euo pipefail

RETRIES=${RETRIES:-1}
DURATION=${DURATION:-180}
WARMUP=${WARMUP:-30}
PRIMARY_BW=${PRIMARY_BW:-300mbit}
MANIFEST=${MANIFEST:-manifest.xml}

NODES_LIST=(4 13)
LATS=(200 500)
RATES=(35000 65000 95000 110000 122500 130000)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results/latency_nodes_cloud"
mkdir -p "$RESULTS_DIR"
OUTPUT_LOG="$RESULTS_DIR/merged_output.log"

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

echo "Latency/nodes sweep (cloudlab)"
echo "NODES_LIST: ${NODES_LIST[*]}, LATS (ms): ${LATS[*]}"
echo "RATES: ${RATES[*]}, DURATION=${DURATION}s, WARMUP=${WARMUP}s, RETRIES=$RETRIES"
echo "PRIMARY_BW=$PRIMARY_BW, MANIFEST=$MANIFEST"
echo "Results: $RESULTS_DIR"
echo "==========================================="

cd "$SCRIPT_DIR"

for NODES in "${NODES_LIST[@]}"; do
    for LAT in "${LATS[@]}"; do
        LABEL="n${NODES}_lat${LAT}ms"
        LATENCY="${LAT}ms"

        for RATE in "${RATES[@]}"; do
            for RETRY in $(seq 1 "$RETRIES"); do
                RUN_DIR="$RESULTS_DIR/${LABEL}_r${RATE}_run_${RETRY}"

                if [[ -d "$RUN_DIR" ]]; then
                    echo "Skipping $RUN_DIR (already exists)"
                    continue
                fi
                mkdir -p "$RUN_DIR"

                echo ""
                echo "--- $LABEL | nodes=$NODES latency=$LATENCY rate=$RATE Run: $RETRY/$RETRIES ---"

                FAB_CMD="NODES=$NODES BASELINE=1 RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP fab cloudlab --manifest=$MANIFEST --latency=$LATENCY --primary-bw=$PRIMARY_BW --worker-bw=600mbit --log-dir=$RUN_DIR"

                echo "CMD: $FAB_CMD" | tee -a "$OUTPUT_LOG"
                OUTPUT=$(eval "$FAB_CMD" 2>&1) || true
                echo "$OUTPUT" | tee "$RUN_DIR/output.log" >> "$OUTPUT_LOG"

                check_certified_tps_consistency "$RUN_DIR" "$RUN_DIR" "$OUTPUT_LOG" || true

                if echo "$OUTPUT" | grep -qiE 'panic|error|failed'; then
                    echo "WARNING: Errors detected in $LABEL rate=$RATE run=$RETRY"
                    echo "$OUTPUT" | grep -iE 'panic|error|failed' > "$RUN_DIR/errors.log"
                fi

                sleep 2
            done
        done
    done
done

echo ""
echo "==========================================="
echo "Latency/nodes sweep (cloudlab) complete. Results in $RESULTS_DIR"
