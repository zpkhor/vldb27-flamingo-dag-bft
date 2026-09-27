#!/usr/bin/env bash
# E2E TPS timeline sweep: both validator and executor layer imbalance scenarios.
# Same CONFIGS as fig1_motivation_run.sh.
# LB_MODE selects which load-balancing layers are enabled (ablation):
#   both      → validator LB (BASELINE=0) + executor LB (NEW_SCHEDULER=1)
#   validator → validator LB only (BASELINE=0, NEW_SCHEDULER=0)
#   executor  → executor LB only  (BASELINE=1, NEW_SCHEDULER=1)
#   none      → neither (BASELINE=1, NEW_SCHEDULER=0)
# All modes use the data-fusion batch_executor (no WRITEBACK_EXECUTOR).
# LB_MODE accepts a comma-separated list; each mode is swept in turn into its own results dir.
# LB_MODE defaults from BASELINE (0 → both, 1 → none) for backward compatibility.
# Fixed: NUM_EXECUTORS=3 IN_MEMORY_STORE=1
# NO_SEND_PAYMENT=1 (default, as in fig1_motivation_run.sh): SendPayment picks its dest
# account uniformly over the global range, and under data fusion each cross-executor tx
# reassigns ownership, so the account->executor map fully decorrelates within ~50 s.
# That erases E_SKEW_WEIGHTS before the measurement window starts. Keeping SendPayment
# out preserves the static partition so executor-level skew survives the whole run.
# Supports cloudlab only.
#
# Rerunning: each invocation derives a fresh timestamped results dir, so preset RESULTS_DIR
# to resume one. Runs then accumulate there: a cell whose output.log has a SUMMARY: block is
# left alone, a cell that died mid-run is quarantined under failed/ and redone. RESULTS_DIR
# pins a single dir, so it takes a single LB_MODE.
#
# Reproduce benchmark/fig4_plots/fig4_combined.pdf (n=4, 3 executors, 900s):
#   LB_MODE=both,validator,executor,none bash benchmark/exp/fig_4_e2e_tps_timeline.sh
# then pass the four resulting dirs to plot_fig4_e2e_commit.py --combined --pdf
# (see the usage block in that script).
set -euo pipefail

MODE=${MODE:-cloudlab}
[[ "$MODE" == "cloudlab" ]] || { echo "ERROR: only cloudlab MODE is supported, got MODE=$MODE" >&2; exit 1; }

BASELINE=${BASELINE:-1}
LB_MODE=${LB_MODE:-validator,executor}
if [[ -z "${LB_MODE:-}" ]]; then
    if [[ "$BASELINE" == "0" ]]; then
        LB_MODE=both
    else
        LB_MODE=none
    fi
fi

RESULTS_DIR_PRESET=1
[[ -z "${RESULTS_DIR:-}" ]] && RESULTS_DIR_PRESET=0

IFS=',' read -ra LB_MODES <<< "$LB_MODE"
for M in "${LB_MODES[@]}"; do
    case "$M" in
        both|validator|executor|none) ;;
        *) echo "ERROR: LB_MODE entries must be one of both|validator|executor|none, got '$M'" >&2; exit 1 ;;
    esac
done

# Multi-mode: re-run this script once per mode so each gets its own results dir.
if [[ ${#LB_MODES[@]} -gt 1 ]]; then
    if [[ "$RESULTS_DIR_PRESET" == "1" ]]; then
        echo "ERROR: RESULTS_DIR pins one dir but LB_MODE lists ${#LB_MODES[@]} modes;" >&2
        echo "       their runs share labels and would collide. Run one mode per invocation." >&2
        exit 1
    fi
    for M in "${LB_MODES[@]}"; do
        echo ""
        echo "########## LB_MODE=$M ##########"
        LB_MODE="$M" "$0" "$@"
    done
    exit 0
fi

LB_MODE="${LB_MODES[0]}"
case "$LB_MODE" in
    both)      BASELINE=0; NEW_SCHEDULER=1; DIR_TAG=lb ;;
    validator) BASELINE=0; NEW_SCHEDULER=0; DIR_TAG=vlb ;;
    executor)  BASELINE=1; NEW_SCHEDULER=1; DIR_TAG=elb ;;
    none)      BASELINE=1; NEW_SCHEDULER=0; DIR_TAG=baseline ;;
esac

# Common defaults
RETRIES=${RETRIES:-1}
NO_SEND_PAYMENT=${NO_SEND_PAYMENT:-1}
DURATION=${DURATION:-900}
WARMUP=${WARMUP:-240}

# CloudLab defaults
LATENCY=${LATENCY:-100ms}
PRIMARY_BW=${PRIMARY_BW:-300mbit}
MANIFEST=${MANIFEST:-manifest.xml}
LOCAL_ORCH=${LOCAL_ORCH:-0}
NODE_OFFSET=${NODE_OFFSET:-0}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if [[ "$RESULTS_DIR_PRESET" == "0" ]]; then
    RESULTS_DIR="$SCRIPT_DIR/results/e2e_tps_timeline_${DIR_TAG}_cloud_$(date +%Y%m%d_%H%M%S)"
fi
mkdir -p "$RESULTS_DIR"
RESULTS_DIR="$(cd "$RESULTS_DIR" && pwd)"
OUTPUT_LOG="$RESULTS_DIR/merged_output.log"

# plot_fig4_e2e_commit.py rebuilds the committed timeline from raw logs when tps_timeline.csv
# is absent, at the same 10s bin and with identical values, so generating the CSVs here only
# saves the reparse - and it is what commit_span_check.py needs to judge the runs.
POSTPROCESS=${POSTPROCESS:-1}

# Each entry: "label|BANDWIDTHS_MBPS|RATE_WEIGHTS|E_SKEW_WEIGHTS|RATES"
# n=4, f=1, m=3 executors
# Validator skew (x=ceil(4/10)=1): 90% -> w_high=27, 60% -> w_high=4.5
# Executor skew  (x=ceil(3/10)=1): 90% -> w_high=18, 60% -> w_high=3
CONFIGS=(
    # 1. balanced: both layers uniform (saturates ~135k)
    # "n4_balanced|600,600,600,600|1,1,1,1|1,1,1|100000"

    # 2. 90% load on first validator (saturates ~70k)
    "n4_v_rate_imb90|600,600,600,600|27,1,1,1|1,1,1|100000"

    # 3. 60% load on first validator (saturates ~80k)
    # "n4_v_rate_imb60|600,600,600,600|4.5,1,1,1|1,1,1|100000"

    # 4. 90% load on first executor (saturates ~65k, TBD)
    "n4_e_rate_imb90|600,600,600,600|1,1,1,1|18,1,1|100000"

    # 5. 60% load on first executor (saturates ~79k)
    # "n4_e_rate_imb60|600,600,600,600|1,1,1,1|3,1,1|100000"

    # 6. 90% load on first validator + 90% on first executor (saturates ~60k, TBD)
    "n4_ve_rate_imb90|600,600,600,600|27,1,1,1|18,1,1|100000"

    # 7. 60% load on first validator + 60% on first executor (saturates ~70k, TBD)
    # "n4_ve_rate_imb60|600,600,600,600|4.5,1,1,1|3,1,1|100000"

    # 8. f=1 low-bw validator (200mbit)
    "n4_bw_f|600,600,600,200|1,1,1,1|1,1,1|72000"

    # 9. f+1=2 low-bw validators (200mbit)
    # "n4_bw_f1|600,600,200,200|1,1,1,1|1,1,1|10000,20000,30000,38000,45000,52000"
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

echo "E2E TPS timeline sweep (cloudlab, LB_MODE=$LB_MODE, BASELINE=$BASELINE, NEW_SCHEDULER=$NEW_SCHEDULER, NO_SEND_PAYMENT=$NO_SEND_PAYMENT)"
echo "Configs: ${#CONFIGS[@]} (per-config rates)"
echo "Duration: ${DURATION}s, Warmup: ${WARMUP}s, Retries: ${RETRIES}"
echo "Results: $RESULTS_DIR"
echo "==========================================="

cd "$BENCH_DIR"

for CONFIG in "${CONFIGS[@]}"; do
    IFS='|' read -r LABEL BANDWIDTHS_MBPS RATE_WEIGHTS E_SKEW_WEIGHTS RATES_STR <<< "$CONFIG"
    IFS=',' read -ra RATES <<< "$RATES_STR"

    for RATE in "${RATES[@]}"; do
        for RETRY in $(seq 1 "$RETRIES"); do
            RUN_DIR="$RESULTS_DIR/${LABEL}_r${RATE}_run_${RETRY}"

            # No SUMMARY: block means the cell died mid-run. Skipping it on the strength of
            # the directory alone would preserve exactly the cells that need redoing.
            if [[ -d "$RUN_DIR" ]]; then
                if grep -q 'SUMMARY:' "$RUN_DIR/output.log" 2>/dev/null; then
                    echo "Skipping $RUN_DIR (already complete)"
                    continue
                fi
                QUARANTINE="$RESULTS_DIR/failed/${LABEL}_r${RATE}_run_${RETRY}_$(date +%Y%m%d_%H%M%S)"
                mkdir -p "$RESULTS_DIR/failed"
                mv "$RUN_DIR" "$QUARANTINE"
                echo "Redoing $RUN_DIR (no SUMMARY); previous attempt kept at $QUARANTINE"
            fi
            mkdir -p "$RUN_DIR"

            echo ""
            echo "--- $LABEL | bw=$BANDWIDTHS_MBPS rate_w=$RATE_WEIGHTS e_skew_w=$E_SKEW_WEIGHTS rate=$RATE Run: $RETRY/$RETRIES ---"

            FAB_CMD="LOCAL_ORCH=$LOCAL_ORCH NODE_OFFSET=$NODE_OFFSET NODES=4 WORKER_BANDWIDTHS_MBPS=$BANDWIDTHS_MBPS RATE_WEIGHTS=$RATE_WEIGHTS E_SKEW_WEIGHTS=$E_SKEW_WEIGHTS BASELINE=$BASELINE NEW_SCHEDULER=$NEW_SCHEDULER NO_SEND_PAYMENT=$NO_SEND_PAYMENT NUM_EXECUTORS=3 RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP IN_MEMORY_STORE=1 fab cloudlab --manifest=$MANIFEST --latency=$LATENCY --primary-bw=$PRIMARY_BW --log-dir=$RUN_DIR"

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
echo "E2E TPS timeline sweep complete. Results in $RESULTS_DIR"
if (( SPAN_STATUS != 0 )); then
    # A truncated run still carries a SUMMARY:, so the skip above cannot catch it.
    echo "ERROR: commit_span_check did not pass (see above). A run that stopped early still" >&2
    echo "       carries a SUMMARY:, so re-invoking skips it - delete those run dirs under" >&2
    echo "       $RESULTS_DIR first, then re-invoke with RESULTS_DIR set to redo them." >&2
    exit "$SPAN_STATUS"
fi
