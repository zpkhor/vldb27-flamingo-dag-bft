#!/usr/bin/env bash
# Baseline (no load balancing) twin of config_c_full_repro.sh: the dashed curves panel (c)
# of plot_paper_configs.py pairs with its LB cells.
#
# The panel plots to 1400s. The (a)-(c) baselines in
# results/tps_timeline_baseline_cloud_20260414_132153 are 900s, so on panel (c) their
# dashed lines stopped at two thirds of the x-axis while the LB lines ran to the edge.
# DURATION=1450 is the smallest run that covers the plotted window with a bin to spare
# (bins are 10s and the last one lands at DURATION-10).
#
# Cell parameters are copied verbatim from config_c_full_repro.sh CELLS, with BASELINE=1
# added. Run subdir names must stay identical to the LB ones: plot_paper_configs.py looks
# the baseline up by subdir name under bl_dir, so a renamed cell silently loses its dashed
# curve instead of failing.
#
# Two cells run concurrently on disjoint machines (NODE_OFFSET 0 and 4 of the 10-node
# manifest). fab writes .committee.json/.parameters.json/.node-i.json into --log-dir, not
# the shared CWD, so concurrent slots do not collide over them; SLOT_STAGGER keeps their
# cargo build and binary-symlink steps from overlapping. Straggler kills are per slot -
# a slot only ever touches its own four hosts, never its neighbour's.
#
# Concurrency does share the cluster network. The LB side of panel (c) was measured
# serially, so a baseline plateau measured here can read slightly lower than a solo run
# would. That is acceptable for these cells - they are the collapsed arm and the panel
# reads their shape, not their exact plateau - but it is not acceptable for anything that
# sets a threshold (see find_max_rate.sh, deliberately serial).
#
# Usage:
#   DURATION=1450 bash benchmark/exp/config_c_baseline_repro.sh [OUT_DIR]
set -euo pipefail

DURATION=${DURATION:-1450}
WARMUP=${WARMUP:-240}
LATENCY=${LATENCY:-100ms}
PRIMARY_BW=${PRIMARY_BW:-300mbit}
MANIFEST=${MANIFEST:-manifest.xml}
SLOTS=${SLOTS:-"0 4"}
SLOT_STAGGER=${SLOT_STAGGER:-60}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

OUT_DIR="${1:-$SCRIPT_DIR/results/config_c_baseline_d${DURATION}_$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"

# "cell|WORKER_BANDWIDTHS_MBPS|RATE_WEIGHTS|RATE"
CELLS=(
    "n4_v_rate_imb90_r110000|600,600,600,600|0.9,0.0333333,0.0333333,0.0333333|110000"
    "n4_bw_f_rate_imb90_r110000|600,600,600,200|0.9,0.0333333,0.0333333,0.0333333|110000"
    "n4_bw_f1_rate_imb90_r65000|600,600,200,200|0.9,0.0333333,0.0333333,0.0333333|65000"
)

command -v fab   >/dev/null || { echo "FATAL: fab not on PATH (activate narwhal39)"; exit 1; }
command -v cargo >/dev/null || { echo "FATAL: cargo not on PATH (fab compiles the binaries)"; exit 1; }

if ! git -C "$BENCH_DIR" diff --quiet && [[ "${ALLOW_DIRTY:-0}" != "1" ]]; then
    echo "ERROR: working tree has unstaged changes; commit, or set ALLOW_DIRTY=1" >&2
    exit 1
fi

cd "$BENCH_DIR"

# Hosts in manifest order, so slot N owns MANIFEST_HOSTS[N..N+3].
mapfile -t MANIFEST_HOSTS < <(grep -o 'name="node-[0-9]*\.[^"]*"' "$MANIFEST" \
    | sed 's/name="//; s/"$//' | sort -u -t- -k2 -n)
(( ${#MANIFEST_HOSTS[@]} >= 8 )) || { echo "FATAL: need >=8 hosts in $MANIFEST, found ${#MANIFEST_HOSTS[@]}"; exit 1; }

SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=20)

# Only this slot's four hosts. A cluster-wide sweep here would kill the concurrent slot.
kill_slot_stragglers() {
    local off="$1" i h
    for ((i=off; i < off + 4; i++)); do
        h="${MANIFEST_HOSTS[$i]}"
        timeout 30 ssh "${SSH_OPTS[@]}" "$h" \
            'tmux kill-server 2>/dev/null; pkill -9 -f "\./node"; pkill -9 -f "\./benchmark_client"; exit 0' \
            >/dev/null 2>&1 || true
    done
}

run_cell() {
    local cell_spec="$1" off="$2"
    local CELL BWS WEIGHTS RATE
    IFS='|' read -r CELL BWS WEIGHTS RATE <<< "$cell_spec"

    local RUN_NAME="${CELL}_run_1"
    local RUN_DIR="$OUT_DIR/$RUN_NAME"
    local SLOT_LOG="$OUT_DIR/slot_off${off}.log"

    if [[ -f "$RUN_DIR/tps_timeline.csv" ]]; then
        echo "--- $RUN_NAME: already present, skipping ---" >> "$SLOT_LOG"
        return 0
    fi
    mkdir -p "$RUN_DIR"
    kill_slot_stragglers "$off"

    git -C "$BENCH_DIR" rev-parse HEAD > "$RUN_DIR/code_head.txt"
    git -C "$BENCH_DIR" diff > "$RUN_DIR/code_delta.patch"

    echo "--- $RUN_NAME  off=$off  start $(date +%H:%M:%S) ---" >> "$SLOT_LOG"

    local FAB_CMD="LOCAL_ORCH=${LOCAL_ORCH:-0} NODE_OFFSET=$off NODES=4 WORKER_BANDWIDTHS_MBPS=$BWS RATE_WEIGHTS=$WEIGHTS BASELINE=1 RATE=$RATE DURATION=$DURATION WARMUP=$WARMUP IN_MEMORY_STORE=1 fab cloudlab --manifest=$MANIFEST --latency=$LATENCY --primary-bw=$PRIMARY_BW --log-dir=$RUN_DIR"
    echo "CMD: LABEL=$RUN_NAME $FAB_CMD" >> "$SLOT_LOG"

    local OUTPUT
    OUTPUT=$(eval "$FAB_CMD" 2>&1) || true
    { echo "CMD: LABEL=$RUN_NAME $FAB_CMD"; echo "$OUTPUT"; } > "$RUN_DIR/output.log"

    # Do NOT gate on a TPS_TIMELINE_CSV block the way the LB scripts do. That block comes
    # from certified_tps_timeline, which is empty with BASELINE=1, so a healthy baseline run
    # never prints it - the April baseline cells have no such block either. The timeline
    # here is built by tps_timeline.py from the per-node logs instead, so the run is healthy
    # iff it printed a SUMMARY and that parse yields rows.
    #
    # Here-string, not a pipe: under `set -o pipefail`, grep -q closes the pipe on its
    # first match and the echo dies of SIGPIPE, so a *successful* match can return 141.
    if ! grep -q "SUMMARY:" <<< "$OUTPUT"; then
        echo "  RUN FAILED (no SUMMARY block); see $RUN_DIR/output.log" >> "$SLOT_LOG"
        return 0
    fi
    if ! python tps_timeline.py "$RUN_DIR" > "$RUN_DIR/tps_timeline.csv" 2> "$RUN_DIR/tps_timeline.err"; then
        echo "  RUN FAILED (tps_timeline.py); see $RUN_DIR/tps_timeline.err" >> "$SLOT_LOG"
        rm -f "$RUN_DIR/tps_timeline.csv"
        return 0
    fi
    local ROWS=$(( $(wc -l < "$RUN_DIR/tps_timeline.csv") - 1 ))
    if (( ROWS < 1 )); then
        echo "  RUN FAILED (timeline has $ROWS rows); see $RUN_DIR/output.log" >> "$SLOT_LOG"
        rm -f "$RUN_DIR/tps_timeline.csv"
        return 0
    fi
    echo "  done $(date +%H:%M:%S): $RUN_DIR ($ROWS bins)" >> "$SLOT_LOG"
}

read -ra OFF <<< "$SLOTS"

echo "Config (c) baseline reproduction"
echo "  code:     $(git -C "$BENCH_DIR" rev-parse --short HEAD)$(git -C "$BENCH_DIR" diff --quiet || echo ' (DIRTY)')"
echo "  duration: ${DURATION}s (warmup ${WARMUP}s)"
echo "  cells:    ${#CELLS[@]} across ${#OFF[@]} slots (offsets ${OFF[*]})"
echo "  output:   $OUT_DIR"
echo "==========================================="

# Round r takes the next ${#OFF[@]} cells; a round waits for all its slots before the next
# starts, so no offset is ever driven by two cells at once.
IDX=0
ROUND=0
while (( IDX < ${#CELLS[@]} )); do
    ROUND=$((ROUND + 1))
    echo ""
    echo "=== round $ROUND  $(date +%H:%M:%S) ==="
    PIDS=()
    for O in "${OFF[@]}"; do
        (( IDX < ${#CELLS[@]} )) || break
        SPEC="${CELLS[$IDX]}"
        IDX=$((IDX + 1))
        echo "  slot off=$O  <-  ${SPEC%%|*}"
        run_cell "$SPEC" "$O" &
        PIDS+=($!)
        sleep "$SLOT_STAGGER"
    done
    for p in "${PIDS[@]}"; do wait "$p" || true; done
    echo "--- round $ROUND complete $(date +%H:%M:%S) ---"
done

for O in "${OFF[@]}"; do kill_slot_stragglers "$O"; done

echo ""
echo "==========================================="
for CELL_SPEC in "${CELLS[@]}"; do
    CELL="${CELL_SPEC%%|*}"
    RUN_DIR="$OUT_DIR/${CELL}_run_1"
    if [[ -f "$RUN_DIR/tps_timeline.csv" ]]; then
        echo "  OK      $RUN_DIR"
    else
        echo "  MISSING $RUN_DIR"
    fi
done
echo ""
echo "Config (c) baseline reproduction complete: $OUT_DIR"
