#!/bin/bash
set -e

# Redirect stderr to the client log file early so startup failures are visible
# even if benchmark_client never launches. In the success path, the 2> in
# CLIENT_CMD re-opens the file (truncating the startup output), so only client
# logs remain.
mkdir -p /logs
exec 2>/logs/client-0-0.log

# SO_MARK-based per-region per-validator TC shaping
if [ -n "$TC_BANDWIDTH" ] && [ "$TC_BANDWIDTH" != "0" ] && [ -n "$NUM_REGIONS" ]; then
    # Strip existing TC rules (containers have default fq_codel)
    tc qdisc del dev eth0 root 2>/dev/null || true
    N=$NUM_REGIONS

    # 9999: must exceed max classid = N^2 - N + 10 (supports up to N=99 validators)
    tc qdisc add dev eth0 root handle 1: htb default 9999
    tc class add dev eth0 parent 1: classid 1:1 htb rate $TC_BANDWIDTH

    # Default class (unmatched/local traffic): no latency
    tc class add dev eth0 parent 1:1 classid 1:9999 htb rate 1mbit ceil $TC_BANDWIDTH

    JITTER_ARG=""
    if [ -n "$TC_JITTER" ] && [ "$TC_JITTER" != "0ms" ]; then
        JITTER_ARG="$TC_JITTER"
    fi
    NETEM_LIMIT=${TC_NETEM_LIMIT_CLIENT:-10000}

    for region_id in $(seq 0 $((N-1))); do
        for v_idx in $(seq 0 $((N-1))); do
            # Local flows (same region as validator) fall to default class 1:99 (no netem)
            if [ "$region_id" == "$v_idx" ]; then
                continue
            fi

            mark=$(( region_id * N + v_idx + 1 ))
            classid=$(( mark + 10 ))

            tc class add dev eth0 parent 1:1 classid 1:$classid htb rate 1mbit ceil $TC_BANDWIDTH

            # fw filter matches SO_MARK on packets
            tc filter add dev eth0 parent 1:0 protocol ip prio 1 \
                handle $mark fw flowid 1:$classid

            if [ -n "$TC_LATENCY" ] && [ "$TC_LATENCY" != "0ms" ]; then
                # Remote: add latency
                tc qdisc add dev eth0 parent 1:$classid handle $classid: \
                    netem delay $TC_LATENCY $JITTER_ARG limit $NETEM_LIMIT
            fi
        done
    done

    echo "tc: SO_MARK shaping for $N regions x $N validators" >&2
    tc qdisc show dev eth0
fi

if [ -n "$WAIT_PORTS" ]; then
    sleep 1
    for addr in $WAIT_PORTS; do
        host="${addr%%:*}"
        port="${addr##*:}"
        echo "Waiting for $host:$port..." >&2
        reachable=0
        for i in $(seq 1 60); do
            if (echo > /dev/tcp/$host/$port) 2>/dev/null; then
                reachable=1
                break
            fi
            sleep 0.5
        done
        if [ "$reachable" -ne 1 ]; then
            echo "Timed out waiting for $host:$port; aborting client startup." >&2
            exit 1
        fi
    done
    echo "All remote ports reachable." >&2
fi

PIDS=()

cleanup() {
    echo "Shutting down client..." >&2
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait
    exit 0
}
trap cleanup SIGTERM SIGINT

ulimit -n 65536
eval "${CLIENT_CMD}" &
PIDS+=($!)

# Wait for all children
wait
