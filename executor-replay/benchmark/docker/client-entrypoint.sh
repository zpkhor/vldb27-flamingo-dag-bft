#!/bin/bash
set -e

# SO_MARK-based per-region per-validator TC shaping
if [ -n "$TC_BANDWIDTH" ] && [ "$TC_BANDWIDTH" != "0" ] && [ -n "$NUM_REGIONS" ]; then
    N=$NUM_REGIONS

    tc qdisc add dev eth0 root handle 1: htb default 99
    tc class add dev eth0 parent 1: classid 1:1 htb rate $TC_BANDWIDTH

    # Default class (unmatched traffic): no latency
    tc class add dev eth0 parent 1:1 classid 1:99 htb rate 1mbit ceil $TC_BANDWIDTH

    JITTER_ARG=""
    if [ -n "$TC_JITTER" ] && [ "$TC_JITTER" != "0ms" ]; then
        JITTER_ARG="$TC_JITTER"
    fi
    NETEM_LIMIT=${TC_NETEM_LIMIT_CLIENT:-1000}

    for region_id in $(seq 0 $((N-1))); do
        for v_idx in $(seq 0 $((N-1))); do
            mark=$(( region_id * N + v_idx + 1 ))
            classid=$(( mark + 10 ))

            tc class add dev eth0 parent 1:1 classid 1:$classid htb rate 1mbit ceil $TC_BANDWIDTH

            # fw filter matches SO_MARK on packets
            tc filter add dev eth0 parent 1:0 protocol ip prio 1 \
                handle $mark fw flowid 1:$classid

            if [ "$region_id" != "$v_idx" ] && [ -n "$TC_LATENCY" ] && [ "$TC_LATENCY" != "0ms" ]; then
                # Remote: add latency
                tc qdisc add dev eth0 parent 1:$classid handle $classid: \
                    netem delay $TC_LATENCY $JITTER_ARG limit $NETEM_LIMIT
            fi
            # Local (region_id == v_idx): no netem, packets pass through HTB only
        done
    done

    echo "tc: SO_MARK shaping for $N regions x $N validators"
    tc qdisc show dev eth0
fi

if [ -n "$WAIT_PORTS" ]; then
    sleep 1
    for addr in $WAIT_PORTS; do
        host="${addr%%:*}"
        port="${addr##*:}"
        echo "Waiting for $host:$port..."
        for i in $(seq 1 60); do
            (echo > /dev/tcp/$host/$port) 2>/dev/null && break
            sleep 0.5
        done
    done
    echo "All remote ports reachable."
fi

PIDS=()

cleanup() {
    echo "Shutting down client..."
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
