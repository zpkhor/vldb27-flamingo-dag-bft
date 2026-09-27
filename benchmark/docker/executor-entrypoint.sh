#!/bin/bash
set -e

# Apply egress HTB shaping to cap inter-executor LAN bandwidth (no netem delay).
# Matches CloudLab's _shape_executor(): cloudlab_bench.py:213-224
if [ -n "$TC_EXECUTOR_BW" ] && [ "$TC_EXECUTOR_BW" != "0" ]; then
    # Strip existing TC rules (containers have default fq_codel)
    tc qdisc del dev eth0 root 2>/dev/null || true
    tc qdisc add dev eth0 root handle 1: htb default 10
    tc class add dev eth0 parent 1: classid 1:1 htb rate $TC_EXECUTOR_BW
    tc class add dev eth0 parent 1:1 classid 1:10 htb rate $TC_EXECUTOR_BW ceil $TC_EXECUTOR_BW
    echo "tc executor shaping applied: rate=$TC_EXECUTOR_BW"
fi

echo "Starting executor: $EXECUTOR_CMD"
eval $EXECUTOR_CMD &
EXECUTOR_PID=$!

# Wait for the executor to exit
wait $EXECUTOR_PID
