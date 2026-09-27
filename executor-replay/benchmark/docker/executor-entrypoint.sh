#!/bin/bash
set -e

# Executor containers run on LAN with no bandwidth shaping.
# Just start the executor process.

echo "Starting executor: $EXECUTOR_CMD"
eval $EXECUTOR_CMD &
EXECUTOR_PID=$!

# Wait for the executor to exit
wait $EXECUTOR_PID
