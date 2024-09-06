#!/bin/bash

# Initialise the cluster with master as the head
ray start --head --node-ip-address=10.168.0.2 --port=6379 --object-store-memory=16000000000

# List only one of the workers
WORKERS=("worker-1")

COMMAND="ray start --address=10.168.0.2:6379"

# Loop through each worker VM and execute the command
for WORKER in "${WORKERS[@]}"; do
    echo "Running command on $WORKER..."
    ssh "$WORKER" "$COMMAND"
    if [ $? -eq 0 ]; then
        echo "Command executed successfully on $WORKER"
    else
        echo "Failed to execute command on $WORKER"
    fi
done
