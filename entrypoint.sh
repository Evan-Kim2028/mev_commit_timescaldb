#!/bin/bash
set -e

# Activate the virtual environment
source /app/.venv/bin/activate

# Function to run Python scripts
run_scripts() {
    echo "Running main.py..."
    python main.py

    echo "Running fetch_l1_txs.py..."
    python fetch_l1_txs.py
}

# Initial run
run_scripts

# Loop to run the scripts every 30 seconds
while true; do
    echo "Waiting for 30 seconds before the next run..."
    sleep 30
    run_scripts
done
