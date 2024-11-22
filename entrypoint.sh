#!/bin/bash

# Activate the virtual environment
source /app/.venv/bin/activate

# Function to run Python scripts with unbuffered output
run_scripts() {
    echo "Running main.py..."
    # Use python -u for unbuffered output
    PYTHONUNBUFFERED=1 python -u main.py &
    MAIN_PID=$!

    echo "Running fetch_l1_txs.py..."
    PYTHONUNBUFFERED=1 python -u fetch_l1_txs.py &
    FETCH_PID=$!

    # Wait for both processes and forward their signals
    wait $MAIN_PID $FETCH_PID
}

# Trap SIGTERM and kill child processes
trap 'kill $(jobs -p)' SIGTERM

# Run the scripts
run_scripts