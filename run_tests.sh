#!/bin/bash
# run_tests.sh
#
# This script runs a complete battery of tests for the HPC tool (mytool.py)
# and saves the output of each test into log files in the tests/ directory.
#
# Tests include:
#   1. inspect-numa
#   2. validate <tasks_file>
#   3. run <tasks_file> (thread-based concurrency)
#   4. run-hpc <tasks_file> (multiprocessing HPC)
#   5. chunked-primes (local HPC mode)
#   6. chunked-primes (Slurm single HPC job mode)
#
# It checks for the existence of the Slurm REST socket (/var/run/slurmrestd/slurmrestd.sock)
# to decide whether to run the Slurm test.
#
# Make sure your virtual environment is activated before running this script.

TESTS_DIR="./tests"
mkdir -p "$TESTS_DIR"

MASTER_LOG="$TESTS_DIR/test_run.log"
echo "==== Starting Test Run at $(date) ====" > "$MASTER_LOG"

# Helper function to run a command and log its output.
run_and_log() {
  local test_name="$1"
  local cmd="$2"
  local logfile="$TESTS_DIR/${test_name}.log"
  echo "==== ${test_name} ====" | tee "$logfile"
  echo "Command: $cmd" | tee -a "$logfile"
  eval "$cmd" 2>&1 | tee -a "$logfile"
  echo -e "\n==== End of ${test_name} ====\n" >> "$logfile"
  echo "Completed ${test_name}" | tee -a "$MASTER_LOG"
}

# Test 1: inspect-numa
run_and_log "test_inspect-numa" "./mytool.py inspect-numa"

# Test 2: validate a DAG tasks file (example: tasks/example.txt)
run_and_log "test_validate" "./mytool.py validate tasks/example.txt"

# Test 3: run tasks using threads
run_and_log "test_run_threads" "./mytool.py run tasks/example.txt --max-workers=2"

# Test 4: run tasks using multiprocessing (HPC)
run_and_log "test_run_hpc" "./mytool.py run-hpc tasks/example.txt --max-procs=2"

# Test 5: Local chunked-primes (simulate prime finding locally)
run_and_log "test_chunked_local" "./mytool.py chunked-primes 1 1000 --chunks=4 --merge-results"

# Test 6: Slurm single HPC job (if Slurm REST API is available)
if [ -e /var/run/slurmrestd/slurmrestd.sock ]; then
  run_and_log "test_chunked_slurm" "./mytool.py chunked-primes 1 20000 --slurm --dynamic --run-id=Test"
else
  echo "Skipping Slurm test because /var/run/slurmrestd/slurmrestd.sock not found." | tee -a "$MASTER_LOG"
fi

echo "==== Test Run Finished at $(date) ====" | tee -a "$MASTER_LOG"

