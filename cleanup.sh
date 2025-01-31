#!/bin/bash
# cleanup.sh
#
# This script removes all generated log and result files.
# Optionally, if the "--tests" flag is passed, it will also
# clean the tests folder.
#
# Usage:
#   ./cleanup.sh           # Cleans logs/ and results/ only.
#   ./cleanup.sh --tests   # Also cleans tests/ directory.
#
# Directories to clean (relative to project root):
#   logs/     - Contains job logs.
#   results/  - Contains aggregated results.
#   tests/    - (Optional) Contains test run logs.

set -e

# Define directories
LOGS_DIR="./logs"
RESULTS_DIR="./results"
TESTS_DIR="./tests"

# Confirm cleanup of logs and results
echo "Cleaning up logs in $LOGS_DIR..."
rm -f "$LOGS_DIR"/*

echo "Cleaning up results in $RESULTS_DIR..."
rm -f "$RESULTS_DIR"/*

if [ "$1" == "--tests" ]; then
  echo "Cleaning up tests logs in $TESTS_DIR..."
  rm -f "$TESTS_DIR"/*
fi

echo "Cleanup complete."

