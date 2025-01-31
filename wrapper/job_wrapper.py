#!/usr/bin/env python3
"""
job_wrapper.py

Wraps the user's command, logs to 'logs' directory in job_<JOB_ID>_<node>.log

Optionally sets RESULT_FILE in environment if --run-id is given:
  results/run_{run_id}_{job_id}.txt
"""

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", required=True, help="SLURM_JOB_ID or local HPC job ID")
    parser.add_argument("--node", required=True, help="Hostname or node name")
    parser.add_argument("--cmd", required=True, help="The command to run under this wrapper")
    parser.add_argument("--run-id", default=None, help="Optional run ID to tag results with")
    args = parser.parse_args()

    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)

    log_file = os.path.join(LOG_DIR, f"job_{args.job_id}_{args.node}.log")
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")

    logging.info(f"== Starting job {args.job_id} on node {args.node} ==")
    logging.info(f"Command to run: {args.cmd}")

    if args.run_id:
        result_file = os.path.join(RESULTS_DIR, f"run_{args.run_id}_{args.job_id}.txt")
        os.environ["RESULT_FILE"] = result_file

    start_time = datetime.now()
    try:
        completed = subprocess.run(args.cmd, shell=True, capture_output=True, text=True, check=True)
        if completed.stdout:
            logging.info(f"Command output:\n{completed.stdout}")
        if completed.stderr:
            logging.info(f"Command stderr:\n{completed.stderr}")
        logging.info("Command completed with return code 0")
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed with return code {e.returncode}. Stderr:\n{e.stderr}")
        sys.exit(e.returncode)

    elapsed = (datetime.now() - start_time).total_seconds()
    logging.info(f"== Completed job {args.job_id} on node {args.node} in {elapsed:.2f} seconds ==")


if __name__ == "__main__":
    main()

