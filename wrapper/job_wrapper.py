#!/tn/5Net/raid-gold/virtual-python-dirs/hpc-public/bin/python3.12
"""
job_wrapper.py

Wraps the user's command, logs to /tn/5Net/raid-gold/private-git-code/hpc/logs
File name: job_<JOB_ID>_<NODE>.log

Usage example from a Slurm script:
  job_wrapper.py --job-id="$SLURM_JOB_ID" --node="$(hostname)" --cmd="sleep 30"
"""

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime

LOG_DIR = "/tn/5Net/raid-gold/private-git-code/hpc/logs"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", required=True, help="SLURM_JOB_ID")
    parser.add_argument("--node", required=True, help="Hostname or node name")
    parser.add_argument("--cmd", required=True, help="The command to run under this wrapper")
    args = parser.parse_args()

    # Ensure logs directory exists
    os.makedirs(LOG_DIR, exist_ok=True)

    # Log file named job_<job_id>_<node>.log
    node_sanitized = args.node.replace("/", "_")
    log_file = os.path.join(LOG_DIR, f"job_{args.job_id}_{node_sanitized}.log")

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s"
    )

    logging.info(f"== Starting job {args.job_id} on node {args.node} ==")
    logging.info(f"Command to run: {args.cmd}")

    start_time = datetime.now()

    try:
        # Run command in a shell, capturing stdout/stderr
        completed = subprocess.run(
            args.cmd, shell=True, check=True,
            capture_output=True, text=True
        )
        if completed.stdout:
            logging.info(f"Command output:\n{completed.stdout}")
        logging.info("Command completed with return code 0")
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed with return code {e.returncode}. Stderr:\n{e.stderr}")
        sys.exit(e.returncode)

    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    logging.info(f"== Completed job {args.job_id} on node {args.node} in {elapsed:.2f} seconds ==")

if __name__ == "__main__":
    main()
