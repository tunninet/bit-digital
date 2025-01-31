"""
modules/single_job_prime_finder.py

One-job approach:
  - command-line: python -m modules.single_job_prime_finder START END TOTAL_CORES RUN_ID
  - spawns multiple processes internally, each core handles a subrange
  - logs to job_{SLURM_JOB_ID}_{hostname}_core_{i}.log
"""

import sys
import os
import math
import socket
import multiprocessing
from datetime import datetime

def is_prime(n: int) -> bool:
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    i = 3
    while i * i <= n:
        if n % i == 0:
            return False
        i += 2
    return True

def worker_subrange(core_id, start, end, run_id):
    job_id = os.environ.get("SLURM_JOB_ID", "NoJobID")
    hostname = socket.gethostname()
    log_name = f"job_{job_id}_{hostname}_core_{core_id}.log"
    subrange_count = 0

    t0 = datetime.now()
    for x in range(start, end+1):
        if is_prime(x):
            subrange_count += 1
    elapsed = (datetime.now() - t0).total_seconds()

    with open(log_name, "w") as f:
        f.write(f"Core {core_id} handling [{start}..{end}]\n")
        f.write(f"Found {subrange_count} primes. Elapsed={elapsed:.2f}s\n")

def main():
    if len(sys.argv) < 4:
        print("Usage: python -m modules.single_job_prime_finder START END TOTAL_CORES [RUN_ID]")
        sys.exit(1)

    start = int(sys.argv[1])
    end = int(sys.argv[2])
    total_cores = int(sys.argv[3])
    run_id = sys.argv[4] if len(sys.argv) > 4 else "noRunID"

    print(f"[SingleJobPrime] start={start}, end={end}, total_cores={total_cores}, run_id={run_id}")

    total_nums = end - start + 1
    chunk_size = math.ceil(total_nums / total_cores)

    subranges = []
    s = start
    for i in range(total_cores):
        e = s + chunk_size - 1
        if e > end:
            e = end
        if s > end:
            break
        subranges.append((i, s, e))
        s = e + 1

    print("[SingleJobPrime] subranges:")
    for (cid, s1, e1) in subranges:
        print(f"  core#{cid}: [{s1}..{e1}]")

    processes = []
    for (cid, s1, e1) in subranges:
        p = multiprocessing.Process(target=worker_subrange, args=(cid, s1, e1, run_id))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    print("[SingleJobPrime] All cores finished. Check job_*_core_*.log files.")

if __name__ == "__main__":
    main()

