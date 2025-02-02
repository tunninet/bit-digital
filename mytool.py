#!/tn/5Net/raid-gold/virtual-python-dirs/hpc-public/bin/python3.12
"""
mytool.py

A unified CLI with subcommands:
  1) inspect-numa          -> Show NUMA topology (if py‑libnuma is installed)
  2) validate <tasks_file>  -> Parse a DAG tasks file and print expected runtime
  3) run <tasks_file>       -> Run tasks using thread‐based concurrency
  4) run-hpc <tasks_file>   -> Run tasks using multiprocessing (HPC-style)
  5) chunked-primes <start> <end>
       --slurm        => Submit a SINGLE HPC job via Slurm (using the REST API)
       --dynamic      => Automatically detect available cores (local or partition idle cores)
       --merge-results=> After job finish, run aggregator to merge logs and produce final results

When running locally (no --slurm), the tool splits the numeric range into chunks and spawns local processes that call 
“python -m modules.find_primes start end”. Their output is captured and written in a log file and a result file 
(with a format that the aggregator script later uses).

When using --slurm, a single job is submitted via the Slurm REST API (using a job template in template/job_template.json)
with overridden environment variables (TOTAL_CORES, RANGE_START, RANGE_END, RUN_ID, nodes, tasks, etc.). An external 
aggregator script (scripts/final_aggregator.sh) is then invoked to merge the per‑rank logs and results.

Prerequisites:
  - A valid job template JSON file in template/job_template.json.
  - A job launcher script in scripts/hpc_job.sh.
  - The module modules/multi_node_prime_finder.py which is invoked by the HPC job.
  - An aggregator script in scripts/final_aggregator.sh.
  - (Optionally) a tasks file (for validate, run, run-hpc).

All paths are determined using relative (absolute resolved) paths.
"""

import sys
import argparse
import os
import time
import subprocess
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

from modules.numa_investigation import investigate_numa_domains
from modules.tasks_parser import parse_tasks, validate_and_topological_sort, compute_expected_runtime
from modules.slurm_detection import is_slurm_available
from modules.slurm_client import (
    list_partitions_detailed,
    prompt_for_partition_detailed,
    get_partition_idle_cpus,
    _build_socket_url,
    create_slurm_payload_and_submit,
)

# Define directories (absolute paths)
LOGS_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), "logs")
RESULTS_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), "results")

def run_chunk_local(rank, run_id, ss, ee):
    """
    Run a single chunk locally by invoking 'python -m modules.find_primes ss ee'
    and capturing its output. Write a log file in LOGS_DIR and a result file in RESULTS_DIR.
    Returns elapsed time in seconds.
    """
    import re
    start_time = datetime.now()
    cmd = ["python", "-m", "modules.find_primes", str(ss), str(ee)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    stdout = proc.stdout.strip()
    # Parse out the prime count and list (assuming find_primes prints a line "Found N prime(s) in range [...]" 
    # and a separate line "Primes=[2,3,5,...]").
    m = re.search(r"Found\s+(\d+)\s+prime\(s\)", stdout)
    found_count = int(m.group(1)) if m else 0
    m_primes = re.search(r"Primes=\[([^\]]*)\]", stdout)
    if m_primes:
        prime_list_str = f"[{m_primes.group(1).strip()}]"
    else:
        prime_list_str = "[]"
    log_path = os.path.join(LOGS_DIR, f"job_{run_id}_rank_{rank}.log")
    with open(log_path, "w") as lf:
        lf.write(f"Rank={rank}, Node=Local, subrange=[{ss}..{ee}], found={found_count} primes\n")
        lf.write(f"Primes={prime_list_str}\n")
        lf.write(f"Elapsed={elapsed:.2f}s\n")
    result_path = os.path.join(RESULTS_DIR, f"run_{run_id}_prime_chunk_{rank}.txt")
    with open(result_path, "w") as rf:
        rf.write(f"Rank={rank}, subrange=[{ss}..{ee}], found={found_count}, Primes={prime_list_str}\n")
    return elapsed

def run_local_prime_finder(subranges, run_id, max_procs):
    """
    Run local prime finding concurrently on all subranges.
    Returns the total elapsed time.
    """
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=max_procs) as executor:
        futures = []
        for i, (ss, ee) in enumerate(subranges, start=1):
            futures.append(executor.submit(run_chunk_local, i, run_id, ss, ee))
        for fut in as_completed(futures):
            fut.result()  # Propagate exceptions if any
    return time.time() - t0

def merge_and_cleanup_results(run_id):
    """
    Merge all result files (run_<run_id>_prime_chunk_*.txt) into a final report in RESULTS_DIR and delete the partial files.
    The final file will be at RESULTS_DIR/final_<run_id>.txt.
    """
    import glob
    pattern = os.path.join(RESULTS_DIR, f"run_{run_id}_prime_chunk_*.txt")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"[Aggregator] No chunk result files found matching {pattern}")
        return
    final_path = os.path.join(RESULTS_DIR, f"final_{run_id}.txt")
    with open(final_path, "w") as outfile:
        for fname in files:
            with open(fname, "r") as f:
                outfile.write(f.read())
    for fname in files:
        os.remove(fname)
    print(f"[Aggregator] Merged {len(files)} chunk files into {final_path}, then deleted them.")

def poll_job_until_finished(job_id, max_wait_seconds=300, poll_interval=5):
    """
    Poll squeue until the job is no longer in the queue (assumed finished) or a timeout occurs.
    """
    waited = 0
    while waited < max_wait_seconds:
        result = subprocess.run(["squeue", "-h", "-j", str(job_id)], capture_output=True, text=True)
        if not result.stdout.strip():
            print(f"Job {job_id} no longer in squeue => finished.")
            return True
        print(f"Job {job_id} still in queue, waiting {poll_interval}s ...")
        time.sleep(poll_interval)
        waited += poll_interval
    return False

def submit_single_hpc_job(partition, idle_cores, node_count, start, end, run_id):
    """
    Submit a single HPC job via the Slurm REST API.
    Reads the job template from template/job_template.json, overrides environment variables and resource values,
    and POSTs the JSON. Returns the job_id as a string or None.
    """
    import json
    import requests_unixsocket
    script_dir = os.path.abspath(os.path.dirname(__file__))
    templ_path = os.path.join(script_dir, "template", "job_template.json")
    if not os.path.isfile(templ_path):
        print(f"Error: job_template.json not found at {templ_path}")
        return None
    with open(templ_path, "r") as f:
        job_data = json.load(f)
    script = job_data["script"]
    script = script.replace("RUN_ID_PLACEHOLDER", run_id)
    job_data["script"] = script
    new_env = []
    for kv in job_data["job"]["environment"]:
        if "TOTAL_CORES=" in kv:
            new_env.append(f"TOTAL_CORES={idle_cores}")
        elif "RANGE_START=" in kv:
            new_env.append(f"RANGE_START={start}")
        elif "RANGE_END=" in kv:
            new_env.append(f"RANGE_END={end}")
        elif "RUN_ID=" in kv:
            new_env.append(f"RUN_ID={run_id}")
        else:
            new_env.append(kv)
    job_data["job"]["environment"] = new_env
    job_data["job"]["nodes"] = node_count
    job_data["job"]["tasks"] = idle_cores
    job_data["job"]["cpus_per_task"] = 1
    job_data["job"]["partition"] = partition
    job_data["job"]["name"] = f"SinglePrimeFinder_{run_id}"
    s = requests_unixsocket.Session()
    url = _build_socket_url("/var/run/slurmrestd/slurmrestd.sock", "/slurm/v0.0.40/job/submit")
    resp = s.post(url, json=job_data)
    resp.raise_for_status()
    submit_resp = resp.json()
    job_id = submit_resp.get("job_id")
    if job_id:
        print(f"   -> Submitted single HPC job_id={job_id}")
    else:
        print(f"   -> Submission failed: {submit_resp}")
    return job_id

def local_cpu_count():
    """
    Return the total number of local cores using py-libnuma if available, else use multiprocessing.cpu_count().
    """
    try:
        from numa import info
        node_count = info.get_max_node() + 1
        total_cores = 0
        for nid in range(node_count):
            cpus = info.node_to_cpus(nid)
            total_cores += len(cpus)
        if total_cores > 0:
            return total_cores
    except ImportError:
        pass
    import multiprocessing
    return multiprocessing.cpu_count()

def main():
    parser = argparse.ArgumentParser(description="HPC Tool CLI")
    subparsers = parser.add_subparsers(dest="command")

    # Subcommand: inspect-numa
    sp_numa = subparsers.add_parser("inspect-numa", help="Show NUMA topology")
    
    # Subcommand: validate
    sp_val = subparsers.add_parser("validate", help="Validate a DAG tasks file and compute expected runtime")
    sp_val.add_argument("task_file", help="Path to tasks file")
    
    # Subcommand: run (threads)
    sp_run = subparsers.add_parser("run", help="Run tasks with thread-based concurrency")
    sp_run.add_argument("task_file", help="Path to tasks file")
    sp_run.add_argument("--max-workers", type=int, default=None, help="Max threads")
    
    # Subcommand: run-hpc (multiprocessing)
    sp_hpc = subparsers.add_parser("run-hpc", help="Run tasks with multiprocessing (HPC-style)")
    sp_hpc.add_argument("task_file", help="Path to tasks file")
    sp_hpc.add_argument("--max-procs", type=int, default=None, help="Max processes")
    
    # Subcommand: chunked-primes
    sp_chunk = subparsers.add_parser("chunked-primes", help="Process prime range in chunks")
    sp_chunk.add_argument("start", type=int, help="Start of range (inclusive)")
    sp_chunk.add_argument("end", type=int, help="End of range (inclusive)")
    sp_chunk.add_argument("--chunks", type=int, default=4, help="Number of chunks for local HPC")
    sp_chunk.add_argument("--dynamic", action="store_true", help="Automatically detect available cores")
    sp_chunk.add_argument("--max-procs", type=int, default=None, help="Max processes for local HPC")
    sp_chunk.add_argument("--slurm", action="store_true", help="Submit as a SINGLE HPC job via Slurm")
    sp_chunk.add_argument("--partition", default=None, help="Partition to use if --slurm is set")
    sp_chunk.add_argument("--run-id", default=None, help="Optional run ID (default timestamp)")
    sp_chunk.add_argument("--merge-results", action="store_true", help="Merge partial results (local HPC only)")
    
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(0)
    
    if args.command == "inspect-numa":
        investigate_numa_domains()
        return
    elif args.command == "validate":
        from modules.tasks_parser import parse_tasks, validate_and_topological_sort, compute_expected_runtime
        try:
            tasks = parse_tasks(args.task_file)
            topo, adj, _ = validate_and_topological_sort(tasks)
            exp = compute_expected_runtime(tasks, topo)
            print(f"Tasks are valid. Expected runtime: {exp:.2f} s.")
        except ValueError as e:
            print(f"Validation error: {e}")
            sys.exit(1)
        return
    elif args.command == "run":
        from modules.tasks_parser import parse_tasks, validate_and_topological_sort, compute_expected_runtime
        from modules.scheduling import run_tasks_with_threads
        try:
            tasks = parse_tasks(args.task_file)
            topo, adj, _ = validate_and_topological_sort(tasks)
            exp = compute_expected_runtime(tasks, topo)
            actual = run_tasks_with_threads(tasks, adj, max_workers=args.max_workers)
            diff = actual - exp
            print(f"[Threads] Expected runtime: {exp:.2f} s")
            print(f"[Threads] Actual runtime:   {actual:.2f} s")
            print(f"[Threads] Difference:       {diff:.2f} s")
        except ValueError as e:
            print(f"Validation error: {e}")
            sys.exit(1)
        return
    elif args.command == "run-hpc":
        from modules.tasks_parser import parse_tasks, validate_and_topological_sort, compute_expected_runtime
        from modules.scheduling import run_tasks_multiprocessing
        try:
            tasks = parse_tasks(args.task_file)
            topo, adj, _ = validate_and_topological_sort(tasks)
            exp = compute_expected_runtime(tasks, topo)
            actual = run_tasks_multiprocessing(tasks, adj, max_procs=args.max_procs)
            diff = actual - exp
            print(f"[HPC] Expected runtime: {exp:.2f} s")
            print(f"[HPC] Actual runtime:   {actual:.2f} s")
            print(f"[HPC] Difference:       {diff:.2f} s")
        except ValueError as e:
            print(f"Validation error: {e}")
            sys.exit(1)
        return
    elif args.command == "chunked-primes":
        s_val, e_val = args.start, args.end
        run_id = args.run_id or str(int(time.time()))
        if args.slurm:
            if not is_slurm_available():
                print("❌ Slurm not detected or missing slurmrestd socket.")
                sys.exit(1)
            from modules.slurm_client import list_partitions_detailed, prompt_for_partition_detailed, get_partition_idle_cpus
            parts_detailed = list_partitions_detailed()
            chosen = prompt_for_partition_detailed(parts_detailed, default="Tunninet")
            if not chosen:
                print("Aborted: no partition chosen.")
                sys.exit(0)
            node_count = 0
            idle_cores = 0
            for pd in parts_detailed:
                if pd["name"] == chosen:
                    node_count = len(pd.get("nodes", []))
                    idle_cores = pd.get("idle_cpus", 0)
                    break
            if idle_cores < 1:
                idle_cores = 1
            print(f"[SingleJob] Partition='{chosen}', idle_cpus={idle_cores}, node_count={node_count}")
            confirm = input(f"Use {idle_cores} cores across {node_count} node(s) for prime-finding in [{s_val}..{e_val}]? [Y/n] ").strip().lower()
            if confirm not in ("y", "yes", ""):
                print("Aborted by user.")
                sys.exit(0)
            job_id = submit_single_hpc_job(chosen, idle_cores, node_count, s_val, e_val, run_id)
            if not job_id:
                print("❌ Submission failed.")
                sys.exit(1)
            print(f"✅ Single HPC job submitted as job_id={job_id}")
            finished = poll_job_until_finished(job_id, max_wait_seconds=300, poll_interval=5)
            if finished:
                print(f"Job {job_id} finished. Running aggregator...")
            else:
                print(f"Job {job_id} did NOT finish within timeout.")
            aggregator_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "scripts", "final_aggregator.sh")
            if os.path.isfile(aggregator_path):
                subprocess.run([aggregator_path, str(job_id)])
            else:
                print(f"No aggregator script found at {aggregator_path}.")
        else:
            chunk_count = args.chunks
            if args.dynamic:
                chunk_count = local_cpu_count()
                print(f"[Local HPC] Detected {chunk_count} cores for concurrency.")
            total_range = e_val - s_val + 1
            chunk_size = total_range // chunk_count
            remainder = total_range % chunk_count
            subranges = []
            current = s_val
            for i in range(chunk_count):
                extra = 1 if i < remainder else 0
                last = current + chunk_size + extra - 1
                if last > e_val:
                    last = e_val
                subranges.append((current, last))
                current = last + 1
            print(f"[Chunked-Primes] run_id={run_id}, subranges = {subranges}")
            maxp = args.max_procs or chunk_count
            actual_time = run_local_prime_finder(subranges, run_id, maxp)
            print(f"[Local HPC] Subranges done in {actual_time:.2f} s.")
            os.makedirs(RESULTS_DIR, exist_ok=True)
            master_primes = []
            for i, (ss, ee) in enumerate(subranges, start=1):
                chunk_file = os.path.join(RESULTS_DIR, f"run_{run_id}_prime_chunk_{i}.txt")
                if os.path.isfile(chunk_file):
                    with open(chunk_file, "r") as cf:
                        line = cf.read().strip()
                        import re
                        m = re.search(r"Primes=\[([^\]]*)\]", line)
                        if m:
                            primes_str = m.group(1)
                            if primes_str.strip():
                                primes_list = [int(x.strip()) for x in primes_str.split(",") if x.strip().isdigit()]
                                master_primes.extend(primes_list)
            master_primes = sorted(set(master_primes))
            master_file = os.path.join(RESULTS_DIR, f"Local-Chunked-Primes-{s_val}..{e_val}-{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
            with open(master_file, "w") as mf:
                for prime in master_primes:
                    mf.write(f"{prime}\n")
            print(f"[Local HPC] Master prime list written to {master_file}")
            if args.merge_results:
                merge_and_cleanup_results(run_id)
        return
    else:
        parser.print_help()

if __name__ == "__main__":
    main()

