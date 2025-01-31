#!/tn/5Net/raid-gold/virtual-python-dirs/hpc-public/bin/python3.12
"""
mytool.py

A unified CLI with subcommands:
  1) inspect-numa          -> Show NUMA info (if py-libnuma is installed)
  2) validate <tasks_file> -> Parse a DAG tasks file and print expected runtime
  3) run <tasks_file>      -> Run tasks using thread-based concurrency
  4) run-hpc <tasks_file>  -> Run tasks using multiprocessing (HPC-style)
  5) chunked-primes <start> <end>
       --slurm   => Submits a SINGLE HPC Slurm job that uses all idle cores,
                     with the job template overridden to request the correct
                     number of nodes and tasks.
       --dynamic => Automatically detect available cores (local or from partition).
       --merge-results => After job finish, run an aggregator script.

Prerequisites:
  - A valid job template JSON in template/job_template.json (see below)
  - A script “scripts/hpc_job.sh” that is the job launcher (calls srun, etc.)
  - A module “modules/multi_node_prime_finder.py” that performs the per‑rank prime‐finding
  - An aggregator script “scripts/final_aggregator.sh” that merges the partial logs/results.
"""

import sys
import argparse
import os
import time
import subprocess

from modules.numa_investigation import investigate_numa_domains
from modules.tasks_parser import (
    parse_tasks,
    validate_and_topological_sort,
    compute_expected_runtime
)
from modules.scheduling import run_tasks_with_threads, run_tasks_multiprocessing, HPC_Task
from modules.slurm_detection import is_slurm_available
from modules.slurm_client import (
    list_partitions_detailed,
    prompt_for_partition_detailed,
    get_partition_idle_cpus,
    _build_socket_url
)

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")


def merge_and_cleanup_results(run_id):
    """
    (Local HPC use only)
    Merge partial result files (run_<run_id>_prime_chunk_*.txt) into
    results/final_<run_id>.txt and remove the partial files.
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


def local_cpu_count_via_numa_or_default():
    """Return total number of local cores (using py-libnuma if available, else multiprocessing.cpu_count)."""
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


def poll_job_until_finished(job_id, max_wait_seconds=300, poll_interval=5):
    """
    Poll squeue until the job is no longer listed (assumed finished) or time out.
    """
    waited = 0
    while waited < max_wait_seconds:
        result = subprocess.run(["squeue", "-h", "-j", str(job_id)],
                                  capture_output=True, text=True)
        if not result.stdout.strip():
            print(f"Job {job_id} no longer in squeue => finished.")
            return True
        print(f"Job {job_id} still in queue, waiting {poll_interval}s ...")
        time.sleep(poll_interval)
        waited += poll_interval
    return False


def submit_single_hpc_job(partition, idle_cores, node_count, start, end, run_id):
    """
    Read the job_template.json file, override its placeholders with the given values,
    and POST the JSON to the Slurm REST API.
    
    Parameters:
      partition   - Chosen partition name (string)
      idle_cores  - Total idle cores to use (int)
      node_count  - Number of nodes available in that partition (int)
      start       - Start of range (int)
      end         - End of range (int)
      run_id      - Run identifier (string)
      
    Returns the submitted job_id (string) or None.
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

    # Replace script placeholder for RUN_ID
    script = job_data["script"]
    script = script.replace("RUN_ID_PLACEHOLDER", run_id)
    job_data["script"] = script

    # Override environment variables
    new_env = []
    for kv in job_data["job"]["environment"]:
        if "TOTAL_CORES" in kv:
            new_env.append(f"TOTAL_CORES={idle_cores}")
        elif "RANGE_START" in kv:
            new_env.append(f"RANGE_START={start}")
        elif "RANGE_END" in kv:
            new_env.append(f"RANGE_END={end}")
        elif "RUN_ID_PLACEHOLDER" in kv:
            new_env.append(kv.replace("RUN_ID_PLACEHOLDER", run_id))
        else:
            new_env.append(kv)
    job_data["job"]["environment"] = new_env

    # Override nodes, tasks, and cpus_per_task based on partition capacity.
    job_data["job"]["nodes"] = node_count
    job_data["job"]["tasks"] = idle_cores
    job_data["job"]["cpus_per_task"] = 1

    # Override partition and job name
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


def main():
    parser = argparse.ArgumentParser(description="Example HPC tool with multiple subcommands.")
    subparsers = parser.add_subparsers(dest="command")

    # 1) inspect-numa
    sp_numa = subparsers.add_parser("inspect-numa", help="Show NUMA topology (if py-libnuma installed).")

    # 2) validate
    sp_val = subparsers.add_parser("validate", help="Validate a DAG tasks file and show expected runtime.")
    sp_val.add_argument("task_file", help="Path to tasks file.")

    # 3) run (threads)
    sp_run = subparsers.add_parser("run", help="Run tasks with thread-based concurrency.")
    sp_run.add_argument("task_file", help="Path to tasks file.")
    sp_run.add_argument("--max-workers", type=int, default=None, help="Max threads.")

    # 4) run-hpc (multiprocessing)
    sp_hpc = subparsers.add_parser("run-hpc", help="Run tasks using multiprocessing (HPC-style).")
    sp_hpc.add_argument("task_file", help="Path to tasks file.")
    sp_hpc.add_argument("--max-procs", type=int, default=None, help="Max processes.")

    # 5) chunked-primes
    sp_chunk = subparsers.add_parser("chunked-primes",
                                     help="Split a prime range into subranges, run either locally or as a single Slurm HPC job.")
    sp_chunk.add_argument("start", type=int, help="Start of range (inclusive).")
    sp_chunk.add_argument("end", type=int, help="End of range (inclusive).")
    sp_chunk.add_argument("--chunks", type=int, default=4,
                          help="Number of local HPC chunks (if not using --slurm).")
    sp_chunk.add_argument("--dynamic", action="store_true",
                          help="Automatically detect available cores (local or partition idle cores).")
    sp_chunk.add_argument("--max-procs", type=int, default=None,
                          help="Max processes for local HPC (optional).")
    sp_chunk.add_argument("--slurm", action="store_true",
                          help="Submit as a SINGLE HPC job via Slurm using all idle cores.")
    sp_chunk.add_argument("--partition", default=None,
                          help="Partition to use if --slurm is set (default prompts and defaults to 'Tunninet').")
    sp_chunk.add_argument("--run-id", default=None,
                          help="Optional run ID (defaults to current timestamp).")
    sp_chunk.add_argument("--merge-results", action="store_true",
                          help="After job finishes, merge partial results (local HPC only).")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(0)

    # --- Subcommand 1: inspect-numa ---
    if args.command == "inspect-numa":
        investigate_numa_domains()
        return

    # --- Subcommand 2: validate ---
    if args.command == "validate":
        try:
            tasks = parse_tasks(args.task_file)
            topo, adj, _ = validate_and_topological_sort(tasks)
            exp_runtime = compute_expected_runtime(tasks, topo)
            print(f"Tasks are valid. Expected runtime: {exp_runtime:.2f} s.")
        except ValueError as e:
            print(f"Validation error: {e}")
            sys.exit(1)
        return

    # --- Subcommand 3: run (threads) ---
    if args.command == "run":
        try:
            tasks = parse_tasks(args.task_file)
            topo, adj, _ = validate_and_topological_sort(tasks)
            exp_runtime = compute_expected_runtime(tasks, topo)
            actual_runtime = run_tasks_with_threads(tasks, adj, max_workers=args.max_workers)
            diff = actual_runtime - exp_runtime
            print(f"[Threads] Expected runtime: {exp_runtime:.2f} s")
            print(f"[Threads] Actual runtime:   {actual_runtime:.2f} s")
            print(f"[Threads] Difference:       {diff:.2f} s")
        except ValueError as e:
            print(f"Validation error: {e}")
            sys.exit(1)
        return

    # --- Subcommand 4: run-hpc ---
    if args.command == "run-hpc":
        try:
            tasks = parse_tasks(args.task_file)
            topo, adj, _ = validate_and_topological_sort(tasks)
            exp_runtime = compute_expected_runtime(tasks, topo)
            actual_runtime = run_tasks_multiprocessing(tasks, adj, max_procs=args.max_procs)
            diff = actual_runtime - exp_runtime
            print(f"[HPC] Expected runtime: {exp_runtime:.2f} s")
            print(f"[HPC] Actual runtime:   {actual_runtime:.2f} s")
            print(f"[HPC] Difference:       {diff:.2f} s")
        except ValueError as e:
            print(f"Validation error: {e}")
            sys.exit(1)
        return

    # --- Subcommand 5: chunked-primes ---
    if args.command == "chunked-primes":
        start, end = args.start, args.end
        run_id = args.run_id or str(int(time.time()))

        if args.slurm:
            # Use Slurm single job submission.
            if not is_slurm_available():
                print("❌ Slurm not detected or missing slurmrestd socket.")
                sys.exit(1)

            # Get detailed partition info and let the user choose.
            from modules.slurm_client import list_partitions_detailed, prompt_for_partition_detailed, get_partition_idle_cpus
            parts_detailed = list_partitions_detailed()
            chosen_partition = prompt_for_partition_detailed(parts_detailed, default="Tunninet")
            if not chosen_partition:
                print("Aborted: no partition chosen.")
                sys.exit(0)

            # From the detailed info, get node count and idle CPU count.
            node_count = 0
            idle = 0
            for pd in parts_detailed:
                if pd["name"] == chosen_partition:
                    node_count = len(pd.get("nodes", []))
                    idle = pd.get("idle_cpus", 0)
                    break
            if idle < 1:
                idle = 1
            print(f"[SingleJob] Partition='{chosen_partition}', idle_cpus={idle}, node_count={node_count}")
            confirm = input(f"Use {idle} cores across {node_count} nodes for prime-finding in [{start}..{end}]? [Y/n] ").strip().lower()
            if confirm not in ("y", "yes", ""):
                print("Aborted by user.")
                sys.exit(0)

            job_id = submit_single_hpc_job(chosen_partition, idle, node_count, start, end, run_id)
            if not job_id:
                print("❌ Submission failed.")
                sys.exit(1)

            print(f"✅ Single HPC job submitted as job_id={job_id}.")
            # Poll until job is no longer in squeue
            poll_job_until_finished(job_id, max_wait_seconds=300, poll_interval=5)
            print(f"Job {job_id} presumably finished. Running aggregator...")
            aggregator_path = os.path.join(os.path.dirname(__file__), "scripts", "final_aggregator.sh")
            if os.path.isfile(aggregator_path):
                subprocess.run([aggregator_path, str(job_id)])
            else:
                print(f"No aggregator found at {aggregator_path}, skipping final merge.")
        else:
            # Local HPC concurrency approach (multiple subjobs launched locally)
            chunk_count = args.chunks
            if args.dynamic:
                chunk_count = local_cpu_count_via_numa_or_default()
                print(f"[Local HPC] dynamic => {chunk_count} chunks (matching local cores).")
            total = end - start + 1
            chunk_size = total // chunk_count
            remainder = total % chunk_count
            subranges = []
            s_val = start
            for i in range(chunk_count):
                r = 1 if i < remainder else 0
                e_val = s_val + chunk_size + r - 1
                if e_val > end:
                    e_val = end
                if s_val > end:
                    break
                subranges.append((s_val, e_val))
                s_val = e_val + 1
            print(f"[Chunked-Primes] run_id={run_id}, subranges = {subranges}")

            tasks_map = {}
            adjacency = {}
            for i, (ss, ee) in enumerate(subranges, start=1):
                tname = f"prime_chunk_{i}"
                tasks_map[tname] = HPC_Task(
                    name=tname,
                    duration=3,  # simulate or use real prime finder
                    dependencies=[],
                    meta={"subrange": (ss, ee)}
                )
                adjacency[tname] = {}

            actual_runtime = run_tasks_multiprocessing(tasks_map, adjacency, max_procs=args.max_procs)
            print(f"[Local HPC] subranges done in {actual_runtime:.2f} s.")
            os.makedirs(RESULTS_DIR, exist_ok=True)
            for i, (ss, ee) in enumerate(subranges, start=1):
                fname = os.path.join(RESULTS_DIR, f"run_{run_id}_prime_chunk_{i}.txt")
                with open(fname, "w") as f:
                    f.write(f"Found ??? prime(s) in [{ss}..{ee}].\n")
            if args.merge_results:
                merge_and_cleanup_results(run_id)
        return

    parser.print_help()

if __name__ == "__main__":
    main()

