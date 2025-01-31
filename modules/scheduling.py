"""
scheduling.py

Provides classes and functions for parallel task scheduling in two flavors:
1) Thread-based scheduling (run_tasks_with_threads)
2) Multiprocessing HPC scheduling with optional NUMA pinning (run_tasks_multiprocessing)

Expose HPC_Task for storing task metadata.
"""

import os
import time
import multiprocessing
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

#################
# HPC_Task Class
#################

class HPC_Task:
    """
    A simple container for a single task's information.

    Attributes:
      - name (str): Unique identifier for the task
      - duration (int|float): Number of seconds to simulate for this task
      - dependencies (list of str): Names of tasks that must finish first
      - meta (dict): Extra metadata (optional)
    """
    def __init__(self, name, duration, dependencies=None, meta=None):
        self.name = name
        self.duration = duration
        self.dependencies = dependencies or []
        self.meta = meta or {}

###############################
# Thread-based Scheduling
###############################

def run_tasks_with_threads(tasks, adjacency_list, max_workers=None):
    """
    Run tasks in parallel using threads. Not HPC due to Python's GIL, but
    it's a simpler approach for demonstration.

    Arguments:
      tasks (dict[str, HPC_Task]): Mapping from task_name -> HPC_Task
      adjacency_list (dict[str, list[str]]): For each task, which tasks depend on it
      max_workers (int|None): If None, defaults to ThreadPoolExecutor logic

    Returns:
      float: The total wall-clock time taken.
    """
    # Compute in-degree for each task
    in_degree = {t_name: 0 for t_name in tasks}
    for t_name, t_obj in tasks.items():
        for d in t_obj.dependencies:
            in_degree[t_name] += 1

    # Initialize queue with tasks having in_degree = 0
    ready_queue = deque([t for t, deg in in_degree.items() if deg == 0])
    running_futures = []

    start_time = time.time()

    def worker(task_name):
        # Just sleep to simulate duration
        # In real code, do the actual CPU/IO work
        duration = tasks[task_name].duration
        time.sleep(duration)
        return task_name

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while ready_queue or running_futures:
            # Submit all currently ready tasks
            while ready_queue:
                next_task = ready_queue.popleft()
                fut = executor.submit(worker, next_task)
                running_futures.append(fut)

            # Wait for at least one to finish
            done, not_done = wait(running_futures, return_when=FIRST_COMPLETED)
            running_futures = list(not_done)

            # For each finished future, mark its dependents
            for completed_fut in done:
                finished_task = completed_fut.result()
                # Decrement in-degree of each dependent
                for dep in adjacency_list[finished_task]:
                    in_degree[dep] -= 1
                    if in_degree[dep] == 0:
                        ready_queue.append(dep)

    end_time = time.time()
    return end_time - start_time

###############################
# Multiprocessing (HPC) Scheduling
###############################

try:
    from numa import info, schedule, memory
    has_pynuma = True
except ImportError:
    has_pynuma = False

def check_libnuma_dependency():
    """
    Return True if system library `libnuma.so` is found; else False.
    Typically installed by `apt-get install -y numactl libnuma-dev` (Debian/Ubuntu)
    or `yum install -y numactl numactl-devel` (RHEL/CentOS).
    """
    possible_paths = ["/usr/lib/libnuma.so", "/usr/lib64/libnuma.so"]
    return any(os.path.exists(path) for path in possible_paths)

def _process_worker(task_name, node_id, duration, pipe_conn):
    """
    Worker function for HPC approach:
      - Optionally pins memory + CPU to a node (NUMA)
      - Sleeps for `duration`
      - Sends back `task_name` on completion
    """
    if has_pynuma and check_libnuma_dependency():
        try:
            # Pin memory allocations to the chosen node
            memory.set_membind_nodes(node_id)
            # Also pin CPU scheduling to that node
            schedule.run_on_nodes(node_id)
            print(f"[Process Worker] Task={task_name} pinned to NUMA node {node_id}")
        except Exception as e:
            print(f"[Process Worker] WARNING: could not set NUMA for {task_name}: {e}")

    time.sleep(duration)
    pipe_conn.send(task_name)
    pipe_conn.close()

def run_tasks_multiprocessing(tasks, adjacency_list, max_procs=None):
    """
    HPC approach with multiple processes, pinned to nodes if possible.
    Works best if py-libnuma and system libnuma.so are installed.

    Arguments:
      tasks (dict[str, HPC_Task]): Mapping from task_name -> HPC_Task
      adjacency_list (dict[str, list[str]]): For each task, which tasks depend on it
      max_procs (int|None): Max concurrency (# processes)

    Returns:
      float: total wall-clock time
    """
    # if NUMA is available
    if has_pynuma and check_libnuma_dependency():
        # get maximum node from the system
        max_node = info.get_max_node()
        node_list = list(range(max_node + 1))
    else:
        # fallback: treat the system as a single node
        node_list = [0]

    if max_procs is None:
        # default = # of nodes or # CPUs, pick your heuristic
        max_procs = len(node_list)

    # Build fresh in-degree
    in_degree = {t_name: 0 for t_name in tasks}
    for t_name, t_obj in tasks.items():
        for d in t_obj.dependencies:
            in_degree[t_name] += 1

    ready_queue = deque([n for n, deg in in_degree.items() if deg == 0])
    running_processes = []

    # track how many tasks are active on each node
    active_on_node = {nid: 0 for nid in node_list}

    start_time = time.time()
    remaining_tasks = len(tasks)

    def spawn_task(task_name):
        node_id = min(node_list, key=lambda n: active_on_node[n])
        active_on_node[node_id] += 1

        parent_conn, child_conn = multiprocessing.Pipe(duplex=False)
        duration = tasks[task_name].duration

        p = multiprocessing.Process(
            target=_process_worker,
            args=(task_name, node_id, duration, child_conn)
        )
        p.start()
        running_processes.append((p, parent_conn, node_id))

    while remaining_tasks > 0:
        # spawn tasks if capacity
        while ready_queue and len(running_processes) < max_procs:
            spawn_task(ready_queue.popleft())

        if not running_processes:
            time.sleep(0.05)
            continue

        # small poll-based wait
        time.sleep(0.05)
        finished = []
        still_running = []
        for (proc, pipe, node_id) in running_processes:
            if pipe.poll():
                finished_task = pipe.recv()
                finished.append((proc, pipe, node_id, finished_task))
            else:
                still_running.append((proc, pipe, node_id))

        running_processes = still_running

        for (proc, pipe, node_id, finished_task) in finished:
            proc.join()
            active_on_node[node_id] -= 1
            remaining_tasks -= 1
            for dep in adjacency_list[finished_task]:
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    ready_queue.append(dep)

    # clean up
    for (proc, _, _) in running_processes:
        proc.join()

    end_time = time.time()
    return end_time - start_time

