"""
tasks_parser.py

Parsing a tasks file:
  name, duration_in_seconds, dep1 dep2 dep3 ...
Compute topological order, detect cycles, etc.
"""

from collections import defaultdict, deque

class Task:
    """
    Basic container for a single task.
    """
    def __init__(self, name, duration, dependencies):
        self.name = name
        self.duration = duration
        self.dependencies = dependencies

def parse_tasks(file_path):
    """
    Read lines from a text file. Format:
      name,duration_in_seconds[,dep1 dep2 ...]
    Return dict {task_name: Task(...) }.
    """
    tasks = {}
    with open(file_path, 'r') as f:
        line_num = 0
        for line in f:
            line_num += 1
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            parts = line.split(',', 2)
            if len(parts) < 2:
                raise ValueError(f"Line {line_num}: must have at least 'name,duration'.")

            name = parts[0].strip()
            try:
                duration = int(parts[1].strip())
            except ValueError:
                raise ValueError(f"Line {line_num}: Duration must be integer.")

            deps = []
            if len(parts) == 3:
                dep_str = parts[2].strip()
                if dep_str:
                    deps = dep_str.split()

            if name in tasks:
                raise ValueError(f"Line {line_num}: Duplicate task name '{name}'.")

            tasks[name] = Task(name, duration, deps)

    return tasks

def validate_and_topological_sort(tasks):
    """
    Check for missing dependencies, cycles. Return (topo_order, adjacency_list, in_degree).
    Raises ValueError on error.
    """
    # Check missing deps
    for t_name, t_obj in tasks.items():
        for d in t_obj.dependencies:
            if d not in tasks:
                raise ValueError(f"Task '{t_name}' depends on unknown '{d}'.")

    adjacency_list = defaultdict(list)
    in_degree = {name: 0 for name in tasks}

    # Build adjacency
    for t_name, t_obj in tasks.items():
        for dep in t_obj.dependencies:
            adjacency_list[dep].append(t_name)
            in_degree[t_name] += 1

    # Kahn's Algorithm
    queue = deque([n for n, deg in in_degree.items() if deg == 0])
    topo_order = []

    while queue:
        current = queue.popleft()
        topo_order.append(current)
        for nxt in adjacency_list[current]:
            in_degree[nxt] -= 1
            if in_degree[nxt] == 0:
                queue.append(nxt)

    if len(topo_order) != len(tasks):
        raise ValueError("Cycle detected in dependencies.")
    return topo_order, adjacency_list, in_degree

def compute_expected_runtime(tasks, topo_order):
    """
    Critical path length:
      earliest_finish[t] = max(earliest_finish[d] for d in deps) + duration(t)
    Return max(earliest_finish.values())
    """
    earliest_finish = {}
    for t_name in topo_order:
        t_obj = tasks[t_name]
        if not t_obj.dependencies:
            earliest_finish[t_name] = t_obj.duration
        else:
            max_dep = max(earliest_finish[d] for d in t_obj.dependencies)
            earliest_finish[t_name] = max_dep + t_obj.duration

    return max(earliest_finish.values())

