import os
import json
import time
import requests
import requests_unixsocket
from urllib.parse import quote_plus

def _build_socket_url(socket_path, subpath):
    encoded = quote_plus(socket_path)
    return f"http+unix://{encoded}{subpath}"

def expand_brackets(s):
    """
    Expand bracket expressions like 'slurm-controller-[1-2]' into
    ['slurm-controller-1', 'slurm-controller-2'].
    If no bracket found, returns [s] unchanged.
    """
    import re
    match = re.search(r"(.*)\[(\d+)-(\d+)\](.*)", s)
    if not match:
        return [s]
    prefix = match.group(1)
    start_num = int(match.group(2))
    end_num = int(match.group(3))
    suffix = match.group(4)
    out = []
    for i in range(start_num, end_num + 1):
        out.append(f"{prefix}{i}{suffix}")
    return out

def get_node_info(node_name, socket_path="/var/run/slurmrestd/slurmrestd.sock"):
    """
    GET /slurm/v0.0.40/node/{node_name}
    Return the first node dict or {} if 404 or no data.
    Example fields:
      {
        "name": "5net",
        "cpus": 16,
        "sockets": 1,
        "cores_per_socket": 8,
        "threads_per_core": 2,
        "real_memory": 126000,
        "state": ["IDLE"]
      }
    """
    import requests_unixsocket
    s = requests_unixsocket.Session()
    url = _build_socket_url(socket_path, f"/slurm/v0.0.40/node/{node_name}")
    resp = s.get(url)
    if resp.status_code == 404:
        return {}
    resp.raise_for_status()
    data = resp.json()
    nds = data.get("nodes", [])
    if nds:
        return nds[0]
    return {}

def list_partitions_detailed(socket_path="/var/run/slurmrestd/slurmrestd.sock"):
    """
    Returns a list of dicts, e.g.:
      [
        {
          "name": "Tunninet",
          "nodes": [
             { "name":"5net", "cpus":16, "real_memory":126000, "sockets":1, "cores_per_socket":8, "threads_per_core":2, "state":"IDLE" },
             ...
          ],
          "total_cpus": 64,
          "idle_cpus": 64,
          "total_memory": 504000
        },
        ...
      ]
    """
    import requests_unixsocket
    s = requests_unixsocket.Session()
    url_parts = _build_socket_url(socket_path, "/slurm/v0.0.40/partitions")
    resp = s.get(url_parts)
    resp.raise_for_status()
    data_part = resp.json()
    part_list = data_part.get("partitions", [])

    detailed = []
    for p in part_list:
        pname = p.get("name", "unknown")
        nodes_dict = p.get("nodes", {})  # e.g. {"configured":"5net,8net", ...}
        configured_str = nodes_dict.get("configured", "")

        # Expand comma-separated tokens + bracket expansions
        all_node_names = []
        for token in configured_str.split(","):
            token = token.strip()
            expanded = expand_brackets(token)
            all_node_names.extend(expanded)

        node_dicts = []
        total_cpus = 0
        idle_cpus = 0
        total_mem = 0

        for nodename in all_node_names:
            ninfo = get_node_info(nodename, socket_path=socket_path)
            if not ninfo:
                continue

            # unify state (list or str) into an uppercase string
            raw_state = ninfo.get("state", "")
            if isinstance(raw_state, list):
                st = " ".join(raw_state).upper()
            else:
                st = str(raw_state).upper()

            c = ninfo.get("cpus", 0)
            m = ninfo.get("real_memory", 0)
            socks = ninfo.get("sockets", 1)
            cps = ninfo.get("cores_per_socket", 1)
            tpc = ninfo.get("threads_per_core", 1)

            node_dicts.append({
                "name": ninfo.get("name", nodename),
                "cpus": c,
                "real_memory": m,
                "sockets": socks,
                "cores_per_socket": cps,
                "threads_per_core": tpc,
                "state": st
            })

            total_cpus += c
            total_mem += m
            if "IDLE" in st:
                # if the node is IDLE or partly IDLE, we consider all its cpus idle
                idle_cpus += c

        detailed.append({
            "name": pname,
            "nodes": node_dicts,
            "total_cpus": total_cpus,
            "idle_cpus": idle_cpus,
            "total_memory": total_mem,
        })

    return detailed

def prompt_for_partition_detailed(partitions_detailed, default="Tunninet"):
    """
    Prints a detailed list of partitions with node info, 
    then asks user to pick a partition (by number or name),
    defaulting to Tunninet if user just hits Enter.
    """
    if not partitions_detailed:
        print("No partitions found!")
        return None

    print("Available partitions (detailed):")
    for idx, pd in enumerate(partitions_detailed, start=1):
        print(f"  {idx}. {pd['name']}")
        print("      Nodes:")
        for nd in pd["nodes"]:
            print(f"         - {nd['name']} (cpus={nd['cpus']}, mem={nd['real_memory']}, sockets={nd['sockets']}, cores/socket={nd['cores_per_socket']}, threads/core={nd['threads_per_core']}, state={nd['state']})")
        print(f"      total_cpus={pd['total_cpus']}, idle_cpus={pd['idle_cpus']}, total_memory={pd['total_memory']}")

    sel = input(f"Choose a partition by number or name (or press ENTER for '{default}'): ").strip()
    if not sel:
        return default  # user pressed enter => default

    # If the user typed a digit, interpret as index
    if sel.isdigit():
        i = int(sel)
        if 1 <= i <= len(partitions_detailed):
            return partitions_detailed[i - 1]["name"]
        else:
            print("Invalid index.")
            return None
    else:
        # Otherwise assume they typed a partition name
        for pd in partitions_detailed:
            if pd["name"] == sel:
                return sel
        print("No matching partition name.")
        return None

def get_partition_idle_cpus(partition_name, socket_path="/var/run/slurmrestd/slurmrestd.sock"):
    """
    Return idle CPU count for the given partition by scanning list_partitions_detailed().
    """
    allp = list_partitions_detailed(socket_path)
    for pd in allp:
        if pd["name"] == partition_name:
            return pd["idle_cpus"]
    return 0

def create_slurm_payload_and_submit(chunk_name,
                                    chunk_task,
                                    subrange,
                                    partition,
                                    job_name,
                                    run_id="default",
                                    socket_path="/var/run/slurmrestd/slurmrestd.sock"):
    """
    1) Load job_template.json
    2) Replace placeholders (partition, run-id, etc.)
    3) Insert prime cmd => "python -m modules.find_primes {start} {end}"
    4) POST to /slurm/v0.0.40/job/submit
    5) Return job_id or None
    """
    import requests_unixsocket

    templ_path = os.path.join(os.path.dirname(__file__), "..", "template", "job_template.json")
    with open(templ_path, "r") as f:
        job_data = json.load(f)

    script = job_data["script"]
    script = script.replace("RUN_ID_PLACEHOLDER", run_id)

    start, end = subrange
    prime_cmd = f"python -m modules.find_primes {start} {end}"
    script = script.replace("sleep 30", prime_cmd)

    job_data["script"] = script
    job_data["job"]["name"] = f"{job_name}_{chunk_name}"
    job_data["job"]["partition"] = partition

    url = _build_socket_url(socket_path, "/slurm/v0.0.40/job/submit")
    s = requests_unixsocket.Session()
    resp = s.post(url, json=job_data)
    resp.raise_for_status()
    submit_resp = resp.json()

    job_id = submit_resp.get("job_id")
    if job_id:
        print(f"   -> Submitted chunk '{chunk_name}' as job_id={job_id}, subrange=({start}..{end})")
        return job_id
    else:
        print(f"   -> Submission for chunk '{chunk_name}' failed: {submit_resp}")
        return None

def get_job_state(job_id, socket_path="/var/run/slurmrestd/slurmrestd.sock"):
    """
    Return job_state (e.g. 'RUNNING','COMPLETED') or 'NOT_FOUND' if 404.
    """
    import requests_unixsocket
    s = requests_unixsocket.Session()
    url = _build_socket_url(socket_path, f"/slurm/v0.0.40/job/{job_id}")
    resp = s.get(url)
    if resp.status_code == 404:
        return "NOT_FOUND"
    resp.raise_for_status()
    data = resp.json()
    jobs = data.get("jobs", [])
    if jobs:
        return jobs[0].get("job_state", "UNKNOWN")
    return "UNKNOWN"

def wait_for_slurm_jobs(job_ids, poll_interval=2, socket_path="/var/run/slurmrestd/slurmrestd.sock"):
    """
    Poll until each job is COMPLETED, FAILED, CANCELLED, TIMEOUT, or NOT_FOUND.
    Returns {job_id: final_state}.
    """
    still_running = set(job_ids)
    final_states = {}

    while still_running:
        time.sleep(poll_interval)
        done_list = []
        for jid in list(still_running):
            st = get_job_state(jid, socket_path)
            if st in ("COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NOT_FOUND"):
                final_states[jid] = st
                done_list.append(jid)
        for d in done_list:
            still_running.remove(d)

    # any job not accounted => "UNKNOWN"
    for jid in job_ids:
        if jid not in final_states:
            final_states[jid] = "UNKNOWN"

    return final_states

