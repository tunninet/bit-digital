#!/usr/bin/env python3
"""
submit_and_monitor_job.py

1) Enumerate partitions
2) Choose a partition
3) List configured nodes in that partition (calls get_nodes to get CPU info)
4) Load a base job template (job_template.json)
5) Override job name, partition
6) Submit job
7) Monitor job in slurmdb
"""

import json
import os
import time
import argparse
import pprint
from datetime import datetime, timezone

from slurmrestd_client import SlurmRESTClient

def list_partition_nodes(partition_data, client):
    """
    partition_data: result of client.get_partition(partition_name)
    Example structure:
    {
      "partitions": [
        {
          "nodes": {
            "configured": "5net,8net,9net,55net",
            "allowed_allocation": "...",
            "total": 4
          },
          ...
          "name": "Tunninet"
        }
      ],
      ...
    }

    We'll parse the "configured" string, then fetch the /slurm/v0.0.40/nodes endpoint
    to display CPU and state info for each node name.
    """
    partitions = partition_data.get("partitions", [])
    if not partitions:
        print("No partition info found in the response!")
        return

    part = partitions[0]
    p_name = part.get("name", "unknown")
    node_info = part.get("nodes", {})
    configured_str = node_info.get("configured", "")
    if not configured_str:
        print(f"Partition '{p_name}' has no 'configured' nodes listed.")
        return

    # e.g. "5net,8net,9net,55net"
    node_names = [n.strip() for n in configured_str.split(",") if n.strip()]

    # Now fetch all nodes from /slurm/v0.0.40/nodes
    all_nodes_data = client.get_nodes()
    all_nodes_list = all_nodes_data.get("nodes", [])

    # Map node name -> node record
    node_map = {nd.get("name", ""): nd for nd in all_nodes_list}

    print(f"Partition '{p_name}' has {len(node_names)} configured nodes:")
    for nm in node_names:
        nd = node_map.get(nm)
        if not nd:
            print(f"  - Node: {nm} (no details found in /slurm/v0.0.40/nodes)")
        else:
            # state might be a list, e.g. ["IDLE"] or ["DOWN"]
            st = nd.get("state", [])
            if isinstance(st, list):
                st = ",".join(st)
            cpus = nd.get("cpus", 0)
            print(f"  - Node: {nm}, State: {st}, CPUs: {cpus}")

def summarize_job_info(job_data):
    """
    Summarizes a single job record from slurmdb.
    Typical fields: job_id, name, user, partition, nodes, state, exit_code, time
    """
    job_id = job_data.get("job_id")
    job_name = job_data.get("name", "UnknownName")
    user = job_data.get("user", "UnknownUser")
    partition = job_data.get("partition", "UnknownPartition")
    nodes = job_data.get("nodes", "UnknownNodes")

    state_info = job_data.get("state", {})
    current_state = state_info.get("current", ["UNKNOWN"])
    state_str = ",".join(current_state)

    exit_code_data = job_data.get("exit_code", {})
    return_code = exit_code_data.get("return_code", {}).get("number", None)

    time_data = job_data.get("time", {})
    start_ts = time_data.get("start")   # epoch int
    end_ts = time_data.get("end")       # epoch int
    elapsed_sec = time_data.get("elapsed", 0)

    # Use timezone-aware datetimes to avoid deprecation
    def fmt_ts(ts):
        if isinstance(ts, int):
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S %Z")
        return "N/A"

    start_str = fmt_ts(start_ts)
    end_str = fmt_ts(end_ts)

    lines = [
        f"Job ID: {job_id}",
        f"Name: {job_name}",
        f"User: {user}",
        f"Partition: {partition}",
        f"Nodes: {nodes}",
        f"State: {state_str}",
        f"Exit Code: {return_code}",
        f"Start Time: {start_str}",
        f"End Time: {end_str}",
        f"Elapsed: {elapsed_sec} sec"
    ]
    return "\n".join(lines)

def main():
    parser = argparse.ArgumentParser(description="Submit and monitor a Slurm job via slurmrestd.")
    parser.add_argument("--socket", default="/var/run/slurmrestd/slurmrestd.sock",
                        help="Path to the slurmrestd UNIX domain socket.")
    parser.add_argument("--job-name", default="MyDefaultJob",
                        help="Name of the job (will override the template).")
    parser.add_argument("--partition", default=None,
                        help="Partition to submit to. If not specified, user chooses from prompt.")
    parser.add_argument("--debug", action="store_true",
                        help="If set, dump raw JSON responses for debugging.")
    args = parser.parse_args()

    client = SlurmRESTClient(socket_path=args.socket)

    # 1) List partitions
    partitions_data = client.get_partitions()
    partitions_list = partitions_data.get("partitions", [])

    print("Available partitions:")
    for idx, part in enumerate(partitions_list, start=1):
        pname = part.get("name", "unknown")
        print(f"  {idx}. {pname}")

    # If user didn't supply --partition, prompt
    selected_partition = args.partition
    if not selected_partition:
        default_part = "Tunninet"
        print(f"\nNo partition was passed via --partition.")
        selection = input(f"Enter partition name or press ENTER to use '{default_part}': ").strip()
        selected_partition = selection or default_part

    print(f"\nChosen partition is: {selected_partition}")

    # 2) Fetch partition info and list nodes
    try:
        part_info = client.get_partition(selected_partition)
        if args.debug:
            print("\n[DEBUG] Partition info JSON:")
            print(json.dumps(part_info, indent=2))

        list_partition_nodes(part_info, client)
    except Exception as e:
        print(f"Could not fetch info for partition '{selected_partition}': {e}")

    # 3) Load the job template
    template_path = os.path.join(os.path.dirname(__file__), "job_template.json")
    with open(template_path, "r") as f:
        job_payload = json.load(f)

    # Override job name & partition
    job_payload["job"]["name"] = args.job_name
    job_payload["job"]["partition"] = selected_partition

    # If your cluster requires a valid account, set it here if needed:
    # job_payload["job"]["account"] = "myValidAccount"

    print(f"\nSubmitting job with name='{args.job_name}' on partition='{selected_partition}'...")

    print("[DEBUG] Final job payload is:")
    pprint.pprint(job_payload)
    submit_response = client.submit_job(job_payload)

    if args.debug:
        print("\n[DEBUG] Raw submission response:")
        print(json.dumps(submit_response, indent=2))

    job_id = submit_response.get("job_id")
    if not job_id:
        print("Failed to retrieve 'job_id' from submission response:", submit_response)
        return

    print(f"Job submitted successfully! job_id = {job_id}")

    # 4) Poll job info from slurmdb
    attempts = 3
    job_info_data = None
    for attempt in range(1, attempts + 1):
        time.sleep(2)
        try:
            job_info = client.get_job_info(job_id)
            if args.debug:
                print(f"\n[DEBUG] Attempt {attempt} -> Full job info JSON:")
                print(json.dumps(job_info, indent=2))

            jobs_array = job_info.get("jobs", [])
            if not jobs_array:
                print(f"[Attempt {attempt}] 'jobs' array is empty.")
            else:
                job_info_data = jobs_array[0]
                break
        except Exception as e:
            print(f"[Attempt {attempt}] Could not retrieve job info yet: {e}")

    if not job_info_data:
        print("Could not find job info in slurmdb after several attempts.")
        return

    # 5) Summarize the job info
    print("\nJob Summary (slurmdb info):")
    summary_text = summarize_job_info(job_info_data)
    print(summary_text)

    # If the job fails with exit code 13 => likely permission or environment error.
    # Check Slurm logs for more details.

if __name__ == "__main__":
    main()
