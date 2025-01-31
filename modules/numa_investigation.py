"""
numa_investigation.py

Show basic NUMA info using py-libnuma (if available),
and also check whether system library libnuma.so is installed.

Usage in code:
  from modules.numa_investigation import investigate_numa_domains
  ...
  investigate_numa_domains()
"""

import os

try:
    from numa import info
    has_pynuma = True
except ImportError:
    has_pynuma = False

def check_libnuma_so():
    """
    Return True if system library 'libnuma.so' is found, else False.
    Typically installed by:
      - Ubuntu/Debian: apt-get install -y numactl libnuma-dev
      - RHEL/CentOS:   yum install -y numactl numactl-devel
    """
    possible_paths = [
        "/usr/lib/libnuma.so",
        "/usr/lib64/libnuma.so",
        "/lib/x86_64-linux-gnu/libnuma.so",
        "/lib64/libnuma.so"
    ]
    return any(os.path.exists(p) for p in possible_paths)

def investigate_numa_domains():
    """
    Investigate NUMA domains using py-libnuma, if both py-libnuma and the
    system library libnuma.so are available.

    If missing, print instructions on how to install them.
    """
    print("=== Investigating NUMA Domains ===")

    # Check presence of Python bindings
    if not has_pynuma:
        print("❌ ERROR: 'py-libnuma' Python package not found.")
        print("➡️  Install it using something like:")
        print("   pip install git+https://github.com/eedalong/pynuma.git#egg=py-libnuma")
        print("Cannot inspect NUMA details.\n")
        return

    # Check presence of system library
    if not check_libnuma_so():
        print("❌ ERROR: System dependency `libnuma.so` is missing.")
        print("➡️  Install it using:")
        print("   - Ubuntu/Debian: `sudo apt-get install -y numactl libnuma-dev`")
        print("   - RHEL/CentOS:   `sudo yum install -y numactl numactl-devel`")
        return

    # If both are present, proceed
    try:
        # We'll do a short usage of numa.info to show the results
        max_node = info.get_max_node()
        node_count = max_node + 1
        print(f"System has {node_count} NUMA node(s).")

        for node_id in range(node_count):
            # Distance might be used for demonstration
            dist = info.numa_distance(0, node_id)
            cpus = info.node_to_cpus(node_id)
            print(f" - Node {node_id}, distance_from_node0={dist}, CPUs={cpus}")

    except AttributeError as e:
        print("❌ The installed `py-libnuma` does not provide a needed function:")
        print(f"   {e}")
        print("➡️  You may need to confirm which calls are in the installed library.")
    except Exception as e:
        print(f"❌ Unexpected error while investigating NUMA: {e}")

    print("==================================\n")

