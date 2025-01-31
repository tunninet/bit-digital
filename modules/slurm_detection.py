"""
slurm_detection.py

Utility to detect if slurmrestd is available on this system.
"""

import os

def is_slurm_available(socket_path="/var/run/slurmrestd/slurmrestd.sock"):
    """
    Returns True if the slurmrestd socket is found, else False.
    """
    return os.path.exists(socket_path)

