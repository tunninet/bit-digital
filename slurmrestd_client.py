#!/usr/bin/env python3
"""
slurmrestd_client.py

Provides a SlurmRESTClient class that wraps slurmrestd 0.0.40 endpoints:
 - get_partitions()
 - get_partition()
 - get_nodes()
 - submit_job()
 - get_job_info()
"""

import requests
import requests_unixsocket
from requests_unixsocket import UnixAdapter
from urllib.parse import quote_plus


class SlurmRESTClient:
    """
    A client for interacting with slurmrestd (v0.0.40) over a UNIX domain socket.
    """

    def __init__(self, socket_path="/var/run/slurmrestd/slurmrestd.sock"):
        # Create a requests session with the 'http+unix://' adapter
        self.session = requests.Session()
        self.session.mount("http+unix://", UnixAdapter())

        # Encode the UNIX socket path for the URL
        self.encoded_socket = quote_plus(socket_path)

    def _build_url(self, path):
        """
        Constructs a URL using the encoded UNIX socket and a relative path.
        Example: http+unix://%2Fvar%2Frun%2Fslurmrestd%2Fslurmrestd.sock/slurm/v0.0.40/partitions
        """
        return f"http+unix://{self.encoded_socket}{path}"

    def get_partitions(self):
        """
        GET /slurm/v0.0.40/partitions
        Returns JSON with all partitions info.
        """
        url = self._build_url("/slurm/v0.0.40/partitions")
        resp = self.session.get(url)
        resp.raise_for_status()
        return resp.json()

    def get_partition(self, partition_name):
        """
        GET /slurm/v0.0.40/partition/{partition_name}
        Returns JSON for one named partition.
        """
        url = self._build_url(f"/slurm/v0.0.40/partition/{partition_name}")
        resp = self.session.get(url)
        resp.raise_for_status()
        return resp.json()

    def get_nodes(self):
        """
        GET /slurm/v0.0.40/nodes
        Returns info about all known nodes in the cluster.
        """
        url = self._build_url("/slurm/v0.0.40/nodes")
        resp = self.session.get(url)
        resp.raise_for_status()
        return resp.json()

    def submit_job(self, job_payload):
        """
        POST /slurm/v0.0.40/job/submit
        Submit a new job using the payload (dict).
        Returns JSON response like {"job_id": 123, ...}.
        """
        url = self._build_url("/slurm/v0.0.40/job/submit")
        headers = {"Content-Type": "application/json"}
        resp = self.session.post(url, headers=headers, json=job_payload)
        resp.raise_for_status()
        return resp.json()

    def get_job_info(self, job_id):
        """
        GET /slurmdb/v0.0.40/job/{job_id}
        Returns accounting info for the job (may be partial if job hasn't been recorded yet).
        """
        url = self._build_url(f"/slurmdb/v0.0.40/job/{job_id}")
        resp = self.session.get(url)
        resp.raise_for_status()
        return resp.json()
