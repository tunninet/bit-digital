import sys, os, socket

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

def main():
    if len(sys.argv) < 5:
        print("Usage: python multi_node_prime_finder.py START END RUN_ID LOGS_DIR", file=sys.stderr)
        sys.exit(1)

    start = int(sys.argv[1])
    end   = int(sys.argv[2])
    run_id= sys.argv[3]
    logs_dir = sys.argv[4]

    rank_str = os.environ.get("SLURM_PROCID","0")
    size_str = os.environ.get("SLURM_NTASKS","1")
    job_id   = os.environ.get("SLURM_JOB_ID","NoJobID")

    rank = int(rank_str)
    size = int(size_str)

    total_elems = (end - start + 1)
    chunk = total_elems // size
    remainder = total_elems % size

    if rank < remainder:
        local_s = start + rank*(chunk+1)
        local_e = local_s + (chunk+1) - 1
    else:
        local_s = start + remainder*(chunk+1) + (rank - remainder)*chunk
        local_e = local_s + chunk - 1

    if local_e > end:
        local_e = end

    # gather primes
    prime_list = []
    for x in range(local_s, local_e+1):
        if is_prime(x):
            prime_list.append(x)

    count = len(prime_list)
    node = socket.gethostname()

    # partial log => logs/job_{jobid}_rank_{rank}.log
    os.makedirs(logs_dir, exist_ok=True)
    log_name = os.path.join(logs_dir, f"job_{job_id}_rank_{rank}.log")
    with open(log_name, "w") as f:
        f.write(f"Rank={rank}, Node={node}, subrange=[{local_s}..{local_e}], found={count} primes\n")
        f.write(f"Primes={prime_list}\n")

if __name__=="__main__":
    main()

