#!/tn/5Net/raid-gold/virtual-python-dirs/hpc-public/bin/python3.12
"""
A simple prime‐finding module invoked as 'python -m modules.find_primes START END'.

If $RESULT_FILE is set, we also append the prime count message to that file.
Now, in addition to printing the count, this script prints the list of primes on a new line
in the format:
  Primes=[p1, p2, p3, ...]
This output is expected by the aggregator.
"""
import os
import sys

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
    if len(sys.argv) != 3:
        print("Usage: python -m modules.find_primes START END", file=sys.stderr)
        sys.exit(1)

    start = int(sys.argv[1])
    end = int(sys.argv[2])
    primes = []
    for num in range(start, end + 1):
        if is_prime(num):
            primes.append(num)
    count = len(primes)

    # Print the count and then the list of primes
    msg = f"Found {count} prime(s) in range [{start}..{end}].\n"
    print(msg, end="")
    # The aggregator expects a line beginning with "Primes="
    print(f"Primes={primes}")

    # Optionally, if RESULT_FILE is set, append the count (you could also append the primes if desired)
    rf = os.getenv("RESULT_FILE")
    if rf:
        try:
            with open(rf, "a") as f:
                f.write(msg)
        except Exception as ex:
            print(f"Warning: could not write to {rf}: {ex}", file=sys.stderr)

if __name__ == "__main__":
    main()

