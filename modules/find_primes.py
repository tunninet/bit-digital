"""
A simple prime-finding module invoked as 'python -m modules.find_primes start end'.

If $RESULT_FILE is set, we append the prime count message to that file as well.
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
    count = 0
    for num in range(start, end + 1):
        if is_prime(num):
            count += 1

    msg = f"Found {count} prime(s) in range [{start}..{end}].\n"
    print(msg, end="")

    rf = os.getenv("RESULT_FILE")
    if rf:
        try:
            with open(rf, "a") as f:
                f.write(msg)
        except Exception as ex:
            print(f"Warning: could not write to {rf}: {ex}", file=sys.stderr)

if __name__ == "__main__":
    main()

