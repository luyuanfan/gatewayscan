import sys
import os
import ipaddress
from collections import Counter
import datetime

now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
FDUP = now + "_duplicates.csv"

def get_host(addr):
    full = addr.exploded.replace(":", "")
    return full[16:]

def main():
    if len(sys.argv) < 2:
        print("Usage: python find_dups.py <non_slaac.csv>")
        sys.exit(1)

    input_file = sys.argv[1]
    if not os.path.exists(input_file):
        print(f"ERROR: {input_file} not found.")
        sys.exit(1)

    print(f"Counting host bits in {input_file}...")
    host_counter = Counter()
    total = 0 

    with open(input_file, "r") as fin:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            columns = line.split(",")
            if len(columns) < 3:
                continue
            try:
                addr = ipaddress.IPv6Address(columns[2].strip())
            except (ipaddress.AddressValueError, ValueError):
                continue
            total += 1
            host_counter[get_host(addr)] += 1

    print(f"  Total lines: {total}")
    print(f"  Unique host bit patterns: {len(host_counter)}")

    print(f"\nWriting duplicates to {FDUP}...")
    dup_count = 0

    with open(input_file, "r") as fin, open(FDUP, "w") as fdup:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            columns = line.split(",")
            if len(columns) < 3:
                continue
            try:
                addr = ipaddress.IPv6Address(columns[2].strip())
            except (ipaddress.AddressValueError, ValueError):
                continue
            if host_counter[get_host(addr)] > 1:
                fdup.write(line + "\n")
                dup_count += 1

    # Stats
    dup_hosts = sum(1 for c in host_counter.values() if c > 1)
    singleton_hosts = sum(1 for c in host_counter.values() if c == 1)
    most_common = host_counter.most_common(20)

    print(f"\n{'='*60}")
    print(f"  SUMMARY")
    print(f"{'='*60}")
    print(f"  Total lines:                     {total}")
    print(f"  Unique host bit patterns:        {len(host_counter)}")
    print(f"  Appearing only once:             {singleton_hosts}")
    print(f"  Appearing more than once:        {dup_hosts}")
    print(f"  Lines written to {FDUP}: {dup_count}")
    print(f"{'='*60}")

    print(f"\n  Top 20 most common host bits:")
    print(f"  {'Count':>8}  {'Host bits (last 64)':>35}")
    print(f"  {'-'*8}  {'-'*35}")
    for host, count in most_common:
        formatted = f"{host[0:4]}:{host[4:8]}:{host[8:12]}:{host[12:16]}"
        print(f"  {count:>8}  {formatted:>35}")

if __name__ == "__main__":
    main()