import os

nproc = 30
chunk_size = 500000000 # in bytes

def get_ranges(filepath):
    f_size = os.path.getsize(filepath)
    nchunk = f_size // chunk_size
    print(f"for file {filepath} we want to split it in {nchunk}")
    ranges = []
    start = 0
    f_in = open(filepath, 'rb')
    while start < f_size:
        f_in.seek(min(start + chunk_size, f_size))
        f_in.readline()
        end = f_in.tell()
        ranges.append((start, end))
        start = end
    f_in.close()
    return ranges

def main():
    file_dir = "/dbdata"

    for file in os.listdir(file_dir):
        filepath = os.path.join(file_dir, file)

        if not os.path.isfile(filepath):
            continue
        ranges = get_ranges(filepath)
        print(ranges)

if __name__ == "__main__":
    main()