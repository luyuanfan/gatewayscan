import os

def main():
    file_dir = "/dbdata"
    target_size = 500000000 # in megabyte
    for file in os.listdir(file_dir):
        if not os.path.isfile(os.path.join(file_dir, file)):
            continue
        size = os.path.getsize(os.path.join(file_dir, file))
        nchunk = size // target_size
        print(f"for file {file} we want to split it in {nchunk}")
if __name__ == "__main__":
    main()