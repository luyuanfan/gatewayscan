import os
import io
import sys
import time
import socket
import binascii
import argparse
import psycopg2 
import ipaddress
import subprocess
import numpy as np
import pandas as pd
from math import log2
from tqdm import tqdm
import multiprocessing as mp
from datetime import datetime
from collections import Counter
from scipy.stats import entropy

# TODO: try to chunk the data, or like at least figure out if seek is good and if we can do parallelized disk read
# TODO: think about using imap_unordered

tmp_data_dir="/dbdata"
chunk_data_dir="/dbdata/chunk"
tablename="test2"
filteridx="filterindex"
srcipidx="srcipindex"
nproc=30
dbcommand="psql -h localhost -p 6789"
db_args = "host=localhost port=6789 dbname=lyspfan user=lyspfan password=lyspfan"

'''
assign portions to the workers (split replacement)
'''
def get_ranges(filepath):
    chunk_size = 100000000 # chunk size in bytes (500MB)
    f_size = os.path.getsize(filepath)
    nchunk = f_size // chunk_size
    if (nchunk == 0):
        chunk_size = f_size
    ranges = []
    start_b = 0
    f_in = open(filepath, 'rb')
    while start_b < f_size:
        f_in.seek(min(start_b + chunk_size, f_size))
        f_in.readline()
        end_b = f_in.tell()
        ranges.append((start_b, end_b))
        start_b = end_b
    f_in.close()
    return ranges

'''
give each worker a connection to database
'''
def init_worker():
    global worker_conn
    worker_conn=psycopg2.connect(db_args)

'''
return entropy of host id
'''
def entropy_hex(hid):
    _, counts = np.unique(list(hid), return_counts=True)
    p = counts / 16.0
    return float(-np.sum(p * np.log2(p)) / log2(16))

def get_hostid(srcip):
    return binascii.hexlify(socket.inet_pton(socket.AF_INET6, srcip)).decode()[16:]

def get_netid(srcip):
    raw = socket.inet_pton(socket.AF_INET6, srcip)
    masked = raw[:8] + b'\x00' * 8
    return socket.inet_ntop(socket.AF_INET6, masked) + '/64'

def get_subnetpfx(srcip, pfxlen):
    raw = socket.inet_pton(socket.AF_INET6, srcip)  # 16 bytes
    full_bytes = pfxlen // 8
    remainder  = pfxlen  % 8
    if remainder:
        mask = 0xFF & (0xFF << (8 - remainder))
        masked = raw[:full_bytes] + bytes([raw[full_bytes] & mask]) + b'\x00' * (15 - full_bytes)
    else:
        masked = raw[:full_bytes] + b'\x00' * (16 - full_bytes)
    return socket.inet_ntop(socket.AF_INET6, masked) + f'/{pfxlen}'

'''
flag rows we don't like
'''
def process_df(df, pfxlen):
    is_aliased = df['srcip'] == df['tgtip']
    is_v6 = ~df['srcip'].str.contains('.', regex=False)
    tmp = df[is_v6 & ~is_aliased].copy()
    tmp['hostid'] = tmp['srcip'].map(get_hostid)
    tmp['is_slaac'] = tmp['hostid'].str[6:10] == 'fffe'
    tmp['entropy'] = [entropy_hex(h) for h in tmp.hostid]
    tmp['netid'] = [get_netid(s) for s in tmp.srcip]
    tmp['subnetpfx'] = [get_subnetpfx(s, pfxlen) for s in tmp.srcip]
    return tmp

'''
take a slice, filter it, and write the cleaned version to table
'''
def filter_n_copy(filepath, pfxlen, start_byte, end_byte):
    start_r, start_now = time.time(), datetime.now()
    print(f"[PID {os.getpid()}] worker started reading files at {start_now}")

    # read file portion TODO: this part is not great
    f_in = open(filepath, 'rb')
    f_in.seek(start_byte)
    chunk = io.BytesIO(f_in.read(end_byte-start_byte)) # read with pandas it uses c
    f_in.close()

    # filter portion 
    colnames = ["protocol", "tgtip", "srcip", "hoplim", "icmpv6type", "icmpv6code", "rtt"]
    df = pd.read_csv(chunk, names=colnames, header=None, comment='#')
    start_f = time.time()
    print(f"[PID {os.getpid()}] worker started filtering (done reading in {start_f-start_r:.2f}s)")
    df_out = process_df(df, pfxlen)
    if df_out is None or df_out.empty:
        return
    filtered_t = time.time()
    print(f"[PID {os.getpid()}] worker started copying (done filtering in {filtered_t-start_f:.2f}s)")

    # copy to table 
    output = io.BytesIO()
    df_out.to_csv(output, sep=',', header=False, index=False)
    output.seek(0)
    cur = worker_conn.cursor()
    cur.copy_expert(f"COPY {tablename} FROM STDIN WITH (FORMAT csv, NULL '')", output)
    worker_conn.commit()
    cur.close()
    copied_t = time.time()
    print(f"[PID {os.getpid()}] worker done copying in {copied_t-filtered_t:.2f}s")

def filter_n_copy_star(args):
    return filter_n_copy(*args)

def main():
    
    # initialize parser
    parser = argparse.ArgumentParser(
        prog="load.py",
        description="Usage: python3 load.py <filename1> --p=<prefixlen>\
                        or python3 load.py --full" 
    )
    parser.add_argument('filename', nargs='?', default=None, help="one file at a time")
    parser.add_argument('-p', '--pfxlen', required=False, type=int)
    parser.add_argument('-f', '--full', action='store_true')
    parser.add_argument('--force', 
                    required=False,
                    action='store_true',
                    help='gonna clean out the target table and rewrite')
    args = parser.parse_args()

    # decide loading mode (full or single file)
    if (args.full == True):
        pathlist = os.listdir(tmp_data_dir)
        pathlist = [os.path.join(tmp_data_dir, p) for p in pathlist]
        print(f'MODE: Loading all source files')
    else:
        if (args.filename == None):
            parser.error("filename is required unless --full is prensent")
            sys.exit(1)
        else:
            print(f'MODE: Loading a single file {args.filename}')
            pathlist = [args.filename]

    # decide whether to create table from scratch
    if (args.force == True):
        subprocess.run(f'{dbcommand} -v tbl={tablename} -f sql/drop-table.sql', shell=True, check=True)
    subprocess.run(f'{dbcommand} -v tbl={tablename} -f sql/main.sql', shell=True, check=True)
    
    # time the entire process for multiple files
    if args.full: start_all = time.time()

    for filepath in pathlist:

        # process only regular file
        if (not os.path.isfile(filepath)) or (not filepath.endswith('.csv')):
            continue
        print(f"Processing file: {filepath}")

        # set file prefix length
        pfxlen = int(filepath.removesuffix('.csv')[-2:]) if args.full else args.pfxlen
        print(f"Setting prefix length of file {filepath} to {pfxlen}")
		
        # split file and initialize progress bar
        ranges = get_ranges(filepath)
        print(f"Splitting file {filepath} in {len(ranges)} portions")
        pbar = tqdm(total=len(ranges))

        # record processing time for one single file
        now = datetime.now()
        start_time = time.time()
        print(f'Started timing at {now}')

        try:
			# initialize workers
            print(f"Initializing all workers for {filepath}")
            pool = mp.Pool(nproc, init_worker)
            
			# let them do work
            args_list = [(filepath, pfxlen, start_byte, end_byte) for start_byte, end_byte in ranges]
            for _ in pool.imap_unordered(filter_n_copy_star, args_list):
                pbar.update()
            
            end_filter = time.time()
            print(f'Finished filtering and loading {filepath} in {end_filter-start_time:.2f}s')

        finally:
            # close workers
            print(f"Closing all workers for {filepath}. Work is done")
            pool.close()
            pool.join()
        
        end_time = time.time()
        print(f"Done processing file {filepath} in {end_time-start_time:.2f}s")

    # add indexes
    print(f"Main process starting adding indexes")
    start_index_t = time.time()
    subprocess.run(f'{dbcommand} -v filteridx={filteridx} -v srcipidx={srcipidx} -v tbl={tablename} -f sql/create-index.sql', shell=True, check=True)
    end_index_t = time.time()
    print(f"Done adding indexes in {end_index_t-start_index_t:.2f}s")

    if args.full:
        end_all = time.time()
        print(f"All done in {end_all-start_all:.2f}s")

if __name__ == "__main__":
    main()