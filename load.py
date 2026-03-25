import os
import io
import sys
import time
import socket
import binascii
import argparse
import psycopg2 
import subprocess
import numpy as np
import pandas as pd
from math import log2
from tqdm import tqdm
import multiprocessing as mp
from datetime import datetime
from collections import Counter
from scipy.stats import entropy

real_chunk_dir="/dbdata/chunks"
test_chunk_dir="data/chunks"
tablename="test3"
filteridx="filterindex"
srcipidx="srcipindex"
nproc=40
dbcommand="psql -h localhost -p 6789"
db_args = "host=localhost port=6789 dbname=lyspfan user=lyspfan password=lyspfan"

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
def filter_n_copy(filepath, pfxlen):
    read_start_t = time.time()
    print(f"[PID {os.getpid()}] worker started reading {filepath}")
    colnames = ["protocol", "tgtip", "srcip", "hoplim", "icmpv6type", "icmpv6code", "rtt"]
    df = pd.read_csv(filepath, names=colnames, header=None, comment='#')

    filter_start_t = time.time()
    print(f"[PID {os.getpid()}] worker started filtering (done reading in {filter_start_t-read_start_t:.2f}s)")
    df_out = process_df(df, pfxlen)
    if df_out is None or df_out.empty:
        return

    copy_start_t = time.time()
    print(f"[PID {os.getpid()}] worker started copying (done filtering in {copy_start_t-filter_start_t:.2f}s)")

    # copy to table 
    output = io.BytesIO()
    df_out.to_csv(output, sep=',', header=False, index=False)
    output.seek(0)
    cur = worker_conn.cursor()
    cur.copy_expert(f"COPY {tablename} FROM STDIN WITH (FORMAT csv, NULL '')", output)
    worker_conn.commit()
    cur.close()
    end_t = time.time()
    print(f"[PID {os.getpid()}] worker done copying in {end_t-copy_start_t:.2f}s")

def filter_n_copy_star(args):
    return filter_n_copy(*args)

def main():
    
    # initialize parser
    parser = argparse.ArgumentParser(
        prog="load.py",
        description="Usage: python3 load.py <filename1> --p=<prefixlen>\
                        or python3 load.py --full" 
    )
    parser.add_argument('--full', action='store_true')
    parser.add_argument('--force', 
                    required=False,
                    action='store_true',
                    help='gonna clean out the target table and rewrite')
    args = parser.parse_args()

    # decide loading mode (full or single file)
    chunk_dir = real_chunk_dir if args.full else test_chunk_dir
    pathlist = []
    pfxlenlist = []
    for p in os.listdir(chunk_dir):
        if p.endswith('.csv'):
            pathlist.append(os.path.join(chunk_dir, p))
            pfxlenlist.append(int(p.removesuffix('.csv')[-2:]))
    args_list = list(zip(pathlist, pfxlenlist))

    # decide whether to create table from scratch
    if (args.force == True):
        subprocess.run(f'{dbcommand} -v tbl={tablename} -f sql/drop-table.sql', shell=True, check=True)
    subprocess.run(f'{dbcommand} -v tbl={tablename} -f sql/main.sql', shell=True, check=True)
    
    # time the entire process for multiple files
    if args.full: start_all = time.time()
    pbar = tqdm(total=len(args_list))

    pool = mp.Pool(nproc, init_worker)
    try:
        for _ in pool.imap_unordered(filter_n_copy_star, args_list):
            pbar.update()
    finally:
        pool.close()
        pool.join()
        pbar.close()
    
    # add indexes
    print(f"Main process starting adding indexes")
    start_index_t = time.time()
    subprocess.run(f'{dbcommand} -v filteridx={filteridx}c -v srcipidx={srcipidx} -v tbl={tablename} -f sql/create-index.sql', shell=True, check=True)
    end_index_t = time.time()
    print(f"Done adding indexes in {end_index_t-start_index_t:.2f}s")

    if args.full:
        end_all = time.time()
        print(f"All done in {end_all-start_all:.2f}s")

if __name__ == "__main__":
    main()