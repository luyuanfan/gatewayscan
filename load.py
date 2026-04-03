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

full_chunk_dir="/dbdata/chunks"
test_chunk_dir="data/chunks"
nproc=40
dbcommand="psql -h localhost -p 6789"
db_args = "host=localhost port=6789 dbname=lyspfan user=lyspfan password=lyspfan"

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
    raw = socket.inet_pton(socket.AF_INET6, srcip)
    full_bytes = pfxlen // 8
    remainder  = pfxlen  % 8
    if remainder:
        mask = 0xFF & (0xFF << (8 - remainder))
        masked = raw[:full_bytes] + bytes([raw[full_bytes] & mask]) + b'\x00' * (15 - full_bytes)
    else:
        masked = raw[:full_bytes] + b'\x00' * (16 - full_bytes)
    return socket.inet_ntop(socket.AF_INET6, masked) + f'/{pfxlen}'

'''
give each worker a connection to database
'''
def init_worker():
    global worker_conn
    worker_conn=psycopg2.connect(db_args)

'''
drop or flag rows we don't like
'''
def process_df(df, pfxlen):
    is_aliased = df['srcip'] == df['tgtip']                # drop aliased addresses
    is_v6 = ~df['srcip'].str.contains('.', regex=False)    # drop v4 addresses
    tmp = df[is_v6 & ~is_aliased].copy()                   # make a copy
    tmp['hostid'] = tmp['srcip'].map(get_hostid)           # get hostid of the rest
    tmp['is_slaac'] = tmp['hostid'].str[6:10] == 'fffe'    # mark slaac 
    tmp['entropy'] = [entropy_hex(h) for h in tmp.hostid]  # get entropy on hostid
    tmp['netid'] = [get_netid(s) for s in tmp.srcip]       # get netid
    tmp['subnetpfx'] = [get_subnetpfx(s, pfxlen) for s in tmp.srcip]
    return tmp

'''
take a slice, clean it, and write the filtered version to table
'''
def filter_n_copy(filepath, pfxlen):
    # read file
    read_start_t = time.time()
    # print(f"[PID {os.getpid()}] worker started reading {filepath}")
    colnames = ["protocol", "tgtip", "srcip", "hoplim", "icmpv6type", "icmpv6code", "rtt"]
    df = pd.read_csv(filepath, names=colnames, header=None, comment='#')

    # filter file
    filter_start_t = time.time()
    # print(f"[PID {os.getpid()}] worker started filtering (done reading in {filter_start_t-read_start_t:.2f}s)")
    df_out = process_df(df, pfxlen)
    if df_out is None or df_out.empty: return

    copy_start_t = time.time()
    # print(f"[PID {os.getpid()}] worker started copying (done filtering in {copy_start_t-filter_start_t:.2f}s)")

    # copy to table 
    output = io.BytesIO()
    df_out.to_csv(output, sep=',', header=False, index=False)
    output.seek(0)
    cur = worker_conn.cursor()
    cur.copy_expert(f"COPY {tablename} FROM STDIN WITH (FORMAT csv, NULL '')", output)
    worker_conn.commit()
    cur.close()
    end_t = time.time()

    # print(f"[PID {os.getpid()}] worker done copying in {end_t-copy_start_t:.2f}s")

def filter_n_copy_star(args):
    return filter_n_copy(*args)

def main():
    
    # initialize parser
    parser = argparse.ArgumentParser(
        prog="load.py",
        description="Usage: python3 load.py <tablename> --full\
                    or python3 load.py <tablename> --test"
    )
    parser.add_argument('tablename')
    parser.add_argument('--full', 
                    action='store_true')
    parser.add_argument('--force', 
                    required=False,
                    action='store_true')
    args = parser.parse_args()

    # set table name
    global tablename
    tablename = args.tablename

    # create a list of all files to load
    chunk_dir = full_chunk_dir if args.full else test_chunk_dir
    pathlist = []
    pfxlenlist = []
    for file in os.listdir(chunk_dir):
        if file.endswith('.csv'):
            pathlist.append(os.path.join(chunk_dir, file))
            pfxlenlist.append(int(file.removesuffix('.csv')[-2:]))
    args_list = list(zip(pathlist, pfxlenlist))

    # if using --force, remove the existing table and create a new one from scratch
    if (args.force == True):
        subprocess.run(f'{dbcommand} -v tbl={tablename} -f sql/drop_table.sql', shell=True, check=True)
    subprocess.run(f'{dbcommand} -v tbl={tablename} -f sql/create_table.sql', shell=True, check=True)
    
    start_full = time.time()
    pbar = tqdm(total=len(args_list))

    # create workers and let them do work
    pool = mp.Pool(nproc, init_worker)
    try:
        for _ in pool.imap_unordered(filter_n_copy_star, args_list):
            pbar.update()
    finally:
        pool.close()
        pool.join()
        pbar.close()

    end_full = time.time()
    print(f"All done in {end_full-start_full:.2f}s")

if __name__ == "__main__":
    main()