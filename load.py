import os
import io
import sys
import csv
import time
import argparse
import psycopg2 
import ipaddress
import subprocess
import pandas as pd
from math import log2
from tqdm import tqdm
import multiprocessing as mp
from datetime import datetime
from collections import Counter

tmp_data_dir="/dbdata"
tablename="test1"
nproc=30
dbcommand="psql -h localhost -p 6789"
db_args = "host=localhost port=6789 dbname=lyspfan user=lyspfan password=lyspfan"

'''
assign portions to the workers (split replacement)
'''
def get_ranges(filepath):
    chunk_size = 10000000 # chunk size in bytes
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
    print(f"[PID {os.getpid()}] worker initialized")

'''
return entropy of host id
'''
def entropy_hex(hid):
    c = Counter(hid)
    score = (- sum([(val / 16) * log2(val / 16) for _ , val in c.items()]))/4
    return score

'''
filter out rows we don't like
'''
def process_df(df, pfxlen):
    df = df[df["tgtip"] != df["srcip"]]                        # if reply comes from aliased network
    df = df[~df['srcip'].str.contains('.', regex=False)]       # if address is v4
    df = df.drop_duplicates(subset=['srcip'], keep='first')    # drop duplicated srcip within chunk
    df['hostid'] = df['srcip'].apply(                          # get host id
        lambda ip: ipaddress.IPv6Address(ip).exploded.replace(':', '')[16:]
    )
    df = df[~df['hostid'].str[6:10].eq('fffe')]                # drop slaac 
    df['entropy'] = df['hostid'].apply(entropy_hex)            # get entropy
    df = df[df['entropy'] > 0.5]                               # drop low entropy addresses
    df['netid'] = df['srcip'].apply(lambda ip: str(ipaddress.IPv6Network(ip + '/64', strict=False)))
    df['subnetpfx'] = df['srcip'].apply(lambda ip: str(ipaddress.IPv6Network(ip + f'/{pfxlen}', strict=False)))
    df['pfxlen'] = pfxlen
    return df

'''
take a slice, filter it, and write the cleaned version to table
'''
def filter_n_copy(filepath, pfxlen, start_byte, end_byte):
    start_r, start_now = time.time(), datetime.now()
    print(f"[PID {os.getpid()}] worker started reading files at {start_now}")

    # read file portion
    f_in = open(filepath, 'rb')
    f_in.seek(start_byte)
    chunk = io.BytesIO(f_in.read(end_byte - start_byte))
    f_in.close()

    # filter portion 
    colnames = ["protocol", "tgtip", "srcip", "hoplim", "icmpv6type", "icmpv6code", "rtt"]
    df = pd.read_csv(chunk, names=colnames, header=None, comment='#')
    start_f = time.time()
    print(f"[PID {os.getpid()}] worker started filtering (done reading in {start_f - start_r:.2f}s)")
    df_out = process_df(df, pfxlen)
    if df_out is None or df_out.empty:
        return
    filtered_t = time.time()
    print(f"[PID {os.getpid()}] worker started copying (done filtering in {filtered_t - start_f:.2f}s)")

    # copy to table 
    output = io.BytesIO()
    df_out.to_csv(output, sep=',', header=False, index=False)
    output.seek(0)
    cur = worker_conn.cursor()
    cur.copy_expert(f"COPY {tablename} FROM STDIN WITH (FORMAT csv, NULL '')", output)
    worker_conn.commit()
    cur.close()
    copied_t = time.time()
    print(f"[PID {os.getpid()}] worker done copying in {copied_t - filtered_t:.2f}s")

    # return add srcip for deduplicating
    return df_out["srcip"].tolist()

'''
initialize cmdline parser
'''
def init_parser():
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
    return args

'''
for the workers to delete
'''
def delete_duplicates(one_group):
    cur = worker_conn.cursor()
    cur.execute(f"""
        DELETE FROM {tablename}
        WHERE ctid IN (
            SELECT ctid FROM (
                SELECT ctid,
                    ROW_NUMBER() OVER (PARTITION BY srcip ORDER BY ctid) AS rn
                FROM {tablename}
                WHERE srcip::text = ANY(%s)
            ) t WHERE rn > 1
        )
    """, (one_group,))
    worker_conn.commit()

'''
remove rows from table that has the duplicated srcip we recorded in 'dups'
'''
def deduplicate(dups):

    if dups:
        print(f"Removing all {len(dups)} duplicated source ips")
        dups_list = list(dups)
        len_dups_list = len(dups_list)
        group_size = 100000
        srcip_groups = []
        for i in range(0, len_dups_list, group_size):
            end_idx = min(len_dups_list, i+group_size)
            srcip_groups.append(dups_list[i:end_idx])

        pbar = tqdm(total=len(srcip_groups))
        pool = mp.Pool(nproc, init_worker)

        wrs = []
        for one_group in srcip_groups:
            wr = pool.apply_async(
                delete_duplicates,
                (one_group,),
                callback=lambda _: pbar.update()
            )
            wrs.append(wr)
        [wr.get() for wr in wrs]

def main():
    # initialize parser
    args = init_parser()

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
        subprocess.run(f'{dbcommand} -v tbl={tablename} -f psql/drop.sql', shell=True, check=True)
    subprocess.run(f'{dbcommand} -v tbl={tablename} -f schemas/main.sql', shell=True, check=True)
    
    # time the entire process for multiple files
    if args.full: start_all = time.time()

    # src ip book keeping to avoid scanning entire db in the end
    seen, dups = set(), set()

    for filepath in pathlist:

        # process only regular file
        if (not os.path.isfile(filepath)) or (not filepath.endswith('.csv')):
            continue
        print(f"Processing file: {filepath}")

        # set file prefix length
        pfxlen = filepath.removesuffix('.csv')[-2:] if args.full else args.pfxlen
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
            wrs = []
            for start_byte, end_byte in ranges:
                wr = pool.apply_async(
                    filter_n_copy,
                    (filepath, pfxlen, start_byte, end_byte,),
                    callback=lambda _: pbar.update(),
                )
                wrs.append(wr)
            rets = [wr.get() for wr in wrs]
            
            end_filter = time.time()
            print(f'Finished filtering and loading {filepath} in {end_filter - start_time:.2f}s')

            for srcip_list in rets:
                if srcip_list is None: continue
                for srcip in srcip_list:
                    dups.add(srcip) if srcip in seen else seen.add(srcip)

        finally:
            # close workers
            print(f"Closing all workers for {filepath}. Work is done")
            pool.close()
            pool.join()
        
        end_time = time.time()
        print(f"Done processing file {filepath} in {end_time - start_time:.2f}s")
    
    print("Main process started deduplicating")
    dup_start = time.time()
    deduplicate(dups)
    dup_end = time.time()
    print(f"Done deduplicating in {dup_end-dup_start:.2f}s")

    if args.full: print(f"All done in {dup_end-start_all:.2f}s")

if __name__ == "__main__":
    main()