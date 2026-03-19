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
import multiprocessing as mp
from datetime import datetime
from collections import Counter

tmp_data_dir="/dbdata"
tablename="test2"
nproc=30
target_size = 500000000 # chunk size in megabyte
dbcommand="psql -h localhost -p 6789"
db_args = "host=localhost port=6789 dbname=lyspfan user=lyspfan password=lyspfan"

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
decide whether to drop a row
'''
def process_row(row, pfxlen):
    tgtip, srcip = row["tgtip"], row["srcip"]
    if (tgtip == srcip or                # if reply comes from aliased network
        '.' in srcip):                   # if address is v4
        return None
    hostid = ipaddress.IPv6Address(srcip).exploded.replace(':', '')[16:]
    entropy_score = entropy_hex(hostid)
    if (hostid[6:10] == "fffe" or        # if address is slaac
        entropy_score <= 0.5):           # if entropy too low
        return None
    netid = ipaddress.IPv6Network(srcip + "/64", strict=False)
    subnetpfx = ipaddress.IPv6Network(srcip + f"/{pfxlen}", strict=False)
    
    return {
        **row,
        "hostid":      hostid,
        "entropy":     entropy_score,
        "netid":       str(netid),
        "subnetpfx":   str(subnetpfx),
        "pfxlen":      pfxlen,
        "deleted":     False
    }

'''
take a chunk, filter it, and write the cleaned version to table
'''
def filter_n_copy(chunk, pfxlen):
    start_t = time.time()
    print(f"[PID {os.getpid()}] worker started filtering")

    colnames = ["protocol", "tgtip", "srcip", "hoplim", "icmpv6type", "icmpv6code", "rtt"]
    df = pd.read_csv(chunk, names=colnames, header=None, comment='#')
    results = df.apply(process_row, args=(pfxlen, ), axis=1)
    df_out =pd.DataFrame([r for r in results if r is not None])
    df_out = df_out.drop_duplicates(subset=["srcip"], keep="first")
    if df_out.empty: return

    filtered_t = time.time()
    print(f"[PID {os.getpid()}] worker done filtering in {filtered_t - start_t:.2f}s")

    output = io.BytesIO()
    df_out.to_csv(output, sep=',', header=False, index=False)
    output.seek(0)
    cur = worker_conn.cursor()
    cur.copy_expert(f"COPY {tablename} FROM STDIN WITH (FORMAT csv, NULL '')", output)
    worker_conn.commit()
    cur.close()

    copied_t = time.time()
    print(f"[PID {os.getpid()}] worker done copying in {copied_t - filtered_t:.2f}s")

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
        print(f'MODE: Loading all raw files {pathlist}')
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
    subprocess.run(f'{dbcommand} -v tbl={tablename} -f schemas/routerips.sql', shell=True, check=True)

    for filepath in pathlist:
        print(f"Processing file: {filepath}")
        now = datetime.now()
        start = time.time()
        print(f'Started timing at {now}')
        if (not os.path.isfile(filepath)) or (not filepath.endswith('.csv')):
            continue
        
        # process only regular file
        if args.full == True:
            pfxlen = filepath.removesuffix('.csv')[-2:]
            outpath = os.path.join(tmp_data_dir, filepath)
        else:
            pfxlen = args.pfxlen
            outpath = filepath
        print(f"Setting prefix length of file {filepath} to {pfxlen}")
        if not os.path.exists(outpath):
            print(f'{outpath} not found, please double check the name')
            sys.exit(1)

		# dynamically decide how many chunks we want to split the raw file into
        fsize = os.path.getsize(filepath)
        nchunk = fsize // target_size

        try:
            # split file
            print(f"Splitting file {filepath} in {nchunk} chunks and placing them in {tmp_data_dir}")
            subprocess.run(f'split --number=l/{nchunk} --additional-suffix=.csv {outpath} {tmp_data_dir}/chunk_', shell=True, check=True)
            print(f"Done splitting file {filepath}")
            
			# initialize workers
            print(f"Initializing all workers for {filepath}")
            pool = mp.Pool(nproc, init_worker)
            
			# let them do work
            wrs = []
            for chunk in os.listdir(tmp_data_dir):
                if not chunk.startswith('chunk_'):
                    continue
                wr = pool.apply_async(filter_n_copy, (os.path.join(tmp_data_dir, chunk), pfxlen))
                wrs.append(wr)
            [wr.get() for wr in wrs]
            
            end_filter = time.time()
            print(f'Finished filtering and loading entries for {filepath} in {end_filter - start:.2f}s')

        finally:
            # close workers
            print(f"Closing all workers for {filepath}. Work is done")
            pool.close()
            pool.join()
            
			# remove the temporary chunks 
            print(f'Removing temporary files in {tmp_data_dir}')
            subprocess.run(f'rm -rf {tmp_data_dir}/chunk_*', shell=True, check=True)
            print("All temporary files removed")
    
    print("All done!")

if __name__ == "__main__":
    main()