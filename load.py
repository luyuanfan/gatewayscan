import io
import os
import sys
import bz2
import csv
import time
import psycopg2 
import ipaddress
import pandas as pd
from math import log2
import multiprocessing as mp
from collections import Counter
from sqlalchemy import create_engine

tmp_data_dir='/dbdata'
tablename='test2'
nproc=40
dbcommand="psql -h localhost -p 6789"

def entropy_hex(hid):
    c = Counter(hid)
    score = (- sum([(val / 16) * log2(val / 16) for _ , val in c.items()]))/4
    return score

def process_row(row, pfxlen):
    tgtip = row['tgtip']
    srcip = row['srcip']
    if (tgtip == srcip or           # if reply comes from aliased network
        '.' in srcip):              # if address is v4
        return None
    hostid = ipaddress.IPv6Address(srcip).exploded.replace(':', '')[16:]
    entropy_score = entropy_hex(hostid)
    if (hostid[6:10] == 'fffe' or   # if address is slaac
        entropy_score <= 0.5):      # if entropy too low
        return None
    netid = ipaddress.IPv6Network(srcip + '/64', strict=False)
    subnetpfx = ipaddress.IPv6Network(srcip + f'/{pfxlen}', strict=False)
    
    return {
        **row,
        'hostid':      hostid,
        'entropy':     entropy_score,
        'netid':       str(netid),
        'subnetpfx':   str(subnetpfx),
        'pfxlen':      pfxlen,
        'deleted':     False
    }

def check(chunk, pfxlen):
    colnames = ['protocol', 'tgtip', 'srcip', 'hoplim', 'icmpv6type', 'icmpv6code', 'rtt']
    df = pd.read_csv(chunk, names=colnames, header=None, comment='#')
    results = df.apply(process_row, args=(pfxlen, ), axis=1)
    return pd.DataFrame([r for r in results if r is not None])

def main():
    if len(sys.argv) < 2:
        print('Usage: python3 load.py <filename1> <filename2> ...')
        print('    or python3 load.py --full')
        sys.exit(1)
    if ("--full" not in sys.argv[1:]):
        print('Using test mode')
        pathlist = sys.argv[1:]
        load_all = False
    else:
        print('Using full mode')
        pathlist = os.listdir(tmp_data_dir)
        print(pathlist)
        load_all = True
    print(f"Target files: {pathlist}")

    engine = create_engine('postgresql+psycopg2://lyspfan:lyspfan@localhost:6789/lyspfan')
    conn = engine.raw_connection()
    cur = conn.cursor()
    os.system(f'{dbcommand} -v tbl={tablename} -f schemas/routerips.sql')
    output = io.StringIO()
    start = time.time()
    print(f'Started timing at {start}')

    for filepath in pathlist:
        print(f"Processing file: {filepath}")
        if not filepath.endswith('.csv'):
            continue

        if load_all:
            pfxlen = filepath.removesuffix('.csv')[-2:]
            outpath = os.path.join(tmp_data_dir, filepath)
        else:
            pfxlen = 56
            outpath = filepath
        print(f"Setting prefix length of file {filepath} to {pfxlen}")
        
        if not os.path.exists(outpath):
            print(f'{outpath} not found, please double check the name')
            sys.exit(1)

        pool = mp.Pool(nproc)

        print(f"Splitting file {filepath} in {nproc} chunks and placed them in {tmp_data_dir}")
        os.system(f'split --number=l/{nproc} --additional-suffix=.csv {outpath} {tmp_data_dir}/chunk_')
        
        wrs = []
        for chunk in os.listdir(tmp_data_dir):
            if not chunk.startswith('chunk_'):
                continue
            wr = pool.apply_async(check, (os.path.join(tmp_data_dir, chunk), pfxlen, ))
            wrs.append(wr)
        end_filter = time.time()
        print(f'Finished filtering entries for {filepath} in {end_filter - start:.2f}s')
        dfs = [wr.get() for wr in wrs]
        df = pd.concat(dfs, ignore_index=True)
        output = io.StringIO()
        df.to_csv(output, sep=',', header=False, index=False)
        output.seek(0)
        cur.copy_expert(f"COPY {tablename} FROM STDIN WITH (FORMAT csv, NULL '')", output)
        conn.commit()
        end_copy = time.time()
        print(f'Finished copying {filepath} in {end_copy - start:.2f}s')
        print(f'Removing temporary files in {tmp_data_dir}')
        os.system(f'rm -rf {tmp_data_dir}/chunk_*')
        pool.close()
        pool.join()

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()