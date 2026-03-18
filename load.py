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

source_data_dir='/mnt/usb'
tmp_data_dir='/dbdata'
tablename='test2'
nproc=30
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
        pathlist = os.listdir(source_data_dir)
        load_all = True

    engine = create_engine('postgresql+psycopg2://lyspfan:lyspfan@localhost:6789/lyspfan')
    conn = engine.raw_connection()
    cur = conn.cursor()
    os.system(f'{dbcommand} -v tbl={tablename} -f schemas/routerips.sql')
    output = io.StringIO()
    
    for filepath in pathlist:
        if load_all:
            if not (filepath.endswith('.csv.bz2') or filepath.endswith('.csv')):
                continue
            full_src = os.path.join(source_data_dir, filepath)
            if not os.path.exists(full_src):
                print(f'{full_src} not found, please check the name')
                sys.exit(1)
            outfilename = filepath.removesuffix('.bz2')
            outpath = os.path.join(tmp_data_dir, outfilename)
            # os.system(f'pbzip2 -dcfk -p{nproc} {full_src} > {outpath}')
            print(f'pbzip2 -dcfk -p{nproc} {full_src} > {outpath}')
            pfxlen = (filepath.removesuffix(".csv.bz2"))[-2:]
        else:
            outfilename = os.path.basename(filepath)
            outpath = os.path.join(tmp_data_dir, outfilename)
            pfxlen = 56

        pool = mp.Pool(nproc)
        start = time.time()
        print(f'Started processing {outfilename}')
        os.system(f'split {filepath} --number=l/{nproc} --additional-suffix=.csv {tmp_data_dir}chunk_')
        # print(f'split {outpath} --number=l/{nproc} --additional-suffix=.csv {tmp_data_dir}chunk_')
        os.remove(outpath)
        
        wrs = []
        for chunk in os.listdir(tmp_data_dir):
            wr = pool.apply_async(check, (os.path.join(tmp_data_dir, chunk), pfxlen, ))
            wrs.append(wr)

        dfs = [wr.get() for wr in wrs]
        df = pd.concat(dfs, ignore_index=True)
        print(f'Finished loading [{filepath}] in {time.time() - start:.2f}s')
        output = io.StringIO()
        df.to_csv(output, sep=',', header=False, index=False)
        output.seek(0)
        cur.copy_expert(f"COPY {tablename} FROM STDIN WITH (FORMAT csv, NULL '')", output)
        conn.commit()
        os.system(f'rm -rf {tmp_data_dir}/*')
        pool.close()
        pool.join()

    end = time.time()
    print(f'Finished [{filepath}] in {end - start:.2f}s')
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()