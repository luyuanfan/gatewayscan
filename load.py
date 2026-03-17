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
tablename='test2'
data_dir='/dbdata/'
nproc = 30

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
    # while chunk.readline().startswith('#'):


    colnames = ['protocol', 'tgtip', 'srcip', 'hoplim', 'icmpv6type', 'icmpv6code', 'rtt']
    df = pd.read_csv(chunk, names=colnames, header=None)
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

    # os.system(f"mkdir -p {data_dir}")
    # os.system(f"chmod a+w+r {data_dir}")
    # TODO: should create the database from making the schema
    engine = create_engine('postgresql+psycopg2://lyspfan:lyspfan@localhost:6789/lyspfan')
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()

    # TODO: also must remove the first few lines (the comments out)!!!!!!!!!!! VERY IMPORYTTANT
    for filepath in pathlist:
        if load_all:
            if not os.path.exists(os.path.join(source_data_dir, filepath)):
                print(f'{filepath} not found, please check the name')
                sys.exit(1)
            if not (filepath.endswith('.csv.bz2') or filepath.endswith('.csv')):
                print(f'skipping file {filepath}')
                continue
            outfilename = filepath.removesuffix('.bz2').removeprefix(f'{source_data_dir}')
            os.system(f'pbzip2 -dfk -p30 {os.path.join(source_data_dir, filepath)} > {os.path.join(data_dir, outfilename)}')
            pfxlen = (filepath.removesuffix(".csv.bz2"))[-2:]
        else:
            outfilename = os.path.join(data_dir, filepath)
            pfxlen = 56

        pool = mp.Pool(nproc)

        start = time.time()
        print(f'Started processing {filepath}')
        os.system(f'split {filepath} --number=l/{nproc} --additional-suffix=.csv {data_dir}')
        
        wrs = []
        for chunk in os.listdir(data_dir):
            wr = pool.apply_async(check, (os.path.join(data_dir, chunk), pfxlen, ))
            wrs.append(wr)

        dfs = [wr.get() for wr in wrs]
        df = pd.concat(dfs, ignore_index=True)
        print(f'Finished loading [{filepath}] in {time.time() - start:.2f}s')
        output = io.StringIO()
        df.to_csv(output, sep=',', header=False, index=False)
        output.seek(0)
        cur.copy_expert(f"COPY {tablename} FROM STDIN WITH (FORMAT csv, NULL '')", output)
        conn.commit()
        end = time.time()
        os.system(f'rm -rf {data_dir}/*')
        pool.close()

    print(f'Finished [{filepath}] in {end - start:.2f}s')
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()