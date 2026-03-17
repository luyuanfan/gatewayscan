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
import logging as log
import multiprocessing as mp
from collections import Counter
from sqlalchemy import create_engine

data_dir='/dbdata/'
tablename='test2'

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
    # print(f'')
    colnames = ['protocol', 'tgtip', 'srcip', 'hoplim', 'icmpv6type', 'icmpv6code', 'rtt']
    df = pd.read_csv(chunk, names=colnames, header=None)
    results = df.apply(process_row, args=(pfxlen, ), axis=1)
    return pd.DataFrame([r for r in results if r is not None])

def main():
    if len(sys.argv) < 2:
        log.error('Usage: python3 load.py <input_file1> <inputfile2> ...')
        sys.exit(1)
    for filepath in sys.argv[1:]:
        if not os.path.exists(filepath):
            log.error(f'{filepath} not found, please check the name')
            sys.exit(1)
        if not (filepath.endswith('.csv.bz2') or filepath.endswith('.csv')):
            log.error('Invalid file format. It should only end with .csv or .csv.bz2')
            sys.exit(1)
    
    # os.system(f"mkdir -p {data_dir}")
    # os.system(f"chmod a+w+r {data_dir}")

    engine = create_engine('postgresql+psycopg2://lyspfan:lyspfan@localhost:6789/lyspfan')
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()

    # TODO: think about where we decompress the files? seems like pbzip2 should do it?
    # and i don't need to deal with work assignment?
    # TODO: also must remove the first few lines (the comments out)

    for filepath in sys.argv[1:]:
        # might want to decide the number of worker depending on 
        nproc = 30
        pool = mp.Pool(nproc)

        start = time.time()
        print(f'Started processing {filepath}')

        os.system(f'split {filepath} --number=l/{nproc} --additional-suffix=.csv {data_dir}')

        # TODO: change pfxlen to the actual length
        pfxlen = 56
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