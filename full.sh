#!/bin/bash

echo 'decompressing file combined-48s-r1-s56.csv.bz2'
bzip2 -dck /mnt/usb/combined-48s-r1-s56.csv.bz2 > /cdbdata/combined-48s-r1-s56.csv
echo 'decompressing file combined-48s-r2-s60.csv.bz2'
bzip2 -dck /mnt/usb/combined-48s-r2-s60.csv.bz2 > /cdbdata/combined-48s-r2-s60.csv
echo 'decompressing file combined-48s-r3-s64.csv.bz2'
bzip2 -dck /mnt/usb/combined-48s-r3-s64.csv.bz2 > /cdbdata/combined-48s-r3-s64.csv
echo 'splitting file combined-48s-r1-s56.csv'
split /cdbdata/combined-48s-r1-s56.csv --number=l/100 --additional-suffix=_56.csv -d /cdbdata/chunks/chunk_
echo 'splitting file combined-48s-r2-s60.csv'
split /cdbdata/combined-48s-r2-s60.csv --number=l/200 --additional-suffix=_60.csv -d /cdbdata/chunks/chunk_
echo 'splitting file combined-48s-r3-s64.csv'
split /cdbdata/combined-48s-r3-s64.csv --number=l/2000 --additional-suffix=_64.csv -d /cdbdata/chunks/chunk_
echo 'creating caida tables'
./import/pfx2as.sh
./import/as2org.sh
echo 'loading all data into db'
python3 load.py full_table2 --full 
echo 'adding index filtered_full_table2 on full_table2'
psql -h localhost -p 6789 <<EOF
create index if not exists filtered_full_table2 on full_table2(is_slaac, entropy)
    where entropy > 0.5 and is_slaac = false;
EOF
echo 'adding index hostid_idx_full_table2 on full_table2'
psql -h localhost -p 6789 <<EOF
create index if not exists hostid_idx_full_table2 on full_table2 (hostid);
EOF
echo 'creating a table on just the host ids that are duplicated'
psql -h localhost -p 6789 <<EOF
create materialized view if not exists duplicate_hostids as 
select
    hostid,
    entropy,
    count(*) over (partition by hostid) as occurrence_count,
    subnetpfx,
    netid,
    count(netid) over (partition by hostid) as netid_count,
    tgtip,
    srcip,
    hoplim,
    icmpv6type,
    icmpv6code,
    rtt
from full_table2
where hostid in (
    select hostid
    from full_table2
    where entropy > 0.5 and is_slaac = false
    group by hostid
    having count(netid) > 1
);
EOF
