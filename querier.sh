#!/bin/bash

dbcommand="psql -h localhost -p 6789"

$dbcommand <<EOF
create index if not exists full_table_hostid_slaac_entropy_netid 
on full_table (hostid, netid)
where entropy > 0.5 and is_slaac = false;

create materialized view if not exists dup_hostids as
with qualifying_hostids as (
    select
        hostid,
        count(*) as occurrence_count,
        count(distinct netid) as netid_count
    from full_table
    where entropy > 0.5 and is_slaac = false
    group by hostid
    having count(distinct netid) > 1
)
select
    f.hostid,
    f.entropy,
    q.occurrence_count,
    f.subnetpfx,
    f.netid,
    q.netid_count,
    f.tgtip,
    f.srcip,
    f.hoplim,
    f.icmpv6type,
    f.icmpv6code,
    f.rtt
from full_table f
inner join qualifying_hostids q on f.hostid = q.hostid;
EOF

echo "dup_hostids ready, building indexes in parallel..."

$dbcommand  <<EOF &
create index if not exists dup_hostids_hostid_idx on dup_hostids (hostid);
EOF

$dbcommand <<EOF &
create index if not exists dup_hostids_netid_idx on dup_hostids (netid);
EOF

$dbcommand <<EOF &
create index if not exists dup_hostids_netid_gist_idx on dup_hostids using gist (netid inet_ops);
EOF

$dbcommand <<EOF & 
create index if not exists dup_hostids_subnetpfx_idx on dup_hostids (subnetpfx);
EOF

wait
echo "indexes ready, starting partition workers..."

$dbcommand <<EOF &
create materialized view dup_hostids_to_asn_org_p1 as
select distinct on (d.hostid, d.netid, p.orgid)
    d.hostid,
    d.entropy,
    d.occurrence_count,
    d.subnetpfx,
    p.prefix as caida_pfx,
    p.prefixlen as caida_pfx_len,
    p.asn as as_number,
    p.autname as aut_name,
    p.orgname as oranization_name,
    p.orgid,
    p.country,
    d.netid,
    d.netid_count,
    d.tgtip,
    d.srcip,
    d.hoplim,
    d.icmpv6type,
    d.icmpv6code,
    d.rtt
from dup_hostids d
left join pfx2as2org p on p.prefix >>= d.netid
where
    d.hostid < '1'
    and substring(d.hostid from 1 for 8) != '00000000'
order by d.hostid, d.netid, p.orgid, p.prefixlen desc;
EOF

$dbcommand <<EOF &
create materialized view dup_hostids_to_asn_org_p2 as
select distinct on (d.hostid, d.netid, p.orgid)
    d.hostid,
    d.entropy,
    d.occurrence_count,
    d.subnetpfx,
    p.prefix as caida_pfx,
    p.prefixlen as caida_pfx_len,
    p.asn as as_number,
    p.autname as aut_name,
    p.orgname as oranization_name,
    p.orgid,
    p.country,
    d.netid,
    d.netid_count,
    d.tgtip,
    d.srcip,
    d.hoplim,
    d.icmpv6type,
    d.icmpv6code,
    d.rtt
from dup_hostids d
left join pfx2as2org p on p.prefix >>= d.netid
where
    d.hostid >= '1'
    and d.hostid < '8'
order by d.hostid, d.netid, p.orgid, p.prefixlen desc;
EOF

$dbcommand <<EOF &
create materialized view dup_hostids_to_asn_org_p3 as
select distinct on (d.hostid, d.netid, p.orgid)
    d.hostid,
    d.entropy,
    d.occurrence_count,
    d.subnetpfx,
    p.prefix as caida_pfx,
    p.prefixlen as caida_pfx_len,
    p.asn as as_number,
    p.autname as aut_name,
    p.orgname as oranization_name,
    p.orgid,
    p.country,
    d.netid,
    d.netid_count,
    d.tgtip,
    d.srcip,
    d.hoplim,
    d.icmpv6type,
    d.icmpv6code,
    d.rtt
from
    dup_hostids d
    left join pfx2as2org p on p.prefix >>= d.netid
where
    d.hostid >= '8'
order by d.hostid, d.netid, p.orgid, p.prefixlen desc;
EOF