#### mapping each duplicated hostid prefix to asn and organziation ####

# get a count of the total number of entries
select count(*) from dup_hostids;

# check the percentage
select 
    substring(hostid, 1, 1) as first_char,
    count(*) as cnt,
    round(count(*) * 100.0 / 3268577207, 2) as pct
from dup_hostids
group by first_char
order by first_char;

-- worker 1
-- wanna remove all entries in table 1 that starts with eight zeros in a row
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

-- worker 2
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

-- worker 3
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

-- chain tables together
create view dup_hostids_to_asn_org as (
select * from dup_hostids_to_asn_org_p1
union all
select * from dup_hostids_to_asn_org_p2
union all
select * from dup_hostids_to_asn_org_p3
);
