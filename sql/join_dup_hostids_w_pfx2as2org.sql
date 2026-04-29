#### mapping each duplicated hostid prefix to asn and organziation ####

-- worker 1
-- wanna remove all entries in table 1 that starts with eight zeros in a row
create materialized view dup_hostids_to_org_p1 as
select distinct on (d.hostid, d.netid, p.orgid)
    d.hostid,
    d.entropy,
    d.netid,
    d.occurrence_count,
    d.netid_count as distinct_net_occurence,
    d.subnetpfx,
    p.prefix as caida_pfx,
    p.asn as as_number,
    p.autname as aut_name,
    p.orgname as oranization_name,
    p.orgid,
    p.country,
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
order by d.hostid, d.netid, p.orgid;

-- worker 2
create materialized view dup_hostids_to_org_p2 as
select distinct on (d.hostid, d.netid, p.orgid)
    d.hostid,
    d.entropy,
    d.netid,
    d.occurrence_count,
    d.netid_count as distinct_net_occurence,
    d.subnetpfx,
    p.prefix as caida_pfx,
    p.asn as as_number,
    p.autname as aut_name,
    p.orgname as oranization_name,
    p.orgid,
    p.country,
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
order by d.hostid, d.netid, p.orgid;

-- worker 3
create materialized view dup_hostids_to_org_p3 as
select distinct on (d.hostid, d.netid, p.orgid)
    d.hostid,
    d.entropy,
    d.netid,
    d.occurrence_count,
    d.netid_count as distinct_net_occurence,
    d.subnetpfx,
    p.prefix as caida_pfx,
    p.asn as as_number,
    p.autname as aut_name,
    p.orgname as oranization_name,
    p.orgid,
    p.country,
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
order by d.hostid, d.netid, p.orgid;

-- chain tables together
create materialized view if not exists dup_hostids_to_asn_org as (
select * from dup_hostids_to_org_p1
union all
select * from dup_hostids_to_org_p2
union all
select * from dup_hostids_to_org_p3
);

create index if not exists pall_orgid_idx on dup_hostids_to_asn_org (orgid);
create index if not exists pall_hostid_idx on dup_hostids_to_asn_org (hostid);