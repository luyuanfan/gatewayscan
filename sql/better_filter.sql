#### mapping each duplicated hostid prefix to asn and organziation ####

create materialized view if not exists better_filter as 
select *
from dup_hostids
where hostid !~ '^(00000000|00010000|00010001|00010002|00020001|00020002|80000)'
  and hostid !~ '^[0-9]{16}$'
  and substring(hostid from 7 for 5) <> 'ff0fe'
  and hostid not like '%000000%';

create index if not exists better_filter_hostid_idx on better_filter (hostid);
create index if not exists better_filter_netid_idx on better_filter (netid);
create index if not exists better_filter_netid_gist_idx ON better_filter USING gist (netid inet_ops);
create index if not exists better_filter_subnetpfx_idx on better_filter (subnetpfx);

-- worker 1
-- wanna remove all entries in table 1 that starts with eight zeros in a row
create materialized view better_filter_to_org_p1 as
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
from better_filter d
left join pfx2as2org p on p.prefix >>= d.netid
where
    d.hostid < '6'
order by d.hostid, d.netid, p.orgid;

-- worker 2
create materialized view better_filter_to_org_p2 as
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
from better_filter d
left join pfx2as2org p on p.prefix >>= d.netid
where
    d.hostid >= '6'
    and d.hostid < 'c'
order by d.hostid, d.netid, p.orgid;

-- worker 3
create materialized view better_filter_to_org_p3 as
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
    better_filter d
    left join pfx2as2org p on p.prefix >>= d.netid
where
    d.hostid >= 'c'
order by d.hostid, d.netid, p.orgid;

-- chain tables together
create materialized view if not exists better_filter_to_asn_org as (
select * from better_filter_to_org_p1
union all
select * from better_filter_to_org_p2
union all
select * from better_filter_to_org_p3
);

create index if not exists pall_orgid_idx on better_filter_to_asn_org (orgid);
create index if not exists pall_hostid_idx on better_filter_to_asn_org (hostid);
