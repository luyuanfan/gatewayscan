create index if not exists filtered_:tbl on :tbl(is_slaac, entropy)
    where entropy > 0.5 and is_slaac = false;

create index if not exists hostid_idx_:tbl on :tbl (hostid);

# create a table on just the host ids that are duplicated
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
from full_table
where hostid in (
    select hostid
    from full_table
    where entropy > 0.5 and is_slaac = false
    group by hostid
    having count(netid) > 1
);

select hostid, entropy, occurrence_count, subnetpfx, netid, srcip, hoplim, rtt, icmpv6type, icmpv6code from duplicate_hostids order by entropy desc, hostid desc;

create index if not exists duplicated_hostids_idx on duplicate_hostids (hostid);

create index if not exists duplicated_hostid_pfx_idx on duplicate_hostids (subnetpfx);

create table if not exists pfx2as2org
as (
    select
        pfx2as.prefix,
        pfx2as.prefixlen,
        pfx2as.asn,
        as2org.autname,
        as2org.orgname,
        as2org.orgid,
        as2org.country
    from pfx2as
    left join as2org
    on pfx2as.asn = as2org.aut
);

# mapping each duplicated hostid prefix to asn and organziation
create materialized view if not exists duplicated_hostids_to_asn_org as
select
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
from duplicate_hostids d
left join pfx2as2org p
on p.prefix >>= d.subnetpfx;

select hostid, entropy, occurrence_count, subnetpfx, netid, asn, orgid, orgname, country from hostid_to_org order by entropy desc, hostid desc;
