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

create index pfx2as2org_prefix_gist_idx ON pfx2as2org USING gist (prefix inet_ops);

create index dup_hostids_subnetpfx_gist_idx ON duplicate_hostids USING gist (subnetpfx inet_ops);

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


-- PARALLEL VERSION BELOW --

SELECT 
    substring(hostid, 1, 1) as first_char,
    count(*) as cnt,
    round(count(*) * 100.0 / 3268577207, 2) as pct
FROM duplicate_hostids
GROUP BY first_char
ORDER BY first_char;

-- Worker 1: '0...' (~1.06B rows)
CREATE MATERIALIZED VIEW duplicated_hostids_to_asn_org_p1 AS
SELECT
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
FROM duplicate_hostids d
LEFT JOIN pfx2as2org p ON p.prefix >>= d.subnetpfx
WHERE d.hostid < '1';

-- Worker 2: '1'-'7' (~1.007B rows)
CREATE MATERIALIZED VIEW duplicated_hostids_to_asn_org_p2 AS
SELECT
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
FROM duplicate_hostids d
LEFT JOIN pfx2as2org p ON p.prefix >>= d.subnetpfx
WHERE d.hostid >= '1' AND d.hostid < '8';

-- Worker 3: '8'-'f' (~1.2B rows)
CREATE MATERIALIZED VIEW duplicated_hostids_to_asn_org_p3 AS
SELECT
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
FROM duplicate_hostids d
LEFT JOIN pfx2as2org p ON p.prefix >>= d.subnetpfx
WHERE d.hostid >= '8';

CREATE VIEW duplicated_hostids_to_asn_org AS
    SELECT * FROM duplicated_hostids_to_asn_org_p1
    UNION ALL
    SELECT * FROM duplicated_hostids_to_asn_org_p2
    UNION ALL
    SELECT * FROM duplicated_hostids_to_asn_org_p3;

select hostid, entropy, occurrence_count, subnetpfx, netid, asn, orgid, orgname, country from hostid_to_org order by entropy desc, hostid desc;
