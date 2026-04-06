# create a table on just the host ids that are duplicated
CREATE MATERIALIZED VIEW IF NOT EXISTS duplicate_hostids AS
SELECT
    hostid,
    entropy,
    COUNT(*) OVER (PARTITION BY hostid) AS occurrence_count,
    subnetpfx,
    netid,
    COUNT(netid) OVER (PARTITION BY hostid) AS netid_count,
    tgtip,
    srcip,
    hoplim,
    icmpv6type,
    icmpv6code,
    rtt
FROM full_table
WHERE hostid IN (
    SELECT hostid
    FROM full_table
    WHERE entropy > 0.5 AND is_slaac = False
    GROUP BY hostid
    HAVING COUNT(netid) > 1
);

select hostid, entropy, occurrence_count, subnetpfx, netid, srcip, hoplim, rtt, ICMPv6Type, ICMPv6Code from duplicate_hostids order by entropy desc, hostid desc;

create index if not exists duplicated_hostids_idx on duplicate_hostids (hostid);

create index if not exists duplicated_hostid_pfx_idx on duplicate_hostids (subnetpfx);

create table if not exists pfx2as2org
as (
    select
        pfx2as.prefix,
        pfx2as.PrefixLen,
        pfx2as.asn,
        as2org.autname,
        as2org.orgname,
        as2org.orgid,
        as2org.country
    from pfx2as
    left join as2org
    on pfx2as.asn = as2org.aut
);

# mapping each duplicated hostid prefix to asn
CREATE MATERIALIZED VIEW IF NOT EXISTS duplicated_hostids_to_asn AS
SELECT
    d.hostid,
    d.entropy,
    d.occurrence_count,
    d.subnetpfx,
    p.prefix AS caida_pfx,
    p.asn AS as_number,
    d.netid,
    d.netid_count,
    d.tgtip,
    d.srcip,
    d.hoplim,
    d.icmpv6type,
    d.icmpv6code,
    d.rtt
FROM duplicate_hostids d
JOIN pfx2as2org p ON p.prefix >>= d.subnetpfx;

-- select hostid, entropy, occurrence_count, subnetpfx, netid, asn from hostid_to_asn order by entropy desc, hostid desc;

# mapping these asn to organization
CREATE MATERIALIZED VIEW IF NOT EXISTS hostid_to_org AS
SELECT
    d.hostid,
    d.subnetpfx,
    d.netid,
    d.entropy,
    d.occurrence_count,
    d.prefix,
    d.asn,
    a.autname,
    a.orgid,
    a.orgname,
    a.country
FROM duplicated_hostids_to_asn d
JOIN as2org a ON a.aut = d.asn;

select hostid, entropy, occurrence_count, subnetpfx, netid, asn, orgid, orgname, country from hostid_to_org order by entropy desc, hostid desc;
