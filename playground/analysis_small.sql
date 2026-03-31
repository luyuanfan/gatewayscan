# create a table on just the host ids that are duplicated
CREATE MATERIALIZED VIEW IF NOT EXISTS duplicate_hostids_small AS
SELECT
    hostid,
    tgtip,
    srcip,
    hoplim,
    icmpv6type,
    icmpv6code,
    rtt,
    entropy,
    subnetpfx,
    netid,
    COUNT(*) OVER (PARTITION BY hostid) AS occurrence_count,
    COUNT(netid) OVER (PARTITION BY hostid) AS netid_count
FROM small_test
WHERE hostid IN (
    SELECT hostid
    FROM small_test
    WHERE entropy > 0.5 AND is_slaac = False
    GROUP BY hostid
    HAVING COUNT(DISTINCT netid) > 1
);

select hostid, entropy, occurrence_count, subnetpfx, netid, srcip, hoplim, rtt, ICMPv6Type, ICMPv6Code from duplicate_hostids_small order by entropy desc, hostid desc;

# mapping each duplicated hostid prefix to asn
CREATE MATERIALIZED VIEW IF NOT EXISTS hostid_to_asn_small AS
SELECT
    d.hostid,
    d.subnetpfx,
    d.netid,
    d.entropy,
    d.occurrence_count,
    p.prefix,
    p.asn
FROM duplicate_hostids_small d
JOIN pfx2as p ON p.prefix >>= d.subnetpfx;

select hostid, entropy, occurrence_count, subnetpfx, netid, asn from hostid_to_asn_small order by entropy desc, hostid desc;

# mapping these asn to organization
CREATE MATERIALIZED VIEW IF NOT EXISTS hostid_to_org_small AS
SELECT
    h.hostid,
    h.subnetpfx,
    h.netid,
    h.entropy,
    h.occurrence_count,
    h.prefix,
    h.asn,
    a.autname,
    a.orgid,
    a.orgname,
    a.country
FROM hostid_to_asn_small h
JOIN as2org a ON a.aut = h.asn;

select hostid, entropy, occurrence_count, subnetpfx, netid, asn, orgid, orgname, country from hostid_to_org_small order by entropy desc, hostid desc;
