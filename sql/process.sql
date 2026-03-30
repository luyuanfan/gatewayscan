\echo 'create a table on just the host ids that are duplicated'
CREATE MATERIALIZED VIEW IF NOT EXISTS duplicate_hostids AS
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
    COUNT(DISTINCT netid) OVER (PARTITION BY hostid) AS distinct_netid_count
FROM full_table
WHERE hostid IN (
    SELECT hostid
    FROM full_table
    WHERE entropy > 0.5 AND is_slaac = False
    GROUP BY hostid
    HAVING COUNT(DISTINCT netid) > 1
);

\echo 'mapping each duplicated hostid prefix to asn'
CREATE MATERIALIZED VIEW IF NOT EXISTS hostid_to_asn AS
SELECT
    d.hostid,
    d.subnetpfx,
    d.netid,
    d.entropy,
    d.occurrence_count,
    p.prefix,
    p.asn
FROM duplicate_hostids d
JOIN pfx2as p ON p.prefix >>= d.subnetpfx;

\echo 'mapping these asn to organization'
CREATE MATERIALIZED VIEW IF NOT EXISTS hostid_to_org AS
SELECT
    h.hostid,
    h.subnetpfx,
    h.netid,
    h.entropy,
    h.occurrence_count,
    h.distinct_netid_count,
    h.prefix,
    h.asn,
    a.autname,
    a.orgid,
    a.orgname,
    a.country
FROM hostid_to_asn h
JOIN as2org a ON a.aut = h.asn;