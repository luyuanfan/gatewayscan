\echo 'order each repeated host id by entropy score AND times repeated'
SELECT hostid, COUNT(hostid) AS host_id_count, MAX(entropy) AS entropy_score
FROM test3
GROUP BY hostid
HAVING COUNT(hostid) > 1
ORDER BY entropy_score DESC, host_id_count DESC;

\echo 'get the netid of networks having the problematic hostids (grouped together)'
SELECT hostid, netid, subnetpfx, entropy
FROM clean_routerips
WHERE entropy > 0.8 AND
    hostid IN (
    SELECT hostid FROM clean_routerips
    GROUP BY hostid
    HAVING COUNT(netid) > 1
)
ORDER BY hostid, netid;

\echo 'get the list of netids and subnet prefixes where these repeated addresses occur'
CREATE TABLE markedsubnets AS (
    SELECT hostid, netid, subnetpfx, entropy
    FROM clean_routerips
    WHERE entropy > 0.8 AND
        hostid IN (
        SELECT hostid FROM clean_routerips
        GROUP BY hostid
        HAVING COUNT(netid) > 1
    )
    ORDER BY hostid, netid
);

\echo 'map subnet to AS numbers'
SELECT m.hostid, m.subnetpfx, p.prefix, p.asn
FROM markedsubnets m
JOIN pfx2as p ON p.prefix >>= m.subnetpfx;

\echo 'map these asn to organizations'
SELECT m.hostid, m.subnetpfx, m.netid, m.entropy, p.prefix, p.asn, a.autname, a.orgid, a.orgname, a.country
FROM markedsubnets m
JOIN pfx2as p ON p.prefix >>= m.subnetpfx
JOIN as2org a ON a.aut = p.asn;