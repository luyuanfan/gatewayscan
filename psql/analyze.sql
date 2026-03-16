SELECT * FROM routerips WHERE deleted = false AND entropy >= 0.5 LIMIT 100;

-- do a unique v.s. all percentage (for all those ones with high enough entropy scores)

-- order each repeated host id by both times repeated AND entropy score
SELECT hostid, COUNT(*) AS host_id_count, MAX(entropy) AS entropy_score
    FROM routerips
    WHERE deleted = false AND entropy >= 0.5
    GROUP BY hostid
	HAVING COUNT(*) > 1
    ORDER BY entropy_score DESC, host_id_count DESC;

-- TODO: it's also possible that multiple different probes trigger the same router to respond
--       so i can filter out those repeated replies coming from the exact same netid
SELECT subnetpfx, netid, hostid, COUNT(*) AS host_id_count, MAX(entropy) AS entropy_score
    FROM routerips
    WHERE deleted = false AND entropy >= 0.5
    GROUP BY subnetpfx, netid, hostid
	HAVING COUNT(hostid) > 1
    ORDER BY subnetpfx, entropy_score DESC, host_id_count DESC;

-- TODO: for each repeated host id, we want to see which subnets they come from
-- might need to keep a targeted subnet field and also a full network portion field
SELECT subnetpfx, netid, hostid, COUNT(*) AS host_id_count, MAX(entropy) AS entropy_score
    FROM routerips
    WHERE deleted = false AND entropy >= 0.5
    GROUP BY subnetpfx, netid, hostid
	HAVING COUNT(hostid) > 1
    ORDER BY subnetpfx, entropy_score DESC, host_id_count DESC;

-- order data the same way as above but also print which subnet it comes from

-- save the subnets as a table just in case that the prefix does not match any ASN
CREATE TABLE markedSubnets
    AS (
        SELECT subnetpfx, hostid, COUNT(*) AS host_id_count, MAX(entropy) AS entropy_score
        FROM routerips
        WHERE deleted = false AND entropy >= 0.5
        GROUP BY subnetpfx, hostid
        HAVING COUNT(*) > 1
        ORDER BY entropy_score DESC, host_id_count DESC
    );

-- map subnet to AS numbers 
SELECT prefix, asn, hostid, host_id_count, entropy_score FROM pfx2as JOIN (
    SELECT subnetpfx, hostid, COUNT(*) AS host_id_count, MAX(entropy) AS entropy_score
    FROM routerips
    WHERE deleted = false AND entropy >= 0.5
    GROUP BY subnetpfx, hostid
	HAVING COUNT(*) > 1
    ORDER BY entropy_score DESC, host_id_count DESC
) ON prefix = subnetpfx;


-- map above to organization
SELECT sub.subnetpfx, sub.hostid, sub.host_id_count, sub.entropy_score,
       p1.asn, p2.autname, p2.orgname, p2.country
FROM (
    SELECT subnetpfx, hostid, COUNT(*) AS host_id_count, MAX(entropy) AS entropy_score
    FROM routerips
    WHERE deleted = false AND entropy >= 0.5
    GROUP BY subnetpfx, hostid
	HAVING COUNT(*) > 1
    ORDER BY entropy_score DESC, host_id_count DESC
) AS sub
JOIN pfx2as AS p1 ON sub.subnetpfx <<= p1.prefix::cidr
JOIN as2org AS p2 ON p1.asn = p2.aut;