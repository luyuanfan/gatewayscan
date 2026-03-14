SELECT * FROM routerips WHERE deleted = false AND entropy >= 0.5 LIMIT 100;

-- do a unique v.s. all percentage (for all those ones with high enough entropy scores)

-- order each repeated host id by entropy score
SELECT hostid, COUNT(*) AS host_id_count, MAX(entropy) AS entropy_score
    FROM routerips
    WHERE deleted = false AND entropy >= 0.5
    GROUP BY hostid
	HAVING COUNT(*) > 1
    ORDER BY entropy_score, host_id_count DESC;

-- order each repeated host id by times repeated
SELECT hostid, COUNT(*) AS host_id_count, MAX(entropy) AS entropy_score
    FROM routerips
    WHERE deleted = false AND entropy >= 0.5
    GROUP BY hostid
	HAVING COUNT(*) > 1
    ORDER BY host_id_count DESC;

-- order each repeated host id by both times repeated AND entropy score
SELECT hostid, COUNT(*) AS host_id_count, MAX(entropy) AS entropy_score
    FROM routerips
    WHERE deleted = false AND entropy >= 0.5
    GROUP BY hostid
	HAVING COUNT(*) > 1
    ORDER BY entropy_score DESC, host_id_count DESC;

SELECT subnetpfx, hostid, COUNT(*) AS host_id_count, MAX(entropy) AS entropy_score
    FROM routerips
    WHERE deleted = false AND entropy >= 0.5
    GROUP BY subnetpfx, hostid
	HAVING COUNT(*) > 1
    ORDER BY entropy_score DESC, host_id_count DESC;

-- map network ids to AS numbers 
SELECT * FROM pfx2as JOIN (
    SELECT subnetpfx, hostid, COUNT(*) AS host_id_count, MAX(entropy) AS entropy_score
    FROM routerips
    WHERE deleted = false AND entropy >= 0.5
    GROUP BY subnetpfx, hostid
	HAVING COUNT(*) > 1
    ORDER BY host_id_count DESC, entropy_score DESC
) ON prefix = subnetpfx;

-- join above to my as2org table
SELECT sub.subnetpfx, sub.hostid, sub.host_id_count, sub.entropy_score,
       p1.asn, p2.autname, p2.orgname, p2.country
FROM (
    SELECT subnetpfx, hostid, COUNT(*) AS host_id_count, MAX(entropy) AS entropy_score
    FROM routerips
    WHERE deleted = false AND entropy >= 0.5
    GROUP BY subnetpfx, hostid
    HAVING COUNT(*) > 1
    ORDER BY host_id_count DESC, entropy_score DESC
) AS sub
JOIN pfx2as AS p1 ON sub.subnetpfx <<= p1.prefix::cidr
JOIN pfx2as2org AS p2 ON p1.asn = p2.aut;