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
    ORDER BY host_id_count DESC, entropy_score DESC;

SELECT netid, hostid, COUNT(*) AS host_id_count, MAX(entropy) AS entropy_score
    FROM routerips
    WHERE deleted = false AND entropy >= 0.5
    GROUP BY netid, hostid
	HAVING COUNT(*) > 1
    ORDER BY host_id_count DESC, entropy_score DESC;


-- map network ids to AS numbers 
SELECT * FROM pfx2as JOIN (
    SELECT netid, hostid, COUNT(*) AS host_id_count, MAX(entropy) AS entropy_score
    FROM routerips
    WHERE deleted = false AND entropy >= 0.5
    GROUP BY netid, hostid
	HAVING COUNT(*) > 1
    ORDER BY host_id_count DESC, entropy_score DESC
) ON prefix = netid;