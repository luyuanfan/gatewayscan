CREATE TABLE smallRouterIPs (
    Protocol         text,                  -- protocol type: ICMP or TCP
    TgtIP            inet,                  -- ICMP probe target IP
    SrcIP            inet,                  -- IP of the replier 
    SubnetPfx        cidr,                  -- subnet prefix 
    HopLim           smallint,              -- Hop Limit
    ICMPv6Type       smallint,              -- 8 bits
    ICMPv6Code       smallint,              -- 8 bits
    RTT              integer,               -- round trip time (in millieseconds)
    Deleted          boolean DEFAULT false, -- flag if a row is soft deleted
    Entropy          real,                  -- entropy score
    NetID            text,                  -- network id 
    HostID           text,                  -- host id
    PfxLen           smallint,              -- subnet prefix length
    IDBuffer         text                   -- network id buffer
);

-- CREATE INDEX ActiveRows ON smallrouterIPs (SrcIP) WHERE Deleted = false;

-- remove replies from aliased networks and v4 routers addresses
UPDATE smallrouterIPs
    SET Deleted = true
    WHERE TgtIP = SrcIP OR family(SrcIP) = 4;

-- expand the rest of the router addresses
UPDATE smallrouterIPs
    SET HostID = right(encode(substring(inet_send(SrcIP) from 5), 'hex'), 16)
    WHERE Deleted = false;

-- remove SLAAC generated addresses
UPDATE smallrouterIPs
    SET Deleted = true
    WHERE substring(HostID from 7 for 4) = 'fffe' AND Deleted = false;

-- get network id, host id, and calculate entropy score on host id
UPDATE smallrouterIPs
    SET Entropy = entropy_hex(HostID),
        SubnetPfx = set_masklen(SrcIP, PfxLen)::cidr
    WHERE Deleted = false;

SELECT hostid, COUNT(*) AS host_id_count, MAX(entropy) AS entropy_score
    FROM smallrouterips
    WHERE deleted = false AND entropy >= 0.5
    GROUP BY hostid
	HAVING COUNT(*) > 1
    ORDER BY entropy_score, host_id_count DESC;

SELECT hostid, COUNT(*) AS host_id_count, MAX(entropy) AS entropy_score
    FROM smallrouterips
    WHERE deleted = false AND entropy >= 0.5
    GROUP BY hostid
	HAVING COUNT(*) > 1
    ORDER BY entropy_score DESC, host_id_count DESC;

-- map network ids to AS numbers 
SELECT * FROM pfx2as JOIN (
    SELECT subnetpfx, hostid, COUNT(*) AS host_id_count, MAX(entropy) AS entropy_score
    FROM smallrouterips
    WHERE deleted = false AND entropy >= 0.5
    GROUP BY subnetpfx, hostid
	HAVING COUNT(*) > 1
    ORDER BY entropy_score DESC, host_id_count DESC
) ON prefix = subnetpfx;

-- join above to my as2org table
SELECT sub.subnetpfx, sub.hostid, sub.host_id_count, sub.entropy_score,
       p1.asn, p2.autname, p2.orgname, p2.country
FROM (
    SELECT subnetpfx, hostid, COUNT(*) AS host_id_count, MAX(entropy) AS entropy_score
    FROM smallrouterips
    WHERE deleted = false AND entropy >= 0.5
    GROUP BY subnetpfx, hostid
    HAVING COUNT(*) > 1
    ORDER BY host_id_count DESC, entropy_score DESC
) AS sub
JOIN pfx2as AS p1 ON sub.subnetpfx <<= p1.prefix::cidr
JOIN as2org AS p2 ON p1.asn = p2.aut;