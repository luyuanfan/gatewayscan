CREATE TABLE smallRouterIPs (
    Protocol         text,                  -- protocol type: ICMP or TCP
    TgtIP            text,                  -- ICMP probe target IP
    SrcIP            text,                  -- IP of the replier 
    SubnetPfx        cidr,                  -- subnet prefix 
    HopLim           smallint,              -- Hop Limit
    ICMPv6Type       smallint,              -- 8 bits (only for ICMP protocol)
    ICMPv6Code       smallint,              -- 8 bits (only for ICMP protocol)
    Flags            smallint,              -- 8 bits (only for TCP protocol)
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
    WHERE TgtIP = SrcIP
        OR SrcIP !~ ':';

-- expand the rest of the router addresses
UPDATE smallrouterIPs
    SET IDBuffer = exploded(SrcIP)
    WHERE Deleted = false;

-- remove SLAAC generated addresses
UPDATE smallrouterIPs
    SET Deleted = true
    WHERE is_slaac(IDBuffer) AND Deleted = false;

-- add index on column: deleted
CREATE INDEX ActiveRows ON smallrouterIPs (Deleted);

-- get network id, host id, and calculate entropy score on host id
UPDATE smallrouterIPs
    SET NetID = left(IDBuffer, 16),
        HostID = right(IDBuffer, 16),
        Entropy = entropy_hex(right(IDBuffer, 16)),
        SubnetPfx = get_subnet_pfx(SrcIP, PfxLen)
    WHERE Deleted = false;

ALTER TABLE smallRouterIPs DROP COLUMN IDBuffer;

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