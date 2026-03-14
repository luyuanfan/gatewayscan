CREATE TABLE routerIPs (
    Protocol         text,                  -- protocol type: ICMP or TCP
    TgtIP            text,                  -- ICMP probe target IP
    SrcIP            text,                  -- IP of the replier 
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
    SubnetPfx        cidr,                  -- subnet prefix 
    IDBuffer         text                   -- network id
);

-- CREATE INDEX ActiveRows ON routerIPs (SrcIP) WHERE Deleted = false;

-- remove replies from aliased networks and v4 routers addresses
UPDATE routerIPs
    SET Deleted = true
    WHERE TgtIP = SrcIP OR NOT is_v6(SrcIP);

-- expand the rest of the router addresses
UPDATE routerIPs
    SET IDBuffer = exploded(SrcIP)
    WHERE Deleted = false;

-- remove SLAAC generated addresses
UPDATE routerIPs
    SET Deleted = true
    WHERE is_slaac(IDBuffer) AND Deleted = false;

-- add index on column: deleted
CREATE INDEX ActiveRows ON routerIPs (Deleted);

-- get network id, host id, and calculate entropy score on host id
UPDATE routerIPs
    SET NetID = left(IDBuffer, 16),
        HostID = right(IDBuffer, 16),
        Entropy = entropy_hex(right(IDBuffer, 16)),
        SubnetPfx = get_subnet_pfx(SrcIP, PfxLen)
    WHERE Deleted = false;

ALTER TABLE RouterIPs DROP COLUMN IDBuffer;