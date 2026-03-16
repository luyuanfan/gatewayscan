DROP TABLE IF EXISTS routerips;
CREATE TABLE routerips (
    Protocol         text,
    TgtIP            inet,
    SrcIP            inet,
    SubnetPfx        cidr,
    PfxLen           smallint,
    NetID            cidr,
    Entropy          real,
    HostID           text,
    HopLim           smallint,
    ICMPv6Type       smallint,
    ICMPv6Code       smallint,
    RTT              integer,
    Deleted          boolean DEFAULT false
);

-- removing replies from aliased networks and v4 routers addresses
UPDATE routerips
    SET Deleted = true
    WHERE TgtIP = SrcIP OR family(SrcIP) = 4;

-- expanding the rest of the router addresses
UPDATE routerips
    SET HostID = right(encode(substring(inet_send(SrcIP) from 5), 'hex'), 16)
    WHERE Deleted = false;

-- removing SLAAC generated addresses
UPDATE routerips
    SET Deleted = true
    WHERE substring(HostID from 7 for 4) = 'fffe' AND Deleted = false;

-- getting subnet prefix, network id, and calculating entropy score on host id
UPDATE routerips
    SET Entropy = entropy_hex(HostID),
        SubnetPfx = set_masklen(SrcIP, PfxLen)::cidr,
        NetID = set_masklen(SrcIP, 64)::cidr
    WHERE Deleted = false;

-- creating index on columns: (netid, hostid)
CREATE INDEX ON routerips (netid, hostid);

-- filtering out the repeated replies and only keep one instance
UPDATE routerips
    SET deleted = true
    WHERE deleted = false
        AND ctid NOT IN (
            SELECT MIN(ctid) FROM routerips
            WHERE deleted = false
            GROUP BY netid, hostid
        );