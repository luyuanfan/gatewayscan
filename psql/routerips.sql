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
    SET HostID = right(IDBuffer, 16),
        Entropy = entropy_hex(right(IDBuffer, 16)),
        SubnetPfx = set_masklen(SrcIP::inet, PfxLen)::cidr
    WHERE Deleted = false;

ALTER TABLE RouterIPs DROP COLUMN IDBuffer;