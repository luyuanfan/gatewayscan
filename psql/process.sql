\timing on

\echo 'removing replies from aliased networks and v4 routers addresses'
UPDATE :tbl
    SET Deleted = true
    WHERE TgtIP = SrcIP OR NOT is_v6(SrcIP);

\echo 'expanding the rest of the router addresses'
UPDATE :tbl
    SET IDBuffer = exploded(SrcIP)
    WHERE Deleted = false;

\echo 'removing SLAAC generated addresses'
UPDATE :tbl
    SET Deleted = true
    WHERE is_slaac(IDBuffer) AND Deleted = false;

\echo 'get subnet prefix, host id, and calculate entropy score on host id'
UPDATE :tbl
    SET HostID = right(IDBuffer, 16),
        Entropy = entropy_hex(right(IDBuffer, 16)),
        SubnetPfx = set_masklen(SrcIP::inet, PfxLen)::cidr
    WHERE Deleted = false;

\echo 'dropping buffer column'
ALTER TABLE :tbl DROP COLUMN IDBuffer;