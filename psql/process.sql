\timing on

\echo 'removing replies from aliased networks and v4 routers addresses'
UPDATE :tbl
    SET Deleted = true
    WHERE TgtIP = SrcIP OR family(SrcIP) = 4;

\echo 'expanding the rest of the router addresses'
UPDATE :tbl
    SET HostID = right(encode(substring(inet_send(SrcIP) from 5), 'hex'), 16)
    WHERE Deleted = false;

\echo 'removing SLAAC generated addresses'
UPDATE :tbl
    SET Deleted = true
    WHERE substring(HostID from 7 for 4) = 'fffe' AND Deleted = false;

\echo 'get subnet prefix, host id, and calculate entropy score on host id'
UPDATE :tbl
    SET Entropy = entropy_hex(HostID),
        SubnetPfx = set_masklen(SrcIP, PfxLen)::cidr
    WHERE Deleted = false;