\timing on

\echo 'creating index on column: deleted'
CREATE INDEX ON :tbl (deleted);

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

\echo 'getting subnet prefix, network id, and calculating entropy score on host id'
UPDATE :tbl
    SET Entropy = entropy_hex(HostID),
        SubnetPfx = set_masklen(SrcIP, PfxLen)::cidr,
        NetID = set_masklen(SrcIP, 64)::cidr
    WHERE Deleted = false;

\echo 'creating index on columns: (netid, hostid)'
CREATE INDEX ON :tbl (netid, hostid);

\echo 'filtering out the repeated replies and only keep one instance'
UPDATE :tbl
    SET deleted = true
    WHERE deleted = false
        AND ctid NOT IN (
            SELECT MIN(ctid) FROM :tbl
            WHERE deleted = false
            GROUP BY netid, hostid
        );