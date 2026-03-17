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
WHERE Deleted = false AND substring(HostID from 7 for 4) = 'fffe';

\echo 'getting subnet prefix, network id, and calculating entropy score on host id'
UPDATE :tbl
SET Entropy = entropy_hex(HostID),
    SubnetPfx = set_masklen(SrcIP, PfxLen)::cidr,
    NetID = set_masklen(SrcIP, 64)::cidr
WHERE Deleted = false;

\echo 'filtering out the repeated replies and only keep one instance'
UPDATE :tbl
SET Deleted = true
WHERE Deleted = false
    AND ctid NOT IN (
        SELECT MIN(ctid) FROM :tbl
        WHERE Deleted = false
        GROUP BY SrcIP
        HAVING COUNT(SrcIP) > 1
    );

DROP TABLE IF EXISTS clean_routerips;

\echo 'creating table clean_routerips with entries that survived and with entropy > 0.5'
CREATE TABLE clean_routerips AS
SELECT * FROM :tbl
    WHERE deleted = false AND entropy > 0.5;