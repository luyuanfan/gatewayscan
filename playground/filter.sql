DROP TABLE IF EXISTS test;
CREATE TABLE test (
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

\echo 'creating index on column: deleted'
CREATE INDEX ON test (deleted);

\echo 'removing replies from aliased networks and v4 routers addresses'
UPDATE test
SET Deleted = true
WHERE TgtIP = SrcIP OR family(SrcIP) = 4;

\echo 'expanding the rest of the router addresses'
UPDATE test
SET HostID = right(encode(substring(inet_send(SrcIP) from 5), 'hex'), 16)
WHERE Deleted = false;

\echo 'removing SLAAC generated addresses'
UPDATE test
SET Deleted = true
WHERE Deleted = false AND substring(HostID from 7 for 4) = 'fffe';

\echo 'getting subnet prefix, network id, and calculating entropy score on host id'
UPDATE test
SET Entropy = entropy_hex(HostID),
    SubnetPfx = set_masklen(SrcIP, PfxLen)::cidr,
    NetID = set_masklen(SrcIP, 64)::cidr
WHERE Deleted = false;

\echo 'filtering out the repeated replies and only keep one instance'
UPDATE test
SET Deleted = true
WHERE Deleted = false
    AND ctid NOT IN (
        SELECT MIN(ctid) FROM test
        WHERE Deleted = false
        GROUP BY SrcIP
        HAVING COUNT(SrcIP) > 1
    );

DROP TABLE IF EXISTS clean_test;

\echo 'creating table clean_test with entries that survived and with entropy > 0.5'
CREATE TABLE clean_test AS
SELECT * FROM test
    WHERE deleted = false AND entropy > 0.5;