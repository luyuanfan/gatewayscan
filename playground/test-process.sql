DROP TABLE IF EXISTS test;
CREATE TABLE test (
    Protocol         text,
    TgtIP            inet,
    SrcIP            inet,
    HopLim           smallint,
    ICMPv6Type       smallint,
    ICMPv6Code       smallint,
    RTT              integer,
    HostID           text,
    Entropy          real,
    NetID            cidr,
    SubnetPfx        cidr,
    PfxLen           smallint,
    Deleted          boolean
);

\echo 'filtering out the repeated replies and only keep one instance'
UPDATE test2
SET Deleted = true
WHERE Deleted = false
    AND ctid NOT IN (
        SELECT MIN(ctid) FROM test2
        WHERE Deleted = false
        GROUP BY SrcIP
        HAVING COUNT(SrcIP) > 1
    );

DROP TABLE IF EXISTS clean_test2;

\echo 'creating table clean_test2 with entries that survived and with entropy > 0.5'
CREATE TABLE clean_test2 AS
SELECT * FROM test
    WHERE deleted = false AND entropy > 0.5;