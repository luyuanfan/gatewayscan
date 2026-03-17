DROP TABLE IF EXISTS :tbl;
CREATE TABLE :tbl (
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
    Deleted          boolean DEFAULT false
);