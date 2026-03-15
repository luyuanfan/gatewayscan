DROP TABLE IF EXISTS :tbl;
CREATE TABLE :tbl (
    Protocol         text,
    TgtIP            inet,
    SrcIP            inet,
    SubnetPfx        cidr,
    PfxLen           smallint,
    Entropy          real,
    HostID           text,
    HopLim           smallint,
    ICMPv6Type       smallint,
    ICMPv6Code       smallint,
    RTT              integer,
    Deleted          boolean DEFAULT false
);
