CREATE TABLE IF NOT EXISTS :tbl (
    Protocol         text,
    TgtIP            inet,
    SrcIP            inet,
    HopLim           smallint,
    ICMPv6Type       smallint,
    ICMPv6Code       smallint,
    RTT              integer,
    is_aliased       boolean,
    is_v6            boolean,
    hostid           text,
    is_slaac         boolean,
    entropy          real,
    netid            cidr,
    subnetpfx        cidr
);