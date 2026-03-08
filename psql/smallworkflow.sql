CREATE TABLE smallRouterIPs (
    Protocol         text,       -- ICMP or TCP
    TgtIP            text,       -- inet datatype isn't too versatile
    SrcIP            text,       -- inet datatype isn't too versatile
    HopLim           smallint,   -- hop prolly won't go over 32767
    ICMPv6Type       smallint,   -- 8 bits (only for ICMP protocol)
    ICMPv6Code       smallint,   -- 8 bits (only for ICMP protocol)
    Flags            smallint,   -- 8 bits (only for TCP protocol)
    RTT              integer     -- in (millieseconds)
    -- Entropy       float(24),  -- entropy score
    -- PfxLen        smallint,   -- which subnet it's sitting on
    -- IPType        boolean,    -- 1 for v6 0 for v4
    -- HostID        text,       -- host id
    -- NetID         text,       -- network id
);

-- add all self-defined columns 
ALTER TABLE smallRouterIPs ADD COLUMN Entropy float(24);
ALTER TABLE smallRouterIPs ADD COLUMN PfxLen smallint;
ALTER TABLE smallRouterIPs ADD COLUMN IPType boolean;
ALTER TABLE smallRouterIPs ADD COLUMN HostID text;
ALTER TABLE smallRouterIPs ADD COLUMN NetID text;

-- this part is completely manual 
UPDATE smallRouterIPs SET PfxLen = 56; 

-- process source ips, delete v4 rows, expand the rest, delete slaac rows
-- UPDATE smallRouterIPs SET IPType = is_v6(SrcIP);
DELETE FROM smallRouterIPs WHERE (is_v6(SrcIP) is false);
UPDATE smallRouterIPs SET SrcIP = exploded(SrcIP);
DELETE FROM smallRouterIPs WHERE (is_slaac(SrcIP));

UPDATE smallRouterIPs SET HostID = get_hid(SrcIP);
UPDATE smallRouterIPs SET NetID = get_nid(SrcIP);

UPDATE smallRouterIPs SET entropy = shannon_bin(HostID);

