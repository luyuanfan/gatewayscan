CREATE TABLE tinyRouterIPs (
    Protocol         boolean,    -- 1 for ICMP 0 for TCP
    TgtIP            text,       -- inet datatype ain't too versatile
    SrcIP            text,       -- inet datatype ain't too versatile
    HopLim           smallint,   -- depends, prolly won't go over 32767
    ICMPv6Type       smallint,   -- 8 bits (only for ICMP protocol)
    ICMPv6Code       smallint,   -- 8 bits (only for ICMP protocol)
    Flags            smallint,   -- 8 bits (only for TCP protocol)
    RTT              integer,    -- in (millieseconds)
    -- self-defined columns
    Entropy          float(24),  -- entropy score
    PfxLen           integer,    -- which subnet it's sitting on
    RouterType       boolean     -- 1 for v6 0 for v4
    -- placeholder for HostID which will be inserted later
    -- placeholder for NetID which will also be inserted later
);

-- add libraries
CREATE EXTENSION plpython3u;

-- define functions
CREATE FUNCTION get_hid(addr text)
  RETURNS text
AS $$
  return addr[64:]
$$ LANGUAGE plpython3u;

CREATE FUNCTION get_nid(addr text)
  RETURNS text
AS $$
  return addr[:64]
$$ LANGUAGE plpython3u;

CREATE FUNCTION exploded(src_ip_str text)
  RETURNS text
AS $$
  import ipaddress
  src_ip_obj = ipaddress.IPv6Address(src_ip_str)
  full_addr = src_ip_obj.exploded.replace(":", "")
  return full_addr
$$ LANGUAGE plpython3u;

CREATE FUNCTION shannon_bin(hid text)
  RETURNS float
AS $$
  from collections import Counter
  from math import log2
  binary = bin(int(hid, 16))[2:].zfill(64)
  c = Counter(binary)
  score = - sum([(val / 64) * log2(val / 64) for key, val in c.items()])
  return score
$$ LANGUAGE plpython3u;

CREATE FUNCTION is_slaac(addr text)
  RETURNS boolean
AS $$
  return addr[22:26].lower() == "fffe"
$$ LANGUAGE plpython3u;

-- insert records
INSERT INTO tinyRouterIPs VALUES (true, '2607:5380:1470:be58:8647:6e47:456e:bfc2', '2607:5380:8000:3e:1621:3ff:fedf:2ba8', NULL, NULL, 48, 1, 0, NULL, 125, NULL, 48, true);
INSERT INTO tinyRouterIPs VALUES (true, '2607:fa49:1c42:3f18:8647:6e47:456e:bfc2', '2607:fa48:c:9500:64c2:882:e885:89bb', NULL, NULL, 46, 1, 0, NULL, 125, NULL, 48, true);
INSERT INTO tinyRouterIPs VALUES (true, '2a01:cb08:9150:4a2b:8647:6e47:456e:bfc2', '2a01:cb08:9150:4a00:c2d7:aaff:febf:34f1', NULL, NULL, 239, 1, 0, NULL, 197, NULL, 48, true);

-- TODO: wanna add a func that filter out v4 addresses
--       but that can also be done through views i guess

-- process ips into manageable state
UPDATE tinyRouterIPs SET SrcIP = exploded(SrcIP);

-- drop SLAAC addresses
DELETE FROM tinyRouterIPs WHERE is_slaac(SrcIP);

ALTER TABLE tinyRouterIPs ADD COLUMN HostID text;
ALTER TABLE tinyRouterIPs ADD COLUMN NetID text;

UPDATE tinyRouterIPs SET HostID = get_hid(SrcIP);
UPDATE tinyRouterIPs SET NetID = get_nid(SrcIP);

UPDATE tinyRouterIPs SET entropy = shannon_bin(HostID);