CREATE TABLE RouterIPs (
    Protocol         text,                  -- protocol type: ICMP or TCP
    TgtIP            text,                  -- ICMP probe target IP
    SrcIP            text,                  -- IP of the replier 
    PfxLen           smallint,              -- subnet prefix length
    SubnetPfx        cidr,                  -- subnet prefix 
    Entropy          real,                  -- entropy score
    HostID           text,                  -- host id
    IDBuffer         text,                  -- network id
    HopLim           smallint,              -- Hop Limit
    ICMPv6Type       smallint,              -- 8 bits
    ICMPv6Code       smallint,              -- 8 bits
    RTT              integer,               -- round trip time (in millieseconds)
    Deleted          boolean DEFAULT false  -- flag if a row is soft deleted
);

CREATE TABLE orgFields (
    orgId          text 
                   PRIMARY KEY,         -- unique ID for the given organization
    dateChanged    date,                -- the changed date provided by WHOIS
    orgName        text,                -- name could be selected from the AUT entry tied to the
                                        --   organization, the AUT entry with the largest customer cone,
                                        --   listed for the organization (if there existed an stand alone
                                        --   organization), or a human maintained file.
    country        text,                -- some WHOIS provide as a individual field. In other cases
                                        -- we inferred it from the addresses
    dataSource     text                 -- the RIR or NIR database which was contained this entry
);

CREATE TABLE asFields (
    aut            text,             -- AS number
    dateChanged    date,             -- the changed date provided by WHOIS
    autName        text,             -- the name provide for the AS number
    orgId          text 
                   REFERENCES orgFields(orgId), -- maps to and organization entry
    opaqueId       text,             -- opaque identifier used by RIR extended delegation format
    dataSource     text              -- the RIR or NIR database which was contained this entry
);

CREATE TABLE pfx2as (
    Prefix        cidr,
    PrefixLen     smallint,
    ASN           text
);

CREATE TABLE as2org
    AS (
        SELECT asf.aut, asf.autname, orgf.orgname, asf.orgid, orgf.country
        FROM asfields AS asf
        JOIN orgfields AS orgf
        ON asf.orgId = orgf.orgid
    );