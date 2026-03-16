CREATE TABLE :tbl (
    aut            text,             -- AS number
    dateChanged    date,             -- the changed date provided by WHOIS
    autName        text,             -- the name provide for the AS number
    orgId          text 
                   REFERENCES orgFields(orgId), -- maps to and organization entry
    opaqueId       text,             -- opaque identifier used by RIR extended delegation format
    dataSource     text              -- the RIR or NIR database which was contained this entry
);