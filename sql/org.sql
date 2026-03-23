CREATE TABLE IF NOT EXISTS orgFields (
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
