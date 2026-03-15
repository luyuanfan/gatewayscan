#!/bin/bash
DB="psql -h localhost -p 6789"
FNAME="data/20250801.as-org2info.txt"
FAS="data/asfields.txt"
FORG="data/orgfields.txt"
LINE=$(grep -n "format:aut" $FNAME | cut -d ":" -f 1)
head -n $((LINE-1)) $FNAME > $FORG
sed -i '/^#/d' $FORG
tail -n +$LINE $FNAME > $FAS
sed -i '/^#/d' $FAS

FORG="/home/lyspfan/ipv6scan/data/orgfields.txt"
TORG="orgFields"
$DB -c "CREATE TABLE $TORG (
    orgId          text 
                   PRIMARY KEY,    -- unique ID for the given organization
    dateChanged    date,           -- the changed date provided by WHOIS
    orgName        text,           -- name could be selected from the AUT entry tied to the
                                   --   organization, the AUT entry with the largest customer cone,
                                   --   listed for the organization (if there existed an stand alone
                                   --   organization), or a human maintained file.
    country        text,           -- some WHOIS provide as a individual field. In other cases
                                   -- we inferred it from the addresses
    dataSource     text            -- the RIR or NIR database which was contained this entry
);"
$DB -c "\COPY $TORG FROM $FORG WITH (DELIMITER '|', FORMAT text, NULL '')"
FAS="/home/lyspfan/ipv6scan/data/asfields.txt"
TAS="asFields"
$DB -c "CREATE TABLE $TAS (
    aut            text,             -- AS number
    dateChanged    date,             -- the changed date provided by WHOIS
    autName        text,             -- the name provide for the AS number
    orgId          text 
                   REFERENCES orgFields(orgId), -- maps to and organization entry
    opaqueId       text,             -- opaque identifier used by RIR extended delegation format
    dataSource     text              -- the RIR or NIR database which was contained this entry
);"
$DB -c "\COPY $TAS FROM $FAS WITH (DELIMITER '|', FORMAT text, NULL '')"