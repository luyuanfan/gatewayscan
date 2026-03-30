\echo 'adding index on all rows that is not slaac and has high entropy score'
CREATE INDEX IF NOT EXISTS filtered ON :tbl(is_slaac, entropy)
    WHERE entropy > 0.5 AND is_slaac = False;

\echo 'adding index on all host ids'
CREATE INDEX IF NOT EXISTS hostid_idx ON :tbl (hostid);