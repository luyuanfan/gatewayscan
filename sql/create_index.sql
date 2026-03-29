\echo 'adding index on all rows that is not slaac and has high entropy score'
CREATE INDEX IF NOT EXISTS filtered ON :tbl(is_slaac, entropy)
    WHERE entropy > 0.5 AND is_slaac = False;

\echo 'adding index on source ips'
CREATE INDEX IF NOT EXISTS srcipidx ON :tbl (srcip)
    WHERE entropy > 0.5 AND is_slaac = False;