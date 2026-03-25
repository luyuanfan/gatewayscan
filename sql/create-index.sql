-- CREATE INDEX IF NOT EXISTS :filteridx ON :tbl (is_v6, is_slaac, is_aliased);
CREATE INDEX IF NOT EXISTS :srcipidx ON :tbl (srcip);