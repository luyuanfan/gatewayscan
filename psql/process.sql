\timing on

\echo 'filtering out the repeated replies and only keep one instance'
UPDATE :tbl
SET Deleted = true
WHERE Deleted = false
    AND ctid NOT IN (
        SELECT MIN(ctid) FROM :tbl
        WHERE Deleted = false
        GROUP BY SrcIP
        HAVING COUNT(SrcIP) > 1
    );

DROP TABLE IF EXISTS clean_routerips;

\echo 'creating table clean_routerips with entries that survived and with entropy > 0.5'
CREATE TABLE clean_routerips AS
SELECT * FROM :tbl
    WHERE deleted = false AND entropy > 0.5;