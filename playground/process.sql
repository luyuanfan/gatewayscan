\timing on

\echo 'filtering out the repeated replies and only keep one instance'
DELETE FROM :tbl
WHERE ctid IN (
    SELECT ctid FROM (
        SELECT ctid,
               ROW_NUMBER() OVER (PARTITION BY SrcIP ORDER BY ctid) AS rn
        FROM :tbl
    ) sub
    WHERE rn > 1
);
