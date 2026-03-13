CREATE TABLE pfx2as (
    Prefix        inet,
    PrefixLen     smallint,
    ASN           text
);

ALTER TABLE pfx2as ALTER COLUMN prefix TYPE text USING CAST ( prefix AS text );
UPDATE pfx2as SET prefix = (regexp_replace(prefix, '/.*', ''));
UPDATE pfx2as SET prefix = exploded(prefix);
UPDATE pfx2as SET prefix = left(prefix, 16);