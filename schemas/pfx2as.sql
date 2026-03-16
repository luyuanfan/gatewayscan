DROP TABLE IF EXISTS pfx2as;
CREATE TABLE pfx2as (
    Prefix        cidr,
    PrefixLen     smallint,
    ASN           text
);