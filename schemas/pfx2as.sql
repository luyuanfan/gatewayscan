CREATE TABLE pfx2as (
    Prefix        cidr,
    PrefixLen     smallint,
    ASN           text
);

UPDATE pfx2as
    SET Prefix = set_masklen(Prefix, PrefixLen)::cidr;
