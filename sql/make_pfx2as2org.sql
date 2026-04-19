# map table pfx2as to table as2org (left join)
create table if not exists pfx2as2org as (
select
    pfx2as.prefix,
    pfx2as.prefixlen,
    pfx2as.asn,
    as2org.autname,
    as2org.orgname,
    as2org.orgid,
    as2org.country
from pfx2as
left join as2org on pfx2as.asn = as2org.aut
);

create index pfx2as2org_prefix_gist_idx ON pfx2as2org USING gist (prefix inet_ops);