-- see where is 'ff0fe' host ids come from
create materialized view if not exists funny_ones as
select *
from dup_hostids
where substring(hostid from 7 for 5) = 'ff0fe'

create index if not exists funny_ones_netid_gist_idx on funny_ones using gist (netid inet_ops);

create index if not exists funny_ones_netid_idx on funny_ones (netid);

create materialized view if not exists funny_ones_mapped as
select
    f.hostid,
    f.entropy,
    f.netid,
    f.occurrence_count,
    f.netid_count as distinct_net_occurence,
    f.subnetpfx,
    p.prefix as caida_pfx,
    p.asn as as_number,
    p.autname as aut_name,
    p.orgname as oranization_name,
    p.orgid,
    p.country
from funny_ones f
left join pfx2as2org p on p.prefix >>= f.netid;
