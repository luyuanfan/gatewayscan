create index if not exists full_table_hostid_slaac_entropy_netid 
on full_table (hostid, netid)
where entropy > 0.5 and is_slaac = false;

create index if not exists full_table_hostid_idx on full_table (hostid);

# create a table on just the host ids that are duplicated
create materialized view if not exists dup_hostids as
with qualifying_hostids as (
    select
        hostid,
        count(*) as occurrence_count,
        count(distinct netid) as netid_count
    from full_table
    where entropy > 0.6 
            and is_slaac = false
            and not hostid like '%000000%'
    group by hostid
    having count(distinct netid) > 1
)
select
    f.hostid,
    f.entropy,
    q.occurrence_count,
    f.subnetpfx,
    f.netid,
    q.netid_count,
    f.tgtip,
    f.srcip,
    f.hoplim,
    f.icmpv6type,
    f.icmpv6code,
    f.rtt
from full_table f
inner join qualifying_hostids q on f.hostid = q.hostid;


create index if not exists dup_hostids_hostid_idx on dup_hostids (hostid);
create index if not exists dup_hostids_netid_idx on dup_hostids (netid);
create index if not exists dup_hostids_netid_gist_idx ON dup_hostids USING gist (netid inet_ops);
create index if not exists dup_hostids_subnetpfx_idx on dup_hostids (subnetpfx);

create materialized view if not exists better_filter as 
select *
from dup_hostids
where hostid !~ '^(00000000|00010000|00010001|00010002|00020001|00020002|80000)'
  and hostid !~ '^[0-9]{16}$'
  and substring(hostid from 7 for 5) <> 'ff0fe'
  and hostid not like '%000000%';

create index if not exists better_filter_hostid_idx on better_filter (hostid);
create index if not exists better_filter_netid_idx on better_filter (netid);
create index if not exists better_filter_netid_gist_idx ON better_filter USING gist (netid inet_ops);
create index if not exists better_filter_subnetpfx_idx on better_filter (subnetpfx);
