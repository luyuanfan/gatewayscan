create index if not exists full_table_hostid_slaac_entropy_netid 
on full_table (hostid, netid)
where entropy > 0.5 and is_slaac = false;

create index if not exists full_table_hostid_idx on full_table (hostid);

-- create a table on just the host ids that are duplicated
create materialized view if not exists dup_hostids as
with qualifying_hostids as (
    select
        hostid,
        count(*) as occurrence_count,
        count(distinct netid) as netid_count
    from full_table
    where entropy > 0.5
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
