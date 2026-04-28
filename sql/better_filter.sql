#### mapping each duplicated hostid prefix to asn and organziation ####

create index if not exists dup_hostids_mapped_p1_idx on dup_hostids_to_org_p1 (hostid);
create index if not exists dup_hostids_mapped_p2_idx on dup_hostids_to_org_p2 (hostid);
create index if not exists dup_hostids_mapped_p3_idx on dup_hostids_to_org_p3 (hostid);

create materialized view if not exists better_filter_p1 as 
select *
from dup_hostids_to_org_p1
where hostid !~ '^(00000000|00010000|00010001|00010002|00020001|00020002|80000)'
  and hostid !~ '^[0-9]{16}$'
  and substring(hostid from 7 for 5) <> 'ff0fe'
  and hostid not like '0000%';

create materialized view if not exists better_filter_p2 as 
select *
from dup_hostids_to_org_p2
where hostid !~ '^(00000000|00010000|00010001|00010002|00020001|00020002|80000)'
  and hostid !~ '^[0-9]{16}$'
  and substring(hostid from 7 for 5) <> 'ff0fe'
  and hostid not like '0000%';

create materialized view if not exists better_filter_p3 as 
select *
from dup_hostids_to_org_p3
where hostid !~ '^(00000000|00010000|00010001|00010002|00020001|00020002|80000)'
  and hostid !~ '^[0-9]{16}$'
  and substring(hostid from 7 for 5) <> 'ff0fe'
  and hostid not like '0000%';

create materialized view if not exists better_filter_mapped as (
select * from better_filter_p1
union all
select * from better_filter_p2
union all
select * from better_filter_p3
);

create index if not exists better_filter_mapped_hostid_idx on better_filter_mapped (hostid);
create index if not exists better_filter_mapped_netid_idx on better_filter_mapped (netid);
create index if not exists better_filter_mapped_orgid_idx on better_filter_mapped (orgid);