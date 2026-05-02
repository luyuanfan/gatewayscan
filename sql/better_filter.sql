#### mapping each duplicated hostid prefix to asn and organziation ####
-- get a count of the total number of entries
select count(*) from dup_hostids;

-- check the percentage
select 
    substring(hostid, 1, 1) as first_char,
    count(*) as cnt,
    round(count(*) * 100.0 / 3268577207, 2) as pct
from dup_hostids
group by first_char
order by first_char;

-- count the number of repeated hostids
select count (distinct hostid) from better_filter;

-- get their entropy as well
create materialized view if not exists list_of_dup_ids as 
select hostid, max(entropy) as entropy
from better_filter
group by hostid;

-------------- PATTERNS BELOW
-- patterns
-- 00000000%
-- 00010000%
-- 00010001%
-- 00010002%
-- 00020001%
-- 00020002%
substring(hostid from 7 for 5) = 'ff0fe'
-- 002416ff0fe%
-- 861c70ff0fe%
-- 8627b6ff0fe%
-- 8687ffff0fe%
-- 7e6a60ff0fe%
-- c22d2eff0fe%
-- 867adfff0fe%
-- 8ee117ff0fe%
-- 8eeefdff0fe%
-- 9a9d39ff0fe%
-- 80000%
-- only contains numbers? yup those are out too
-- not gonna filter out these? not sure? 
-- also gonna filter out those who falls out of range

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
create index if not exists better_filter_mapped_asnum_idx on better_filter_mapped (as_number);
create index if not exists better_filter_mapped_orgname_idx on better_filter_mapped (oranization_name);

create table if not exists hostid_ratio as
select
  hostid, 
  NULLIF(bit_count(convert_to(hostid, 'UTF8'))::numeric / bit_length(convert_to(hostid, 'UTF8')), 0) as ratio
from better_filter_mapped;

create table if not exists qualifying_iids_two_stddev as
select
  hostid,
  ratio
from hostid_ratio
where ratio >= 0.375 and ratio <= 0.625;

create table if not exists qualifying_iids_one_stddev as
select
  hostid,
  ratio
from hostid_ratio
where ratio >= 0.4375 and ratio <= 0.5625;