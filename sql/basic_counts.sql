# get a list of repeated hostids
create materialized view if not exists list_of_dup_ids as 
select distinct hostid
from better_filter_mapped;

# get a count of the repeated hostids
select count (hostid) from better_filter_mapped;

# get a count of across how many organizations
select count (distinct orgid) from better_filter_mapped;

# get a count of unique ASes
select count (distinct as_number) from better_filter_mapped;

# get a sense of what countries they come from
create materialized view if not exists countries_we_see as
select distinct country
from better_filter_mapped;

# organize it such that each hostids is followed by a little list of organization info
create materialized view if not exists dups_grouped as
select
    hostid,
    entropy,
    distinct_net_occurence,
    jsonb_agg(
        json_build_object(
            'net', netid,
            'organizations', oranization_name,
            'ASes', as_number,
            'countries', country
        )
    ) as info,
    count (distinct country) as distinct_countries,
    array_agg(country) as countries,
    count (distinct oranization_name) as distinct_orgs,
    array_agg(oranization_name) as orgs,
    count (distinct as_number) as distinct_ases,
    array_agg(as_number) as ASes
from better_filter_mapped
group by hostid, entropy, distinct_net_occurence;

# use it with the client-side cmd below
select * from dups_grouped order by distinct_net_occurence desc;
# psql -h localhost -p 6789 -c "\copy (select * from dups_grouped order by entropy desc, distinct_net_occurence desc) to /home/lyspfan/gatewayscan/data/grouped_ordered.csv

# we wanna do another grouping thing but this time we only want the group with US organizations in them
create materialized view if not exists us_grouped as 
select * from dups_grouped where 'US'=ANY(countries);

select * from us_grouped order by distinct_net_occurence desc, entropy desc;
# psql -h localhost -p 6789 -c "\copy (select * from us_grouped order by distinct_net_occurence desc, entropy desc) to '/home/lyspfan/gatewayscan/data/us_grouped_ordered.csv' with csv header"