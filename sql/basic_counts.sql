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
            'nets', net_,
            'organizations', oranization_name,
            'ASes', as_number,
            'countries', country
        )
    ) as info
from better_filter_mapped
group by hostid, entropy, distinct_net_occurence;

select * from dups_grouped order by distinct_net_occurence;