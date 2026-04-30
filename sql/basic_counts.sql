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

# just get the names of the US organizations
create materialized view if not exists us_orgs as
select distinct oranization_name
from better_filter_mapped
where country = 'US';

# for each of the US organizations out there, we want to see how many UUID they own are having collisions,
# and we want to order them by how many collisions there are
# HONESTLY this query does not give much useful information other than we have things ordered nicely
create materialized view if not exists us_orgs_ranked as
select 
    oranization_name,
    country, 
    array_agg(distinct hostid) as hostid_list,
    count (distinct hostid) as colliding_hostid_count,
    (sum(distinct_net_occurence) - count (*)) as collided_nets_count
from better_filter_mapped
group by oranization_name, country
having country = 'US';

select * from us_orgs_ranked order by colliding_hostid_count desc, collided_nets_count desc;

# doing the same type of counting but for the whole world instead
# TODO: seeing the results i'd say it needs more filtering for the zero paddings
create materialized view if not exists all_orgs_ranked as
select 
    oranization_name,
    array_agg(distinct hostid) as hostid_list,
    count (distinct hostid) as colliding_hostid_count,
    (sum(distinct_net_occurence) - count (*)) as collided_nets_count
from better_filter_mapped
group by oranization_name;

select * from all_orgs_ranked order by colliding_hostid_count desc, collided_nets_count desc;

# for each of the organizations, we can run the anderson test, which can help us get a better sense whether
# they were drawn from an uniform distribution

# after that or before that, we can do a 1-0 ratio test