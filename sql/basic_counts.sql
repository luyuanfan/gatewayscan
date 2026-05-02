# from full_table, let's get a sense of k
alter table full_table
add column random_looking boolean
default false;

alter table full_table
add column ratio float
default 0;

update full_table
set random_looking = true
where
    entropy > 0.5
    and is_slaac = false
    and hostid !~ '^(00000000|00010000|00010001|00010002|00020001|00020002|80000)'
    and hostid !~ '^[0-9]{16}$'
    and substring(hostid from 7 for 5) <> 'ff0fe'
    and hostid not like '%0000%';

create index if not exists full_table_random_idx
on full_table(random_looking)
where random_looking = true;

update full_table
set ratio = NULLIF(bit_count(convert_to(hostid, 'UTF8'))::numeric / bit_length(convert_to(hostid, 'UTF8')), 0)
where random_looking = true;


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
# they were drawn from an uniform distribution. we don't even want to get the distinct hostid from them.
# take them as is and do a test to at least filter out some orgs that are in disguise?
# BUT THAT DOESN'T MAKE SENSE.

# TODO: as long as we cannot get a good filter we cannot try to do a global type of summary

# after that or before that, we can do a 1-0 ratio test

# it's also possible that the same AS uses serveral different address assignment methods 
# MAYBE we can draw a tree that visualize this but idk how yet

# at least for each of the organizations that we are suspecting we can try to run a k-s test on them

# for each of the ASes we can also try to see if they use slaac, if they do, we'd be able to see the 
# manufracturer identifiers from the mac address, hopefully