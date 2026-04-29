#!/bin/bash

dbcommand="psql -h localhost -p 6789"
cmd="
create materialized view if not exists funny_grouped as
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
from funny_ones_mapped
group by hostid, entropy, distinct_net_occurence;
"
echo "$cmd"

$dbcommand <<EOF
$cmd
EOF