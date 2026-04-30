#!/bin/bash

dbcommand="psql -h localhost -p 6789"
cmd="
create materialized view if not exists us_orgs as
select distinct oranization_name
from better_filter_mapped
where country = 'US';
"
echo "$cmd"

$dbcommand <<EOF
$cmd
EOF