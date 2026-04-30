#!/bin/bash

dbcommand="psql -h localhost -p 6789"
cmd="
create materialized view udelaware as
select * from us_grouped 
where 'University of Delaware'=ANY(orgs);
"

echo "$cmd"

$dbcommand <<EOF
$cmd
EOF