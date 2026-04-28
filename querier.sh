#!/bin/bash

dbcommand="psql -h localhost -p 6789"
cmd="select count (distinct as_number) from better_filter_mapped;"
echo $cmd

$dbcommand <<EOF
$cmd
EOF