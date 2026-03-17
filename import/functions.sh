#!/bin/bash
set -e
db="psql -h localhost -p 6789"
$db -f psql/functions.sql