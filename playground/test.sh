#!/bin/bash
set -e
CSV="data/medium.csv"
TBL="test"
DB="psql -h localhost -p 6789"
echo "creating table $TBL"
$DB -v tbl=$TBL -f sql/routerips.sql
echo "copying file to database"
$DB -c "\COPY $TBL (Protocol, TgtIP, SrcIP, HopLim, ICMPv6Type, ICMPv6Code, RTT) FROM STDIN WITH (FORMAT csv)"< <(grep '^icmp,' "$CSV")
echo "adding subnet prefix length to database"
$DB -c "UPDATE $TBL SET PfxLen = 56;"
$DB -v tbl=$TBL -f psql/process.sql
