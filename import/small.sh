#!/bin/bash

CSV="data/small.csv"
TBL="smallRouterIPs"
DB="psql -h localhost -p 6789"
echo "creating table $TBL"
$DB -v tbl=$TBL -f psql/routerips-schema.sql
echo "copying file to table"
$DB -c "\COPY $TBL (Protocol, TgtIP, SrcIP, HopLim, ICMPv6Type, ICMPv6Code, RTT) FROM STDIN WITH (FORMAT csv)"< <(grep '^icmp,' "$CSV")
echo "adding subnet prefix length to table"
$DB -c "UPDATE $TBL SET PfxLen = 56;"
$DB -v tbl=$TBL -f psql/process.sql