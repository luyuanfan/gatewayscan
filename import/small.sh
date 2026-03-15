#!/bin/bash

CSV="../data/small.csv"
TBL="smallRouterIPs"
DB="psql -h localhost -p 6789"
$DB -c "\COPY $TBL (Protocol, TgtIP, SrcIP, HopLim, ICMPv6Type, ICMPv6Code, RTT) FROM STDIN WITH (FORMAT csv)"< <(grep '^icmp,' "$CSV")
$DB -c "UPDATE $TBL SET PfxLen = 56;"
$DB -f ../psql/routerips.sql