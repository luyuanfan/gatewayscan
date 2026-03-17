#!/bin/bash
set -e
db="psql -h localhost -p 6789"
location='https://publicdata.caida.org/datasets/routing/routeviews6-prefix2as/2025/07/routeviews-rv6-20250730-0600.pfx2as.gz'
fname=data/${location##*/}
csv=${fname%%.gz*}
wget -P data/ $location
gunzip -f $fname
$db -f schemas/pfx2as.sql
$db -c "\COPY pfx2as (Prefix, PrefixLen, ASN) FROM $csv"
$db -c "UPDATE pfx2as SET Prefix = set_masklen(Prefix, PrefixLen)::cidr;"