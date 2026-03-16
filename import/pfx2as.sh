#!/bin/bash

DB="psql -h localhost -p 6789"
TBL="pfx2as"
wget https://publicdata.caida.org/datasets/routing/routeviews6-prefix2as/2025/07/routeviews-rv6-20250730-0600.pfx2as.gz
gunzip routeviews-rv6-20250730-0600.pfx2as.gz
CSV="data/routeviews-rv6-20250730-0600.pfx2as"
$DB -v tbl=$TBL -f schemas/pfx2as.sql
$DB -c "\COPY $TBL (Prefix, PrefixLen, ASN) FROM $CSV"