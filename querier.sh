#!/bin/bash

dbcommand="psql -h localhost -p 6789"

$dbcommand <<EOF
CREATE MATERIALIZED VIEW IF NOT EXISTS duplicated_hostids_to_asn_org AS
SELECT
    d.hostid,
    d.entropy,
    d.occurrence_count,
    d.subnetpfx,
    p.prefix AS caida_pfx,
    p.prefixlen AS caida_pfx_len,
    p.asn AS as_number,
    p.autname AS aut_name,
    p.orgname AS oranization_name,
    p.orgid,
    p.country,
    d.netid,
    d.netid_count,
    d.tgtip,
    d.srcip,
    d.hoplim,
    d.icmpv6type,
    d.icmpv6code,
    d.rtt
FROM duplicate_hostids d
LEFT JOIN pfx2as2org p
ON p.prefix >>= d.subnetpfx;
EOF