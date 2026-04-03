#!/bin/bash

dbcommand="psql -h localhost -p 6789"

$dbcommand <<EOF
CREATE MATERIALIZED VIEW IF NOT EXISTS duplicate_hostids AS
    SELECT
        hostid,
        entropy,
        COUNT(*) OVER (PARTITION BY hostid) AS occurrence_count,
        subnetpfx,
        netid,
        COUNT(netid) OVER (PARTITION BY hostid) AS netid_count,
        tgtip,
        srcip,
        hoplim,
        icmpv6type,
        icmpv6code,
        rtt
    FROM full_table
    WHERE hostid IN (
        SELECT hostid
        FROM full_table
        WHERE entropy > 0.5 AND is_slaac = False
        GROUP BY hostid
        HAVING COUNT(netid) > 1
    ); 
EOF