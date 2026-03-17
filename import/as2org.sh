#!/bin/bash
set -e
db="psql -h localhost -p 6789"
location='https://data.caida.org/datasets/as-organizations/20250801.as-org2info.txt.gz'
fpath=data/${location##*/}
txt=${fpath%%.gz*}
wget -P data/ $location
gunzip -f $fpath
line=$(grep -n "format:aut" $txt | cut -d ":" -f 1)
fas="data/asfields.txt"
forg="data/orgfields.txt"
rm -f $fas
rm -f $forg
head -n $((line-1)) $txt > $forg
tail -n +$line $txt > $fas

$db -c "DROP TABLE IF EXISTS asFields;" >/dev/null
$db -c "DROP TABLE IF EXISTS orgFields;" >/dev/null
$db -f schemas/org.sql
$db -c "\COPY orgFields FROM STDIN WITH (DELIMITER '|', FORMAT text, NULL '')"< <(grep -v '^#' "$forg")
$db -f schemas/as.sql
$db -c "\COPY asFields FROM STDIN WITH (DELIMITER '|', FORMAT text, NULL '')"< <(grep -v '^#' "$fas")
$db -c "DROP TABLE IF EXISTS as2org;" >/dev/null
$db -c "CREATE TABLE as2org
    AS (
        SELECT asf.aut, asf.autname, orgf.orgname, asf.orgid, orgf.country
        FROM asfields AS asf
        JOIN orgfields AS orgf
        ON asf.orgId = orgf.orgid
    );"