#!/bin/bash
DB="psql -h localhost -p 6789"
FNAME="data/20250801.as-org2info.txt"
FAS="data/asfields.txt"
FORG="data/orgfields.txt"
LINE=$(grep -n "format:aut" $FNAME | cut -d ":" -f 1)
head -n $((LINE-1)) $FNAME > $FORG
sed -i '/^#/d' $FORG
tail -n +$LINE $FNAME > $FAS
sed -i '/^#/d' $FAS

FORG="/home/lyspfan/ipv6scan/data/orgfields.txt"
TORG="orgFields"
$DB -c "\COPY $TORG FROM $FORG WITH (DELIMITER '|', FORMAT text, NULL '')"
FAS="/home/lyspfan/ipv6scan/data/asfields.txt"
TAS="asFields"
$DB -c "\COPY $TAS FROM $FAS WITH (DELIMITER '|', FORMAT text, NULL '')"