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

FORG="data/orgfields.txt"
TORG="orgFields"
TAS="asFields"
$DB -c "DROP TABLE IF EXISTS $TAS;"
$DB -c "DROP TABLE IF EXISTS $TORG;"
$DB -v tbl=$TORG -f schemas/org.sql
$DB -c "\COPY $TORG FROM $FORG WITH (DELIMITER '|', FORMAT text, NULL '')"
FAS="data/asfields.txt"
$DB -v tbl=$TAS -f schemas/as.sql
$DB -c "\COPY $TAS FROM $FAS WITH (DELIMITER '|', FORMAT text, NULL '')"