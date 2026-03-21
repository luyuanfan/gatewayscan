#!/bin/bash
set -e
DB="psql -h localhost -p 6789"
TBL="routerIPs"
echo "creating table $TBL"
$DB -v tbl=$TBL -f schemas/routerips.sql

# file 1
BZ1="combined-48s-r1-s56.csv.bz2" # set input file
CSV1="${BZ1%.bz2}" # set output file
lbzip2 -dkc -n $(nproc) /mnt/usb/$BZ1 > /tmp/$CSV1 # decompress input
$DB -c "\COPY $TBL (Protocol, TgtIP, SrcIP, HopLim, ICMPv6Type, ICMPv6Code, RTT) FROM STDIN WITH (FORMAT csv)"< <(grep '^icmp,' "/tmp/$CSV1") # load into DB
$DB -c "UPDATE $TBL SET PfxLen = 56 WHERE PfxLen IS NULL;" # set prefix length
# file 2
BZ2="combined-48s-r2-s60.csv.bz2"
CSV2="${BZ2%.bz2}"
lbzip2 -dkc -n $(nproc) /mnt/usb/$BZ2 > /tmp/$CSV2
$DB -c "\COPY $TBL (Protocol, TgtIP, SrcIP, HopLim, ICMPv6Type, ICMPv6Code, RTT) FROM STDIN WITH (FORMAT csv)"< <(grep '^icmp,' "/tmp/$CSV2")
$DB -c "UPDATE $TBL SET PfxLen = 60 WHERE PfxLen IS NULL;"
# file 3
BZ3="combined-48s-r3-output.csv.bz2" 
CSV3="${BZ3%.bz2}"
lbzip2 -dkc -n $(nproc) /mnt/usb/$BZ3 > /tmp/$CSV3
$DB -c "\COPY $TBL (Protocol, TgtIP, SrcIP, HopLim, ICMPv6Type, ICMPv6Code, RTT) FROM STDIN WITH (FORMAT csv)"< <(grep '^icmp,' "/tmp/$CSV3")
$DB -c "UPDATE $TBL SET PfxLen = 64 WHERE PfxLen IS NULL;"
# set soft delete column to false
$DB -c "UPDATE $TBL SET Deleted = false;"

$DB -v tbl=$TBL -f psql/process.sql