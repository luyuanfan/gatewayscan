Connect to PSQL:
```bash
psql -h localhost -p 6789 -U lyspfan
```

**Remove comments on top of the files before import**

## Import CSV files
Take `small.csv` as example:
```bash
CSV="data/small.csv"
sed -i -e 1,4d /tmp/$CSV
psql -h localhost -p 6789 -c "\COPY smallRouterIPs (Protocol, TgtIP, SrcIP, HopLim, ICMPv6Type, ICMPv6Code, RTT) FROM STDIN WITH (FORMAT csv)"< <(grep '^icmp,' "$CSV")
psql -h localhost -p 6789 -c "\COPY smallRouterIPs (Protocol, TgtIP, SrcIP, HopLim, Flags, RTT) FROM STDIN WITH (FORMAT csv)"< <(grep '^tcp,' "$CSV")
```

## Import compressed CSV files
Take `/mnt/usb/combined-48s-r1-s56.csv.bz2` as example:
```bash
BZ="/mnt/usb/combined-48s-r1-s56.csv.bz2"
CSV="${BZ%.bz2}"
lbzip2 -dk -n $(nproc) $BZ > /tmp/$CSV # decompress file
sed -i -e 1,4d /tmp/$CSV # strip first four lines (comments)
psql -c "\COPY routerIPs (Protocol, TgtIP, SrcIP, HopLim, ICMPv6Type, ICMPv6Code, RTT) FROM STDIN WITH (FORMAT csv)"< <(grep '^icmp,' "$CSV")
psql -c "\COPY routerIPs (Protocol, TgtIP, SrcIP, HopLim, Flags, RTT) FROM STDIN WITH (FORMAT csv)"< <(grep '^tcp,' "$CSV")
```

## Import CAIDA's pfx2as dataset
```bash
wget https://publicdata.caida.org/datasets/routing/routeviews6-prefix2as/2025/07/routeviews-rv6-20250730-0600.pfx2as.gz
gunzip routeviews-rv6-20250730-0600.pfx2as.gz
psql -h localhost -p 6789 -c "\COPY pfx2as (Prefix, PrefixLen, ASN) FROM routeviews-rv6-20250730-0600.pfx2as"
```