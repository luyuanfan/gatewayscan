# Gateway routers scan analysis

Set up environment:
```bash
source venv/bin/activate
pip install -r requirements.txt
```

Start services:
```bash
docker compose up
```

Connect to PSQL from terminal:
```bash
psql -h localhost -p 6789 -U lyspfan
```

Connect to database GUI (pgadmin) from browser:
```bash
ssh ss -L 9876:localhost:9876
```

Set auto-fill password:
```bash
echo "localhost:6789:*:lyspfan:lyspfan" > ~/.pgpass
chmod 600 ~/.pgpass
```

DB:
- Database name is `lyspfan`
- Table names are `routerIPs`, `pfx2as`, `asfields`, `orgfields`

## Import functions and files

Import test files
```bash
python3 load.py testfile1.csv testfile2.csv ...
```
Decompress:
```bash
pbzip2 -dckf -p4 /mnt/usb/combined-48s-r1-s56.csv.bz2 > /dbdata/combined-48s-r1-s56.csv
pbzip2 -dckf -p4 /mnt/usb/combined-48s-r2-s60.csv.bz2 > /dbdata/combined-48s-r2-s60.csv
pbzip2 -dckf -p4 /mnt/usb/combined-48s-r3-s64.csv.bz2 > /dbdata/combined-48s-r3-s64.csv
```

Import all the files
```bash
python3 load.py --full
```

Import CAIDA's pfx2as dataset
- [Link to all datasets](https://publicdata.caida.org/datasets/routing/routeviews6-prefix2as/)
- Here we use `routeviews-rv6-20250730-0600.pfx2as`
```bash
./import/pfx2as.sh
```

Import CAIDA's as2org dataset
```bash
./import/as2org.sh
```