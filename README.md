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
./connect.sh
```

Connect to database GUI (pgadmin) from browser:
```bash
./pgadmin.sh
```

Set auto-fill password:
```bash
echo "localhost:6789:*:lyspfan:lyspfan" > ~/.pgpass
chmod 600 ~/.pgpass
```

DB:
- Database name is `lyspfan`
- Table names are `main`, `pfx2as`, `asfields`, `orgfields`

## Import files

Chunk:
```bash
split data/medium.csv --number=l/30 --additional-suffix=_56.csv -d data/chunks/chunk_
```

Decompress:
```bash
nohup bzip2 -dckf -p4 /mnt/usb/combined-48s-r1-s56.csv.bz2 > /dbdata/combined-48s-r1-s56.csv &
nohup bzip2 -dckf -p4 /mnt/usb/combined-48s-r2-s60.csv.bz2 > /dbdata/combined-48s-r2-s60.csv &
nohup bzip2 -dckf -p4 /mnt/usb/combined-48s-r3-s64.csv.bz2 > /dbdata/combined-48s-r3-s64.csv &
```

Preprocess raw files and put them in the database:
```bash
# you can load everything in /dbdata/ in one go: 
python3 load.py --full
# or you can pick any file to load, but you have to specify a prefix length, such as: 
python3 load.py /dbdata/file1.csv -p 56
# you can also overwrite the existing table by doing: 
python3 load.py /dbdata/file1.csv -p 56 --force
```

Import CAIDA's pfx2as dataset: 
- [Link to all datasets](https://publicdata.caida.org/datasets/routing/routeviews6-prefix2as/)
- Here we use `routeviews-rv6-20250730-0600.pfx2as`
```bash
./import/pfx2as.sh
```

Import CAIDA's as2org dataset: 
- Here we use `20250801.as-org2info.txt`
```bash
./import/as2org.sh
```