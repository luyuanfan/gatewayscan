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

Connect to PSQL in terminal:
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

File locations:
- Import instruction is in `./import.md`
- `routerIPs` processing queries are in `./psql/process.sql`
- `pfx2as`, `asfields`, `orgfields` processing queries are in `./psql/pfx2as2org.sql`
- `./playground/` holds test files

## Import files

Import CSV files (take `medium.csv` as example)
```bash
./import/medium.sh
```

Import compressed CSV files
```bash
./import/full.sh
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

## Process database

TODO 