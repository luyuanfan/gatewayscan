ipv6 scan analysis. 

Set up environment:
```bash
source venv/bin/activate
pip install -r requirements.txt
```

DB:
- Database name is `lyspfan`
- Table names are `routerIPs`, `pfx2as`, `asfields`, `orgfields`

File locations:
- Import instruction is in `./import.md`
- `routerIPs` processing queries are in `./psql/process.sql`
- `pfx2as`, `asfields`, `orgfields` processing queries are in `./psql/pfx2as2org.sql`
- `./playground/` holds test files