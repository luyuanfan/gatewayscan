import time
import argparse
import subprocess

dbcommand="psql -h localhost -p 6789"

def main():

    # initialize parser
    parser = argparse.ArgumentParser(
        prog="index.py",
        description="Add indexes to the table specified"
    )
    parser.add_argument('tableame', help='Must specify the table name on which you want to add indexes')
    args = parser.parse_args()
    tablename = args.tableame

    # add indexes
    print(f"Started adding indexes")
    start_index_t = time.time()
    subprocess.run(f'{dbcommand} -v tbl={tablename} -f sql/create_index.sql', shell=True, check=True)
    end_index_t = time.time()
    print(f"Done adding indexes in {end_index_t-start_index_t:.2f}s")

if __name__ == "__main__":
    main()