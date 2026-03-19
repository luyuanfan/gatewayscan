import argparse

def main():
    parser = argparse.ArgumentParser(
        prog="load.py",
        description="Usage: python3 load.py <filename1> --p=<prefixlen>\
                         or python3 load.py --full" 
    )
    parser.add_argument('filename')
    parser.add_argument('-p', '--pfxlen', required=False, type=int)
    parser.add_argument('-f', '--full', required=False, action='store_true')
    parser.add_argument('--force', 
                        required=False,
                        action='store_true',
                        help='gonna clean out the target table and rewrite')
    
    args =  parser.parse_args()
    print(args.full, args.pfxlen, args.filename, args.force)

if __name__ == "__main__":
    main()