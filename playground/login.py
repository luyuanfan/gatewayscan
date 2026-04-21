import requests
import itertools
from collection import defaultdict

def main():
    url = 'https://192.168.1.1'
    digits = '0123456789'
    passwds = itertools.combinations_with_replacement(digits, 10)
    # digalps = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    # passwds = itertools.combinations_with_replacement(digalps, 10)
    for pwd in passwds:
        if len(set(pwd)) <= 4:
            continue
        pwd = ''.join(pwd)
        creds = { 'admin': pwd }
        x = requests.post(url, json = creds, verify=False)

if __name__ == "__main__":
    main()



