import numpy as np 
from scipy.stats import entropy
from collections import Counter
from math import log2

'''
16 hex numbers
'''
def shannon_hex(hid):
    c = Counter(hid)
    score = - sum([(val / 16) * log2(val / 16) for key, val in c.items()])
    return score

'''
64 binary bits, each can take on the value of 0 or 1
'''
def shannon_bin(hid):
    binary = bin(int(hid, 16))[2:].zfill(64)
    c = Counter(binary)
    score = - sum([(val / 64) * log2(val / 64) for key, val in c.items()])
    return score

def main():
    hex_host_ids = [
        "0000000000000001", # least random looking
        "0000000000000002", # least random looking
        "0000000000000010", # least random looking
        "0000000000000020", # least random looking
        "0000000000002170", # slightly more
        "0193025300770135", # even more
        "cec7cb3dce4f938f", # made by PRNG
        "f9198bc53b127e76", # made by PRNG
        "0123456789abcdef", # manual string hitting all hex numbers
        "0468ac7db32e9f15", # same as above but in different order
    ]
    for hid in hex_host_ids: 
        print(f"{hid} --> {shannon_bin(hid)}")
        # print(f"{" "*len(hid)} --> {shannon_hex(hid)} hex")

if __name__ == "__main__":
    main()