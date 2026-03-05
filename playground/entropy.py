import numpy as np 
from scipy.stats import entropy

'''
64 bits, each can take on the value 1 or 0
'''
def shannon(id):
    id_lst = np.array()
    score = 0
    return score

def main():
    host_ids = [
        "0000:0000:0000:0001", # least random looking
        "0000:0000:0000:2170", # slightly more
        "0193:0253:0077:0135", # slightly more
        "cec7:cb3d:ce4f:938f", # made by PRNG
        "f919:8bc5:3b12:7e76", # made by PRNG
    ]



if __name__ == "__main__":
    main()