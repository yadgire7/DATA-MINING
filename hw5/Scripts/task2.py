import sys
import timeit
import binascii
import random
from blackbox import BlackBox
from statistics import mean, median

# global variables
p = 9973
m = 69997
num_hash_func = 15
hash_functions = [(random.randint(1, p), random.randint(0, p))
                  for i in range(num_hash_func)]
# a, b = random.randint(1, p), random.randint(0, p)

# command line arguments
input_file = sys.argv[1]
stream_size = int(sys.argv[2])
num_of_asks = int(sys.argv[3])
output_file = sys.argv[4]

# function for hashing
def hashs(s):
    result = []
    s = int(binascii.hexlify(s.encode('utf8')), 16)
    for hf in hash_functions:
        result.append(bin(((hf[0]*s + hf[1]) % p) % m)[2:])
    return result

def combine_estimations(estimated):
    means = []
    for i in range(0,num_hash_func, 3):
        group = estimated[i:i+3]
        means.append(mean(group))
    return int(median(means))



def flajolet_martin(stream):
    gt = len(set(stream))
    estimated = []
    res = []
    for s in stream:
        res.append(hashs(s))
    for i in range(num_hash_func):
        max_trailing_zeros = -5
        for r in res:
            trailing_zeros = len(str(r[i])) - len(str(r[i]).rstrip('0'))
            if trailing_zeros > max_trailing_zeros:
                max_trailing_zeros = trailing_zeros
        estimated.append(int(2**max_trailing_zeros))
    est = combine_estimations(estimated)
    return gt, est

if __name__ == '__main__':
    start = timeit.default_timer()
    # gts = []
    # ests = []
    bb = BlackBox()
    op = open(output_file, 'w')
    op.write("Time,Ground Truth,Estimation\n")
    for i in range(num_of_asks):
        stream = bb.ask(input_file,stream_size)
        gt, est = flajolet_martin(stream)
        gts.append(gt)
        ests.append(est)
        op.write(f"{i},{gt},{est}\n")
    end = timeit.default_timer()
    # print(sum(ests)/sum(gts))
    print(f"Duration: {end- start}")
# python Scripts/test2.py Data/users.txt 300 30 output1.csv
