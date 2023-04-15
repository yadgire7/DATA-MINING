import sys
import timeit
import binascii
import random
from blackbox import BlackBox


# global variables
p = 9973
m = 69997
num_hash_func = 5
hash_functions = [(random.randint(1, p), random.randint(0, p))
                  for i in range(num_hash_func)]
filter_bit_array = [0]*p
prev_users = set()
# print(len(filter_bit_array))

# command line arguments
input_file = sys.argv[1]
stream_size = int(sys.argv[2])
num_of_asks = int(sys.argv[3])
output_file = sys.argv[4]

# function for hashing
def hashs(s):
    s = int(binascii.hexlify(s.encode('utf8')), 16)
    result = []
    for hf in hash_functions:
        result.append(((hf[0]*s + hf[1]) % p) % m)
    return result

# function for bloom filter


def bloom_filter(stream):
    fp = 0
    tn = 0
    for s in stream:
        ones = 0
        res = hashs(s)
        for hash_value in res:
            if filter_bit_array[hash_value] == 0:
                filter_bit_array[hash_value] = 1
            else:
                ones = ones + 1

        if s not in prev_users:
            if ones != len(res):
                tn += 1
            else:
                fp += 1
        prev_users.add(s)

    if tn == 0 and fp == 0:
        fpr = 0.0
    else:
        fpr = fp / (fp + tn)

    return fpr


if __name__ == '__main__':
    start = timeit.default_timer()
    bb = BlackBox()
    op = open(output_file, 'w')
    op.write("Time,FPR\n")
    for i in range(num_of_asks):
        stream = bb.ask(input_file, stream_size)
        fpr = bloom_filter(stream)
        op.write(f"{i},{fpr}\n")
    end = timeit.default_timer()
    print(f"Duration: {end- start}")
# python Scripts/test1.py Data/users.txt 100 30 output1.csv
