import sys
import timeit
import random
from blackbox import BlackBox


# command line arguments
input_file = sys.argv[1]
stream_size = int(sys.argv[2])
num_of_asks = int(sys.argv[3])
output_file = sys.argv[4]


# function to implement reservoir sampling
def reservoir_sampling(stream, n):
    for s in stream:
        n+=1
        if len(sample) < stream_size:
            sample.append(s)
        else:
            if random.random() < stream_size / n:
                idx = random.randint(0,stream_size - 1)
                sample[idx] = s     
    return [n,sample[0],sample[20],sample[40],sample[60],sample[80]]


if __name__ == '__main__':
    start = timeit.default_timer()
    bb = BlackBox()
    random.seed(553)
    sample = []
    n = 0
    op = open(output_file, 'w')
    op.write("seqnum,0_id,20_id,40_id,60_id,80_id\n")
    for i in range(num_of_asks):
        stream = bb.ask(input_file, stream_size)
        res = reservoir_sampling(stream, n)
        n = res[0]
        op.write(f"{res[0]},{res[1]},{res[2]},{res[3]},{res[4]},{res[5]}\n")
    end = timeit.default_timer()
    print(f"Duration: {end- start}")
# python Scripts/test3.py Data/users.txt 100 30 output3.csv


