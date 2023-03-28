# import libraries
import sys
import os
import random
from pyspark import SparkContext
import math
from itertools import islice, combinations
import timeit
import csv

# command line arguments
input_file = sys.argv[1]
output_file = sys.argv[2]


# system arguments
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# start spark context
sc = SparkContext.getOrCreate()
sc.setLogLevel('WARN')

# -----------------------------------------------------------------------------

# global variables
num_hash_func = 50
rows = 2
bands = int(num_hash_func / rows)
p = 9991
n = 9973
threshold = 0.5
hash_functions = [(random.randint(1, p), random.randint(0, p))
                  for i in range(num_hash_func)]

# -----------------------------------------------------------------------------


def minHashing(index):
    signature = []
    for hf in hash_functions:
        minimum = math.inf
        for ind in index:
            value = ((hf[0] * ind + hf[1]) % p)%n
            if value < minimum:
                minimum = value
        signature.append(minimum)
    return signature

# -----------------------------------------------------------------------------

# lsh function
def lsh_buckets(sig_matrix):
    # get bands with index b and group the bands with key as (b, band) i.e. (0, [1,2,3,4,5]) and value as business_id
    # so now if we have two businesses with same band no. and same band then they will be grouped together
    candidate_pairs = sig_matrix.flatMap(get_bands)\
        .groupByKey()\
        .map(lambda x: list(x[1]))\
        .filter(lambda x: len(x) > 1).flatMap(make_pairs)\
        .sortBy(lambda x: x)\
        .distinct()
    return candidate_pairs


# -----------------------------------------------------------------------------

# function to get all bands of every single buziness
def get_bands(signature):
    all_bands = []
    for b in range(bands):
        all_bands.append(
            ((b, hash(tuple(signature[1][b * rows:(b + 1) * rows]))), signature[0]))
    return all_bands

# -----------------------------------------------------------------------------

def make_pairs(candidates):
    return list(combinations(candidates, 2))

# definition for Jaccard similarity
def jaccard_similarity(cp):
    set1 = set(biz_rated_users_dict[cp[0]])
    set2 = set(biz_rated_users_dict[cp[1]])
    sim = len(set1.intersection(set2)) / len(set1.union(set2))
    return sim

# -----------------------------------------------------------------------------

# start time
start_time = timeit.default_timer()
# -----------------------------------------------------------------------------
# creating rdd
data = sc.textFile(input_file)
# header = data.first()
# data = data.filter(lambda x: x != header)

data = data.mapPartitionsWithIndex(lambda index, iteartor: islice(iteartor, 1, None) if index == 0 else iteartor)\
    .map(lambda x: x.split(","))\
    .map(lambda x: (x[0], x[1]))

# -------------------------------------------------------------------------------

# creating dictionary of business_id and index

distinct_users = data.map(lambda x: x[0]).distinct()
distinct_users_dict = distinct_users.zipWithIndex().map(
    lambda x: (x[0], x[1])).collectAsMap()

biz_rated_users = data.map(lambda x: (
    x[1], distinct_users_dict[x[0]])).groupByKey().mapValues(lambda x: list(set(x)))
biz_rated_users_dict = biz_rated_users.collectAsMap()

# -----------------------------------------------------------------------------

sig_mat = biz_rated_users.map(lambda x: (x[0], minHashing(x[1])))

# -----------------------------------------------------------------------------

cp = lsh_buckets(sig_mat)

# -----------------------------------------------------------------------------

similar_biz = cp.map(lambda x: sorted(x)).map(lambda x: (x[0], x[1], jaccard_similarity(x)))\
    .filter(lambda x: x[2]>=threshold)

# -----------------------------------------------------------------------------

# write results to a csv file
similar_biz = similar_biz.collect()
with open(output_file, mode = "w", newline='') as op:
    op.write("business_id_1, business_id_2, similarity\n")
    writer = csv.writer(op)
    for row in sorted(similar_biz):
        writer.writerow(row)

# end time
end_time = timeit.default_timer()

print(f"Duration: {end_time-start_time}")

# python task1.py yelp_train.csv output.csv
