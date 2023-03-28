#  import libraries

import sys
import os
import math
import timeit
from itertools import combinations
from pyspark import SparkConf, SparkContext

# create spark context with necessary configuration


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


sc = SparkContext.getOrCreate()
sc.setLogLevel('WARN')

# global arguments
case_no = int(sys.argv[1])
support = int(sys.argv[2])
input_file = sys.argv[3]
output_file = sys.argv[4]

# read the input file
data = sc.textFile(input_file,7)
headers = data.first()

# # for case 1
# rdd1 = data.filter(lambda x: x != headers).map(lambda x: x.split(",")).map(
#     lambda x: (x[0], (x[1]))).groupByKey().mapValues(set).map(lambda x: (x[0], sorted(list(x[1]))))
# baskets = rdd1.values()

# chunks (case 1)


def get_chunks(key):
    return int(key) % 10

# function to get candidate itemsets


def get_candidates(frequent_items):
    candidate_itemsets = set()
    for x in frequent_items:
        for y in frequent_items:
            if len(x.union(y)) == len(x) + 1:
                c = x.union(y)
                candidate_itemsets.add(c)
    pruned_candiate_itemsets = candidate_itemsets.copy()
    for c in candidate_itemsets:
        sub = list(combinations(c, len(c)-1))
        if all(frozenset(c) not in frequent_items for c in sub):
            pruned_candiate_itemsets.remove(c)
    return pruned_candiate_itemsets


# function for a priori algorithm
def apriori(all_chunks, support):
    # pass 1: get frequent singletons

    frequent_itemsets = []
    candidate_itemsets = []
    frequent_singletons = []
    support_threshold = math.ceil(support / len(all_chunks))
    for i, chunk in enumerate(all_chunks):
        # print(chunk)
        if chunk is not None:
            baskets_chunk = sc.parallelize(chunk).values()
            singles = baskets_chunk.flatMap(lambda x: x).map(lambda i: (i, 1))\
                .reduceByKey(lambda a, b: a+b).filter(lambda k: k[1] >= int(support_threshold))\
                .keys().sortBy(lambda x: x).collect()
            frequent_singletons.append(singles)
            # print(singles)
# The contents of frequent_singletons is each chunk's elements who have passed the scaled_down threshold
    candidates = sorted(
        list(set([item for sublist in frequent_singletons for item in sublist])))
    # print(candidates)

    candidate_items = {frozenset([i]) for i in candidates}
    candidate_itemsets.append({1: candidate_items})
    # print(candidate_items)
    candidate_items_count = baskets.flatMap(lambda i: [(items, 1) for items in candidate_items if set(items)
                                                       .issubset(set(i))])\
        .reduceByKey(lambda a, b: a+b).filter(lambda x: x[1] >= support)\
        .keys().sortBy(lambda x: x).collect()
    # print(candidate_items_count)
    
    frequent_items = {i for i in candidate_items_count}
    # print(frequent_items)
    frequent_itemsets.append({1: frequent_items})
    size = 2
    while frequent_items:
        candidate_items = get_candidates(frequent_items=frequent_items)
        candidate_itemsets.append({size: candidate_items})
        # print(f"candidate_items {candidate_items}\n")
        candidate_items_count = baskets.flatMap(lambda i: [(items, 1) for items in candidate_items if set(items).issubset(
            set(i))]).reduceByKey(lambda a, b: a+b).filter(lambda x: x[1] >= support).collect()
        # print(f"Candidate_item_count {candidate_items_count}")
        if len(candidate_items_count) == 0:
            # print(size)
            break
        # print(f"frequent_item_counts {candidate_items_count}")
        frequent_items = [items for (items, count) in candidate_items_count]
        # print(f"frequent_items {frequent_items}\n")
        frequent_itemsets.append({size: frequent_items})
        size += 1
    return candidate_itemsets, frequent_itemsets

# case 1 results
# c, f = apriori(all_chunks=all_chunks, support= support)

#  get tuples of candidate itemsets except the frozenset keyword

def get_final_candidates(c):
    final_candidate = []
    for i in c:
        for k, v in i.items():
            # print(f"{sorted([tuple(sorted(item)) for item in v])} \n")
            final_candidate.append(sorted([tuple(sorted(item)) for item in v]))
    return final_candidate

# final candidates and frequent itemsets
# final_candidates = get_final_candidates(c)
# final_frequent_itemsets = get_final_candidates(f)

# function to format output and write to file
def write_to_file(final_candidates, final_frequent_itemsets):
    with open(output_file, "w") as f:
        f.write("Candidates:\n")
        for sublist in final_candidates:
            for i, tup in enumerate(sublist):
                if len(tup) == 1:
                    f.write("('" + str(tup[0]) + "')")
                else:
                    f.write(str(tup))
                if i != len(sublist)-1:
                    f.write(",")
            f.write("\n")
        f.write("Frequent Itemsets:\n")
        for sublist in final_frequent_itemsets:
            for i, tup in enumerate(sublist):
                if len(tup) == 1:
                    f.write("('" + str(tup[0]) + "')")
                else:
                    f.write(str(tup))
                if i != len(sublist)-1:
                    f.write(",")
            f.write("\n")



# main
if case_no == 1:
    start = timeit.default_timer()
    rdd = data.filter(lambda x: x != headers).map(lambda x: x.split(",")).map(
        lambda x: (x[0], (x[1]))).groupByKey().mapValues(set).map(lambda x: (x[0], sorted(list(x[1]))))
    baskets = rdd.values()
    chunks = rdd.partitionBy(10, get_chunks)
    non_empty_chunks = chunks.filter(lambda x: len(x[1]) > 0)
    all_chunks = non_empty_chunks.glom().collect()
    c, f = apriori(all_chunks=all_chunks, support= support)
    final_candidates = get_final_candidates(c)
    final_frequent_itemsets = get_final_candidates(f)
    write_to_file(final_candidates, final_frequent_itemsets)
    end = timeit.default_timer()
    time = end - start
    print(f"Duration: {time}")
elif case_no == 2:
    start = timeit.default_timer()
    rdd = data.filter(lambda x: x != headers).map(lambda x: x.split(",")).map(
        lambda x: (x[1], (x[0]))).groupByKey().mapValues(set).map(lambda x: (x[0], sorted(list(x[1]))))
    baskets = rdd.values()
    chunks = rdd.partitionBy(10, get_chunks)
    all_chunks = chunks.glom().collect()
    c, f = apriori(all_chunks=all_chunks, support= support)
    final_candidates = get_final_candidates(c)
    final_frequent_itemsets = get_final_candidates(f)
    write_to_file(final_candidates, final_frequent_itemsets)
    end = timeit.default_timer()
    time = end - start
    print(f"Duration: {time}")

