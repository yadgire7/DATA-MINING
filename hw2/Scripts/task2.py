import sys
import os
import csv
import math
import timeit
from itertools import combinations
from collections import defaultdict
from pyspark import SparkConf, SparkContext

# environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# global variables
filter_threshold = int(sys.argv[1])
support = int(sys.argv[2])
input_file = sys.argv[3]
output_file = sys.argv[4]
# -------------------------------------------------------------------------
# data preprocessing for the task
temp_arr = []
with open(sys.argv[3], newline='') as f:
    reader = csv.reader(f, delimiter=',', quotechar='"')
    next(reader)
    for row in reader:
        date = row[0].split("/")
        date = date[0]+"/"+date[1]+"/"+date[2][2:]
        id = str(int(row[1]))
        col1 = date+"-"+id
        col2 = int(row[5])
        temp_arr.append([col1, col2])
# print(temp_arr[:5])
with open("refined_data.csv", "w", newline='') as o:
    writer = csv.writer(o)
    writer.writerow(['DATE-CUSTOMER_ID', 'PRODUCT_ID'])
    for r in temp_arr:
        writer.writerow(r)
# ---------------------------------------------------------------------------------

# functions from task1
# ---------------------------------------------------------------------------------


def getchunks(key):
    return hash(key)

# -------------------------------------------------------------------------------

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


def apriori(all_chunks, support):
    # pass 1: get frequent singletons
    frequent_itemsets = []
    candidate_itemsets = []
    frequent_singletons = []
    support_threshold = math.ceil(support / 10)
    for partition in all_chunks:
        if len(partition) != 0:
            singles = sc.parallelize(partition).map(lambda x: x[1]).flatMap(lambda x: x)\
                .map(lambda x: (x, 1)).reduceByKey(lambda a, b: a+b)\
                .filter(lambda x: x[1] > support_threshold)\
                .keys().sortBy(lambda x: x).collect()
            frequent_singletons.append(singles)
# --------------------------------------------------------------------------------------------------------
# The contents of frequent_singletons is each chunk's elements who have passed the scaled_down threshold
    candidates = sorted(
        list(set([item for sublist in frequent_singletons for item in sublist])))
    candidate_items = {frozenset([i]) for i in candidates}
    candidate_itemsets.append({1: candidate_items})
    candidate_items_count = baskets.flatMap(lambda i: [(items, 1) for items in candidate_items if set(items)
                                                       .issubset(set(i))])\
        .reduceByKey(lambda a, b: a+b).filter(lambda x: x[1] >= support)\
        .keys().collect()
    frequent_items = {i for i in candidate_items_count}
    frequent_itemsets.append({1: frequent_items})
    # --------------------------------------------------------------------------------------------------------
    size = 2
    while frequent_items:
        candidate_items = get_candidates(frequent_items=frequent_items)
        candidate_itemsets.append({size: candidate_items})
        candidate_items_count = baskets.flatMap(lambda i: [(items, 1) for items in candidate_items if set(items).issubset(
            set(i))]).reduceByKey(lambda a, b: a+b).filter(lambda x: x[1] >= support).collect()
        if len(candidate_items_count) == 0:
            break
        frequent_items = [items for (items, count) in candidate_items_count]
        frequent_itemsets.append({size: frequent_items})
        size += 1
    return candidate_itemsets, frequent_itemsets
# ------------------------------------------------------------------------------------------------------


def get_final_candidates(c):
    final_candidate = []
    for i in c:
        for k, v in i.items():
            # print(f"{sorted([tuple(sorted(item)) for item in v])} \n")
            final_candidate.append(sorted([tuple(sorted(item)) for item in v]))
    return final_candidate

# ---------------------------------------------------------------------------------------------------------

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
            f.write("\n\n")
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


# ---------------------------------------------------------------------------------


# create an rdd for the task
sc = SparkContext().getOrCreate()
sc.setLogLevel('WARN')
start = timeit.default_timer()
data = sc.textFile("refined_data.csv",7)
h = data.first()
rdd = data.filter(lambda row: row != h)\
    .map(lambda row: row.split(","))\
    .map(lambda x: (x[0], str(x[1]))).groupByKey()\
    .mapValues(set).map(lambda x: (x[0], sorted(list(x[1]))))
rdd = rdd.filter(lambda x: len(x[1]) > filter_threshold)
baskets = rdd.values()
# ----------------------------------------------------------
rdd1 = rdd.partitionBy(10, getchunks)
non_empty_chunks = rdd1.filter(lambda x: len(x[1]) > 0)
all_chunks = non_empty_chunks.glom().collect()
# ----------------------------------------------------------
c, f = apriori(all_chunks=all_chunks, support=support)
final_candidates = get_final_candidates(c)
final_frequent_itemsets = get_final_candidates(f)
write_to_file(final_candidates, final_frequent_itemsets)
end = timeit.default_timer()
time = end - start
print(f"Duration: {time}")
