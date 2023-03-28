# importing required libraries
import os
import sys
import json
from pyspark import SparkConf, SparkContext
import timeit

# assigning path variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# creating a dictionary for output file
output = {"default": {"n_partition": 2, "n_items": [1111, 2222], "exe_time":0},
"customized": {"n_partition": 2, "n_items": [3332, 1], "exe_time":0} }

# creating  SparkContext instance
sc = SparkContext.getOrCreate()

# create an RDD for reading the .json file
rdd_reviews = sc.textFile(sys.argv[1]).map(
    lambda line: json.loads(line))

# creating an rdd for task 1.f
businesses = rdd_reviews.map(lambda obj: obj["business_id"])

# get default number of partitions and the number of entries per partition
businesses_map = businesses.map(
    lambda b: (b, 1))
output["default"]["n_partition"] = businesses_map.getNumPartitions()
output["default"]["n_items"] = businesses_map.glom().map(len).collect()

# get time elapsed
default_start_time = timeit.default_timer()
sorted_businesses_map = businesses_map.sortBy(lambda b: (-b[1], b[0]))
sorted_businesses_map = sorted_businesses_map.map(lambda b: list(b))
sorted_businesses_map.take(10)
default_end_time = timeit.default_timer()
time_for_default = default_end_time - default_start_time
output["default"]["exe_time"] = time_for_default

# get custom number of partitions and partition size
custom_partition_businesses = businesses_map.partitionBy(int(sys.argv[3]), hash)
output["customized"]["n_partition"] = custom_partition_businesses.getNumPartitions()
output["customized"]["n_items"] = custom_partition_businesses.glom().map(lambda partition: len(partition)).collect()
custom_business_map = custom_partition_businesses.map(lambda b: (b, 1)).reduceByKey(lambda a, b: a+b)

# calculate time elapsed
custom_start_time = timeit.default_timer()
sorted_custom_business_map = custom_business_map.sortBy(
    lambda x: (-x[1], x[0]))
sorted_custom_business_map = sorted_custom_business_map.map(
    lambda b: list(b))
sorted_custom_business_map.take(10)
custom_end_time = timeit.default_timer()
total_custom_time = custom_end_time - custom_start_time

output["customized"]["exe_time"] = total_custom_time

# writing the results to the output file
with open(sys.argv[2] , "w") as op_file:
    json.dump(output,op_file)