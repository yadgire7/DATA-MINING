# importing required libraries
import os
import sys
import json
import timeit
from pyspark import SparkConf, SparkContext

# assigning path variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# create a SparkContext() instrance
sc = SparkContext.getOrCreate()
sc.setLogLevel('WARN')

# create an RDD for reading the .json file
rdd_reviews = sc.textFile(sys.argv[1]).map(lambda line: json.loads(line))
rdd_business = sc.textFile(sys.argv[2]).map(lambda line: json.loads(line))

# task execution
rdd_b = rdd_business.map(lambda b: (b["business_id"], b["city"]))
rdd_r = rdd_reviews.map(lambda r: (r["business_id"], r["stars"]))
joined_rdd = rdd_b.join(rdd_r)
city_reviews = joined_rdd.map(lambda c: (c[1][0], (c[1][1], 1))).reduceByKey(
    lambda a, b: (a[0]+b[0], a[1]+b[1]))
average_ratings = city_reviews.map(
    lambda x: (x[0], float(x[1][0]/x[1][1])))
sorted_ratings = average_ratings.sortBy(lambda b: (-b[1],b[0]))

# writing the results to a text file
with open(sys.argv[3], "w") as f:
    f.write("city,stars\n")
    for line in sorted_ratings.collect():
        f.write(",".join(map(str,line)) + "\n")

# task B

# part a
start_time_python = timeit.default_timer()
rdd_b = rdd_business.map(lambda b: (b["business_id"], b["city"]))
rdd_r = rdd_reviews.map(lambda r: (r["business_id"], r["stars"]))
joined_rdd = rdd_b.join(rdd_r)
city_reviews = joined_rdd.map(lambda c: (c[1][0], (c[1][1], 1))).reduceByKey(
    lambda a, b: (a[0]+b[0], a[1]+b[1]))
average_ratings = city_reviews.map(
    lambda x: (x[0], float(x[1][0]/x[1][1])))
sorted_top10_python = sorted(
    average_ratings.collect(), key=lambda x: (-x[1], x[0]))
top10_python = sorted_top10_python[:10]
end_time_python = timeit.default_timer()
total_time_python = end_time_python - start_time_python


# part b
start_time_pyspark = timeit.default_timer()
rdd_b = rdd_business.map(lambda b: (b["business_id"], b["city"]))
rdd_r = rdd_reviews.map(lambda r: (r["business_id"], r["stars"]))
joined_rdd = rdd_b.join(rdd_r)
city_reviews = joined_rdd.map(lambda c: (c[1][0], (c[1][1], 1))).reduceByKey(
    lambda a, b: (a[0]+b[0], a[1]+b[1]))
average_ratings = city_reviews.map(
    lambda x: (x[0], float(x[1][0]/x[1][1])))
sorted_ratings = average_ratings.sortBy(lambda b: (-b[1], b[0])).take(10)
end_time_pyspark = timeit.default_timer()
total_time_pyspark = end_time_pyspark - start_time_pyspark


# creating a dictionary for task B
output = {"m1": 0, "m2": 0, "reason": ""}

output["m1"] = total_time_python
output["m2"] = total_time_pyspark
output["reason"] = "for smaller RDDs, sorting using python may or may be faster as it needs less memory and computation power. But, in case of large RDDs, sorting using Spark would be faster as the number of compute nodes increases."

with open(sys.argv[4] , "w") as op_file:
    json.dump(output,op_file)