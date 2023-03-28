# importing required libraries
import os
import sys
import json
from pyspark import SparkConf, SparkContext

# assigning path variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# creating a dictionary for output file
output = {"n_review": -1, "n_review_2018": -1, "n_user": 0,
          "top10_user": [], "n_business": -1, "top10_business": []}

# create a SparkContext() instrance
sc = SparkContext.getOrCreate()
sc.setLogLevel('WARN')

# create an RDD for reading the .json file
rdd_reviews = sc.textFile(sys.argv[1]).map(
    lambda line: json.loads(line))

# task1.A : total number of reviews
output["n_review"] = rdd_reviews.count()

# task1.b : number of reviews in 2018
output["n_review_2018"] = rdd_reviews.filter(
    lambda obj: obj["date"].split("-")[0] == '2018').count()

# task1.c : number of distinct reviews
# users= rdd_reviews.map(lambda r: r['user_id'])
output["n_user"] = rdd_reviews.map(lambda r: r['user_id']).distinct().count()

# task1.d : top 10 users who wrote largest number of reviews
output["top10_user"] = rdd_reviews.map(lambda r: [r['user_id'], 1]).reduceByKey(
    lambda a, b: a+b).sortBy(lambda u: [-u[1], u[0]]).take(10)
# sorted_users_map = users_map.map(lambda u: list(u))
#  users_map

# task1.e : number of distinct businesses
# businesses = rdd_reviews.map(lambda obj: obj["business_id"])
output["n_business"] = rdd_reviews.map(
    lambda obj: obj["business_id"]).distinct().count()

# task1.f : top 10 business that had largest number of reviews
output["top10_business"] = rdd_reviews.map(lambda r: [r['business_id'], 1]).reduceByKey(
    lambda a, b: a+b).sortBy(lambda u: [-u[1], u[0]]).take(10)

# writing the results to the output file
with open(sys.argv[2], "w") as op_file:
    json.dump(output, op_file)
