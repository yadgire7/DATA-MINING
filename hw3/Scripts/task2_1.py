# import libraries
import sys
import os
import random
from pyspark import SparkContext
import math
from itertools import combinations, islice
import timeit
import csv

# -------------------------------------------------------------------

# system variables

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# -------------------------------------------------------------------

# spark variables

sc = SparkContext.getOrCreate()
sc.setLogLevel('WARN')
k = 3

# -------------------------------------------------------------------

# command line arguments

train = sys.argv[1]
test = sys.argv[2]
output_file = sys.argv[3]

# -------------------------------------------------------------------

# functions used in the code


def get_biz_avg_rating(biz):
    return biz_avg_rating_dict[biz]


def get_user_avg_rating(user):
    return user_avg_rating_dict[user]


def get_pairs(query):
    biz_rated_by_user = user_rated_biz_dict[query[0]]
    return (query, [(query[1], biz) for biz in biz_rated_by_user])


def common_users(pair):
    return (pair, biz_rated_users_dict[pair[0]].intersection(biz_rated_users_dict[pair[1]]))


def pearson_correlation(pair, commons, user_rated_biz_ratings):
    weights_list = []
    ans = 0.0
    if len(commons) > 1:
        avg1 = sum(user_rated_biz_ratings[user][pair[0]]
                   for user in commons)/len(commons)
        avg2 = sum(user_rated_biz_ratings[user][pair[1]]
                   for user in commons)/len(commons)
        numerator = sum((user_rated_biz_ratings[user][pair[0]] - avg1) * (
            user_rated_biz_ratings[user][pair[1]] - avg2) for user in commons)
        denominator = math.sqrt(sum((user_rated_biz_ratings[user][pair[0]] - avg1)**2 for user in commons)) * math.sqrt(
            sum((user_rated_biz_ratings[user][pair[1]] - avg2)**2 for user in commons))
        if denominator > 0 and numerator > 0:
            ans = numerator/denominator
    else:
        numerator = get_biz_avg_rating(pair[0])
        denominator = get_biz_avg_rating(pair[1])
        ans = numerator / denominator
        if ans > 1:
            ans = 1 / ans
    return ans


def pred_rating(entry, user_rated_biz_ratings):
    user = entry[0][0]
    numerator = 0
    denominator = 0
    for pair, pc in entry[1]:
        numerator += user_rated_biz_ratings[user][pair[1]] * pc
        denominator += pc
    pred = numerator / denominator
    return (entry[0], pred)


def handle_cold_start(query):
    user = query[0]
    biz = query[1]
    if biz not in distinct_biz_dict.keys():   #cold start for business
        # add_new_biz(biz)
        if user not in distinct_users_dict.keys():
            return (user, biz, 3.0)
        return (user,biz,get_user_avg_rating(user))
    elif user not in distinct_users_dict.keys():
        return (user, biz, get_biz_avg_rating(biz))
    else:
        return (user,biz,"hello")
    

def write_to_file(final, output_file):
    final = final.collect()
    with open(output_file, mode="w", newline='') as op:
        op.write("user_id, business_id, prediction\n")
        writer = csv.writer(op)
        for row in final:
            writer.writerow(row)


# def cal_rmse(final):
#     true = val.map(lambda x: ((x[0], x[1]), x[2]))
#     test = final.map(lambda x: ((x[0], x[1]), x[2]))
#     mse = true.join(test).map(lambda x: (x[1][0], x[1][1]))\
#         .map(lambda x: (x[0] - x[1])**2).mean()
#     rmse = math.sqrt(mse)
#     return rmse

# -------------------------------------------------------------------
    

# --------------------------------------------------------------------------

# creating required rdds
start = timeit.default_timer()
data = sc.textFile(train)
data = data.mapPartitionsWithIndex(lambda index, iteartor: islice(iteartor, 1, None) if index == 0 else iteartor)\
    .map(lambda x: tuple(x.split(","))).map(lambda row: (row[0],row[1],float(row[2])))

#  get all users and all businesses (distinct)
distinct_users_dict = data.map(lambda x: x[0]).distinct().zipWithIndex()\
    .map(lambda x: (x[0], x[1])).collectAsMap()


distinct_biz_dict = data.map(lambda x: x[1]).distinct().zipWithIndex()\
    .map(lambda x: (x[0], x[1])).collectAsMap()


biz_avg_rating_dict = data.map(lambda row: (row[1], row[2])).groupByKey().mapValues(list)\
    .map(lambda b: (b[0], float(sum(b[1]) / len(b[1])))).collectAsMap()

user_avg_rating_dict = data.map(lambda row: (row[0], row[2])).groupByKey().mapValues(list)\
    .map(lambda b: (b[0], float(sum(b[1]) / len(b[1])))).collectAsMap()

user_rated_biz_ratings = data.map(lambda x: (x[0], (x[1], x[2])))\
    .groupByKey().mapValues(dict).collectAsMap()

biz_rated_users_ratings = data.map(lambda x: (x[1], (x[0], x[2])))\
    .groupByKey().mapValues(dict).collectAsMap()

biz_rated_users_dict = data.map(lambda x: (
    x[1], x[0])).groupByKey().mapValues(set).collectAsMap()


user_rated_biz_dict = data.map(lambda x: (
    x[0], x[1])).groupByKey().mapValues(set).collectAsMap()


val = sc.textFile(test)
val = val.mapPartitionsWithIndex(lambda index, iteartor: islice(iteartor, 1, None) if index == 0 else iteartor)\
    .map(lambda x: tuple(x.split(","))).map(lambda row: (row[0],row[1]))

val_user_biz = val.map(lambda x: (x[0],x[1]))

rdd1 = val_user_biz.map(handle_cold_start)
rdd1 = rdd1.filter(lambda x: x[2] != 'hello')
# print(rdd1.take(10))
rdd2 = val_user_biz.map(handle_cold_start)
rdd2 = rdd2.filter(lambda x: x[2] == 'hello')\
    .map(lambda x: (x[0], x[1]))

pred = rdd2.map(get_pairs)\
.map(lambda query_pair_list: (query_pair_list[0], [common_users(pair) for pair in query_pair_list[1]]))\
    .map(lambda query_pairs_commons: (query_pairs_commons[0], sorted([(pair, pearson_correlation(pair, commons, user_rated_biz_ratings)) for pair, commons in query_pairs_commons[1]],key= lambda x: x[1], reverse=True))[0:3])\
    .map(lambda final: pred_rating(final, user_rated_biz_ratings))\
.map(lambda x: (x[0][0],x[0][1],x[1]))

final = rdd1.union(pred)

write_to_file(final, "output2_1.csv")

end = timeit.default_timer()

print(f"Duration: {end - start}")
# rmse = cal_rmse(final)
# print(f"RMSE: {rmse}")
# python task2_1.py yelp_train.csv yelp_val.csv output2_1.csv
