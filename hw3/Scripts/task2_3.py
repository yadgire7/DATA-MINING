# import libraries
import sys
import os
import random
from pyspark import SparkContext
import math
from itertools import combinations, islice
import timeit
import csv
import json
import xgboost
import pandas as pd

# -------------------------------------------------------------------

# system variables

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# -------------------------------------------------------------------

# create spark context

sc = SparkContext.getOrCreate()
sc.setLogLevel('WARN')

# -------------------------------------------------------------------

# command line arguments and global variables
folder_path = sys.argv[1]
train = pd.read_csv(folder_path + "yelp_train.csv")
test = pd.read_csv(sys.argv[2])
output_file = sys.argv[3]

# -------------------------------------------------------------------

# task2_1.py

# *******************************************************************

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
    if biz not in distinct_biz_dict.keys():  # cold start for business
        # add_new_biz(biz)
        if user not in distinct_users_dict.keys():
            return (user, biz, 3.0)
        return (user, biz, get_user_avg_rating(user))
    elif user not in distinct_users_dict.keys():
        return (user, biz, get_biz_avg_rating(biz))
    else:
        return (user, biz, "hello")


def write_to_file(final, output_file):
    final = final.collect()
    with open(output_file, mode="w", newline='') as op:
        op.write("user_id, business_id, prediction\n")
        writer = csv.writer(op)
        for row in final:
            writer.writerow(row)


# -------------------------------------------------------------------

# *******************************************************************

# task2_2.py

# *******************************************************************


def read_data_from_folder(folder_path, file_name):
    data = sc.textFile(
        folder_path + file_name).map(lambda row: json.loads(row))
    return data


def get_avg_stars(data):
    avg_stars = data.map(lambda x: float(x[1][1])).mean()
    return avg_stars


def get_avg_review_count(data):
    avg_review_count = data.map(lambda x: float(x[1][0])).mean()
    return avg_review_count


def get_price_range(a, k):
    if a:
        if k in a.keys():
            return int(a.get(k))
    return 0


def user_preprocessing(df, user_dict):
    user_review_count = []
    user_avg_stars = []
    for index, row in df.iterrows():
        user_id = row["user_id"]
        if user_id in user_dict.keys():
            user_review_count.append(user_dict[user_id][0])
            user_avg_stars.append(user_dict[user_id][1])
        else:
            user_review_count.append(u_avg_review_count)
            user_avg_stars.append(user_avg_stars)
    df["user_review_count"] = user_review_count
    df["user_avg_stars"] = user_avg_stars
    return df


def business_preprocessing(df, business_dict):
    biz_review_count = []
    biz_avg_stars = []
    biz_price_range = []
    for index, row in df.iterrows():
        business_id = row["business_id"]
        if business_id in business_dict.keys():
            biz_review_count.append(business_dict[business_id][0])
            biz_avg_stars.append(business_dict[business_id][1])
            biz_price_range.append(business_dict[business_id][2])
        else:
            biz_review_count.append(b_avg_review_count)
            biz_avg_stars.append(biz_avg_stars)
            biz_price_range.append(0)

    df["biz_review_count"] = biz_review_count
    df["biz_avg_stars"] = biz_avg_stars
    df["biz_price_range"] = biz_price_range
    return df


def cal_rmse(true, pred):
    a = zip(true, pred)
    rmse = 0
    for pair in a:
        rmse += (pair[0] - pair[1]) ** 2
    rmse = math.sqrt(rmse / len(true))
    return rmse

# *******************************************************************
start_time = timeit.default_timer()
# get predictions from task2_1.py

data = sc.textFile(folder_path + "yelp_train.csv")
data = data.mapPartitionsWithIndex(lambda index, iteartor: islice(iteartor, 1, None) if index == 0 else iteartor)\
    .map(lambda x: tuple(x.split(","))).map(lambda row: (row[0], row[1], float(row[2])))

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


val = sc.textFile(sys.argv[2])
val = val.mapPartitionsWithIndex(lambda index, iteartor: islice(iteartor, 1, None) if index == 0 else iteartor)\
    .map(lambda x: tuple(x.split(","))).map(lambda row: (row[0], row[1]))

val_user_biz = val.map(lambda x: (x[0], x[1]))

rdd1 = val_user_biz.map(handle_cold_start)
rdd1 = rdd1.filter(lambda x: x[2] != 'hello')
# print(rdd1.take(10))
rdd2 = val_user_biz.map(handle_cold_start)
rdd2 = rdd2.filter(lambda x: x[2] == 'hello')\
    .map(lambda x: (x[0], x[1]))

pred = rdd2.map(get_pairs)\
    .map(lambda query_pair_list: (query_pair_list[0], [common_users(pair) for pair in query_pair_list[1]]))\
    .map(lambda query_pairs_commons: (query_pairs_commons[0], sorted([(pair, pearson_correlation(pair, commons, user_rated_biz_ratings)) for pair, commons in query_pairs_commons[1]], key=lambda x: x[1], reverse=True))[0:3])\
    .map(lambda final: pred_rating(final, user_rated_biz_ratings))\
    .map(lambda x: (x[0][0], x[0][1], x[1]))

final = rdd1.union(pred)

task2_1 = pd.DataFrame(final.collect(), columns=["user_id", "business_id", "prediction1"])

# get predictions from task2_2.py

user = read_data_from_folder(folder_path, "user.json")
user_data = user.map(lambda u: (
    u["user_id"], (u["review_count"], u["average_stars"])))
business = read_data_from_folder(folder_path, "business.json")
business_data = business.map(lambda b: (
    b["business_id"], (b["review_count"], b["stars"], get_price_range(b["attributes"], "RestaurantsPriceRange2"))))


# -------------------------------------------------------------------

u_avg_stars = get_avg_stars(user_data)
b_avg_stars = get_avg_stars(business_data)
u_avg_review_count = get_avg_review_count(user_data)
b_avg_review_count = get_avg_review_count(business_data)

# -------------------------------------------------------------------

user_dict = user_data.collectAsMap()
business_dict = business_data.collectAsMap()

# -------------------------------------------------------------------

train = user_preprocessing(train, user_dict)
train = business_preprocessing(train, business_dict)
train_y = train["stars"]
train_x = train.drop(["stars", "user_id", "business_id"], axis=1)

# -------------------------------------------------------------------

test = user_preprocessing(test, user_dict)
test = business_preprocessing(test, business_dict)
true_y = test["stars"]

test_x = test.drop(["stars", "user_id", "business_id"], axis=1)

# -------------------------------------------------------------------

# fit model
mod = xgboost.XGBRegressor()
mod = mod.fit(train_x, train_y)

# -------------------------------------------------------------------

# predict
y_pred = mod.predict(test_x)

task2_2 = pd.DataFrame({"user_id": test["user_id"], "business_id": test["business_id"], "prediction2": y_pred})

result = pd.merge(task2_1, task2_2, on=["user_id", "business_id"], how="inner")


alpha = 0.1
result["prediction"] = (alpha * result["prediction1"]) + ((1 - alpha) * result["prediction2"])

result = result.drop(["prediction1", "prediction2"], axis=1)
result.to_csv(sys.argv[3], index=False)
end_time = timeit.default_timer()

print(f"Duration: {end_time - start_time}")



# python task2_3.py data/ yelp_val.csv output2_3.csv
