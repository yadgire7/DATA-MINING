# import libraries
import sys
import os
from pyspark import SparkContext
import math
from itertools import combinations, islice
import timeit
import json
import xgboost
import pandas as pd
# reference: https://machinelearningmastery.com/xgboost-for-regression/

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

# functions used in the code


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


# def cal_rmse(true, pred):
#     a = zip(true, pred)
#     rmse = 0
#     for pair in a:
#         rmse += (pair[0] - pair[1]) ** 2
#     rmse = math.sqrt(rmse / len(true))
#     return rmse

# -------------------------------------------------------------------
start_time = timeit.default_timer()
# read data from folder
# get user_id, review_count, average_stars from user.json
# get business_id, latitude, longitude, stars, review_count from business.json
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

output = test[['user_id', 'business_id']]
output['stars'] = y_pred
output.to_csv(output_file, index=False, header=['user_id', 'business_id', 'prediction'])

end_time = timeit.default_timer()
print(f"Duration: {end_time - start_time}")
# print(f"RMSE: {cal_rmse(true_y, y_pred)}")