*Task 1*

In this task, you will implement the Locality Sensitive Hashing algorithm with Jaccard similarity using
yelp_train.csv.
In this task, we focus on the “0 or 1” ratings rather than the actual ratings/stars from the users.
Specifically, if a user has rated a business, the user’s contribution in the characteristic matrix is 1. If the
user hasn’t rated the business, the contribution is 0. You need to identify similar businesses whose
similarity >= 0.5.
You can define any collection of hash functions that you think would result in a consistent permutation
of the row entries of the characteristic matrix. Some potential hash functions are:
f(x)= (ax + b) % m or f(x) = ((ax + b) % p) % m
where p is any prime number and m is the number of bins. Please carefully design your hash functions.
After you have defined all the hashing functions, you will build the signature matrix. Then you will divide
the matrix into b bands with r rows each, where b x r = n (n is the number of hash functions). You should
carefully select a good combination of band rin your implementation (b>1 and r>1). Remember that
two items are a candidate pair if their signatures are identical in at least one band.
Your final results will be the candidate pairs whose original Jaccard similarity is >= 0.5. You need to write
the final results into a CSV file according to the output format below

*Task 2*

In task 2, you are going to build different types of recommendation systems using the yelp_train.csv to
predict the ratings/stars for given user ids and business ids. You can make any improvement to your
recommendation system in terms of speed and accuracy. You can use the validation dataset
(yelp_val.csv) to evaluate the accuracy of your recommendation systems, but please don’t include it as
your training data.
There are two options to evaluate your recommendation systems. You can compare your results to the
corresponding ground truth and compute the absolute differences. You can divide the absolute
differences into 5 levels and count the number for each level as following:
>=0 and <1: 12345
>=1 and <2: 123
>=2 and <3: 1234
>=3 and <4: 1234
>=4: 12
This means that there are 12345 predictions with < 1 difference from the ground truth. This way you will
be able to know the error distribution of your predictions and to improve the performance of your
recommendation systems.
Additionally, you can compute the RMSE (Root Mean Squared Error) by using following formula:
Where Predi is the prediction for business i and Ratei is the true rating for business i. n is the total
number of the business you are predicting.

In this task, you are required to implement:
Case 1: Item-based CF recommendation system with Pearson similarity (2 points)
Case 2: Model-based recommendation system (1 point)
Case 3: Hybrid recommendation system (2 point)

4.2.1. Item-based CF recommendation system
Please strictly follow the slides to implement an item-based recommendation system with Pearson
similarity.
Note: Since it is a CF-based recommendation system, there are some inherent limitations to this
approach like cold-start. You need to come up with a default rating mechanism for such cases. This
includes cases where the user or the business does not exist in the training dataset but is present in the
test dataset. This is a part of the assignment and you are supposed to come up with ways to handle such
issues on your own.

4.2.2. Model-based recommendation system
You need to use XGBregressor (a regressor based on Decision Tree) to train a model. You need to use this
API https://xgboost.readthedocs.io/en/latest/python/python_api.html, the XGBRegressor inside
the package xgboost.
Please use version 0.72 of xbgoost package on your local system to avoid any discrepancies you might
see between the results on your local system and Vocareum.
Please choose your own features from the provided extra datasets and you can think about it with
customer thinking. For example, the average stars rated by a user and the number of reviews most likely
influence the prediction result. You need to select other features and train a model based on that. Use
the validation dataset to validate your result and remember don’t include it into your training data.

4.2.3. Hybrid recommendation system.
Now that you have the results from previous models, you will need to choose a way from the slides to
combine them together and design a better hybrid recommendation system.
Here are two examples of hybrid systems:

Example 1:
You can combine them together as a weighted average, which means:

𝑓𝑖𝑛𝑎𝑙 𝑠𝑐𝑜𝑟𝑒=α×𝑠𝑐𝑜𝑟𝑒_𝑖𝑡𝑒𝑚_𝑏𝑎𝑠𝑒𝑑 +(1−α)×𝑠𝑐𝑜𝑟𝑒_𝑚𝑜𝑑𝑒𝑙_𝑏𝑎𝑠𝑒𝑑

The key idea is: the CF focuses on the neighbors of the item and the model-based RS focuses on the user
and items themselves. Specifically, if the item has a smaller number of neighbors, then the weight of the
CF should be smaller. Meanwhile, if two restaurants both are 4 stars and while the first one has 10
reviews, the second one has 1000 reviews, the average star of the second one is more trustworthy, so
the model-based RS score should weigh more. You may need to find other features to generate your own
weight function to combine them together.

Example 2:
You can combine them together as a classification problem:
Again, the key idea is: the CF focuses on the neighbors of the item and the model-based RS focuses on
the user and items themselves. As a result, in our dataset, some item-user pairs are more suitable for the
CF while the others are not. You need to choose some features to classify which model you should
choose for each item-user pair.
If you train a classifier, you are allowed to upload the pre-trained classifier model named “model.md” to
save running time on Vocareum. You can use pickle library, joblib library or others if you want. Here is an
example: https://scikit-learn.org/stable/modules/model_persistence.html.
You also need to upload the training script named “train.py” to let us verify your model.
Some possible features (other features may also work):
-Average stars of a user, average stars of business, the variance of history review of a user or a business.
-Number of reviews of a user or a business.
-Yelp account starting date, number of fans.
-The number of people who think a users’ review is useful/funny/cool. Number of compliments (Be
careful with these features. For example, sometimes when I visit a horrible restaurant, I will give full stars
because I hope I am not the only one who wasted money and time here. Sometimes people are satirical.)