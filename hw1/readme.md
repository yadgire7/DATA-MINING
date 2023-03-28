Dataset: https://www.yelp.com/dataset

*Task 1*
You will work on test_review.json, which contains the review information from users, and write a
program to automatically answer the following questions:
A. The total number of reviews (0.5 point)
B. The number of reviews in 2018 (0.5 point)
C. The number of distinct users who wrote reviews (0.5 point)
D. The top 10 users who wrote the largest numbers of reviews and the number of reviews they wrote
(0.5 point)
E. The number of distinct businesses that have been reviewed (0.5 point)
F. The top 10 businesses that had the largest numbers of reviews and the number of reviews they had
(0.5 point)

*Task 2*
Since processing large volumes of data requires performance optimizations, properly partitioning the
data for processing is imperative.
In this task, you will show the number of partitions for the RDD used for Task 1 Question F and the
number of items per partition.
Then you need to use a customized partition function to improve the performance of map and reduce
tasks. A time duration (for executing Task 1 Question F) comparison between the default partition and
the customized partition (RDD built using the partition function) should also be shown in your results.

Hint:
Certain operations within Spark trigger an event known as the shuffle. The shuffle is Spark’s mechanism
for redistributing data so that it’s grouped differently across partitions. This typically involves copying
data across executors and machines, making the shuffle a complex and costly operation. So, trying to
design a partition function to avoid the shuffle will improve the performance a lot.

*Task 3*
In task3, you are asked to explore two datasets together containing review information
(test_review.json) and business information (business.json) and write a program to answer the following
questions:

A. What are the average stars for each city? (1 point)
1. (DO NOT use the stars information in the business file).
2. (DO NOT discard records with empty “city” field prior to aggregation - this just means that you
should not worry about performing any error handling, input data cleanup or handling edge case
scenarios).
3. (DO NOT perform any round off for the average stars).

B. You are required to compare the execution time of using two methods to print top 10 cities with
highest average stars. Please note that this task – (Task 3(B)) is not graded. You will get full points only if
you implement the logic to generate the output file required for this task.
1. To evaluate the execution time, start tracking the execution time from the point you load the file.
For M1: execution time = loading time + time to create and collect averages, sort using Python
and print the first 10 cities.
For M2: execution time = loading time + time to create and collect averages, sort using Spark
and print the first 10 cities.
The loading time will stay the same for both methods, the idea is to compare the overall
execution time for both methods and understand which method is more efficient for an
end-to-end solution.
Please note that for Method 1, only sorting is to be done in Python. Creating and collecting
averages needs to be done via RDD.
You should store the execution time in the json file with the tags “m1” and “m2”.
2. Additionally, add a “reason” field and provide a hard-coded explanation for the observed
execution times.
3. Do not round off the execution times.
