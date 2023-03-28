Dataset: Provided in the data folder and Ta-Feng dataset from Kaggle

*Task 1*

There are two CSV files (small1.csv and small2.csv) in Vocareum under ‘/resource/asnlib/publicdata’. The
small1.csv is just a test file that you can use to debug your code. For task1, we will only test your code
on small2.csv.
In this task, you need to build two kinds of market-basket models.

Case 1 (1.5 pts):
You will calculate the combinations of frequent businesses (as singletons, pairs, triples, etc.) that are
qualified as frequent given a support threshold. You need to create a basket for each user containing the
business ids reviewed by this user. If a business was reviewed more than once by a reviewer, we consider
this product was rated only once. More specifically, the business ids within each basket are unique. The
generated baskets are similar to:
user1: [business11, business12, business13, ...]
user2: [business21, business22, business23, ...]
user3: [business31, business32, business33, ...]

Case 2 (1.5 pts):
You will calculate the combinations of frequent users (as singletons, pairs, triples, etc.) that are qualified as frequent given a support threshold. You need to create a basket for each business containing the user ids
that commented on this business. Similar to case 1, the user ids within each basket are unique. The
generated baskets are similar to:
business1: [user11, user12, user13, ...]
business2: [user21, user22, user23, ...]
business3: [user31, user32, user33, ...]

*Task 2*

In task 2, you will explore the Ta Feng dataset to find the frequent itemsets (only case 1). You will use data
found here from Kaggle (https://bit.ly/2miWqFS) to find product IDs associated with a given customer ID
each day. Aggregate all purchases a customer makes within a day into one basket. In other words, assume
a customer purchases at once all items purchased within a day.
Note: Be careful when reading the csv file as spark can read the product id numbers with leading zeros.
You can manually format Column F (PRODUCT_ID) to numbers (with zero decimal places) in the csv file
before reading it using spark.
SON Algorithm on Ta Feng data:
You will create a data pipeline where the input is the raw Ta Feng data, and the output is the file
described under “output file”. You will pre-process the data, and then from this pre-processed data,
you will create the final output. Your code is allowed to output this pre-processed data during
execution, but you should NOT submit homework that includes this pre-processed data.

(1) Data preprocessing

You need to generate a dataset from the Ta Feng dataset with following steps:
1. Find the date of the purchase (column TRANSACTION_DT), such as December 1, 2000 (12/1/00)
2. At each date, select “CUSTOMER_ID” and “PRODUCT_ID”.
3. We want to consider all items bought by a consumer each day as a separate transaction (i.e., “baskets”).
For example, if consumer 1, 2, and 3 each bought oranges December 2, 2000, and consumer 2 also bought
celery on December 3, 2000, we would consider that to be 4 separate transactions. An easy way to do this
is to rename each CUSTOMER_ID as “DATE-CUSTOMER_ID”. For example, if CUSTOMER_ID is 12321, and
this customer bought apples November 14, 2000, then their new ID is “11/14/00-12321”
4. Make sure each line in the CSV file is “DATE-CUSTOMER_ID1, PRODUCT_ID1”. 5.
The header of CSV file should be “DATE-CUSTOMER_ID, PRODUCT_ID”
You need to save the dataset in CSV format. Figure below shows an example of the output file
(please note DATE-CUSTOMER_ID and PRODUCT_ID are strings and integers, respectively)
Figure: customer_product file
Do NOT submit the output file of this data preprocessing step, but your code is allowed to create this
file.

(2) Apply SON Algorithm

The requirements for task 2 are similar to task 1. However, you will test your implementation with the
large dataset you just generated. For this purpose, you need to report the total execution time. For this
execution time, we take into account the time from reading the file till writing the results to the output
file. You are asked to find the candidate and frequent itemsets (similar to the previous task) using the file
you just generated. 
The following are the steps you need to do:
1. Reading the customer_product CSV file in to RDD and then build the case 1 market-basket model
2. Find out qualified customers-date who purchased more than kitems. (kis the filter threshold);
3. Apply the SON Algorithm code to the filtered market-basket model;