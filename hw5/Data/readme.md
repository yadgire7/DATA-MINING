Dataset: Yelp Dataset

*Task 1*

You will implement the Bloom Filtering algorithm to estimate whether the user_id in the data stream has
shown before. The details of the Bloom Filtering Algorithm can be found at the streaming lecture slide.
Please find proper hash functions and the number of hash functions in the Bloom Filtering algorithm.
In this task, you should keep a global filter bit array and the length is 69997.
The hash functions used in a Bloom filter should be independent and uniformly distributed.  Some
possible the hash functions are:
f(x)= (ax + b) % m or f(x) = ((ax + b) % p) % m
where p is any prime number and m is the length of the filter bit array. You can use any combination for
the parameters (a, b, p). The hash functions should keep the same once you created them.
As the user_id is a string, you need to convert the type of user_id to an integer and then apply hash
functions to it.

You also need to encapsulate your hash functions into a function called myhashs. The input of myhashs
function is a user_id (string) and the output is a list of hash values. For example, if you have three hash
functions, the size of the output list should be three and each element in the list corresponds to an
output value of your hash function.

*Task 2*

In task2, you will implement the Flajolet-Martin algorithm (including the step of combining estimations
from groups of hash functions) to estimate the number of unique users within a window in the data
stream. The details of the Flajolet-Martin Algorithm can be found at the streaming lecture slide. You
need to find proper hash functions and the number of hash functions in the Flajolet-Martin algorithm.

*Task 3*

The goal of task3 is to implement the fixed size sampling method (Reservoir Sampling Algorithm).
In this task, we assume that the memory can only save 100 users, so we need to use the fixed size
sampling method to only keep part of the users as a sample in the streaming. When the streaming of the
users comes, for the first 100 users, you can directly save them in a list. After that, for the nth(n starts
from 1) user in the whole sequence of users, you will keep the nth user with the probability of 100/n,
otherwise discard it. If you keep the nthuser, you need to randomly pick one in the list to be replaced.
You also need to keep a global variable representing the sequence number of the users.