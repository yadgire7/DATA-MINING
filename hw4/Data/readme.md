Dataset: Yelp Dataset

In this assignment, you will explore the spark GraphFrames library as well as implement your own
Girvan-Newman algorithm using the Spark Framework to detect communities in graphs. You will use the
ub_sample_data.csv dataset to find users who have similar business tastes. The goal of this assignment is
to help you understand how to use the Girvan-Newman algorithm to detect communities in an efficient
way within a distributed environment

*Task 0*

To construct the social network graph, assume that each node is uniquely labeled and that links are
undirected and unweighted.
Each node represents a user. There should be an edge between two nodes if the number of common
businesses reviewed by two users is greater than or equivalent to the filter threshold. For example,
suppose user1 reviewed set{business1, business2, business3} and user2 reviewed set{business2,
business3, business4, business5}. If the threshold is 2, there will be an edge between user1 and user2.
If the user node has no edge, we will not include that node in the graph.
The filter threshold will be given as an input parameter when running your code.

*Task 1*

In task1, you will explore the Spark GraphFrames library to detect communities in the network graph you
constructed in 4.1. In the library, it provides the implementation of the Label Propagation Algorithm
(LPA) which was proposed by Raghavan, Albert, and Kumara in 2007. It is an iterative community
detection solution whereby information “flows” through the graph based on underlying edge structure.
In this task, you do not need to implement the algorithm from scratch, you can call the method provided
by the library. The following websites may help you get started with the Spark GraphFrames:
https://docs.databricks.com/spark/latest/graph-analysis/graphframes/user-guide-python.html
https://docs.databricks.com/spark/latest/graph-analysis/graphframes/user-guide-scala.html

*Task 2*

In task 2, you will implement your own Girvan-Newman algorithm to detect the communities in the
network graph. You can refer to Chapter 10 from the Mining of Massive Datasets book for the algorithm
details.
Because your task1 and task2 code will be executed separately, you need to construct the graph again in
this task following the rules in section 4.1.
For task 2, you can ONLY use Spark RDD and standard Python or Scala libraries. Remember to delete
your code that imports graphframes. Usage of Spark DataFrame is NOT allowed in this task.

4.3.1 Betweenness Calculation (2 pts)
In this part, you will calculate the betweenness of each edge in the original graph you constructed in 4.1.
Then you need to save your result in a txt file. The format of each line is
(‘user_id1’, ‘user_id2’), betweenness value
Your result should be firstly sorted by the betweenness values in descending order and then the first
user_id in the tuple in lexicographical order (the user_id is type of string). The two user_ids in each tuple
should also be in lexicographical order.

For output, you should use the python built-in round() function to round the betweenness value to five
digits after the decimal point. (Rounding is for output only, please do not use the rounded numbers for
further calculation)
