*implement the Bradley-Fayyad-Reina (BFR) algorithm. The goal is to let you
be familiar with the process of clustering in general and various distance measurements. The datasets you
are going to use is synthetic dataset.*

Since the BFR algorithm has a strong assumption that the clusters are normally distributed with independent dimensions, we generated synthetic datasets by initializing some random centroids and creating some data points with the centroids and some standard deviations to form the clusters. We also added some other data points as outliers in the dataset to evaluate the algorithm. Data points which are outliers belong to clusters that are named or indexed as “-1”. Figure 1 shows an example of a part of the dataset. The first column is the data point index. The second column is the name index of the cluster that the data point belongs to. The rest columns represent the features dimensions of the data point.



