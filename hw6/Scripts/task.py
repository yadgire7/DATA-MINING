import sys
import os
import math
import timeit
import copy
from pyspark import SparkContext
from collections import defaultdict
import numpy as np
from sklearn.cluster import KMeans

# set environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# start spark context
sc = SparkContext.getOrCreate()
sc.setLogLevel('WARN')

# -----------------------------------------------------------------------------

# command line arguments
# sys.argv[1]
input_file = sys.argv[1]
num_clusters = int(sys.argv[2])
output_file = sys.argv[3]
op = open(output_file, 'w')

# -----------------------------------------------------------------------------


def write_intermediate_results(op, discard_set_dict, compressed_set_dict, rs, rd):
    n_discard_points = 0
    n_compressed_clusters = 0
    n_compressed_points = 0
    n_rs = 0
    for key in discard_set_dict.keys():
        n_discard_points += discard_set_dict[key]['N']
    for key in compressed_set_dict.keys():
        n_compressed_clusters += 1
        n_compressed_points += compressed_set_dict[key]['N']
    n_rs = len(rs)
    op.write('Round ' + str(rd + 1) + ': ' + str(n_discard_points) + ',' +
             str(n_compressed_clusters) + ',' + str(n_compressed_points) + ',' + str(n_rs) + '\n')


# -----------------------------------------------------------------------------
def str_to_float(arr):
    for i in range(len(arr)):
        arr[i] = float(arr[i])
    return arr

# -----------------------------------------------------------------------------


def generate_cluster_dict(arr, clusters):
    cluster_dict = defaultdict(list)
    for i in range(len(clusters)):
        idx = clusters[i]
        if idx not in cluster_dict:
            cluster_dict[idx] = [arr[i]]
        else:
            cluster_dict[idx].append(arr[i])
    return cluster_dict

# -----------------------------------------------------------------------------


def generate_discard_set(key, points_list):
    discard_set = {'N': 0, 'sum': [
        0]*dim, 'sum_sq': [0]*dim}
    discard_set['N'] = len(points_list)
    for point in points_list:
        for i in range(len(point)):
            discard_set['sum'][i] += point[i]
            discard_set['sum_sq'][i] = discard_set['sum_sq'][i] + point[i]**2
    discard_set['centroid'] = [x/discard_set['N'] for x in discard_set['sum']]
    discard_set['std_dev'] = [0]*dim
    for i in range(len(discard_set['std_dev'])):
        discard_set['std_dev'][i] = math.sqrt(
            discard_set['sum_sq'][i] / discard_set['N'] - (discard_set['sum'][i] / discard_set['N'])**2)
    return discard_set
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------

def calculate_md(point, dictionary):
    md = {}
    # print(len(md))
    for k in dictionary.keys():
        y = [0]*len(point)
        for i in range(len(point)):
            yi = ((point[i]-dictionary[k]['centroid'][i]) /dictionary[k]['std_dev'][i])**2
            y[i] = yi
        md[k] = math.sqrt(sum(y))
    cluster_id = min(md, key=md.get)
    md = md[cluster_id]
    return cluster_id, md

# -----------------------------------------------------------------------------


def calculate_cluster_md(old_stats, new_stats):
    d1 = 0.0
    d2 = 0.0
    for i in range(dim):
        if old_stats['std_dev'][i] != 0 and new_stats['std_dev'][i] != 0:
            d1 += ((old_stats['centroid'][i] - new_stats['centroid']
                    [i]) / old_stats['std_dev'][i])**2
            if new_stats['std_dev'][i] == 0:
                d2 += 0.0
            else:
                d2 += ((old_stats['centroid'][i] - new_stats['centroid']
                        [i]) / new_stats['std_dev'][i])**2
    return (math.sqrt(d1) + math.sqrt(d2)) / 2
# -----------------------------------------------------------------------------
def generate_rs(temp):
    rs = []
    for k, v in temp.items():
        if len(v) == 1:
            rs.append(v[0])
    return rs
# -----------------------------------------------------------------------------

def genrate_cs(temp):
    cs = copy.copy(temp)
    for k, v in temp.items():
        if len(v) == 1:
            cs.pop(k)
    return cs

# -----------------------------------------------------------------------------
start = timeit.default_timer()
split = 0.2
n_rounds = int(1/split)
rs = []
total = 0
'''
step 1:
Load 20% of the data randomly into main memory
'''
data = sc.textFile(input_file)

random_pick = data.map(lambda x: x.split(",")).map(
    str_to_float)
'''
[0.0, 8.0, -127.64433989643463, -112.93438512156577, -123.4960457961025, 114.4630547261514, 121.64570029890073, -119.54171797733461, 10
9.9719289517553, 134.23436237925256, -117.61527240771153, 120.42207629196271]
'''


# print(len(all_dict.keys()))

all_points = random_pick.collect()
random_pick = random_pick.randomSplit([split]*n_rounds)
initial_chunk = random_pick[0].map(lambda dim: tuple(dim[2:])).collect()
rd = 0
dim = len(initial_chunk[rd])
threshold = 2*math.sqrt(dim)


# Step 2. Run K-Means (e.g., from sklearn) with a large K (e.g., 5 times of the number of the input clusters)
initial_clusters = KMeans(n_clusters=10*num_clusters,
                          random_state=5).fit_predict(initial_chunk)
pass1_count = len(initial_chunk)
'''
Step 3.
In the K-Means result from Step 2, move all the clusters that contain only one point to RS(outliers).
create a dictionary. Key = index of cluster, Value = list of points belonging to that cluster index
'''

index_points_dict = generate_cluster_dict(initial_chunk, initial_clusters)
# check if cluster has only 1 point

rs = generate_rs(index_points_dict)

'''
step 4:
Run K-Means again to cluster the rest of the data points with K = the number of input clusters.
'''

updated_chunk = copy.copy(initial_chunk)
# print(len(updated_chunk))
for point in rs:
    updated_chunk.remove(point)
# print(len(updated_chunk))
total = len(updated_chunk)
clusters = KMeans(n_clusters=num_clusters).fit_predict(updated_chunk)

'''
Step 5:
Use the K-Means result from Step 4 to generate the DS clusters (i.e., discard their points and generate statistics).
'''
cluster_dict = generate_cluster_dict(updated_chunk, clusters)
discard_set_dict = {}
for k in cluster_dict.keys():
    discard_set_dict[k] = generate_discard_set(k, cluster_dict[k])

'''
Step 6:
Run K-Means on the points in the RS with a large K (e.g., 5 times of the number of the input
clusters) to generate CS (clusters with more than one points) and RS (clusters with only one point).
choosing k to be half the size of rs.
'''
rs_clusters = KMeans(n_clusters=int(len(rs)/2 + 1),
                     random_state=0).fit_predict(rs)
rs_cluster_dict = generate_cluster_dict(rs, rs_clusters)
cs_clusters_dict = defaultdict(list)

rs = generate_rs(rs_cluster_dict)
cs_clusters_dict = genrate_cs(rs_cluster_dict)
compressed_set_dict = {}
for k in cs_clusters_dict.keys():
    compressed_set_dict[k] = generate_discard_set(k, cs_clusters_dict[k])
# print(compressed_set_dict)
op.write('The intermediate results:\n')
write_intermediate_results(op, discard_set_dict, compressed_set_dict, rs, rd)


rd = rd + 1
for r in range(rd, n_rounds):
    '''
    Step 7.
    Load another 20 % of the data 
    '''
    chunk = random_pick[r].map(lambda dim: tuple(dim[2:])).collect()
    pass2_count = len(chunk)
    '''
    Step 8
    For the new points, compare them to each of the DS using the Mahalanobis Distance and assign
    them to the nearest DS clusters if the distance is < 2 sqrt(𝑑)
    '''

    cs_candidates = []
    for point in chunk:
        cluster_id, md = calculate_md(point, discard_set_dict)
        if md < threshold:
            total = total + 1
            cluster_dict[cluster_id].append(point)
        else:
            cs_candidates.append(point)

    # update statistics after the loop
    discard_set_dict = {}
    for k in cluster_dict.keys():
        discard_set_dict[k] = generate_discard_set(k, cluster_dict[k])

    '''
    Step 9
    For the new points that are not assigned to DS clusters, using the Mahalanobis Distance and
    assign the points to the nearest CS clusters if the distance is < 2 sqrt(𝑑)
    '''
    
    for point in cs_candidates:
        cluster_id, md = calculate_md(point, compressed_set_dict)
        if md < threshold:
            cs_clusters_dict[cluster_id].append(point)
        else:
            '''
    Step 10
    For the new points that are not assigned to a DS cluster or a CS cluster, assign them to RS.
    '''
            rs.append(point)

    # update statistics after the loop
    compressed_set_dict = {}
    for k in cs_clusters_dict.keys():
        compressed_set_dict[k] = generate_discard_set(k, cs_clusters_dict[k])
    
    '''
    Step 11
    Run K-Means on the RS with a large K (e.g., 5 times of the number of the input clusters) to
generate CS (clusters with more than one points) and RS (clusters with only one point)
    '''
    if len(rs) > 0:
        if int(len(rs)/2) > 5*num_clusters:
            k = 5 * num_clusters
        else:
            k = int(len(rs)/2 + 1)
        rs_clusters = KMeans(n_clusters=k, random_state=0).fit_predict(rs)
        rs_cluster_dict = generate_cluster_dict(rs, rs_clusters)
        new_cs_clusters_dict = {}
        new_cs_stats = {}
        rs = generate_rs(rs_cluster_dict)
        new_cs_clusters_dict = genrate_cs(rs_cluster_dict)
        for k in new_cs_clusters_dict.keys():
            new_cs_stats[k] = generate_discard_set(k, new_cs_clusters_dict[k])
        # print(new_cs_stats)

        '''
        Step 12
        Merge CS clusters that have a Mahalanobis Distance < 2 .𝑑
        '''
        
        for new_id in new_cs_stats.keys():
            o_id = -1
            min_dist = math.inf
            for old_id in compressed_set_dict.keys():
                md = calculate_cluster_md(
                    new_cs_stats[new_id], compressed_set_dict[old_id])
                if md < min_dist:
                    min_dist, o_id = md, old_id
            if min_dist < threshold:
                for point in new_cs_clusters_dict[new_id]:
                    cs_clusters_dict[o_id].append(point)
            else:
                new_key = max(cs_clusters_dict.keys()) + 1
                for point in (new_cs_clusters_dict[new_id]):
                    cs_clusters_dict[new_key].append(point)

        compressed_set_dict = {}
        for k in cs_clusters_dict.keys():
            compressed_set_dict[k] = generate_discard_set(k, cs_clusters_dict[k])

    '''
    If this is the last run (after the last chunk of data), merge CS clusters with DS clusters that have a
Mahalanobis Distance < 2 .𝑑
    '''
    if r == n_rounds - 1:
        final_cs_dict = defaultdict(list)
        for new_id in cs_clusters_dict.keys():
            for old_id in discard_set_dict.keys():
                md = calculate_cluster_md(
                    compressed_set_dict[new_id], discard_set_dict[old_id])
                if md < min_dist:
                    min_dist, o_id = md, old_id
            if min_dist < threshold:
                for point in cs_clusters_dict[new_id]:
                    total = total + 1
                    cluster_dict[o_id].append(point)
                    cs_clusters_dict[new_id].remove(point)
            else:
                final_cs_dict[new_id] = cs_clusters_dict[new_id]
        discard_set_dict = {}
        for k in cluster_dict.keys():
            discard_set_dict[k] = generate_discard_set(k, cluster_dict[k])
        compressed_set_dict = {}
        for k in final_cs_dict.keys():
            compressed_set_dict[k] = generate_discard_set(k, final_cs_dict[k])
        write_intermediate_results(
            op, discard_set_dict, compressed_set_dict, rs, r)
    else:
        write_intermediate_results(
            op, discard_set_dict, compressed_set_dict, rs, r)

all_dict = {}
for k, v in cluster_dict.items():
    for p in v:
        all_dict[str(p)] = k
op.write("The clustering results:\n")
for point in all_points:
    if str(tuple(point[2:])) in all_dict.keys():
        op.write(str(int(point[0])) + "," +
                 str(all_dict[str(tuple(point[2:]))]) + "\n")

end = timeit.default_timer()
print('Time: ', end - start)
# python Scripts/test.py Data/hw6_clustering.txt
