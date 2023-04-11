from itertools import islice, combinations
import os
import timeit
import sys
from sys import maxsize as MAX_DEPTH
from itertools import islice
from pyspark import SparkContext, SparkConf
from collections import defaultdict
import copy


# -------------------------------------------------------------------

# command line arguments
threshold = 7  # int(sys.argv[1])
input_file = "Data/ub_sample_data.csv"  # sys.argv[2]
output_file = "output2.txt"  # sys.argv[3]

#  -------------------------------------------------------------------

# system variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# -------------------------------------------------------------------

# defining functions

# functon to write to file
def write_bet_to_file(final, output_file):
    final = final.collect()
    with open(output_file, 'w') as op:
        for tup in final:
            op.write("(\'" + str(tup[0][0] + "\', \'" +
                                 str(tup[0][1]) + "\'), " + str(tup[1]) + "\n"))


# function to design an undirected graph
# reference: https://www.geeksforgeeks.org/building-an-undirected-graph-and-finding-shortest-path-using-dictionaries-in-python/
def get_graph(edges):
    # cleaned_rdd = rdd\
    #     .groupByKey().mapValues(list).map(lambda x: (x[0], set(x[1]))).filter(lambda x: len(x[1]) >= threshold)\
    #     .collectAsMap()
    # get all possible pairs
    # pairs = list(combinations(list(cleaned_rdd.keys()), 2))
    v = []
    e = []
    for edge in edges:
        #     if len(cleaned_rdd[pair[0]].intersection(cleaned_rdd[pair[1]])) >= threshold:
        v.append(edge[0])
        v.append(edge[1])
        e.append(list(edge))
    v = list(set(v))
    graph = defaultdict(list)
    for edge in e:
        graph[edge[0]].append(edge[1])
        graph[edge[1]].append(edge[0])

    return graph, v, e

    # return my_dict, graph, v, e

# function for calculating betweenness


def get_betweenness(start, graph, my_dict):
    visited = []
    traversed = []
    result = []
    get_parent = defaultdict(list)
    num_shortest_paths = [0] * len(my_dict)
    # print(len(my_dict))
    get_depth = [MAX_DEPTH] * len(my_dict)
    weights = [1] * len(my_dict)
    weights[my_dict[start]] = 0
    queue = [start]
    visited.append(start)
    get_depth[my_dict[start]] = 0
    num_shortest_paths[my_dict[start]] = 1
    while queue:
        vertex = queue.pop(0)
        traversed.append(vertex)
        # print(f"Traversed: {traversed}")
        neighbours = graph[vertex]
        # print(f"Neighbours: {neighbours}")
        for neighbour in neighbours:
            if neighbour not in visited:
                queue.append(neighbour)
                # print(f"Queue: {queue}")
                visited.append(neighbour)
                # print(f"Visited: {visited}")
                # get_parent[neighbour].append(vertex)

            if get_depth[my_dict[neighbour]] > get_depth[my_dict[vertex]] + 1:
                get_depth[my_dict[neighbour]] = get_depth[my_dict[vertex]] + 1
                get_parent[neighbour].append(vertex)
                # print(f"Depth: {get_depth}")
                num_shortest_paths[my_dict[neighbour]
                                   ] = num_shortest_paths[my_dict[vertex]]
                # print(f"Num shortest paths: {num_shortest_paths}")
            else:
                if get_depth[my_dict[neighbour]] == get_depth[my_dict[vertex]] + 1:
                    num_shortest_paths[my_dict[neighbour]
                                       ] += num_shortest_paths[my_dict[vertex]]
                    get_parent[neighbour].append(vertex)
                    # print(f"Num shortest paths: {num_shortest_paths}")
    # print(len(traversed))
    # print(f"Traversed: {traversed}")
    # print(f"Visited: {visited}")
    # print(f"Depth: {get_depth}")
    # print(f"Num shortest paths: {num_shortest_paths}")

    rev_visited = traversed[1:][::-1]

    # print(f"Rev visited: {rev_visited}")
    for node in rev_visited:
        # print(get_parent[node])
        for parent in get_parent[node]:
            # print(parent)
            temp = (weights[my_dict[node]] / num_shortest_paths[my_dict[node]]
                    ) * num_shortest_paths[my_dict[parent]]
            weights[my_dict[parent]] += temp
            result.append((tuple(sorted([node, parent])), temp))
    return result

# function fro traversing communities
def bfs_community(vertices, graph):
    comms = []
    queue = []
    visited = []
    for vertex in vertices:
        if vertex not in visited:
            comms_visited = [vertex]
            queue.append(vertex)
            visited.append(vertex)
            while queue:
                node = queue.pop(0)
                neighbours = graph[node]
                for neighbour in neighbours:
                    if neighbour not in comms_visited:
                        queue.append(neighbour)
                        comms_visited.append(neighbour)
            
            comms_visited = sorted(comms_visited)
            visited.extend(comms_visited)
            comms.append(comms_visited)
    return comms

# function to calculate modularity
def get_modularity(communities, graph, m, original_graph):
    # m is the total number of edges in the original graph
    # it does not change while calculating modularity each time
    for community in communities:
        modularity = 0.0
        for i, j in combinations(community, 2):
            if j in graph[i]:
                # if there is an edge, add 1 to the numerator
                aij = 1
            else:
                aij = 0
            expected = (len(original_graph[i])
                        * len(original_graph[j])) / (2 * m)
            modularity += (aij - expected)
    return modularity / (2 * m)

# function to remove edges
def remove_edges(graph, edge):
    if edge[1] in graph[edge[0]]:
        graph[edge[0]].remove(edge[1])
        graph[edge[1]].remove(edge[0])
    return graph



# -------------------------------------------------------------------
#  create spark context
sc = SparkContext.getOrCreate()
sc.setLogLevel('WARN')


data = sc.textFile(input_file)
data = data.mapPartitionsWithIndex(lambda index, iteartor: islice(iteartor, 1, None) if index == 0 else iteartor).map(lambda x: x.split(","))\
    .map(lambda x: (x[0], x[1]))

# my_dict, g, vertices, edges = get_graph(data)
my_dict = {"A": 0, "B": 1, "C": 2, "D": 3, "E": 4, "F": 5, "G": 6}
e = [("E", "D"), ("E", "F"), ("D", "B"), ("D", "G"), ("D", "F"),
     ("F", "G"),("C", "A"), ("B", "C"), ("B", "A")]
g, vertices, edges = get_graph(e)
original_graph = g.copy()
print(vertices)
print(g)
temp = sc.parallelize(vertices)
# print(betweenness.collect())
betweenness = temp.map(lambda x: get_betweenness(x, g, my_dict)).flatMap(lambda x: x)\
    .reduceByKey(lambda a, b: a + b).map(lambda divide: (divide[0], divide[1]/2))\
    .sortBy(lambda entry: (-entry[1], entry[0][0], entry[0][1]))

betweenness = betweenness.collect()

# write_bet_to_file(betweenness, bet_output_file)

# task 2.2

start = timeit.default_timer()
num_edges = len(edges)
print(f"Number of edges: {num_edges}")
m = len(edges)
max_modularity = -1.0
max_comms = []
while num_edges > 0:
    comms = bfs_community(vertices, g)
    # print(comms)
    # print(f"Number of communities: {len(comms)}")
    modularity = get_modularity(comms, g, m, original_graph)
    # print(f"Modularity: {modularity}")
    if modularity > max_modularity:
        max_modularity = modularity
        max_comms = comms
    cut_max = temp.map(lambda x: get_betweenness(x, g, my_dict)).flatMap(lambda x: x)\
        .reduceByKey(lambda a, b: a + b).map(lambda divide: (divide[0], divide[1]/2))\
        .sortBy(lambda entry: (-entry[1])).map(lambda x: x[0]).first()
    print(comms)
    print(cut_max)
    print("initial graph: ", g)
    g = remove_edges(g, cut_max)
    print(f"Updated graph: {g}")
    num_edges -= 1
comms = sc.parallelize(max_comms).sortBy(lambda x: (len(x), x))
end = timeit.default_timer()


    # .reduceByKey(lambda a, b: a + b).map(lambda divide: (divide[0], divide[1]/2))\
    # .sortBy(lambda entry: (-entry[1], entry[0][0], entry[0][1]))
# betweenness = sc.parallelize(vertices).map(lambda x: x).map(lambda x: get_betweenness(my_dict, g, x))
# .reduceByKey(lambda a, b: a + b).map(lambda divide: (divide[0], divide[1]/2))\
#     .sortBy(lambda entry: (-entry[1], entry[0][0], entry[0][1]))
# print(len(vertices))
# print(betweenness.count())
# write_bet_to_file(betweenness, output_file)
# print(betweenness.collect())
