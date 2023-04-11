from itertools import islice, combinations
import os
import timeit
import sys
import copy
from sys import maxsize as MAX_DEPTH
from itertools import islice
from pyspark import SparkContext, SparkConf
from collections import defaultdict


# -------------------------------------------------------------------

# command line arguments
threshold =  int(sys.argv[1])
input_file = sys.argv[2]
bet_output_file = sys.argv[3]
com_output_file = sys.argv[4]
#  -------------------------------------------------------------------

# system variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# -------------------------------------------------------------------

# defining functions

# functon to write to file
def write_bet_to_file(final, bet_output_file):
    with open(bet_output_file, 'w') as op:
        for tup in final:
            op.write("(\'" + str(tup[0][0] + "\', \'" +
                    str(tup[0][1]) + "\'), " + str(round(tup[1], 5)) + "\n"))

# functon to write to file
def write_com_to_file(final, com_output_file):
    final = final.collect()
    with open(com_output_file, 'w') as op:
        for tup in final:
            string = str(tup)
            string = string.replace("[", "")
            string = string.replace("]", "")
            op.write(string + "\n")

            

# function to design an undirected graph
# reference: https://www.geeksforgeeks.org/building-an-undirected-graph-and-finding-shortest-path-using-dictionaries-in-python/
def get_graph(rdd):
    cleaned_rdd = rdd\
        .groupByKey().mapValues(list).map(lambda x: (x[0], set(x[1]))).filter(lambda x: len(x[1]) >= threshold)\
        .collectAsMap()
    # get all possible pairs
    pairs = list(combinations(list(cleaned_rdd.keys()), 2))
    v = []
    e = []
    for pair in pairs:
        if len(cleaned_rdd[pair[0]].intersection(cleaned_rdd[pair[1]])) >= threshold:
            v.append(pair[0])
            v.append(pair[1])
            e.append(list(pair))
    v = list(set(v))
    my_dict = {v[i]: i for i in range(len(v))}
    graph = defaultdict(list)
    for edge in e:
        graph[edge[0]].append(edge[1])
        graph[edge[1]].append(edge[0])
    
    return my_dict, graph, v, e

# function for calculating betweenness
def get_betweenness(start, graph, my_dict):
    visited = [] # acts like a flag to check if the node has been visited
    traversed = [] # keep track of the traveresed nodes to use for calculating betweenness in the end
    result = [] # append the betweenness for each pass

    # initialise the variables
    get_parent = defaultdict(list) 
    num_shortest_paths = [0] * len(my_dict)
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
        neighbours = graph[vertex]
        for neighbour in neighbours:
            if neighbour not in visited:
                queue.append(neighbour)
                visited.append(neighbour)

                
# get number of shortest paths from start to the current node (neighbour)  

            if get_depth[my_dict[neighbour]] > get_depth[my_dict[vertex]] + 1:
                get_depth[my_dict[neighbour]] = get_depth[my_dict[vertex]] + 1
                # print(f"Depth: {get_depth}")
                get_parent[neighbour].append(vertex)
                num_shortest_paths[my_dict[neighbour]] = num_shortest_paths[my_dict[vertex]]
                # print(f"Num shortest paths: {num_shortest_paths}")
            else: 
                if get_depth[my_dict[neighbour]] == get_depth[my_dict[vertex]] + 1:
                    num_shortest_paths[my_dict[neighbour]] += num_shortest_paths[my_dict[vertex]]
                    get_parent[neighbour].append(vertex)

# reverse the traversed list to go from bottom to top
# slice the root / start from the list
    rev_visited = traversed[1:][::-1]
    for node in rev_visited:
        for parent in get_parent[node]:
            temp = (weights[my_dict[node]] / num_shortest_paths[my_dict[node]]) * num_shortest_paths[my_dict[parent]]
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
        for i,j in combinations(community, 2):
            if j in graph[i]:
                # if there is an edge, add 1 to the numerator
                aij = 1
            else:
                aij = 0
            expected = (len(original_graph[i]) * len(original_graph[j])) / (2 * m)
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

my_dict, g, vertices, edges = get_graph(data)
original_graph = g.copy()
m = len(edges)/2
temp = sc.parallelize(vertices)

betweenness = temp.map(lambda x: get_betweenness(x, g, my_dict)).flatMap(lambda x: x)\
    .reduceByKey(lambda a, b: a + b).map(lambda divide: (divide[0], divide[1]/2))\
                .sortBy(lambda entry: (-entry[1], entry[0][0], entry[0][1]))
betweenness = betweenness.collect()

write_bet_to_file(betweenness, bet_output_file)

# task 2.2

start = timeit.default_timer()
num_edges = len(edges)/2
max_modularity = -1.0
max_comms = []
while num_edges > 0:
    comms = bfs_community(vertices, g)
    # print(f"Number of communities: {len(comms)}")
    modularity = get_modularity(comms, g, m, original_graph)
    # print(f"Modularity: {modularity}")
    if modularity > max_modularity:
        max_modularity = modularity
        max_comms = comms
    cut_max = temp.map(lambda x: get_betweenness(x, g, my_dict)).flatMap(lambda x: x)\
        .reduceByKey(lambda a, b: a + b).map(lambda divide: (divide[0], divide[1]/2))\
        .sortBy(lambda entry: (-entry[1])).map(lambda x: x[0]).first()
    # print(type(cut_max))
    g = remove_edges(g, cut_max)
    num_edges -= 1
comms = sc.parallelize(max_comms).sortBy(lambda x: (len(x), x))
end = timeit.default_timer()
write_com_to_file(comms, com_output_file)
print(f"Duration: {end - start}")
# print(max_comms)
# write_bet_to_file(betweenness, bet_output_file)
# write_com_to_file(comms, com_output_file)