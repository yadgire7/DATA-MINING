from itertools import islice, combinations
from pyspark import SparkContext
import os
import timeit
import sys
from itertools import islice
from pyspark import SparkContext
from pyspark.sql import SQLContext
from graphframes import GraphFrame


# -------------------------------------------------------------------

# command line arguments
threshold = int(sys.argv[1])
input_file = sys.argv[2]
output_file = sys.argv[3]

#  -------------------------------------------------------------------

# system variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# -------------------------------------------------------------------


def write_to_file(final, output_file):
    final = final.collect()
    with open(output_file, 'w') as op:
        for tup in final:
            string = ""
            for ele in tup:
                string = string + "'" + ele + "', "
            op.write(string[:-2])
            op.write("\n")


#  create spark context
sc = SparkContext.getOrCreate()
sc.setLogLevel('WARN')
sql_con = SQLContext(sc)


# -------------------------------------------------------------------
start = timeit.default_timer()
data = sc.textFile(input_file)
data = data.mapPartitionsWithIndex(lambda index, iteartor: islice(iteartor, 1, None) if index == 0 else iteartor).map(lambda x: x.split(","))\
    .map(lambda x: (x[0], x[1]))

# filter data above threshold
cleaned_data = data\
    .groupByKey().mapValues(list).map(lambda x: (x[0], set(x[1]))).filter(lambda x: len(x[1]) >= threshold)\
    .collectAsMap()

# get all possible pairs
pairs = list(combinations(list(cleaned_data.keys()), 2))

v = []
e = []
for pair in pairs:
    if len(cleaned_data[pair[0]].intersection(cleaned_data[pair[1]])) >= threshold:
        v.extend(pair)
        e.append(pair)
        e.append(tuple(reversed(pair)))
    
v = list(set(v))
v = map(lambda x: (x,), v)




vertices = sql_con.createDataFrame(v, ["id"])
edges = sql_con.createDataFrame(e, ["src", "dst"])


# define GraphFrame variable
graph = GraphFrame(vertices, edges)

# run label propagation
comm = graph.labelPropagation(maxIter=5)

# get final data to write to output file
final = comm.rdd.map(lambda row: (row[1], row[0])).groupByKey().mapValues(list)\
    .map(lambda value: (value[0], sorted(value[1]))).sortBy(lambda x: (len(x[1]), x[1]))\
    .map(lambda s: tuple(s[1]))

# write to output file
write_to_file(final, output_file)

end = timeit.default_timer()
print("Duration: ", end - start)
