from graphframes import GraphFrame
from cPickle import dump, load
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from collections import Counter
from Queue import PriorityQueue
#from machine import Machine

sc = SparkContext()

num_vertices, edge_tuples = sc.pickleFile('hdfs:///pickles/default_run/connections.pkl')
vertex_reads = sc.pickleFile('hdfs:///pickles/default_run/ovr_list.pkl')

vertex_ids = set()
for item in edge_tuples:
    vertex_ids.add((str(item[0]),))
    vertex_ids.add((str(item[1]),))

vertex_strings = list(vertex_ids)
edge_tuples_strings = [(str(item[0]), str(item[1])) for item in edge_tuples]

e = sqlContext.createDataFrame(edge_tuples_strings, ['src', 'dst'])
v = sqlContext.createDataFrame(vertex_strings, ['id'])
g = GraphFrame(v, e)
result = g.connectedComponents()

mapped_result = result.rdd.flatMap(lambda row: [(row[1], vertex_reads[int(row[0])][i]) for i in range(len(vertex_reads[int(row[0])]))])
result = mapped_result.groupByKey()
