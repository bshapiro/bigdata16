from graphframes import GraphFrame
from cPickle import dump, load
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

num_vertices, edge_tuples = load(open('pickles/connections.pkl', 'rb'))
vertex_reads = load(open('pickles/ovr_list.pkl', 'rb'))

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
