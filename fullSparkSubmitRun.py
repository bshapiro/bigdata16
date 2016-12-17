from graphframes import GraphFrame
import sys, os, argparse, re, pysam
import cPickle as pickle
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark import SparkFiles

conf = SparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

sc.addPyFile('/home/Rachel/bigdata16/gtf_merge.py')
sc.addPyFile('/home/Rachel/bigdata16/overlapParser.py')
sc.addFile('/home/Rachel/ERR188044_chrX_sorted.bam')
sc.addFile('/home/Rachel/ERR188044_chrX_sorted.bam.bai')
#sc.addFile('/home/Rachel/test.bam')
#sc.addFile('/home/Rachel/test.bam.bai')
#sc.addFile('/home/Rachel/ERR188044_chrX_reduced.bam')
#sc.addFile('/home/Rachel/ERR188044_chrX_reduced.bam.bai')
sc.addFile("/home/Rachel/bigdata16/stringtie_mod")
sc.addFile("/usr/bin/stringtie")
sc.addFile("/usr/bin/samtools")

from overlapParser import OverlapParser
from gtf_merge import *

PRINT_DEBUG = False
MULTIMAP_FLAG = "NH"
MAX_GAP = 100
        
def rowLambda(row):
#    in_fname = SparkFiles.get('test.bam')
    in_fname = SparkFiles.get('ERR188044_chrX_sorted.bam')
    ovr = 100
    ovr = OverlapParser(in_fname, max_gap)
    group = groups[int(row[0])]
    return [(row[1], (samLine, group[0], row[0])) for samLine in ovr.get_group(group)]

#in_fname = '/home/Rachel/test.bam'
in_fname ='/home/Rachel/ERR188044_chrX_sorted.bam'
out_dir = '/pickles'
max_gap = 100
num_partitions = 16
ovr = OverlapParser(in_fname, max_gap)
    
edge_tuples_strings = list()
groups = list()
groupCount = 0

while True:
    group, cons = ovr.next_group()
    groups.append(group)  
    if not group:
        break
    groupCount += 1
    edge_tuples_strings.extend([(str(item[0]), str(item[1])) for item in cons])

vertex_strings = [(str(i),) for i in  xrange(groupCount)]
e = sqlContext.createDataFrame(edge_tuples_strings, ['src', 'dst'])
v = sqlContext.createDataFrame(vertex_strings, ['id'])
g = GraphFrame(v,e)
result_rdd = g.connectedComponents().rdd

component_to_group_rdd = result_rdd.flatMap(lambda row: rowLambda(row))
partitioned_rdd = component_to_group_rdd.partitionBy(num_partitions)
partitioned_rdd.persist()

def sumAll(partitionIter):
    yield sum([tup[0] for tup in set([line[1][1:] for line in partitionIter])])

group_nums_rdd = partitioned_rdd.mapPartitions(sumAll)
sam_only_partitions = partitioned_rdd.map(lambda line: line[1][0], preservesPartitioning=True)
piped_output = sam_only_partitions.pipe("stringtie_mod")

gtfList = piped_output.collect() #glom().collect()
gtfFileL = merge_gtfs(gtfList, group_nums_rdd.collect())

# Super hacky, but I'm putting the list into an rdd to I can save to hdfs.
# Should either have Sam's code work entirely with rdds, or use pydoop or 
# something to write to hdfs.
gtfFile_rdd = sc.parallelize(gtfFileL)
gtfFile_rdd.saveAsTextFile('/output/ERR188044_chrX.gtf')

sc.stop()

