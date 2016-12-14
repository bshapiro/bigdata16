from graphframes import GraphFrame
import sys, os, argparse, re, pysam
import cPickle as pickle
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark import SparkFiles

sc.addPyFile('/home/Rachel/bigdata16/overlapParser.py')
sc.addFile('/home/Rachel/ERR188044_chrX_sorted.bam')
sc.addFile('/home/Rachel/ERR188044_chrX_sorted.bam.bai')

from overlapParser import OverlapParser

PRINT_DEBUG = False
MULTIMAP_FLAG = "NH"
MAX_GAP = 100
        
def rowLambda(row):
    in_fname = SparkFiles.get('ERR188044_chrX_sorted.bam')
    ovr = 100
    ovr = OverlapParser(in_fname, max_gap)
    group = groups[int(row[0])]
    return [(row[1], (samLine, group[0])) for samLine in ovr.get_group(group)]

in_fname ='/home/Rachel/ERR188044_chrX_sorted.bam'
out_dir = '/pickles'
max_gap = 100
num_partitions = 10
ovr = OverlapParser(in_fname, max_gap)
    
edge_tuples_strings = []
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
    
result_rdd = sc.pickleFile('/pickles/connCompParitionsRDD.pkl')   

component_to_group_rdd = result_rdd.flatMap(lambda row: rowLambda(row))
#component_to_group_rdd = result_rdd.flatMap(lambda row: [(row[1], samLine) for samLine in ovr.get_group(groups[int(row[0])])])
partitioned_rdd = component_to_group_rdd.partitionBy(num_partitions)

def sumAll(partitionIter): 
    yield sum([line[1][1] for line in partitionIter])

group_nums_rdd = partitioned_rdd.mapPartitions(sumAll)

for line in component_to_group_rdd.take(5): 
    print line

#Gives a list of length num_partitions, of the sum total of unique reads corresponding to the partitions (in order) 
readCountsByPartition = group_nums_rdd.collect()
    
sc.stop()

