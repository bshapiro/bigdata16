from graphframes import GraphFrame
import sys, os, argparse, re, pysam
import cPickle as pickle
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark import SparkFiles

conf = SparkConf()
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

parser = argparse.ArgumentParser(description="")
parser.add_argument('-f', dest='in_fname', required=True, type=str, help="Position sorted BAM file. Indexed bam.bai file must be present in same directory.")
parser.add_argument('-o', dest='out_dir', default='/', type=str, help="Output directory. Default is home hdfs directory / ")
parser.add_argument('-g', dest='max_gap', default=100, type=int, help="Maximum gap between two reads for them to be part of same group")
parser.add_argument('-d', dest='debug', action='store_true', help="Print debug messages to stderr (if -O not also included)")
parser.add_argument('-k', dest='num_parts', default=10, type=int, help="Number partitions, default=10")

args = parser.parse_args(sys.argv[1:])
in_fname = args.in_fname
out_dir = args.out_dir
max_gap = args.max_gap
num_partitions = args.num_parts

sc.addPyFile('/home/Rachel/bigdata16/gtf_merge.py')
sc.addPyFile('/home/Rachel/bigdata16/overlapParser.py')
sc.addFile(in_fname)
sc.addFile(in_fname+'.bai')
sc.addFile("/home/Rachel/bigdata16/stringtie_mod")
sc.addFile("/usr/bin/stringtie")
sc.addFile("/usr/bin/samtools")

from overlapParser import OverlapParser
from gtf_merge import *

PRINT_DEBUG = False
MULTIMAP_FLAG = "NH"
MAX_GAP = 100
        
def rowLambda(row):
    spark_fname = SparkFiles.get(in_fname[in_fname.rfind('/')+1:])
    ovr = OverlapParser(spark_fname, max_gap)
    group = groups[int(row[0])]
    return [(row[1], (samLine, group[0], row[0])) for samLine in ovr.get_group(group)]

ovr = OverlapParser(in_fname, max_gap)
    
edge_tuples_strings = list()
groups = list()

while True:
    group, cons = ovr.next_group()
    if not group:
        break
    groups.append(group)
    edge_tuples_strings.extend([(str(item[0]), str(item[1])) for item in cons])

vertex_strings = [(str(i),) for i in  xrange(len(groups))]
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

gtfList = piped_output.glom().collect()
gtfFileL = merge_gtfs(gtfList, group_nums_rdd.collect())

# Super hacky, but I'm putting the list into an rdd to I can save to hdfs.
# Should either have Sam's code work entirely with rdds, or use pydoop or 
# something to write to hdfs.
gtfFile_rdd = sc.parallelize(gtfFileL)
gtfFile_rdd.saveAsTextFile(out_dir.strip('/')+'/'+in_fname.strip('bam')[in_fname.rfind('/')+1:]+'gtf')

sc.stop()



