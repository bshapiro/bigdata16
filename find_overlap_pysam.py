from graphframes import GraphFrame
import sys, os, argparse, re, pysam
import cPickle as pickle
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from overlapParser import OverlapParser

PRINT_DEBUG = False

MULTIMAP_FLAG = "NH"
MAX_GAP = 100
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('in_fname', default=None, nargs='?', type=str, help="Position sorted SAM file, or stdin if not included.")
    parser.add_argument('-o', dest='out_dir', default='/', type=str, help="Output directory. Default is home hdfs directory / ")
    parser.add_argument('-g', dest='max_gap', default=100, type=int, help="Maximum gap between two reads for them to be part of same group")
    parser.add_argument('-d', dest='debug', action='store_true', help="Print debug messages to stderr (if -O not also included)")
    parser.add_argument('-k', dest='numParts', default=10, type=int, help="Number partitions, default=10")

    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    args = parser.parse_args(sys.argv[1:])
    ovr = OverlapParser(args.in_fname, args.max_gap)
    
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
    
#    e = sqlContext.createDataFrame(edge_tuples_strings, ['src', 'dst'])
#    v = sqlContext.createDataFrame(vertex_strings, ['id'])

#    e.rdd.saveAsPickleFile(args.out_dir+"edges.pkl")
#    v.rdd.saveAsPickleFile(args.out_dir+"vertices.pkl")

#    e = sc.pickleFile('hdfs:///pickles/edges.pkl')
#    v = sc.pickleFile('hdfs:///pickles/vertices.pkl')    

#    g = GraphFrame(v, e)
 #   result = g.connectedComponents()

#    result.rdd.saveAsPickleFile(args.out_dir+"connCompParitionsRDD.pkl")

    result_rdd = sc.pickleFile('/pickles/connCompParitionsRDD.pkl')   
#    component_to_group_rdd = result_rdd.flatMap(lambda row: [(row[1], row[0]) for i in range(groups[int(row[0])][0])])
    component_to_group_rdd = result_rdd.flatMap(lambda row: [(row[1], samLine) for samLine in ovr.getGroup(groups[int(row[0])])])
#    partitioned_rdd = component_to_group_rdd.partitionBy(args.numParts)

    for line in component_to_group_rdd.take(5): 
        print line
    
#    for line in partitioned_rdd.take(5):
#        print line

    # Maps component ID to group size, and sums group sizes per component ID
#    group_nums_rdd = result.rdd.map(lambda row: (row[1], groups[int(row[0])][0])).sum()

#    for line in group_nums_rdd.take(5):
#        print line

#    partitioned_rdd.saveAsPickleFile(args.out_dir+"connCompParitionsRDD.pkl")


#    stringtie_results_rdd = partioned_rdd.pipe('run_stringtie.sh')

    sc.stop()

