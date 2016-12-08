import sys, os, argparse, re, pysam
import cPickle as pickle
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
#import pydoop.hdfs as hdfs


PRINT_DEBUG = False

MULTIMAP_FLAG = "NH"
MAX_GAP = 100

class OverlapParser:

    def __init__(self, filename=None, max_gap=100):
        self.group_size = 0
        self.gmax = 0
        self.gmin = 0
        self.group_nm = 0
        self.multimappers = dict()
        self.connections = set()
        self.prev_ref = None
        self.max_gap=max_gap

#        f = hdfs.open(filename)
#        self.infile = pysam.AlignmentFile(f)
        self.infile = pysam.AlignmentFile(filename if filename else '-', 'rb')
        self.reads_iter = self.infile.fetch()

        self.eof = False
        
    def next_group(self):

        if self.eof:
            return None, None

        group = None
        con_list = list()

        while group == None:
            try:
                read = self.reads_iter.next()
            except StopIteration:
                read = None
                self.eof = True

            group, con = self.parse_read(read)
           
            if con:
                con_list.extend(con)

        return group, con_list

    def parse_read(self, read):
        
        ovr_ret = con_ret = None
        
        if read == None:
            return (self.group_size, self.prev_ref, self.gmin, self.gmax), None

        if read.flag & 0x4 > 0:
            return None, None

        if read.reference_start - self.gmax > self.max_gap or self.prev_ref != read.reference_name:
            if self.group_size > 0:
                ovr_ret = (self.group_size, self.prev_ref, self.gmin, self.gmax)
            self.gmin = read.reference_start
            self.gmax = read.reference_end
            self.group_nm += 1
            self.group_size = 0

        else:
            self.gmax = max(self.gmax, read.reference_end)
        
        if read.flag & 0x900 == 0:
            self.group_size += 1

        self.prev_ref = read.reference_name
        
        if read.get_tag(MULTIMAP_FLAG) > 1:
            neighbors = self.multimappers.get(read.query_name, None)
            if not neighbors:
                neighbors = self.multimappers[read.query_name] = set([self.group_nm])

            if not self.group_nm in neighbors:

                con_ret = list()
                for g in sorted(neighbors):
                    if not (g, self.group_nm) in self.connections:
                        con_ret.append((g, self.group_nm))
                        self.connections.add((g, self.group_nm))

                neighbors.add(self.group_nm)

        return ovr_ret, con_ret
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('in_fname', default=None, nargs='?', type=str, help="Position sorted SAM file, or stdin if not included.")
    parser.add_argument('-o', dest='out_dir', default='hdfs:///', type=str, help="Output directory. Default='hdfs:///'")
    parser.add_argument('-g', dest='max_gap', default=100, type=int, help="Maximum gap between two reads for them to be part of same group")
    parser.add_argument('-d', dest='debug', action='store_true', help="Print debug messages to stderr (if -O not also included)")
    
    args = parser.parse_args(sys.argv[1:])

    ovr = OverlapParser(args.in_fname, args.max_gap)

    groupCount = 0
    edge_tuples_strings = []
    while True:
        group, cons = ovr.next_group()
        
        if not group:
            break
        
        groupCount += 1
        edge_tuples_strings.extend([(str(item[0]), str(item[1])) for item in cons])

        
    
#    vertex_ids = set()
#    for item in edge_tuples:
#        vertex_ids.add((str(item[0]),))
#        vertex_ids.add((str(item[1]),))

    vertex_strings = map(str, range(groupCount))
    
    e = sqlContext.createDataFrame(edge_tuples_strings, ['src', 'dst'])
    v = sqlContext.createDataFrame(vertex_strings, ['id'])

    e.rdd.saveAsPickleFile(args.out_dir)
    v.rdd.saveAsPickleFile(args.out_dir)
