from graphframes import GraphFrame
from cPickle import dump, load
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from collections import Counter
from Queue import PriorityQueue
from machine import Machine


def run_connected_components(edge_tuples, vertex_ids):
    vertex_strings = list(vertex_ids)
    edge_tuples_strings = [(str(item[0]), str(item[1])) for item in edge_tuples]

    e = sqlContext.createDataFrame(edge_tuples_strings, ['src', 'dst'])
    v = sqlContext.createDataFrame(vertex_strings, ['id'])
    g = GraphFrame(v, e)
    result = g.connectedComponents()
    return result


def gen_vertex_ids(edge_tuples):
    ids = set()
    for item in edge_tuples:
        ids.add((str(item[0]),))
        ids.add((str(item[1]),))
    return ids


def gen_read_counts(cc_result, vertex_reads):
    mapped_result = cc_result.flatMap(lambda row: [(row[1], vertex_reads[row[0]][i]) for i in range(len(vertex_reads[row[0]]))])
    return component_read_counter, component_read_map


def create_machines(num_machines):
    machines = PriorityQueue()
    for i in range(num_machines):
        new_machine = Machine()
        machines.put(new_machine)
    return machines


def run_greedy_load_balancing(num_machines, component_read_counter, component_read_map):
    machines = create_machines(num_machines)
    component_read_counter_list = component_read_counter.most_common()
    for component_id, read_count in component_read_counter_list:
        component_reads = component_read_map[component_id]
        machine = machines.get()
        machine.assign_reads(component_reads)
        machines.put(machine)
    return list(machines.queue)


if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    num_vertices, edge_tuples = load(open('pickles/connections.pkl', 'rb'))
    vertex_reads = load(open('pickles/ovr_list.pkl', 'rb'))

    vertex_ids = gen_vertex_ids(edge_tuples)
    cc_result = run_connected_components(edge_tuples, vertex_ids)

    component_read_counter, component_read_map = gen_read_counts(cc_result, vertex_reads)
    machines = run_greedy_load_balancing(5, component_read_counter, component_read_map)
    dump(machines, open('machines.dump', 'wb'))
