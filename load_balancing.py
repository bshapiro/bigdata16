from machine import Machine
from Queue import PriorityQueue


def create_machines(num_machines):
    machines = PriorityQueue()
    for i in range(num_machines):
        new_machine = Machine(i)
        machines.put(new_machine)
    return machines


def run_greedy_load_balancing(num_machines, component_read_counter):
    machines = create_machines(num_machines)
    component_read_counter_list = component_read_counter.most_common()
    component_to_machine_id = {}
    for component_id, read_count in component_read_counter_list:
        machine = machines.get()
        machine.assign_component(component_id, read_count)
        component_to_machine_id[component_id] = machine.get_id()
        machines.put(machine)
    return component_to_machine_id
