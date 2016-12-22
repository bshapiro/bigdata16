class Machine:
    """
    Machine object for doing load balancing. In future, can extend this object
    to track partitions, partition results, etc.
    """

    def __init__(self, id):
        self.load = 0
        self.reads = []
        self.components = []
        self.id = id

    def assign_reads(self, reads):
        self.reads.extend(reads)
        self.load += len(reads)

    def assign_component(self, component, num_reads):
        self.components.append(component)
        self.load += num_reads

    def get_load(self):
        return self.load

    def clear_load(self):
        self.load = 0

    def get_id(self):
        return self.id

    def __cmp__(self, other):
        if self.load < other.get_load():
            return -1
        elif self.load > other.get_load():
            return 1
        else:
            return 0
