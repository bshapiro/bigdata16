class Machine:

    def __init__(self):
        self.load = 0
        self.reads = []

    def assign_reads(self, reads):
        self.reads.extend(reads)
        self.load += len(reads)

    def get_load(self):
        return self.load

    def clear_load(self):
        self.load = 0

    def __cmp__(self, other):
        if self.load < other.get_load():
            return -1
        elif self.load > other.get_load():
            return 1
        else:
            return 0
