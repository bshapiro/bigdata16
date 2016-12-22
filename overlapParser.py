import pysam

MAX_GAP = 100
MULTIMAP_FLAG = "NH"

###################################################################
# Creates groups of overlapping/neighboring reads from BAM file   #
# Contains methods for iterating over groups and extracting reads #
###################################################################
class OverlapParser:
    
    # Constructor
    # Takes filename for BAM file and maximum distance for reads to be in same group
    def __init__(self, filename=None, max_gap=100):
        self.group_size = 0
        self.unique_reads = 0
        self.gmax = 0
        self.gmin = 0
        self.group_nm = 0
        self.multimappers = dict()
        self.connections = set()
        self.prev_ref = None
        self.max_gap = max_gap
        self.filename = filename
        self.infile = pysam.AlignmentFile(filename, 'rb')
        self.reads_iter = self.infile.fetch()
        self.eof = False
    
    # Returns tuple of next group in list and any new edges between groups
    def next_group(self):
        
        #File contains no more reads
        if self.eof:
            return None, None

        group = None
        con_list = list()
        
        #Keep parsing reads until next group is found
        while group is None:

            #Get next read
            try:
                read = self.reads_iter.next()
            except StopIteration:
                read = None
                self.eof = True
            
            #Parse read
            #Group will be None unless this read ends a group
            group, con = self.parse_read(read)

            #Add new edge if any returned
            if con:
                con_list.extend(con)

        return group, con_list

    #Returns all SAM lines contained in a group
    def get_group(self, group):
        lines = list()
        _, ref, st, en = group
        for read in self.infile.fetch(ref, st, en):
            if not read.is_unmapped:
                lines.append(read.tostring(self.infile))
        return lines

    #Writes SAM/BAM lines from a list of groups to a file
    def write_groups(self, groups, filename):
        outfile = pysam.AlignmentFile(filename, "wb", template=self.infile)

        if type(groups[0]) != tuple:
            groups = [groups]

        for size, ref, st, en in groups:
            for read in self.infile.fetch(ref, st, en):
                outfile.write(read)

        outfile.close()

    #Parses a SAM/BAM file read and returns group information
    #Returns new group if read is outside of the current group (new group will be started)
    #Also returns edges if a read is multimapped or mated with a read in another group
    def parse_read(self, read):

        #Return values
        ovr_ret = con_ret = None

        #Return current group if no read provided
        if not read:
            return (self.unique_reads, self.prev_ref, self.gmin, self.gmax), None

        #No new group or connection if read unmapped
        if read.is_unmapped:
            return None, None

        #Check if read is outside of current group
        if read.reference_start - self.gmax > self.max_gap or self.prev_ref != read.reference_name:

            #Set group return value if it contains any reads
            #Reset group number and read counts
            if self.group_size > 0:
                ovr_ret = (self.unique_reads, self.prev_ref, self.gmin, self.gmax)
                self.group_nm += 1
                self.group_size = 0
                self.unique_reads = 0

            #Set new group bounds
            self.gmin = read.reference_start
            self.gmax = read.reference_end

        #Update current group bounds
        else:
            self.gmax = max(self.gmax, read.reference_end)

        #Add read to group size
        self.group_size += 1

        #Update number of primary alignments
        if read.flag & 0x900 == 0:
            self.unique_reads += 1

        #Update previous reference (chromosome) value
        #Used to keep track of group bounds
        self.prev_ref = read.reference_name

        #Check if read is multimapped - need to keep track of edges
        if read.get_tag(MULTIMAP_FLAG) > 1:

            #Find current read edges if they exist
            neighbors = self.multimappers.get(read.query_name, None)
            if not neighbors:
                neighbors = self.multimappers[read.query_name] = set([self.group_nm])

            #Create new group edge if it doesn't exist
            if not self.group_nm in neighbors:
                
                #Create edge for each matching read
                con_ret = list()
                for g in sorted(neighbors):
                    if not (g, self.group_nm) in self.connections:
                        con_ret.append((g, self.group_nm))
                        self.connections.add((g, self.group_nm))
                neighbors.add(self.group_nm)

        return ovr_ret, con_ret
