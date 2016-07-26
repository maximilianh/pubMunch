# Copyright 2006-2012 Mark Diekhans
"""Module to access output of ClusterGenes"""

from pycbio.hgdata.AutoSql import strArrayType
from pycbio.tsv.TSVReader import TSVReader
from pycbio.sys.MultiDict import MultiDict

def cgBoolParse(val):
    if val == "y":
        return True
    elif val == "n":
        return False
    else:
        raise ValueError("expected y or n: " + val)
    
def cgBoolFormat(val):
    if val == True:
        return "y"
    elif val == False:
        return "n"
    else:
        raise ValueError("expected bool type, got: " + type(val))
cgBoolSpec = (cgBoolParse, cgBoolFormat)

#cluster        table        gene        chrom        txStart        txEnd        strand        hasExonConflicts        hasCdsConflicts        exonConflicts        cdsConflicts
#cluster        table        gene        chrom        txStart        txEnd        strand
typeMap = {
    "cluster": int,
    "chrom": intern,
    "table": intern,
    "strand": intern,
    "txStart": int,
    "txEnd": int,
    "hasExonConflicts": cgBoolSpec,
    "hasCdsConflicts": cgBoolSpec,
    "exonConflicts": strArrayType,
    "cdsConflicts": strArrayType,
}

class Cluster(list):
    """one gene cluster, a list of gene objects from file, A field
    clusterObj is added to each row that links back to this object"""
    def __init__(self, clusterId):
        self.clusterId = clusterId

        self.chrom = None
        self.start = None
        self.end = None
        self.strand = None
        self.tableSet = set()

        # set to None if conflicts were not collected
        self.hasExonConflicts = None
        self.hasCdsConflicts = None

    def add(self, row):
        self.append(row)
        row.clusterObj = self
        if (self.chrom == None):
            self.chrom = row.chrom
            self.start = row.txStart
            self.end = row.txEnd
            self.strand = row.strand
        else:
            self.start = min(self.start, row.txStart)
            self.end = max(self.end, row.txEnd)
        if "hasExonConflicts" in row.__dict__:
            self.hasExonConflicts = row.hasExonConflicts
            self.hasCdsConflicts = row.hasCdsConflicts
        self.tableSet.add(row.table)

    def getTableGenes(self, table):
        genes = None
        for g in self:
            if g.table == table:
                if genes == None:
                    genes = []
                genes.append(g)
        return genes

    def __hash__(self):
        return hash(id(self))

    def __eq__(self, other):
        return id(self) == id(other)

    def write(self, fh, trackSet=None):
        "if trackSet is specified, only output genes in this set"
        for gene in self:
            if (trackSet == None) or (gene.table in trackSet):
                gene.write(fh)

class ClusterGenes(list):
    """Object to access output of ClusterGenes.  List of Cluster objects,
    indexed by clusterId.  Note that clusterId is one based, entry 0 is
    None, however generator doesn't return it or other Null clusters.
    """
    def __init__(self, clusterGenesOut):
        self.genes = MultiDict()
        tsv = TSVReader(clusterGenesOut, typeMap=typeMap)
        self.columns = tsv.columns
        self.tableSet = set()
        for gene in tsv:
            self.__addGene(gene)

    def haveCluster(self, clusterId):
        " determine if the specified cluster exists"
        if clusterId >= len(self):
            return False
        return self[clusterId] != None

    def __getCluster(self, clusterId):
        while len(self) <= clusterId:
            self.append(None)
        if self[clusterId] == None:
            self[clusterId] = Cluster(clusterId)
        return self[clusterId]

    def __addGene(self, row):
        cluster = self.__getCluster(row.cluster)
        cluster.add(row)
        self.genes.add(row.gene, row)
        self.tableSet.add(row.table)
        
    def __iter__(self):
        "get generator over non-null clusters"
        return self.generator()

    def generator(self):
        "generator over non-null clusters"
        for cl in list.__iter__(self):
            if cl != None:
                yield cl
