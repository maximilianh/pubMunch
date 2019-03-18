"""
Object with chromosome information that can be loaded from a variety of
sources.
"""
# Copyright 2006-2012 Mark Diekhans
from pm_pycbio.sys.procOps import callProc
from pm_pycbio.tsv import TabFile

class ChromInfo(object):
    "object with chromosome information"
    def __init__(self, chrom, size):
        self.chrom = chrom
        self.size = size

class ChromInfoTbl(dict):
    "object to manage information about chromosomes"
    
    def __init__(self, chromClass=ChromInfo):
        self.chromClass = chromClass

    def loadChromSizes(self, chromSizes):
        "Load from chrom.sizes file"
        for row in TabFile(chromSizes):
            cs = self.chromClass(row[0], int(row[1]))
            self[cs.chrom] = cs

