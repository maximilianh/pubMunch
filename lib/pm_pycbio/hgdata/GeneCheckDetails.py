# Copyright 2006-2012 Mark Diekhans
from pm_pycbio.tsv.TSVRow import TSVRow
from pm_pycbio.tsv.TSVTable import TSVTable
from pm_pycbio.tsv.TSVReader import TSVReader
from pm_pycbio.sys.Enumeration import Enumeration
import sys

def strOrNone(val):
    "return None if val is zero length otherwise val"
    if len(val) == 0:
        return None
    else:
        return sys.intern(val)

#acc	problem	info	chr	chrStart	chrEnd
typeMap = {"acc": intern,
           "problem": intern,
           "info": strOrNone,
           "chr": intern,
           "chrStart": int,
           "chrEnd": int}

def cmpByLocation(gcd1, gcd2):
    d = cmp(gcd1.chr, gcd2.chr)
    if d == 0:
        d = cmp(gcd1.chrStart, gcd2.chrStart)
        if d == 0:
            d = cmp(gcd1.chrEnd, gcd2.chrEnd)
    return d

class GeneCheckDetailsReader(TSVReader):
    def __init__(self, fileName, isRdb=False):
        TSVReader.__init__(self, fileName, typeMap=typeMap, isRdb=isRdb)

class GeneCheckDetailsTbl(TSVTable):
    """Table of GeneCheckDetails objects loaded from a TSV.    """
    
    def __init__(self, fileName, isRdb=False):
        TSVTable.__init__(self, fileName, typeMap=typeMap, isRdb=isRdb)
        
