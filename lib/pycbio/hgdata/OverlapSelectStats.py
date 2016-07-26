# Copyright 2006-2012 Mark Diekhans
from pycbio.tsv.TSVTable import TSVTable
from pycbio.tsv.TSVReader import TSVReader
#inId        selectId        inOverlap        selectOverlap        overBases        similarity
typeMap =  {
    "inOverlap": float,
    "selectOverlap": float,
    "overBases": int,
    "similarity": float
    }

class OverlapSelectStatsReader(TSVReader):
    "reader for output from overlapSelect -statsOutput"

    def __init__(self, fileName):
        TSVReader.__init__(self, fileName, typeMap=typeMap)

class OverlapSelectStatsTbl(TSVTable):
    "table of overlapSelect -statsOutput results"
    def __init__(self, fileName):
        TSVTable.__init__(self, fileName, typeMap=typeMap, multiKeyCols=("inId","selectId"))
        
__all__ = [OverlapSelectStatsReader.__name__, OverlapSelectStatsTbl.__name__]
