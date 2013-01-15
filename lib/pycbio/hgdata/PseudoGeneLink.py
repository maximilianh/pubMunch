# Copyright 2006-2012 Mark Diekhans
""" Object to load PseudoGeneLink table"""

from pycbio.tsv.TSVTable import TSVTable
from pycbio.hgdata.AutoSql import intArrayType

class PseudoGeneLink(TSVTabl):
    """TSV of PseudoGeneLink table"""

    _typeMap = {"chrom": str, "name": str, "strand": str,
                "blockSizes": intArrayType,
                "chromStarts": intArrayType,
                "type": str,
                "gChrom": str, "gStrand": str,
                "intronScores": intArrayType,
                "refSeq": str, "mgc": str, "kgName": str,
                "overName": str, "overStrand": str,
                "adaBoost": float, "posConf": float, "negConf": float}
    
    def __init__(self, pglFile):
        TSV.__init__(self, pglFile, multiKeyCols="name", typeMap=PseudoGeneLink._typeMap, defaultColType=int)

    def getNameIter(self, name):
        """get iter over rows for name"""
        return self.indices.name.iterkeys()
