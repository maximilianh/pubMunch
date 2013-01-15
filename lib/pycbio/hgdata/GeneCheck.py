# Copyright 2006-2012 Mark Diekhans
from pycbio.tsv.TSVRow import TSVRow
from pycbio.tsv.TSVTable import TSVTable
from pycbio.tsv.TSVReader import TSVReader
from pycbio.hgdata.AutoSql import strArrayType
from pycbio.sys.Enumeration import Enumeration

def statParse(val):
    "parse a value of the stat column (ok or err)"
    if val == "ok":
        return True
    elif val == "err":
        return False
    else:
        raise ValueError("invalid stat column value: \"" + val + "\"")

def statFmt(val):
    "format a value of the stat column (ok or err)"
    if val:
        return "ok"
    else:
        return "err"
statType = (statParse, statFmt)

def startStopParse(val):
    "parse value of the start or stop columns (ok or no)"
    if val == "ok":
        return True
    elif val == "no":
        return False
    else:
        raise ValueError("invalid start/stop column value: \"" + val + "\"")

def startStopFmt(val):
    "format value of the start or stop columns (ok or no)"
    if val:
        return "ok"
    else:
        return "no"
startStopType = (startStopParse, startStopFmt)


def nmdParse(val):
    "parse value of the NMD column (ok or nmd)"
    if val == "ok":
        return True
    elif val == "nmd":
        return False
    else:
        raise ValueError("invalid nmd column value: \"" + val + "\"")

def nmdFmt(val):
    "format value of the NMD column (ok or nmd)"
    if val:
        return "ok"
    else:
        return "nmd"
nmdType = (nmdParse, nmdFmt)


# frame status
Frame = Enumeration("FrameStat",
                    ["ok", "bad", "mismatch", "discontig", "noCDS"])


#acc	chr	chrStart	chrEnd	strand	stat	frame	start	stop	orfStop	cdsGap	cdsMult3Gap	utrGap	cdsUnknownSplice	utrUnknownSplice	cdsNonCanonSplice	utrNonCanonSplice	numExons	numCds	numUtr5	numUtr3	numCdsIntrons	numUtrIntrons	nmd	causes
typeMap = {"acc": intern,
           "chrStart": int,
           "chrEnd": int,
           "strand": intern,
           "stat": statType,
           "frame": Frame,
           "start": startStopType,
           "stop": startStopType,
           "orfStop": int,
           "cdsGap": int,
           "cdsMult3Gap": int,
           "utrGap": int,
           "cdsUnknownSplice": int,
           "utrUnknownSplice": int,
           "cdsNonCanonSplice": int,
           "utrNonCanonSplice": int,
           "cdsSplice": int,   # old column
           "utrSplice": int,   # old column
           "numExons": int,
           "numCds": int,
           "numUtr5": int,
           "numUtr3": int,
           "numCdsIntrons": int,
           "numUtrIntrons": int, 
           "nmd": nmdType, 
           "causes": strArrayType
    }


class GeneCheckReader(TSVReader):
    def __init__(self, fileName, isRdb=False):
        TSVReader.__init__(self, fileName, typeMap=typeMap, isRdb=isRdb)

class GeneCheckTbl(TSVTable):
    """Table of GeneCheck objects loaded from a TSV or RDB.  acc index is build
    """
    
    def __init__(self, fileName, isRdb=False, idIsUniq=False):
        self.idIsUniq = idIsUniq
        if idIsUniq:
            uniqKeyCols="acc"
            multiKeyCols=None
        else:
            uniqKeyCols=None
            multiKeyCols="acc"
        TSVTable.__init__(self, fileName, typeMap=typeMap, isRdb=isRdb, uniqKeyCols=uniqKeyCols, multiKeyCols=multiKeyCols)
        self.idIndex = self.indices.acc

    def _sameLoc(self, chk, chrom, start, end):
        return (chk != None) and (chk.chr == chrom) and (chk.chrStart == start) and (chk.chrEnd == end)

    def getByGeneLoc(self, id, chrom, start, end):
        "get check record by id and location, or None if not found"
        if self.idIsUniq:
            chk = self.idIndex.get(id)
            if self._sameLoc(chk, chrom, start, end):
                return chk
        else:
            for chk in self.idIndex.get(id):
                if self._sameLoc(chk, chrom, start, end):
                    return chk
        return None
            
    def getByGenePred(self, gp):
        return self.getByGeneLoc(gp.name, gp.chrom, gp.txStart, gp.txEnd)
