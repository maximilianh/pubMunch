# Copyright 2006-2012 Mark Diekhans
from pycbio.tsv.TabFile import TabFile, TabFileReader
from pycbio.hgdata.AutoSql import intArraySplit, intArrayJoin
from pycbio.sys.MultiDict import MultiDict


# FIXME: not complete

class Bed(object):
    """Object wrapper for a parsing a BED record"""

    class Block(object):
        __slots__ = ("relStart", "size", "start", "end")
        def __init__(self, relStart, size, start):
            self.relStart = relStart
            self.size = size
            self.start = start
            self.end = start+size

        def __str__(self):
            return str(self.start) + "-" + str(self.end) + "[+" + str(self.relStart) + "/" + str(self.size) + "]"

    def __init__(self, row):
        self.numCols = len(row)
        self.chrom = row[0]
        self.chromStart = int(row[1])
        self.chromEnd = int(row[2])
        self.name = row[3]
        if self.numCols > 4:
            self.score = int(row[4])
        else:
            self.score = None
        if self.numCols > 5:
            self.strand = row[5]
        else:
            self.strand = None
        if self.numCols > 7:
            self.thickStart = int(row[6])
            self.thickEnd = int(row[7])
        else:
            self.thickStart = None
            self.thickEnd = None
            
        if self.numCols > 8:
            self.itemRgb = row[8]
        else:
            self.itemRgb = None
        if self.numCols > 11:
            sizes = intArraySplit(row[10])
            relStarts = intArraySplit(row[11])
            self.blocks = []
            for i in range(len(relStarts)):
                self.blocks.append(Bed.Block(relStarts[i], sizes[i], self.chromStart+relStarts[i]))
        else:
            self.blocks = None

    def getRow(self):
        row = [self.chrom, str(self.chromStart), str(self.chromEnd), self.name]
        if self.numCols > 4:
            row.append(str(self.score));
        if self.numCols > 5:
            row.append(self.strand)
        if self.numCols > 7:
            row.append(str(self.thickStart))
            row.append(str(self.thickEnd))
        if self.numCols > 8:
            row.append(str(self.itemRgb))
        if self.numCols > 11:
            row.append(str(len(self.blocks)))
            relStarts = []
            sizes = []
            for blk in self.blocks:
                relStarts.append(str(blk.relStart))
                sizes.append(str(blk.size))
            row.append(intArrayJoin(self.relStart))
            row.append(intArrayJoin(self.sizes))
        return row

    def __str__(self):
        "return BED as a tab-separated string"
        return str.join("\t", self.getRow())
        
    def write(self, fh):
        """write BED to a tab-seperated file"""
        fh.write(str(self))
        fh.write('\n')        

class BedReader(TabFileReader):
    """Reader for BED objects loaded from a tab-file"""

    def __init__(self, fileName):
        TabFileReader.__init__(self, fileName, rowClass=Bed, hashAreComments=True, skipBlankLines=True)

class BedTbl(TabFile):
    """Table of BED objects loaded from a tab-file
    """

    def _mkNameIdx(self):
        self.nameMap = MultiDict()
        for bed in self:
            self.nameMap.add(bed.name, bed)

    def __init__(self, fileName, nameIdx=False):
        TabFile.__init__(self, fileName, rowClass=Bed, hashAreComments=True)
        self.nameMap = None
        if nameIdx:
            self._nameIdx()

