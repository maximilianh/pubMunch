#!/usr/bin/env python2.7
import sys, optparse, gzip
from collections import defaultdict
from pycbio.hgdata.Psl import Psl
from pycbio.hgdata.PslMap import PslMap

def indexPsls(fname, isProt=False):
    " parse Psls and index by query name, return as a dict query -> list of Psl objects "
    psls = defaultdict(list)
    if fname.endswith(".gz"):
        ifh = gzip.open(fname)
    else:
        ifh = open(fname)
    for line in ifh:
        psl = Psl(line.strip("\n").split("\t"))
        if isProt:
            psl.protToNa()
        psls[psl.qName].append(psl)
    return psls
        
class PslMapBedMaker(object):
    " object that collects blocks from pslMap and creates a bed in the end"
    __slots__ = ("chrom", "chromStart", "chromEnd", "name", "score", "strand", "thickStart", "thickEnd", "itemRgb", "blockCount", "blockSizes", "blockStarts", "mapper")

    def __init__(self):
        self.mapper = PslMap(self)
        self.clear()

    def clear(self):
        " reset for next mapping "
        self.chrom = None
        self.chromStart = None
        self.chromEnd = None
        self.name = None
        self.score = 0
        self.strand = None
        self.thickStart = None
        self.thickEnd = None
        self.itemRgb = "0"
        self.blockCount = 0
        self.blockSizes = []
        self.blockStarts = []

    def mapBlock(self, psl, blk, qRngStart, qRngEnd, tRngStart, tRngEnd):
        " callback for pslMap "
        self.chromEnd = tRngEnd
        self.blockCount += 1
        self.blockSizes.append(tRngEnd-tRngStart)
        self.blockStarts.append(tRngStart)

    def mapGap(self, psl, prevBlk, nextBlk, qRngStart, qRngEnd, tRngStart, tRngEnd):
        " call back pslMap "
        pass

    def mapQuery(self, psl, qRngStart, qRngEnd):
        " call this method to get qRngStart-qRngEnd mapped through the psl as a bed "
        self.mapper.queryToTargetMap(psl, qRngStart, qRngEnd)
        self.chrom = psl.tName
        self.name = psl.qName
        self.strand = psl.strand

    def getBed(self, name=None):
        " return bed feature as a 12-tuple, default name is query name "
        if len(self.blockStarts)==0:
            return None
        self.chromStart = self.blockStarts[0]
        self.thickStart = self.chromStart
        self.thickEnd = self.chromEnd
        self.score = sum(self.blockSizes)
        blockSizeStr = ",".join([str(x) for x in self.blockSizes])

        chromStart = self.chromStart
        blockStartStr = ",".join([str(x-chromStart) for x in self.blockStarts])

        # default bed name is query name
        if not name:
            name = self.name

        bedRow = [self.chrom, self.chromStart, self.chromEnd, name, \
                self.score, self.strand, self.thickStart, self.thickEnd, \
                self.itemRgb, len(self.blockSizes), blockSizeStr, blockStartStr]
        bedRow = tuple([str(x) for x in bedRow])
        return bedRow

def parseBed(fname):
    " yield chrom, start, end, name of bed file "
    for line in open(fname):
        f = line.strip().split("\t")
        yield f[0], int(f[1]), int(f[2]), f[3]

if __name__ == '__main__':
    parser = optparse.OptionParser("usage: %prog [options] inBed mapPsl outBed - map bed features through psl")
    #parser.add_option("-f", "--file", dest="file", action="store", help="run on file") 
    (options, args) = parser.parse_args()
    if args==[]:
        parser.print_help()
        sys.exit(1)
    inBedFname, mapPslFname, outBedFname = args
    ofh = open(outBedFname, "w") if outBedFname!="stdout" else sys.stdout
    psls = indexPsls(mapPslFname)

    mapper = PslMapBedMaker()
    for chrom, start, end, name, in parseBed(inBedFname):
        for psl in psls.get(chrom, []):
            newBed = mapper.mapQuery(psl, start, end)
            if newBed!=None:
                ofh.write("\t".join(newBed)+"\n")
            mapper.clear()

    print("output written to %s" % outBedFname)
