#!/usr/bin/env python2.7
from __future__ import print_function
import sys, optparse, gzip
from collections import defaultdict
from pm_pycbio.hgdata.Psl import Psl
from pm_pycbio.hgdata.PslMap import PslMap
import logging

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
    __slots__ = ("chrom", "chromStart", "chromEnd", "name", "score", "strand",
                 "thickStart", "thickEnd", "itemRgb", "blockCount", "blockSizes",
                 "blockStarts", "mapper", "chromSize")

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
        self.chromSize = 0

    def mapBlock(self, psl, blk, qRngStart, qRngEnd, tRngStart, tRngEnd):
        " callback for pslMap "
        logging.debug("Got blk: %d-%d, len %d" % (tRngStart, tRngEnd, tRngEnd - tRngStart))
        self.blockCount += 1
        self.blockSizes.append(tRngEnd - tRngStart)
        self.blockStarts.append(tRngStart)

    def mapGap(self, psl, prevBlk, nextBlk, qRngStart, qRngEnd, tRngStart, tRngEnd):
        " call back pslMap "
        pass

    def mapQuery(self, psl, qRngStart, qRngEnd):
        " call this method to get qRngStart-qRngEnd mapped through the psl as a bed "
        # must be reversed somewhere upstream
        assert(psl.strand[0] == "+")
        # if psl.strand=="-":
            # the +1 here is weird but seems to be true
            # end   = mapPsl.qSize - rnaVar.start + 1
            # start = mapPsl.qSize - rnaVar.end + 1
            # logging.debug("Inversing map coordinates to %d/%d" % (start, end))
            # logging.debug("Reversing map psl")
            # psl = psl.reverseComplement()
        self.chromSize = psl.tSize
        self.mapper.queryToTargetMap(psl, qRngStart, qRngEnd)
        self.chrom = psl.tName
        self.name = psl.qName
        self.strand = psl.strand

    def getBed(self, name=None):
        " return bed feature as a 12-tuple, default name is query name "
        if len(self.blockStarts) == 0:
            return None
        if self.strand[-1] == "-":
            logging.debug("Reversing coords")
            self.blockStarts.reverse()
            self.blockSizes.reverse()
            # pslToBed.c:
            #    for (i=0; i<blockCount; ++i)
            #       chromStarts[i] = chromSize - chromStarts[i] - bed->blockSizes[i];
            chromSize = self.chromSize
            chromStarts = self.blockStarts
            blockSizes = self.blockSizes
            for i in range(0, len(self.blockStarts)):
                self.blockStarts[i] = chromSize - chromStarts[i] - blockSizes[i]
        self.chromStart = self.blockStarts[0]
        self.chromEnd = self.blockStarts[-1] + self.blockSizes[-1]

        self.thickStart = self.chromStart
        self.thickEnd = self.chromEnd
        self.score = sum(self.blockSizes)
        blockSizeStr = ",".join([str(x) for x in self.blockSizes])

        chromStart = self.chromStart
        blockStartStr = ",".join([str(x - chromStart) for x in self.blockStarts])

        # default bed name is query name
        if not name:
            name = self.name

        bedRow = [self.chrom, self.chromStart, self.chromEnd, name, \
                self.score, self.strand[-1], self.thickStart, self.thickEnd, \
                self.itemRgb, len(self.blockSizes), blockSizeStr, blockStartStr]
        bedRow = [str(x) for x in bedRow]
        return bedRow

def parseBed(fname):
    " yield chrom, start, end, name of bed file "
    for line in open(fname):
        f = line.strip().split("\t")
        yield f[0], int(f[1]), int(f[2]), f[3]

def test():
    logging.basicConfig(level=1)
    l = """5148 0  415 0 0 0  27 208231 - NM_017651 5564 0 5563 chr6 171115067 135605109 135818903 28      1676,103,59,98,163,56,121,27,197,141,131,119,107,230,124,133,153,186,96,193,220,182,560,54,125,64,62,183,      1,1677,1780,1839,1937,2100,2156,2277,2304,2501,2642,2773,2892,2999,3229,3353,3486,3639,3825,3921,4114,4334,4516,5076,5130,5255,5319,5381,      135605109,135611560,135621637,135639656,135644299,135679269,135715913,135726088,135732485,135748304,135749766,135751019,135752345,135754164,135759512,135763719,135768145,135769427,135774478,135776871,135778631,135784262,135786951,135788718,135811760,135813365,135818325,135818720,"""
    mapPsl = Psl(l.split())
    mapper = PslMapBedMaker()
    mapper.mapQuery(mapPsl, 0, 500)
    newBed = " ".join(mapper.getBed())
    exp = "chr6 135787499 135818903 NM_017651 500 - 135787499 135818903 0 6 12,54,125,64,62,183 0,1219,24261,25866,30826,31221"
    assert(exp == newBed)

if __name__ == '__main__':
    parser = optparse.OptionParser("usage: %prog [options] inBed mapPsl outBed - map bed features through psl")
    parser.add_option("-t", "--test", dest="test", action="store_true", help="run tests")
    (options, args) = parser.parse_args()
    if options.test:
        test()
        sys.exit(0)

    if args == []:
        parser.print_help()
        sys.exit(1)
    inBedFname, mapPslFname, outBedFname = args
    ofh = open(outBedFname, "w") if outBedFname != "stdout" else sys.stdout
    psls = indexPsls(mapPslFname)

    mapper = PslMapBedMaker()
    for chrom, start, end, name, in parseBed(inBedFname):
        for psl in psls.get(chrom, []):
            newBed = mapper.mapQuery(psl, start, end)
            if newBed != None:
                ofh.write("\t".join(newBed) + "\n")
            mapper.clear()

    print("output written to %s" % outBedFname)
