# Copyright 2006-2012 Mark Diekhans
import copy
from pm_pycbio.sys import fileOps, dbOps
from pm_pycbio.hgdata.AutoSql import intArraySplit, intArrayJoin
from pm_pycbio.sys.Enumeration import Enumeration

CdsStat = Enumeration("CdsStat", [
    ("none", "none"),             # No CDS (non-coding)
    ("unknown", "unk"),           # CDS is unknown (coding, but not known)
    ("incomplete", "incmpl"),     # CDS is not complete at this end
    ("complete", "cmpl")])        # CDS is complete at this end

genePredColumns = ("name", "chrom", "strand", "txStart", "txEnd", "cdsStart", "cdsEnd", "exonCount", "exonStarts", "exonEnds", "score", "name2", "cdsStartStat", "cdsEndStat", "exonFrames")
genePredExtColumns = ("name", "chrom", "strand", "txStart", "txEnd", "cdsStart", "cdsEnd", "exonCount", "exonStarts", "exonEnds")

class Range(object):
    "start and end coordinates"
    __slots__ = ("start", "end")
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def __eq__(self, other):
        return (other != None) and (self.start == other.start) and (self.end == other.end)

    def __ne__(self, other):
        return (other == None) or (self.start != other.start) or (self.end != other.end)

    def size(self):
        return self.end - self.start

    def __len__(self):
        return self.end - self.start

    def overlapAmt(self, other):
        maxStart = max(self.start, other.start)
        minEnd = min(self.end, other.end)
        return (minEnd - maxStart) if maxStart < minEnd else 0

    def overlaps(self, other):
        return (self.start < other.end) and (self.end > other.start)

    def overlappingAmt(self, start, end):
        maxStart = max(self.start, start)
        minEnd = min(self.end, end)
        return (minEnd - maxStart) if maxStart < minEnd else 0

    def overlapping(self, start, end):
        return (self.start < end) and (self.end > start)

    def __str__(self):
        return str(self.start) + ".." + str(self.end)

    def reverse(self, chromSize):
        "get a range on the opposite strand"
        return Range(chromSize - self.end, chromSize - self.start)

class ExonFeatures(object):
    "object the holds the features of a single exon"
    __slots__ = ("utr5", "cds", "utr3")
        
    def __init__(self, utr5=None, cds=None, utr3=None):
        self.utr5 = utr5
        self.cds = cds
        self.utr3 = utr3

    def __str__(self):
        return "utr5=" + str(self.utr5) + " cds=" + str(self.cds) + " utr3=" + str(self.utr3)

class Exon(object):
    "an exon in a genePred annotation"
    __slots__ = ("gene", "iExon", "start", "end", "frame")

    def __init__(self, gene, iExon, start, end, frame=None):
        self.gene = gene
        self.iExon = iExon
        self.start = start
        self.end = end
        self.frame = frame

    def __str__(self):
        s = str(self.start) + "-" + str(self.end)
        if self.frame != None:
            s += "/" + str(self.frame)
        return s

    def getCdsExonIdx(self):
        if (self.gene.cdsStartIExon != None) and (self.gene.cdsStartIExon <= self.iExon) and (self.iExon <= self.gene.cdsEndIExon):
            return self.iExon - self.gene.cdsStartIExon
        else:
            return None

    def getCds(self):
        "get the start, end (Range object) of cds in this exon, or None if no CDS"
        cdsSt = self.start
        if cdsSt < self.gene.cdsStart:
            cdsSt = self.gene.cdsStart
        cdsEnd = self.end
        if cdsEnd > self.gene.cdsEnd:
            cdsEnd = self.gene.cdsEnd
        if cdsSt >= cdsEnd:
            return None
        else:
            return Range(cdsSt, cdsEnd)

    def featureSplit(self):
        """split exon into a length-3 tuple in the form (utr5, cds, utr3)
`        where each element is either None, or (start, end).  utr5 is 5' UTR
        in the exon, utr3, is 3'UTR, and cds is coding sequence of the exon."""
        feats = ExonFeatures()

        # UTR before CDS in exon
        start = self.start
        if (start < self.gene.cdsStart):
            end = min(self.end, self.gene.cdsStart)
            if self.gene.inDirectionOfTranscription():
                feats.utr5 = Range(start, end)
            else:
                feats.utr3 = Range(start, end)
            start = end

        # CDS in exon
        if (start < self.end) and (start < self.gene.cdsEnd):
            end = min(self.end, self.gene.cdsEnd)
            feats.cds = Range(start, end)
            start = end

        # UTR after CDS in exon
        if (start < self.end) and (start >= self.gene.cdsEnd):
            if self.gene.inDirectionOfTranscription():
                feats.utr3 = Range(start, self.end)
            else:
                feats.utr5 = Range(start, self.end)

        return feats

    def contains(self, pos):
        "does exon contain pos?"
        return (self.start <= pos) and (pos < self.end)

    def overlaps(self, start, end):
        "does exon overlap range?"
        return (self.start < end) and (self.end > start)

    def size(self):
        "size of the exon"
        return self.end-self.start

    def getRelCoords(self, chromSize):
        "get a range object of strand-relative coordinates"
        if self.gene.inDirectionOfTranscription():
            return Range(self.start, self.end)
        else:
            return Range(chromSize - self.end, chromSize - self.start)

class GenePred(object):
    """Object wrapper for a genePred"""

    def __buildExons(self, exonStarts, exonEnds, exonFrames):
        "build array of exon objects"
        self.exons = []
        frame = None
        self.cdsStartIExon = None
        self.cdsEndIExon = None
        for i in xrange(len(exonStarts)):
            if exonFrames != None:
                frame = exonFrames[i]
            self.addExon(exonStarts[i], exonEnds[i], frame)

    def __initParse(self, row):
        self.name = row[0]
        self.chrom = row[1]
        self.strand = row[2]
        self.strandRel = False    # are these strand-relative coordinates
        self.txStart = int(row[3])
        self.txEnd = int(row[4])
        self.cdsStart = int(row[5])
        self.cdsEnd = int(row[6])
        exonStarts = intArraySplit(row[8])
        exonEnds = intArraySplit(row[9])
        iCol = 10
        numCols = len(row)
        self.score = None
        if iCol < numCols:  # 10
            self.score = int(row[iCol])
            iCol = iCol+1
        self.name2 = None
        if iCol < numCols:  # 11
            self.name2 = row[iCol]
            iCol = iCol+1
        self.cdsStartStat = None
        self.cdsEndStat = None
        if iCol < numCols:  # 12,13
            self.cdsStartStat = CdsStat(row[iCol])
            self.cdsEndStat = CdsStat(row[iCol+1])
            iCol = iCol+2
        exonFrames = None
        self.hasExonFrames = False
        if iCol < numCols:  # 14
            exonFrames = intArraySplit(row[iCol])
            iCol = iCol+1
            self.hasExonFrames = True
        self.__buildExons(exonStarts, exonEnds, exonFrames)

    @staticmethod
    def __colOrNone(row, dbColIdxMap, colName, typeCnv):
        idx = dbColIdxMap.get(colName)
        return None if (idx == None) else typeCnv(row[idx])

    def __initDb(self, row, dbColIdxMap):
        self.name = row[dbColIdxMap["name"]]
        self.chrom = row[dbColIdxMap["chrom"]]
        self.strand = row[dbColIdxMap["strand"]]
        self.strandRel = False    # are these strand-relative coordinates
        self.txStart = int(row[dbColIdxMap["txStart"]])
        self.txEnd = int(row[dbColIdxMap["txEnd"]])
        self.cdsStart = int(row[dbColIdxMap["cdsStart"]])
        self.cdsEnd = int(row[dbColIdxMap["cdsEnd"]])
        exonStarts = intArraySplit(row[dbColIdxMap["exonStarts"]])
        exonEnds = intArraySplit(row[dbColIdxMap["exonEnds"]])
        self.score = self.__colOrNone(row, dbColIdxMap, "score", int)
        self.name2 = self.__colOrNone(row, dbColIdxMap, "name2", str)
        self.cdsStartStat = self.__colOrNone(row, dbColIdxMap, "cdsStartStat", CdsStat)
        self.cdsEndStat = self.__colOrNone(row, dbColIdxMap, "cdsEndStat", CdsStat)
        exonFrames = self.__colOrNone(row, dbColIdxMap, "exonFrames", intArraySplit)
        self.hasExonFrames = (exonFrames != None)
        self.__buildExons(exonStarts, exonEnds, exonFrames)

    def __initEmpty(self):
        self.name = None
        self.chrom = None
        self.strand = None
        self.strandRel = False    # are these strand-relative coordinates
        self.txStart = None
        self.txEnd = None
        self.cdsStart = None
        self.cdsEnd = None
        self.score = None
        self.name2 = None
        self.cdsStartStat = None
        self.cdsEndStat = None
        self.hasExonFrames = False
        self.exons = []
        self.cdsStartIExon = None
        self.cdsEndIExon = None

    def __initClone(self, gp):
        self.name = gp.name
        self.chrom = gp.chrom
        self.strand = gp.strand
        self.strandRel = gp.strandRel
        self.txStart = gp.txStart
        self.txEnd = gp.txEnd
        self.cdsStart = gp.cdsStart
        self.cdsEnd = gp.cdsEnd
        self.score = gp.score
        self.name2 = gp.name2
        self.cdsStartStat = gp.cdsStartStat
        self.cdsEndStat = gp.cdsEndStat
        self.hasExonFrames = gp.hasExonFrames
        self.exons = copy.deepcopy(gp.exons)
        self.cdsStartIExon = gp.cdsStartIExon
        self.cdsEndIExon = gp.cdsEndIExon
        
    def __initSwapToOtherStrand(self, gp, chromSize):
        "swap coordinates to other strand"
        self.name = gp.name
        self.chrom = gp.chrom
        self.strand = gp.strand
        self.strandRel = True
        self.txStart = chromSize - gp.txEnd
        self.txEnd = chromSize - gp.txStart
        self.cdsStart = chromSize - gp.cdsEnd
        self.cdsEnd = chromSize - gp.cdsStart
        self.score = gp.score
        self.name2 = gp.name2
        self.cdsStartStat = gp.cdsEndStat
        self.cdsEndStat = gp.cdsStartStat
        self.hasExonFrames = gp.hasExonFrames
        self.exons = []
        self.cdsStartIExon = None
        self.cdsEndIExon = None
        for exon in reversed(gp.exons):
            self.addExon(chromSize - exon.end, chromSize - exon.start, exon.frame)

    def __init__(self, row=None, dbColIdxMap=None, noInitialize=False):
        "If row is not None, parse a row, otherwise initialize to empty state"
        if dbColIdxMap != None:
            self.__initDb(row, dbColIdxMap)
        elif row != None:
            self.__initParse(row)
        elif not noInitialize:
            self.__initEmpty()

    def getStrandRelative(self, chromSize):
        """create a copy of this GenePred object that has strand relative
        coordinates."""
        gp = GenePred(noInitialize=True)
        if self.inDirectionOfTranscription():
            gp.__initClone(self)
            gp.strandRel = True
        else:
            gp.__initSwapToOtherStrand(self, chromSize)
        return gp

    def inDirectionOfTranscription(self):
        "are exons in the direction of transcriptions"
        return (self.strand == "+") or self.strandRel

    def addExon(self, exonStart, exonEnd, frame=None):
        "add an exon; which must be done in assending order"
        i = len(self.exons)
        self.exons.append(Exon(self, i, exonStart, exonEnd, frame))
        if (self.cdsStart < self.cdsEnd) and (exonStart < self.cdsEnd) and (exonEnd > self.cdsStart):
            if self.cdsStartIExon == None:
                self.cdsStartIExon = i
            self.cdsEndIExon = i

    def assignFrames(self):
        "set frames on exons, assuming no frame shift"
        if self.inDirectionOfTranscription():
            iStart = 0
            iEnd = len(self.exons)
            iDir = 1
        else:
            iStart = len(self.exons)-1
            iEnd = -1
            iDir = -1
        cdsOff = 0
        for i in xrange(iStart, iEnd, iDir):
            e = self.exons[i]
            c = e.getCds()
            if c != None:
                e.frame = cdsOff%3
                cdsOff += (c.end-c.start)
            else:
                e.frame = -1
        self.hasExonFrames = True

    def inCds(self, pos):
        "test if a position is in the CDS"
        return (self.cdsStart <= pos) and (pos < self.cdsEnd)

    def overlapsCds(self, startOrRange, end=None):
        "test if a position is in the CDS. startOrRange is a Range or Exon if end is None"
        if end == None:
            return (startOrRange.start < self.cdsEnd) and (startOrRange.end > self.cdsStart)
        else:
            return (startOrRange < self.cdsEnd) and (end > self.cdsStart)

    def sameCds(self, gene2):
        "test if another gene has the same CDS as this gene"
        if id(self) == id(gene2):
            return True # same object
        if (gene2.chrom != self.chrom) or (gene2.strand != self.strand) or (gene2.cdsStart != self.cdsStart) or (gene2.cdsEnd != self.cdsEnd):
            return False
        nCds1 = self.getNumCdsExons()
        nCds2 = gene2.getNumCdsExons()
        if (nCds1 != nCds2):
            return False

        # check each exon
        checkFrame = self.hasExonFrames and gene2.hasExonFrames
        iCds2 = 0
        for iCds1 in xrange(nCds1):
            exon1 = self.getCdsExon(iCds1)
            exon2 = gene2.getCdsExon(iCds2)
            if exon1.getCds() != exon2.getCds():
                return False
            if checkFrame and (exon1.frame != exon2.frame):
                return False
            iCds2 += 1
        return True

    def hasCds(self):
        return (self.cdsStartIExon != None)

    def getCds(self):
        "get Range of CDS, or None if there isn't any"
        if self.cdsStart < self.cdsEnd:
            return Range(self.cdsStart, self.cdsEnd)
        else:
            return None

    def getLenExons(self):
        "get the total length of all exons"
        l = 0
        for e in self.exons:
            l += e.size()
        return l

    def getLenCds(self):
        "get the total length of CDS"
        l = 0
        for e in self.exons:
            cds = e.getCds()
            if cds != None:
                l += cds.end - cds.start
        return l

    def getSpan(self):
        "get the genomic span (txStart to txEnd length)"
        return self.txEnd - self.txStart

    def getNumCdsExons(self):
        "get the number of exons containing CDS"
        if self.cdsStartIExon == None:
            return 0
        else:
            return (self.cdsEndIExon - self.cdsStartIExon)+1
    
    def getCdsExon(self, iCdsExon):
        "get a exon containing CDS, by CDS exon index"
        return self.exons[self.cdsStartIExon + iCdsExon]

    def getStepping(self):
        """get (start, stop, step) to step through exons in direction of
        transcription"""
        # FIXME this is stupid, just store in both directions
        if self.inDirectionOfTranscription():
            return (0, len(self.exons), 1)
        else:
            return (len(self.exons)-1, -1, -1)

    def getStepper(self):
        """generator to step through exon indexes in direction of
        transcription"""
        if self.inDirectionOfTranscription():
            return xrange(0, len(self.exons), 1)
        else:
            return xrange(len(self.exons)-1, -1, -1)

    def getRow(self):
        row = [self.name, self.chrom, self.strand, str(self.txStart), str(self.txEnd), str(self.cdsStart), str(self.cdsEnd)]
        row.append(str(len(self.exons)))
        starts = []
        ends = []
        for e in self.exons:
            starts.append(e.start)
            ends.append(e.end)
        row.append(intArrayJoin(starts))
        row.append(intArrayJoin(ends))

        hasExt = (self.score != None) or (self.name2 != None) or (self.cdsStartStat != None) or self.hasExonFrames

        if self.score != None:
            row.append(str(self.score))
        elif hasExt:
            row.append("0");
        if self.name2 != None:
            row.append(self.name2)
        elif hasExt:
            row.append("");
        if self.cdsStartStat != None:
            row.append(str(self.cdsStartStat))
            row.append(str(self.cdsEndStat))
        elif hasExt:
            row.append(str(CdsStat.unknown))
            row.append(str(CdsStat.unknown))
        if self.hasExonFrames or  hasExt:
            frames = []
            if self.hasExonFrames:
                for e in self.exons:
                    frames.append(e.frame)
            else:
                for e in self.exons:
                    frames.append(-1)
            row.append(intArrayJoin(frames))
        return row

    def getFeatures(self):
        """Get each exon, split into features; see Exon.featureSplit.
        List is returned in positive strand order"""
        feats = []
        for e in self.exons:
            feats.append(e.featureSplit())
        return feats

    def findContainingExon(self, pos):
        "find the exon contain pos, or None"
        for exon in self.exons:
            if exon.contains(pos):
                return exon
        return None

    def __cdsOverlapCnt(self, gp2):
        "count cds bases that overlap"
        if (self.chrom != gp2.chrom) or (self.strand != gp2.strand):
            return 0
        feats2 = gp2.getFeatures()
        cnt = 0
        for e1 in self.exons:
            f1 = e1.featureSplit()
            if f1.cds != None:
                for f2 in feats2:
                    if f2.cds != None:
                        cnt += f1.cds.overlapAmt(f2.cds)
        return cnt

    def cdsSimilarity(self, gp2):
        "compute similariy of CDS of two genes"
        overCnt = self.__cdsOverlapCnt(gp2)
        if overCnt == 0:
            return 0.0
        else:
            return float(2*overCnt)/float(self.getLenCds()+gp2.getLenCds())

    def cdsCover(self, gp2):
        "compute faction of CDS is covered a gene"
        overCnt = self.__cdsOverlapCnt(gp2)
        if overCnt == 0:
            return 0.0
        else:
            return float(overCnt)/float(self.getLenCds())

    def __str__(self):
        return "\t".join(self.getRow())

    def write(self, fh):
        fh.write(str(self))
        fh.write("\n")
        
class GenePredTbl(list):
    """Table of GenePred objects loaded from a tab-file"""
    def __init__(self, fileName, buildIdx=False, buildUniqIdx=False, buildRangeIdx=False):
        if buildIdx and buildUniqIdx:
            raise Exception("can't specify both buildIdx and buildUniqIdx")
        for row in GenePredReader(fileName):
            self.append(row)
        self.names = None
        self.rangeMap = None
        if buildUniqIdx:
            self.__buildUniqIdx()
        if buildIdx:
            self.__buildIdx()
        if buildRangeIdx:
            self.__buildRangeIdx()

    def __buildUniqIdx(self):
        self.names = dict()
        for row in self:
            if row.name in self.names:
                raise Exception("gene with this name already in index: " + row.name)
            self.names[row.name] = row

    def __buildIdx(self):
        from pm_pycbio.sys.MultiDict import MultiDict
        self.names = MultiDict()
        for row in self:
            self.names.add(row.name, row)

    def __buildRangeIdx(self):
        from pm_pycbio.hgdata.RangeFinder import RangeFinder
        self.rangeMap = RangeFinder()
        for gene in self:
            self.rangeMap.add(gene.chrom, gene.txStart, gene.txEnd, gene, gene.strand)

class GenePredReader(object):
    """Read genePreds from a tab file."""
    def __init__(self, fileName):
        self.fh = fileOps.opengz(fileName)

    def __iter__(self):
        return self

    def next(self):
        "GPR next"
        while True:
            line = self.fh.readline()
            if (line == ""):
                self.fh.close();
                raise StopIteration
            if not ((len(line) == 1) or line.startswith('#')):
                line = line[0:-1]  # drop newline
                return GenePred(line.split("\t"))

class GenePredFhReader(object):
    """Read genePreds from an open."""
    def __init__(self, fh):
        self.fh = fh

    def __iter__(self):
        return self

    def next(self):
        "GPR next"
        while True:
            line = self.fh.readline()
            if (line == ""):
                raise StopIteration
            if not ((len(line) == 1) or line.startswith('#')):
                line = line[0:-1]  # drop newline
                return GenePred(line.split("\t"))

class GenePredDbReader(object):
    """Read genePreds from a db query"""
    def __init__(self, conn, query):
        self.cur = conn.cursor()
        try:
            self.cur.execute(query)
        except:
            try:
                self.cur.close()
            except:
                pass
            raise
        self.colIdxMap = dbOps.cursorColIdxMap(self.cur)

    def __iter__(self):
        return self

    def next(self):
        "GPR next"
        while True:
            row = self.cur.fetchone()
            if row == None:
                self.cur.close()
                self.cur = None
                raise StopIteration
            return GenePred(row, dbColIdxMap=self.colIdxMap)
