# Copyright 2006-2012 Mark Diekhans
import unittest, sys, copy
if __name__ == '__main__':
    sys.path.append("../../..")
from pycbio.sys.TestCaseBase import TestCaseBase
from pycbio.sys.fileOps import prRowv
from pycbio.align.PairAlign import loadPslFile

class WithSrcCds(object):
    "container for map CDS results"
    def __init__(self, srcAln, destAln):
        self.srcAln = srcAln
        self.destAln = destAln

    def dump(self, fh):
        prRowv(fh, "srcQCds: ", self.srcAln.qSeq)
        prRowv(fh, "srcTCds: ", self.srcAln.tSeq)
        self.destAln.dump(fh)

class PairAlignPsl(TestCaseBase):
    def __dumpNDiff(self, alns, suffix):
        fh = open(self.getOutputFile(suffix), "w")
        for pa in alns:
            pa.dump(fh)
        fh.close()
        self.diffExpected(suffix)
        
    def testLoad(self):
        alns = loadPslFile(self.getInputFile("hsRefSeq.psl"),
                           self.getInputFile("hsRefSeq.cds"))
        self.__dumpNDiff(alns, ".out")

    def testRevCmpl(self):
        alns = loadPslFile(self.getInputFile("hsRefSeq.psl"),
                               self.getInputFile("hsRefSeq.cds"))
        ralns = []
        rralns = []
        for pa in alns:
            rpa = pa.revCmpl()
            ralns.append(rpa)
            rralns.append(rpa.revCmpl())
        self.__dumpNDiff(ralns, ".rout")
        self.__dumpNDiff(rralns, ".rrout")

    def testLoadUnaln(self):
        alns = loadPslFile(self.getInputFile("hsRefSeq.psl"),
                           self.getInputFile("hsRefSeq.cds"),
                           inclUnaln=True)
        self.__dumpNDiff(alns, ".out")


    def doTestProjectCds(self, contained):
        alns = loadPslFile(self.getInputFile("refseqWeird.psl"),
                           self.getInputFile("refseqWeird.cds"),
                           inclUnaln=True)
        qalns = []
        for pa in alns:
            pa.projectCdsToTarget(contained=contained)
            # project back
            qpa = pa.copy()
            qpa.qSubSeqs.clearCds()
            qpa.projectCdsToQuery(contained=contained)
            qalns.append(qpa)
        self.__dumpNDiff(alns, ".tout")
        self.__dumpNDiff(qalns, ".qout")

    def testProjectCds(self):
        self.doTestProjectCds(False)

    def testProjectCdsContained(self):
        self.doTestProjectCds(True)

    def doTestMapCds(self, contained):
        destAlns = loadPslFile(self.getInputFile("clonesWeird.psl"),
                               inclUnaln=True)
        srcAlns = loadPslFile(self.getInputFile("refseqWeird.psl"),
                              self.getInputFile("refseqWeird.cds"),
                              inclUnaln=True,
                              projectCds=True)
        mappedAlns = []
        for destAln in destAlns:
            for srcAln in srcAlns:
                if srcAln.qSeq.cds == None:
                    raise Exception("no qCDS: " + srcAln.qSeq.id + " <=> " + srcAln.tSeq.id)
                if srcAln.tSeq.cds == None:
                    raise Exception("no tCDS: " + srcAln.qSeq.id + " <=> " + srcAln.tSeq.id)
                if srcAln.targetOverlap(destAln):
                    ma = destAln.copy()
                    ma.mapCds(srcAln, srcAln.tSeq, ma.tSeq, contained=contained)
                    mappedAlns.append(WithSrcCds(srcAln, ma))
        self.__dumpNDiff(mappedAlns, ".out")

    def testMapCds(self):
        self.doTestMapCds(contained=False)

    def testMapCdsContained(self):
        self.doTestMapCds(contained=True)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(PairAlignPsl))
    return suite

if __name__ == '__main__':
    unittest.main()
