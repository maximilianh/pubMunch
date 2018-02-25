# Copyright 2006-2012 Mark Diekhans
import unittest, sys
if __name__ == '__main__':
    sys.path.append("../../..")
from pm_pycbio.sys.TestCaseBase import TestCaseBase
from pm_pycbio.hgdata.GenePred import GenePred
from pm_pycbio.hgdata.GenePred import GenePredTbl

# lists defining expected results from exon features.
# they are in the form (utr5 cds utr3)
featsNM_000017 = (((119575618, 119575641), (119575641, 119575687), None),
                  (None, (119576781, 119576945), None),
                  (None, (119586741, 119586891), None),
                  (None, (119587111, 119587223), None),
                  (None, (119587592, 119587744), None),
                  (None, (119588035, 119588206), None),
                  (None, (119588288, 119588426), None),
                  (None, (119588575, 119588671), None),
                  (None, (119588895, 119588952), None),
                  (None, (119589051, 119589204), (119589204, 119589763)))
featsNM_000066 = ((None, (56764994, 56765149), (56764802, 56764994)),
                  (None, (56767400, 56767469), None),
                  (None, (56768925, 56769079), None),
                  (None, (56776439, 56776603), None),
                  (None, (56779286, 56779415), None),
                  (None, (56781411, 56781652), None),
                  (None, (56785145, 56785343), None),
                  (None, (56787638, 56787771), None),
                  (None, (56790276, 56790418), None),
                  (None, (56792359, 56792501), None),
                  (None, (56795610, 56795767), None),
                  ((56801540, 56801567), (56801447, 56801540), None))

chromSizes = {"chr1": 247249719,
              "chr7": 158821424,
              "chr11": 134452384,
              "chr12": 132349534}


def featRangeEq(featRange, expectRange):
    "compare a feature range and an expected range tuple, either maybe None"
    if ((featRange == None) or (expectRange == None)):
        return ((featRange == None) and (expectRange == None))
    else:
        return ((featRange.start == expectRange[0]) and
                (featRange.end == expectRange[1]))
    
def featureEq(feat, expected):
    "compare exon feature object with expected tuple"
    return featRangeEq(feat.utr5, expected[0]) and featRangeEq(feat.cds, expected[1]) and featRangeEq(feat.utr3, expected[2])

def featureExpectedSwap1(feat, chromSize):
    "swap either (start, end) or pass back none"
    if feat == None:
        return None
    else:
        return (chromSize - feat[1], chromSize - feat[0])

def featureExpectedSwap(expected, chromSize):
    reved = []
    for feat in reversed(expected):
        reved.append((featureExpectedSwap1(feat[0], chromSize),
                      featureExpectedSwap1(feat[1], chromSize),
                      featureExpectedSwap1(feat[2], chromSize)))
    return tuple(reved)

class ReadTests(TestCaseBase):
    def chkFeatures(self, gene, expect):
        feats = gene.getFeatures()
        self.failUnlessEqual(len(feats), len(expect))
        for i in xrange(len(feats)):
            self.failUnless(featureEq(feats[i], expect[i]))
        
    def testLoadMin(self):
        gpTbl = GenePredTbl(self.getInputFile("fromPslMinTest.gp"))
        self.failUnlessEqual(len(gpTbl), 9)
        r = gpTbl[0]
        self.failUnlessEqual(len(r.exons), 10)
        self.failUnlessEqual(r.name, "NM_000017.1")
        self.failUnlessEqual(r.exons[9].start, 119589051)
        self.failUnless(not r.hasExonFrames)

        # positive strand features
        self.chkFeatures(r, featsNM_000017)

        # negative strand features
        r = gpTbl[1]
        self.failUnlessEqual(r.name, "NM_000066.1")
        self.chkFeatures(r, featsNM_000066)

    def testLoadFrameStat(self):
        gpTbl = GenePredTbl(self.getInputFile("fileFrameStatTest.gp"))
        self.failUnlessEqual(len(gpTbl), 5)
        r = gpTbl[0]
        self.failUnlessEqual(len(r.exons), 10)
        self.failUnlessEqual(r.name, "NM_000017.1")
        self.failUnlessEqual(r.exons[9].start, 119589051)
        self.failUnless(r.hasExonFrames)

    def testStrandRelative(self):
        gpTbl = GenePredTbl(self.getInputFile("fromPslMinTest.gp"))
        self.failUnlessEqual(len(gpTbl), 9)
        r = gpTbl[0]
        r = r.getStrandRelative(chromSizes[r.chrom])
        self.failUnless(r.strandRel)
        self.failUnlessEqual(len(r.exons), 10)
        self.failUnlessEqual(r.name, "NM_000017.1")
        self.failUnless(not r.hasExonFrames)

        # positive strand features
        self.chkFeatures(r, featsNM_000017)

        # negative strand features
        r = gpTbl[1]
        r = r.getStrandRelative(chromSizes[r.chrom])
        self.failUnless(r.strandRel)
        self.failUnlessEqual(r.name, "NM_000066.1")

        self.chkFeatures(r,  featureExpectedSwap(featsNM_000066, chromSizes[r.chrom]))

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ReadTests))
    return suite

if __name__ == '__main__':
    unittest.main()
