# Copyright 2006-2012 Mark Diekhans
import unittest, sys
if __name__ == '__main__':
    sys.path.append("../../..")
from pycbio.stats.Histogram import Histogram
from pycbio.sys.TestCaseBase import TestCaseBase
from pycbio.sys.fileOps import prRowv

# FIXME: not implemented

class HistoTests(TestCaseBase):
    def __getBinInfo(self, b):
        return (b.idx, b.binMin, b.binMin+b.binSize, b.binSize, b.cnt, b.freq)

    def __getBinsInfo(self, bins):
        return [self.__getBinInfo(b) for b in bins]

    def testNumBins(self):
        h = Histogram([-1.0, 1.0], numBins=2)
        bins = h.build()
        self.failUnlessEqual(self.__getBinsInfo(bins),
                             [(0, -2.0, 0.0, 2.0, 1, 0.0),
                              (1, 0.0, 2.0, 2.0, 1, 0.0)])

    def XtestBinSize(self):
        # FIXME: doesn't work
        h = Histogram([-1.0, 1.0], binSize=1)
        h.dump(sys.stdout)
        bins = h.build()
        self.failUnlessEqual(self.__getBinsInfo(bins),
                             [(0, -2.0, 0.0, 2.0, 1, 0.0),
                              (1, 0.0, 2.0, 2.0, 1, 0.0)])

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(HistoTests))
    return suite

if __name__ == '__main__':
    unittest.main()
