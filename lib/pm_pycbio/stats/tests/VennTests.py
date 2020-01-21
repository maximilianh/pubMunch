# Copyright 2006-2012 Mark Diekhans
import unittest, sys
if __name__ == '__main__':
    sys.path.append("../../..")
from pm_pycbio.stats.Venn import Venn
from pm_pycbio.sys.TestCaseBase import TestCaseBase
from pm_pycbio.sys.fileOps import prRowv

class VennTests(TestCaseBase):
    def _checkVenn(self, venn):
        with open(self.getOutputFile(".vinfo"), "w") as viFh:
            venn.writeSets(viFh)
        self.diffExpected(".vinfo")
        with open(self.getOutputFile(".vcnts"), "w") as viFh:
            venn.writeCounts(viFh)
        self.diffExpected(".vcnts")
        
    def testVenn1(self):
        venn = Venn()
        venn.addItems("A", (1, 2, 3))
        venn.addItems("B", (3, 4, 5))
        venn.addItems("C", (3, 5, 6))
        self._checkVenn(venn)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(VennTests))
    return suite

if __name__ == '__main__':
    unittest.main()
