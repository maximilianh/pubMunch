# Copyright 2006-2012 Mark Diekhans
import unittest, sys
if __name__ == '__main__':
    sys.path.append("../../..")
from pm_pycbio.sys.TestCaseBase import TestCaseBase
from pm_pycbio.hgdata.GeneCheck import GeneCheckTbl

class ReadTests(TestCaseBase):
    def _checkDmp(self, checks):
        dmp = self.getOutputFile(".dmp")
        dmpfh = open(dmp, "w")
        for g in checks:
            g.dump(dmpfh)
        dmpfh.close()
        self.diffExpected(".dmp")
        
    def testRdbUniq(self):
        checks = GeneCheckTbl(self.getInputFile("geneCheck.rdb"), idIsUniq=True, isRdb=True)
        self.failUnlessEqual(len(checks), 53)
        self._checkDmp(checks)
        
    def testTsvUniq(self):
        checks = GeneCheckTbl(self.getInputFile("geneCheck.tsv"), idIsUniq=True, isRdb=False)
        self.failUnlessEqual(len(checks), 53)
        self._checkDmp(checks)
        
    def testRdbMulti(self):
        checks = GeneCheckTbl(self.getInputFile("geneCheckMulti.rdb"), isRdb=True)
        self.failUnlessEqual(len(checks), 8)
        self._checkDmp(checks)
        
    def testTsvMulti(self):
        checks = GeneCheckTbl(self.getInputFile("geneCheckMulti.tsv"), isRdb=False)
        self.failUnlessEqual(len(checks), 8)
        self._checkDmp(checks)
        
def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ReadTests))
    return suite

if __name__ == '__main__':
    unittest.main()
