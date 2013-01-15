# Copyright 2006-2012 Mark Diekhans
import unittest, sys
if __name__ == '__main__':
    sys.path.append("../../..")
from pycbio.sys.TestCaseBase import TestCaseBase
from pycbio.hgdata.Psl import Psl
from pycbio.hgdata.Psl import PslTbl

class ReadTests(TestCaseBase):
    def testLoad(self):
        pslTbl = PslTbl(self.getInputFile("pslTest.psl"))
        self.failUnlessEqual(len(pslTbl), 14)
        r = pslTbl[1]
        self.failUnlessEqual(r.blockCount, 13)
        self.failUnlessEqual(len(r.blocks), 13)
        self.failUnlessEqual(r.qName, "NM_198943.1")

    def countQNameHits(self, pslTbl, qName):
        cnt = 0
        for p in pslTbl.getByQName(qName):
            self.failUnlessEqual(p.qName, qName)
            cnt += 1
        return cnt

    def testQNameIdx(self):
        pslTbl = PslTbl(self.getInputFile("pslTest.psl"), qNameIdx=True)
        self.failIf(pslTbl.haveQName("fred"))
        self.failUnless(pslTbl.haveQName("NM_001327.1"))
        self.failUnlessEqual(self.countQNameHits(pslTbl, "NM_198943.1"), 1)
        self.failUnlessEqual(self.countQNameHits(pslTbl, "fred"), 0)
        self.failUnlessEqual(self.countQNameHits(pslTbl, "NM_000014.3"), 2)
        self.failUnlessEqual(self.countQNameHits(pslTbl, "NM_001327.1"), 4)

    def testPslXCdsGenome(self):
        pslTbl = PslTbl(self.getInputFile("refseq.hg19.prot-genome.pslx"))
        self.failUnlessEqual(len(pslTbl), 4)
        for psl in pslTbl:
            for blk in psl.blocks:
                self.failUnlessEqual(len(blk.qSeq), len(blk))
                self.failUnlessEqual(len(blk.tSeq), len(blk))

class OpsTests(TestCaseBase):
    "test operations of PSL objects"
    # test data as a string (ps = psl string)
    psPos = "0	10	111	0	1	13	3	344548	+	NM_025031.1	664	21	155	chr22	49554710	48109515	48454184	4	17,11,12,81,	21,38,49,74,	48109515,48109533,48453547,48454103,"
    psNeg = "24	14	224	0	5	18	7	1109641	-	NM_144706.2	1430	1126	1406	chr22	49554710	16248348	17358251	9	24,23,11,14,12,20,12,17,129,	24,48,72,85,111,123,145,157,175,	16248348,16612125,16776474,16911622,17054523,17062699,17291413,17358105,17358122,"
    psTransPosPos = "771	167	0	0	0	0	0	0	++	NM_001000369	939	1	939	chr1	245522847	52812	53750	1	938,	1,	52812,"
    psTransPosNeg = "500	154	0	0	2	504	5	6805	+-	NM_001020776	1480	132	1290	chr1	245522847	92653606	92661065	6	57,84,30,135,164,184,	132,339,777,807,942,1106,	152861782,152862321,152864639,152864784,152866513,152869057,"
    psTransNegPos = "71	5	0	0	1	93	2	213667	-+	AA608343.1a	186	0	169	chr5	180857866	157138232	157351975	3	27,29,20,	17,137,166,	157138232,157351058,157351955,"
    psTransNegNeg = "47	4	0	0	1	122	1	46	--	AA608343.1b	186	5	178	chr6	170899992	29962882	29962979	2	30,21,	8,160,	140937013,140937089,"

    @staticmethod
    def __splitToPsl(ps):
        return Psl(ps.split("\t"))
    
    def __rcTest(self, psIn, psExpect):
        self.failUnlessEqual(str(OpsTests.__splitToPsl(psIn).reverseComplement()), psExpect)

    def testReverseComplement(self):
        self.__rcTest(OpsTests.psPos, "0	10	111	0	1	13	3	344548	--	NM_025031.1	664	21	155	chr22	49554710	48109515	48454184	4	81,12,11,17,	509,603,615,626,	1100526,1101151,1445166,1445178,")
        self.__rcTest(OpsTests.psNeg, "24	14	224	0	5	18	7	1109641	+-	NM_144706.2	1430	1126	1406	chr22	49554710	16248348	17358251	9	129,17,12,20,12,14,11,23,24,	1126,1256,1273,1287,1307,1331,1347,1359,1382,	32196459,32196588,32263285,32491991,32500175,32643074,32778225,32942562,33306338,")
        self.__rcTest(OpsTests.psTransPosPos, "771	167	0	0	0	0	0	0	--	NM_001000369	939	1	939	chr1	245522847	52812	53750	1	938,	0,	245469097,")
        self.__rcTest(OpsTests.psTransPosNeg, "500	154	0	0	2	504	5	6805	-+	NM_001020776	1480	132	1290	chr1	245522847	92653606	92661065	6	184,164,135,30,84,57,	190,374,538,673,1057,1291,	92653606,92656170,92657928,92658178,92660442,92661008,")
        self.__rcTest(OpsTests.psTransNegPos, "71	5	0	0	1	93	2	213667	+-	AA608343.1a	186	0	169	chr5	180857866	157138232	157351975	3	20,29,27,	0,20,142,	23505891,23506779,23719607,")
        self.__rcTest(OpsTests.psTransNegNeg, "47	4	0	0	1	122	1	46	++	AA608343.1b	186	5	178	chr6	170899992	29962882	29962979	2	21,30,	5,148,	29962882,29962949,")

    def __swapTest(self, psIn, psExpect):
        self.failUnlessEqual(str(OpsTests.__splitToPsl(psIn).swapSides(keepTStrandImplicit=True)), psExpect)

    def testSwapSizes(self):
        self.__swapTest(OpsTests.psPos, "0	10	111	0	3	344548	1	13	+	chr22	49554710	48109515	48454184	NM_025031.1	664	21	155	4	17,11,12,81,	48109515,48109533,48453547,48454103,	21,38,49,74,")
        self.__swapTest(OpsTests.psNeg, "24	14	224	0	7	1109641	5	18	-	chr22	49554710	16248348	17358251	NM_144706.2	1430	1126	1406	9	129,17,12,20,12,14,11,23,24,	32196459,32196588,32263285,32491991,32500175,32643074,32778225,32942562,33306338,	1126,1256,1273,1287,1307,1331,1347,1359,1382,")
        self.__swapTest(OpsTests.psTransPosPos, "771	167	0	0	0	0	0	0	++	chr1	245522847	52812	53750	NM_001000369	939	1	939	1	938,	52812,	1,")
        self.__swapTest(OpsTests.psTransPosNeg, "500	154	0	0	5	6805	2	504	-+	chr1	245522847	92653606	92661065	NM_001020776	1480	132	1290	6	57,84,30,135,164,184,	152861782,152862321,152864639,152864784,152866513,152869057,	132,339,777,807,942,1106,")
        self.__swapTest(OpsTests.psTransNegPos, "71	5	0	0	2	213667	1	93	+-	chr5	180857866	157138232	157351975	AA608343.1a	186	0	169	3	27,29,20,	157138232,157351058,157351955,	17,137,166,")
        self.__swapTest(OpsTests.psTransNegNeg, "47	4	0	0	1	46	1	122	--	chr6	170899992	29962882	29962979	AA608343.1b	186	5	178	2	30,21,	140937013,140937089,	8,160,")
        
    def __swapDropImplicitTest(self, psIn, psExpect):
        self.failUnlessEqual(str(OpsTests.__splitToPsl(psIn).swapSides(keepTStrandImplicit=False)), psExpect)

    def testSwapSizesDropImplicit(self):
        self.__swapDropImplicitTest(OpsTests.psPos, "0	10	111	0	3	344548	1	13	++	chr22	49554710	48109515	48454184	NM_025031.1	664	21	155	4	17,11,12,81,	48109515,48109533,48453547,48454103,	21,38,49,74,")
        self.__swapDropImplicitTest(OpsTests.psNeg, "24	14	224	0	7	1109641	5	18	+-	chr22	49554710	16248348	17358251	NM_144706.2	1430	1126	1406	9	24,23,11,14,12,20,12,17,129,	16248348,16612125,16776474,16911622,17054523,17062699,17291413,17358105,17358122,	24,48,72,85,111,123,145,157,175,")
        self.__swapDropImplicitTest(OpsTests.psTransPosPos, "771	167	0	0	0	0	0	0	++	chr1	245522847	52812	53750	NM_001000369	939	1	939	1	938,	52812,	1,")
        self.__swapDropImplicitTest(OpsTests.psTransPosNeg, "500	154	0	0	5	6805	2	504	-+	chr1	245522847	92653606	92661065	NM_001020776	1480	132	1290	6	57,84,30,135,164,184,	152861782,152862321,152864639,152864784,152866513,152869057,	132,339,777,807,942,1106,")
        self.__swapDropImplicitTest(OpsTests.psTransNegPos, "71	5	0	0	2	213667	1	93	+-	chr5	180857866	157138232	157351975	AA608343.1a	186	0	169	3	27,29,20,	157138232,157351058,157351955,	17,137,166,")
        self.__swapDropImplicitTest(OpsTests.psTransNegNeg, "47	4	0	0	1	46	1	122	--	chr6	170899992	29962882	29962979	AA608343.1b	186	5	178	2	30,21,	140937013,140937089,	8,160,")

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ReadTests))
    suite.addTest(unittest.makeSuite(OpsTests))
    return suite

if __name__ == '__main__':
    unittest.main()

