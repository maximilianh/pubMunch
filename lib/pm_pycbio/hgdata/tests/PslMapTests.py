from __future__ import print_function
# Copyright 2006-2012 Mark Diekhans
import unittest, sys
if __name__ == '__main__':
    sys.path.append("../../..")
from pm_pycbio.sys.TestCaseBase import TestCaseBase
from pm_pycbio.hgdata.Psl import Psl
from pm_pycbio.hgdata.PslMap import PslMap

class MapTester(object):
    "test object that collects results"
    def __init__(self):
        self.mappings = []
        self.mapper = PslMap(self)

    def mapBlock(self, psl, blk, qRngStart, qRngEnd, tRngStart, tRngEnd):
        self.mappings.append(("blk", psl.qName, blk.iBlk, qRngStart, qRngEnd, tRngStart, tRngEnd))

    @staticmethod
    def __iBlkOrNone(blk):
        return blk.iBlk if blk != None else None

    def mapGap(self, psl, prevBlk, nextBlk, qRngStart, qRngEnd, tRngStart, tRngEnd):
        self.mappings.append(("gap", psl.qName, MapTester.__iBlkOrNone(prevBlk), MapTester.__iBlkOrNone(nextBlk), qRngStart, qRngEnd, tRngStart, tRngEnd))

    def __joinMappings(self):
        "join mappings into a tuple for testing and clear for next test"
        m = tuple(self.mappings)
        self.mappings = []
        return m

    def targetToQueryMap(self, psl, tRngStart, tRngEnd):
        self.mapper.targetToQueryMap(psl, tRngStart, tRngEnd)
        return self.__joinMappings()

    def queryToTargetMap(self, psl, qRngStart, qRngEnd):
        self.mapper.queryToTargetMap(psl, qRngStart, qRngEnd)
        return self.__joinMappings()

# test PSLs
_psPosMRna = "2515	2	0	0	0	0	16	26843	+	NM_012341	2537	0	2517	chr10	135374737	1024348	1053708	17	119,171,104,137,101,93,192,66,90,111,78,52,101,198,66,144,694,	0,119,290,394,531,632,725,917,983,1073,1184,1262,1314,1415,1613,1679,1823,	1024348,1028428,1031868,1032045,1033147,1034942,1036616,1036887,1041757,1042957,1044897,1045468,1046359,1048404,1050186,1051692,1053014,"
_psDoubleDel1 = "1226	150	0	0	0	0	0	0	-	NM_017069.1-1.1	1792	8	1387	chrX	154913754	151108857	151370996	12	212,153,144,83,221,68,118,4,182,2,169,20,	405,617,770,914,997,1218,1286,1404,1408,1591,1593,1764,	151108857,151116760,151127128,151143890,151174905,151203795,151264708,151264832,151283558,151370804,151370807,151370976,"
_psDoubleDel2 = "298	106	0	0	0	0	0	0	-	DQ216042.1-1.1	1232	53	593	chrX	154913754	151747020	151748962	5	18,65,22,138,161,	639,658,729,751,1018,	151747020,151747038,151747108,151747736,151748801,"
_psNegMrna = "5148	0	415	0	0	0	27	208231	-	NM_017651	5564	0	5563	chr6	171115067	135605109	135818903	28	1676,103,59,98,163,56,121,27,197,141,131,119,107,230,124,133,153,186,96,193,220,182,560,54,125,64,62,183,	1,1677,1780,1839,1937,2100,2156,2277,2304,2501,2642,2773,2892,2999,3229,3353,3486,3639,3825,3921,4114,4334,4516,5076,5130,5255,5319,5381,	135605109,135611560,135621637,135639656,135644299,135679269,135715913,135726088,135732485,135748304,135749766,135751019,135752345,135754164,135759512,135763719,135768145,135769427,135774478,135776871,135778631,135784262,135786951,135788718,135811760,135813365,135818325,135818720,"


def splitToPsl(ps):
    return Psl(ps.split("\t"))
        
class TargetToQueryTests(TestCaseBase):
    def testPosMRna(self):
        mapper = MapTester()

        # MAX MOD
        # within a single block and on neg strand
        pslNegMrna = splitToPsl(_psNegMrna)
        #got = mapper.targetToQueryMap(pslNegMrna, 135818902, 135818903)
        #got = mapper.queryToTargetMap(pslNegMrna, 0, 1)
        #print repr(got)
        pslNegMrna = pslNegMrna.reverseComplement()
        got = mapper.queryToTargetMap(pslNegMrna, 0, 100)
        print(repr(got))
        assert(False)
        self.failUnlessEqual(got, (('blk', 'NM_017651', 135818901, 135818902, 510, 0, 1),))
        # end MAX MOD

        pslPosMrna = splitToPsl(_psPosMRna)
        # within a single block
        got = mapper.targetToQueryMap(pslPosMrna, 1024444, 1024445)
        self.failUnlessEqual(got, (('blk', 'NM_012341', 0, 96, 97, 1024444, 1024445),))
        # crossing gaps
        got = mapper.targetToQueryMap(pslPosMrna, 1024398, 1031918)
        self.failUnlessEqual(got, (('blk', 'NM_012341', 0, 50, 119, 1024398, 1024467),
                                   ('gap', 'NM_012341', 0, 1, None, None, 1024467, 1028428),
                                   ('blk', 'NM_012341', 1, 119, 290, 1028428, 1028599),
                                   ('gap', 'NM_012341', 1, 2, None, None, 1028599, 1031868),
                                   ('blk', 'NM_012341', 2, 290, 340, 1031868, 1031918)))
        # gap before
        got = mapper.targetToQueryMap(pslPosMrna, 1024309, 1028420)
        self.failUnlessEqual(got, (('gap', 'NM_012341', None, 0, None, None, 1024309, 1024348),
                                   ('blk', 'NM_012341', 0, 0, 119, 1024348, 1024467),
                                   ('gap', 'NM_012341', 0, 1, None, None, 1024467, 1028420)))
        # gap after
        got = mapper.targetToQueryMap(pslPosMrna, 1051793, 1053908)
        self.failUnlessEqual(got, (('blk', 'NM_012341', 15, 1780, 1823, 1051793, 1051836),
                                   ('gap', 'NM_012341', 15, 16, None, None, 1051836, 1053014),
                                   ('blk', 'NM_012341', 16, 1823, 2517, 1053014, 1053708),
                                   ('gap', 'NM_012341', 16, None, None, None, 1053708, 1053908)))

    def testDoubleDel1(self):
        "gap with deletions on both sizes, query one a single base"
        mapper = MapTester()
        pslPosMrna = splitToPsl(_psDoubleDel1)
        got = mapper.targetToQueryMap(pslPosMrna, 151283730, 151370810)
        self.failUnlessEqual(got, (('blk', 'NM_017069.1-1.1', 8, 1580, 1590, 151283730, 151283740), ('gap', 'NM_017069.1-1.1', 8, 9, None, None, 151283740, 151370804), ('blk', 'NM_017069.1-1.1', 9, 1591, 1593, 151370804, 151370806), ('gap', 'NM_017069.1-1.1', 9, 10, None, None, 151370806, 151370807), ('blk', 'NM_017069.1-1.1', 10, 1593, 1596, 151370807, 151370810)))
        got = mapper.queryToTargetMap(pslPosMrna, 1408, 1784)
        self.failUnlessEqual(got, (('blk', 'NM_017069.1-1.1', 8, 1408, 1590, 151283558, 151283740), ('gap', 'NM_017069.1-1.1', 8, 9, 1590, 1591, None, None), ('blk', 'NM_017069.1-1.1', 9, 1591, 1593, 151370804, 151370806), ('blk', 'NM_017069.1-1.1', 10, 1593, 1762, 151370807, 151370976), ('gap', 'NM_017069.1-1.1', 10, 11, 1762, 1764, None, None), ('blk', 'NM_017069.1-1.1', 11, 1764, 1784, 151370976, 151370996)))

class QueryToTargetTests(TestCaseBase):
    def testPosMRna(self):
        mapper = MapTester()
        pslPosMrna = splitToPsl(_psPosMRna)
        # within a single block
        got = mapper.queryToTargetMap(pslPosMrna, 96, 97)
        self.failUnlessEqual(got, (('blk', 'NM_012341', 0, 96, 97, 1024444, 1024445),))
        # crossing gaps
        got = mapper.queryToTargetMap(pslPosMrna, 50, 340)
        self.failUnlessEqual(got, (('blk', 'NM_012341', 0, 50, 119, 1024398, 1024467),
                                   ('blk', 'NM_012341', 1, 119, 290, 1028428, 1028599),
                                   ('blk', 'NM_012341', 2, 290, 340, 1031868, 1031918)))
        # gap after
        got = mapper.queryToTargetMap(pslPosMrna, 1780, 2537)
        self.failUnlessEqual(got, (('blk', 'NM_012341', 15, 1780, 1823, 1051793, 1051836),
                                   ('blk', 'NM_012341', 16, 1823, 2517, 1053014, 1053708),
                                   ('gap', 'NM_012341', 16, None, 2517, 2537, None, None)))

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TargetToQueryTests))
    suite.addTest(unittest.makeSuite(QueryToTargetTests))
    return suite

if __name__ == '__main__':
    unittest.main()

