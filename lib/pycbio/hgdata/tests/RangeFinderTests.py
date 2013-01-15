# Copyright 2006-2012 Mark Diekhans
import unittest, sys
if __name__ == '__main__':
    sys.path.append("../../..")
from pycbio.sys.TestCaseBase import TestCaseBase
from pycbio.hgdata.RangeFinder import *

debug = True

# (seqId start end strand value)
data1 = (
    ("chr22", 100,  1000, '+', "val1.1"),
    ("chr12", 100,  1000, '+', "val1.2"),
    ("chr12", 100,   500, '-', "val1.3"),
    ("chr12", 150, 10000, '+', "val1.4"),
    ("chr32", 1000000000, 2000000000, '+', "val1.5"),  # outside of basic range
    ("chr32", 100000, 2000000000, '-', "val1.6"),  # crossing of basic and extended range
    )
# (seqId start end strand expectWithStrand expectWithoutStrand)
queries1 = (
    ("chr20", 100,  1000, '+', (), ()),
    ("chr22", 100,  1000, '+', ("val1.1",), ("val1.1",)),
    ("chr22", 100,  1000, '-', (), ("val1.1",)),
    ("chr22", 110,  111, '+', ("val1.1",), ("val1.1",)),
    ("chr22", 110,  111, '-', (), ("val1.1",)),
    ("chr12", 1,  150, '-', ("val1.3",), ("val1.2","val1.3")),
    ("chr12", 10000,  1500000, '+', (), ()),
    ("chr12", 1,  151, '-', ("val1.3",), ("val1.2", "val1.3", "val1.4")),
    ("chr12", 1,  151, '+', ("val1.2","val1.4"), ("val1.2", "val1.3", "val1.4")),
    ("chr32", 10,  100001, '+', (), ("val1.6",)),
    ("chr32", 10,  1000000001, '+', ("val1.5",), ("val1.5", "val1.6",)),
    ("chr32", 1900000001, 2000000002, '+', ("val1.5",), ("val1.5", "val1.6",)),
    )

# potential regression with exact range matches
data2 = (
    ("chr1", 100316598, 100387207, '+', "NM_000643.2"),
    ("chr1", 100316598, 100387207, '+', "NM_000644.2"),
    )
queries2 = (
    ("chr1", 100316598, 100387207, '+', ("NM_000643.2", "NM_000644.2"), ("NM_000643.2", "NM_000644.2")),
    )

class RangeTests(TestCaseBase):
    def mkRangeFinder(self, data, useStrand):
        rf = RangeFinder()
        for row in data:
            if useStrand:
                rf.add(row[0], row[1], row[2], row[4], row[3])
            else:
                rf.add(row[0], row[1], row[2], row[4], None)
        return rf

    def failTrace(self, desc, query, expect, got):
        sys.stderr.write(("%s query failed: %s:%d-%d %s: expect: %s\n\tgot: %s\n"
                          % (desc, query[0], query[1], query[2], query[3], str(expect), str(got))))
        sys.stderr.flush()

    def doQuery(self, rf, seqId, start, end, strand):
        "do query and sort results, returning tuple result"
        val = list(rf.overlapping(seqId, start, end, strand))
        val.sort()
        return tuple(val)

    def doStrandQuery(self, rf, query):
        if rf.haveStrand:
            expect = query[4]
        else:
            expect = query[5]
        val = self.doQuery(rf, query[0], query[1], query[2], query[3])
        if debug and (val != expect):
            self.failTrace("strand of haveStrand="+str(rf.haveStrand), query, expect, val)
        self.failUnlessEqual(val, expect)
        
    def doNoStrandQuery(self, rf, query):
        expect = query[5]
        val = self.doQuery(rf, query[0], query[1], query[2], None)
        if debug and (val != expect):
            self.failTrace("nostrand of haveStrand="+str(rf.haveStrand), query, expect, val)
        self.failUnlessEqual(val, expect)
        
    def doQueries(self, rf, queries, useStrand):
        for query in queries:
            if useStrand:
                self.doStrandQuery(rf, query)
            else:
                self.doNoStrandQuery(rf, query)
            
    def testOverlapStrand(self):
        "stranded queries of have-strand RangeFinder"
        rf = self.mkRangeFinder(data1, True)
        self.doQueries(rf, queries1, True)

    def testOverlapStrandNoStrand(self):
        "stranded queries of no-strand RangeFinder"
        rf = self.mkRangeFinder(data1, False)
        self.doQueries(rf, queries1, True)

    def testOverlapNoStrand(self):
        "no-stranded queries of no-strand RangeFinder"
        rf = self.mkRangeFinder(data1, False)
        self.doQueries(rf, queries1, False)

    def testOverlapNoStrandStrand(self):
        "no-stranded queries of have-strand RangeFinder"
        rf = self.mkRangeFinder(data1, True)
        self.doQueries(rf, queries1, False)

    def testExactRange(self):
        "exact range matches"
        rf = self.mkRangeFinder(data2, True)
        self.doQueries(rf, queries2, True)
        self.doQueries(rf, queries2, False)
        
def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(RangeTests))

    return suite

if __name__ == '__main__':
    unittest.main()
