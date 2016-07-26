# Copyright 2006-2012 Mark Diekhans
import unittest, sys
if __name__ == '__main__':
    sys.path.append("../../..")
from pycbio.sys.DbDict import DbDict
from pycbio.sys.TestCaseBase import TestCaseBase

class DbDictTests(TestCaseBase):

    def __assertKeyValues(self, dbd, expect):
        "gets sort tuple of (key values) and compare"
        keyValues = [(k, dbd[k]) for k in list(dbd.keys())]
        keyValues.sort(key=lambda kv: kv[0])
        keyValues = tuple(keyValues)
        self.failUnlessEqual(keyValues, expect)

    def testBasic(self):
        dbfile = self.getOutputFile("basic.sqlite")
        dbd = DbDict(dbfile)
        dbd["one"] = "value 1" 
        dbd["two"] = "value 2" 
        dbd["three"] = "value 3" 
        keys = list(dbd.keys())
        keys.sort()
        self.__assertKeyValues(dbd, (('one', 'value 1'), ('three', 'value 3'), ('two', 'value 2')))

        dbd["two"] = "value 2.1" 
        self.__assertKeyValues(dbd, (('one', 'value 1'), ('three', 'value 3'), ('two', 'value 2.1')))

        del dbd["two"]
        self.__assertKeyValues(dbd, (('one', 'value 1'), ('three', 'value 3')))


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(DbDict))
    return suite

if __name__ == '__main__':
    unittest.main()
