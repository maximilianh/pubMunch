# Copyright 2006-2012 Mark Diekhans
import unittest, sys, cPickle
if __name__ == '__main__':
    sys.path.append("../../..")
from pm_pycbio.sys.Immutable import Immutable
from pm_pycbio.sys.TestCaseBase import TestCaseBase

class ImmutableTests(TestCaseBase):
    def testDictClass(self):
        class DictClass(Immutable):
            def __init__(self, val):
                Immutable.__init__(self)
                self.val = val
                self.mkImmutable()

        obj = DictClass(10)
        ex = None
        try:
            obj.val = 111
        except Exception as ex:
            pass
        
        self.failUnlessEqual(obj.val, 10)
        self.failUnless(isinstance(ex, TypeError))

    def testSlotsClass(self):
        class SlotsClass(Immutable):
            __slots__ = ("val", )
            def __init__(self, val):
                Immutable.__init__(self)
                self.val = val
                self.mkImmutable()
        obj = SlotsClass(10)
        ex = None
        try:
            obj.val = 111
        except Exception as ex:
            pass
        
        self.failUnlessEqual(obj.val, 10)
        self.failUnless(isinstance(ex, TypeError))

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ImmutableTests))
    return suite

if __name__ == '__main__':
    unittest.main()
