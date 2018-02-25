# Copyright 2006-2012 Mark Diekhans
import unittest, sys
if __name__ == '__main__':
    sys.path.append("../../..")
from pm_pycbio.sys import PycbioException
from pm_pycbio.sys.TestCaseBase import TestCaseBase

class TestExcept(PycbioException):
    pass

class ExceptTests(TestCaseBase):
    def testBasicExcept(self):
        def fn1():
            fn2()
        def fn2():
            fn3()
        def fn3():
            raise TestExcept("testing 1 2 3")

        ex = None
        try:
            fn1()
        except Exception, e:
            ex = e
        self.failUnless(ex != None)
        self.failUnlessEqual(str(ex), "testing 1 2 3")
        self.failUnlessMatch(ex.format(), """^TestExcept: testing 1 2 3.+in testBasicExcept.+fn1\(\).+fn2\(\).+fn3\(\).+raise TestExcept\("testing 1 2 3"\)\n$""")
        
    def testChainedExcept(self):
        def fn1():
            try:
                fn2()
            except Exception,e:
                raise TestExcept("in-fn1", e)
        def fn2():
            fn3()
        def fn3():
            try:
                fn4()
            except Exception,e:
                raise TestExcept("in-fn3", e)
        def fn4():
            fn5()
        def fn5():
            fn6()
        def fn6():
            try:
                fn7()
            except Exception,e:
                raise TestExcept("in-fn6", e)
        def fn7():
            raise OSError("OS meltdown")

        ex = None
        try:
            fn1()
        except Exception, e:
            ex = e
        self.failUnless(ex != None)
        self.failUnlessEqual(str(ex), "in-fn1,\n    caused by: TestExcept: in-fn3,\n    caused by: TestExcept: in-fn6,\n    caused by: OSError: OS meltdown")
        self.failUnlessMatch(ex.format(), """^TestExcept: in-fn1.+fn1\(\).+raise TestExcept\("in-fn1", e\).+caused by: TestExcept: in-fn3.+fn3\(\).+caused by: TestExcept: in-fn6.+fn4\(\).+fn5\(\).+fn6\(\).+caused by: OSError: OS meltdown.+fn7\(\).+raise OSError\("OS meltdown"\)$""")
        
def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ExceptTests))
    return suite

if __name__ == '__main__':
    unittest.main()
