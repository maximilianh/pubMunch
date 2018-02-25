# Copyright 2006-2012 Mark Diekhans
"tests of CmdRule"

import unittest, sys, os, re
if __name__ == '__main__':
    sys.path.append("../../..")
from pm_pycbio.sys import strOps,fileOps
from pm_pycbio.sys.Pipeline import ProcException
from pm_pycbio.exrun import ExRunException, ExRun, CmdRule, Cmd, File, FileIn, FileOut, Verb
from pm_pycbio.exrun.Graph import ProdState,RuleState
from pm_pycbio.exrun.tests import ExRunTestCaseBase

# change this for debugging:
verbFlags=set()
#verbFlags=set((Verb.error, Verb.trace, Verb.details))
#verbFlags=set((Verb.error,))
#verbFlags=Verb.all

def prExceptions(er, ex):
    "print and exception and any recorded ones"
    sys.stdout.flush()
    sys.stderr.flush()
    fh = sys.stderr
    fileOps.prLine(fh, "\n"+strOps.dup(78, '='))
    fileOps.prLine(fh, "Unexpected exception:")
    fileOps.prLine(fh, ProcException.formatExcept(ex))
    for e in er.errors:
        fileOps.prLine(fh, strOps.dup(78, '-'))
        fileOps.prLine(fh, ProcException.formatExcept(e))
    fileOps.prLine(fh, strOps.dup(78, '^'))
    sys.stderr.flush()

class CmdSuppliedTests(ExRunTestCaseBase):
    "tests of CmdRule with commands supplied to class"

    def testSort1(self):
        "single command to sort a file"
        er = ExRun(verbFlags=verbFlags)
        ifp = er.getFile(self.getInputFile("numbers.txt"))
        ofp = er.getFile(self.getOutputFile(".txt"))
        # auto add requires and produces
        c = Cmd(("sort", "-n"), stdin=ifp, stdout=ofp)
        er.addRule(CmdRule(c))
        try:
            er.run()
        except Exception, ex:
            prExceptions(er, ex)
            raise
        self.diffExpected(".txt")
        self.checkGraphStates(er)

    def testSortPipe(self):
        "pipeline command to sort a file"
        er = ExRun(verbFlags=verbFlags)
        ifp = er.getFile(self.getInputFile("numbers.txt"))
        ofp = er.getFile(self.getOutputFile(".txt"))
        c = Cmd((("sort", "-r", FileIn(ifp)), ("sort", "-nr")), stdout=ofp)
        er.addRule(CmdRule(c, requires=ifp, produces=ofp))
        try:
            er.run()
        except Exception, ex:
            prExceptions(er, ex)
            raise
        self.diffExpected(".txt")
        self.checkGraphStates(er)

    def testSort2(self):
        "two commands"
        er = ExRun(verbFlags=verbFlags)
        ifp = er.getFile(self.getInputFile("numbers.txt"))
        ofp1 = er.getFile(self.getOutputFile(".txt"))
        ofp2 = er.getFile(self.getOutputFile(".linecnt"))
        c1 = Cmd((("sort", "-r", FileIn(ifp)), ("sort", "-nr")), stdout=ofp1)
        c2 = Cmd((("wc", "-l"), ("sed", "-e", "s/ //g")), stdin=ofp1, stdout=ofp2)
        er.addRule(CmdRule((c1, c2), requires=ifp))
        try:
            er.run()
        except Exception, ex:
            prExceptions(er, ex)
            raise
        self.diffExpected(".txt")
        self.diffExpected(".linecnt")
        self.checkGraphStates(er)

    def testSort2Rules(self):
        "two commands in separate rules"
        er = ExRun(verbFlags=verbFlags)
        ifp = er.getFile(self.getInputFile("numbers.txt"))
        ofp1 = er.getFile(self.getOutputFile(".txt"))
        ofp2 = er.getFile(self.getOutputFile(".linecnt"))
        c1 = Cmd((("sort", "-r", FileIn(ifp)), ("sort", "-nr")), stdout=ofp1)
        c2 = Cmd((("wc", "-l"), ("sed", "-e", "s/ //g")), stdin=ofp1, stdout=ofp2)
        er.addRule(CmdRule(c2))
        er.addRule(CmdRule(c1, requires=ifp))
        try:
            er.run()
        except Exception, ex:
            prExceptions(er, ex)
            raise
        self.diffExpected(".txt")
        self.diffExpected(".linecnt")
        self.checkGraphStates(er)

    def testSort2RulesSub(self):
        "two commands in separate rules, with file ref subtitution"
        er = ExRun(verbFlags=verbFlags)
        ifp = er.getFile(self.getInputFile("numbers.txt"))
        ofp1 = er.getFile(self.getOutputFile(".txt"))
        ofp2 = er.getFile(self.getOutputFile(".linecnt"))
        c1 = Cmd((("sort", "-r", FileIn(ifp)), ("sort", "-nr"), ("tee", FileOut(ofp1))), stdout="/dev/null")
        c2 = Cmd((("cat", FileIn(ofp1)), ("wc", "-l"), ("sed", "-e", "s/ //g"), ("tee", FileOut(ofp2))), stdout="/dev/null")
        er.addRule(CmdRule(c2))
        er.addRule(CmdRule(c1))
        try:
            er.run()
        except Exception, ex:
            prExceptions(er, ex)
            raise
        self.diffExpected(".txt")
        self.diffExpected(".linecnt")
        self.checkGraphStates(er)

    def testFilePrefix(self):
        "test prefixes to FileIn/FileOut"
        er = ExRun(verbFlags=verbFlags)
        ifp = er.getFile(self.getInputFile("numbers.txt"))
        ofp = er.getFile(self.getOutputFile(".txt"))
        c = Cmd(("dd", "if="+FileIn(ifp), "of="+FileOut(ofp)))
        er.addRule(CmdRule(c))
        try:
            er.run()
        except Exception, ex:
            prExceptions(er, ex)
            raise
        self.diffExpected(".txt")
        self.checkGraphStates(er)

class CmdSubclassTests(ExRunTestCaseBase):
    "tests of CmdRule with subclassing"
    def testSort1(self):
        "single command to sort a file"

        class Sort(CmdRule):
            def __init__(self, ifp, ofp):
                CmdRule.__init__(self, requires=ifp, produces=ofp)
                self.ifp = ifp
                self.ofp = ofp
            def run(self):
                self.call(Cmd(("sort", "-n", self.ifp.getInPath()), stdout=self.ofp))
        er = ExRun(verbFlags=verbFlags)
        ifp = er.getFile(self.getInputFile("numbers.txt"))
        ofp = er.getFile(self.getOutputFile(".txt"))
        er.addRule(Sort(ifp, ofp))
        try:
            er.run()
        except Exception, ex:
            prExceptions(er, ex)
            raise
        self.diffExpected(".txt")
        self.checkGraphStates(er)

    def testSortPipe(self):
        "pipeline command to sort a file"

        class Sort(CmdRule):
            def __init__(self, ifp, ofp):
                CmdRule.__init__(self, requires=ifp, produces=ofp)
                self.ifp = ifp
                self.ofp = ofp
            def run(self):
                self.call(Cmd((("sort", "-n", self.ifp), ("sort", "-nr")),
                              stdout=self.ofp))
        er = ExRun(verbFlags=verbFlags)
        ifp = er.getFile(self.getInputFile("numbers.txt"))
        ofp = er.getFile(self.getOutputFile(".txt"))
        er.addRule(Sort(ifp, ofp))
        try:
            er.run()
        except Exception, ex:
            prExceptions(er, ex)
            raise
        self.diffExpected(".txt")
        self.checkGraphStates(er)

    def testSort2(self):
        "two commands"

        class Sort(CmdRule):
            def __init__(self, ifp, ofp1, ofp2):
                CmdRule.__init__(self, requires=ifp, produces=(ofp1, ofp2))
                self.ifp = ifp
                self.ofp1 = ofp1
                self.ofp2 = ofp2
            def run(self):
                self.call(Cmd((("sort", "-r", self.ifp), ("sort", "-nr")), stdout=self.ofp1))
                self.call(Cmd((("wc", "-l"), ("sed", "-e", "s/ //g")), stdin=self.ofp1, stdout=self.ofp2))

        er = ExRun(verbFlags=verbFlags)
        ifp = er.getFile(self.getInputFile("numbers.txt"))
        ofp1 = er.getFile(self.getOutputFile(".txt"))
        ofp2 = er.getFile(self.getOutputFile(".linecnt"))
        er.addRule(Sort(ifp, ofp1, ofp2))
        try:
            er.run()
        except Exception, ex:
            prExceptions(er, ex)
            raise
        self.diffExpected(".txt")
        self.diffExpected(".linecnt")
        self.checkGraphStates(er)

    def testSort2Rules(self):
        "two commands in separate rules"
        class Sort(CmdRule):
            def __init__(self, ifp, ofp):
                CmdRule.__init__(self, requires=ifp, produces=ofp)
                self.ifp = ifp
                self.ofp = ofp
            def run(self):
                self.call(Cmd((("sort", "-n", self.ifp), ("sort", "-nr")),
                              stdout=self.ofp))

        class Count(CmdRule):
            def __init__(self, ifp, ofp):
                CmdRule.__init__(self, requires=ifp, produces=ofp)
                self.ifp = ifp
                self.ofp = ofp
            def run(self):
                self.call(Cmd((("wc", "-l"), ("sed", "-e", "s/ //g")),
                              stdin=self.ifp, stdout=self.ofp))

        
        er = ExRun(verbFlags=verbFlags)
        ifp = er.getFile(self.getInputFile("numbers.txt"))
        ofp1 = er.getFile(self.getOutputFile(".txt"))
        ofp2 = er.getFile(self.getOutputFile(".linecnt"))
        er.addRule(Count(ofp1, ofp2))
        er.addRule(Sort(ifp, ofp1))
        try:
            er.run()
        except Exception, ex:
            prExceptions(er, ex)
            raise
        self.diffExpected(".txt")
        self.diffExpected(".linecnt")
        self.checkGraphStates(er)

class CmdCompressTests(ExRunTestCaseBase):
    "tests of CmdRule with automatic compression"

    def testStdio(self):
        er = ExRun(verbFlags=verbFlags)
        ifp = er.getFile(self.getInputFile("numbers.txt"))
        ofp1 = er.getFile(self.getOutputFile(".txt.gz"))
        ofp2 = er.getFile(self.getOutputFile(".txt"))
        er.addCmd(["sort", "-r"], stdin=ifp, stdout=ofp1)
        er.addCmd((["zcat", FileIn(ofp1, autoDecompress=False)], ["sed", "-e", "s/^/= /"]), stdout=ofp2)
        try:
            er.run()
        except Exception, ex:
            prExceptions(er, ex)
            raise
        self.diffExpected(".txt")
        self.checkGraphStates(er)
        
    def testArgs(self):
        er = ExRun(verbFlags=verbFlags)
        ifp = er.getFile(self.getInputFile("numbers.txt"))
        ofp1 = er.getFile(self.getOutputFile(".txt.gz"))
        ofp2 = er.getFile(self.getOutputFile(".txt"))
        er.addCmd((["sort", "-r", FileIn(ifp)], ["tee", FileOut(ofp1)]), stdout="/dev/null")
        er.addCmd(["sed", "-e", "s/^/= /", FileIn(ofp1)], stdout=FileOut(ofp2))
        try:
            er.run()
        except Exception, ex:
            prExceptions(er, ex)
            raise
        self.diffExpected(".txt")
        self.checkGraphStates(er)

    def testCmdErr(self):
        "handling of pipes when process has error"
        er = ExRun(verbFlags=set())
        ifp = er.getFile(self.getInputFile("numbers.txt"))
        ofp1 = er.getFile(self.getOutputFile(".txt.gz"))
        ofp2 = er.getFile(self.getOutputFile(".txt"))
        r1 = er.addCmd((["sort", "-r", FileIn(ifp)], ["tee", FileOut(ofp1)]), stdout="/dev/null")
        r2 = er.addCmd((["zcat", FileIn(ofp1, autoDecompress=False)], ["false"]), stdout=FileOut(ofp2))
        ex = None
        try:
            er.run()
        except ExRunException, ex:
            self.failUnlessEqual("Experiment failed: 1 error(s) encountered", str(ex))
            ex1 = er.errors[0]
            self.failUnless(isinstance(ex1, ExRunException))
            ex2 = ex1.cause
            self.failUnless(isinstance(ex2, ExRunException))
            ex3 = ex2.cause
            self.failUnless(isinstance(ex3, ProcException))
            exre = "process exited 1: false"
            self.failUnless(str(ex3),exre)
        if ex == None:
            self.fail("expected ProcException")
        self.checkGraphStates(er,
                              ((ofp1, ProdState.current),
                               (ofp2, ProdState.failed),
                               (r1,   RuleState.ok),
                               (r2,   RuleState.failed)))
            
    def testCmdSigPipe(self):
        "test command recieving SIGPIPE with no error"
        er = ExRun(verbFlags=verbFlags)
        ofp = er.getFile(self.getOutputFile(".txt"))
        er.addCmd((["yes"], ["true"]), stdout=FileOut(ofp))
        ex = None
        try:
            er.run()
        except Exception, ex:
            prExceptions(er, ex)
            raise
        self.checkGraphStates(er)

class CmdMiscTests(ExRunTestCaseBase):
    "misc tests, regressions, etc"
    def __mkDependOnNoDepend(self, priFile, secFile, secContents):
        ex = ExRun(verbFlags=verbFlags)
        priFp = ex.getFile(priFile)
        secFp = ex.getFile(secFile)
        ex.addCmd(["touch", FileOut(priFp)])
        ex.addCmd(["echo", secContents], requires=priFp, stdout=secFp)
        ex.run()
        return ex

    def testDependOnNoDepend(self):
        "test of a dependency on a file that is just created and has no dependencies"
        priFile = self.getOutputFile(".primary.txt")
        secFile = self.getOutputFile(".secondary.txt")
        # build once
        self.__mkDependOnNoDepend(priFile, secFile, "one")
        self.verifyOutputFile(".secondary.txt", "one\n")

        # should not be rebuilt
        self.__mkDependOnNoDepend(priFile, secFile, "two")
        self.verifyOutputFile(".secondary.txt", "one\n")

            
def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(CmdSuppliedTests))
    suite.addTest(unittest.makeSuite(CmdSubclassTests))
    suite.addTest(unittest.makeSuite(CmdCompressTests))
    suite.addTest(unittest.makeSuite(CmdMiscTests))
    return suite

if __name__ == '__main__':
    unittest.main()

