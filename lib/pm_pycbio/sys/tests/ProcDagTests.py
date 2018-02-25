# Copyright 2006-2012 Mark Diekhans
import unittest, sys, re
if __name__ == '__main__':
    sys.path.append("../../..")
from pm_pycbio.sys.Pipeline import ProcDag, ProcException, ProcDagException, Pipe, DataReader, DataWriter, File, PIn, POut
from pm_pycbio.sys import procOps
from pm_pycbio.sys.TestCaseBase import TestCaseBase


class ProcDagTests(TestCaseBase):

    def commonChecks(self, nopen, pd, expectStr, isRe=False):
        """check that files, threads, and processes have not leaked. Check str(pd)
        against expectStr, which can be a string, or an regular expression if
        isRe==True, or None to not check."""
        s = str(pd)
        if expectStr != None:
            if isRe:
                if not re.search(expectStr, s):
                    self.fail("'" +s+ "' doesn't match RE '" + expectStr + "'")
            else:
                self.failUnlessEqual(s, expectStr)
        self.failIfChildProcs()
        self.failIfNumOpenFilesChanged(nopen)
        self.failIfMultipleThreads()

    def testTrivial(self):
        nopen = self.numOpenFiles()
        pd = ProcDag()
        pd.create(("true",))
        pd.wait()
        self.commonChecks(nopen, pd, "true")

    def testTrivialFail(self):
        nopen = self.numOpenFiles()
        pd = ProcDag()
        pd.create(("false",))
        err = None
        try:
            pd.wait()
        except Exception,err:
            pass
        self.failUnless(isinstance(err, ProcException))
        self.commonChecks(nopen, pd, "false")

    def testSimplePipe(self):
        nopen = self.numOpenFiles()
        pd = ProcDag()
        p = Pipe()
        pd.create(("true",), stdout=p)
        pd.create(("true",), stdin=p)
        pd.wait()
        self.commonChecks(nopen, pd, "true | true")

    def testSimplePipeFail(self):
        nopen = self.numOpenFiles()
        pd = ProcDag()
        p = Pipe()
        pd.create(("false",), stdout=p)
        pd.create(("true",), stdin=p)
        err = None
        try:
            pd.wait()
        except Exception,err:
            pass
        self.failUnless(isinstance(err, ProcException))
        self.commonChecks(nopen, pd, "false | true")

    def testExecFail(self):
        "invalid executable"
        nopen = self.numOpenFiles()
        pd = ProcDag()
        dw = DataWriter("one\ntwo\nthree\n")
        pd.create(("procDoesNotExist","-r"), stdin=dw)
        ex = None
        try:
            pd.wait()
        except Exception, ex:
            pass
        expect = "exec failed: procDoesNotExist -r,\n    caused by: OSError: [Errno 2] No such file or directory"
        msg = str(ex)
        if not msg.startswith(expect):
            self.fail("'"+ msg + "' does not start with '"
                      + expect + "', cause: " + str(getattr(ex,"cause", None)))
        self.commonChecks(nopen, pd, "procDoesNotExist -r <[DataWriter]")

    def testStdinMem(self):
        "write from memory to stdin"
        nopen = self.numOpenFiles()
        pd = ProcDag()
        dw = DataWriter("one\ntwo\nthree\n")
        outf = self.getOutputFile(".out")
        pd.create(("sort","-r"), stdin=dw, stdout=outf)
        pd.wait()
        self.diffExpected(".out")
        self.commonChecks(nopen, pd, "^sort -r <\\[DataWriter\\] >.+/output/ProcDagTests\\.ProcDagTests\\.testStdinMem\\.out$", isRe=True)

    def testStdoutMem(self):
        "read from stdout into memory"
        nopen = self.numOpenFiles()
        pd = ProcDag()
        inf = self.getInputFile("simple1.txt")
        dr = DataReader()
        pd.create(("sort","-r"), stdin=inf, stdout=dr)
        pd.wait()
        self.failUnlessEqual(dr.get(), "two\nthree\nsix\none\nfour\nfive\n")
        self.commonChecks(nopen, pd, "^sort -r <.+/input/simple1\\.txt >\\[DataWriter\\]", isRe=True)

    def testInArgMem(self):
        "write from memory to a pipe argument"
        nopen = self.numOpenFiles()
        pd = ProcDag()
        dw = DataWriter("one\ntwo\nthree\n")
        outf = self.getOutputFile(".out")
        pr = pd.create(("sort","-r", PIn(dw)), stdin="/dev/null", stdout=outf)
        pd.wait()
        self.diffExpected(".out")
        self.commonChecks(nopen, pd, "^sort -r \\[DataWriter\\] </dev/null >.+/output/ProcDagTests\\.ProcDagTests\\.testInArgMem\\.out$", isRe=True)

    def testOutArgMem(self):
        "read from a pipe argument into memory"
        nopen = self.numOpenFiles()
        pd = ProcDag()
        inf = self.getInputFile("simple1.txt")
        dr = DataReader()
        pr = pd.create(("tee",POut(dr)), stdin=inf, stdout="/dev/null")
        pd.wait()
        self.failUnlessEqual(dr.get(), "one\ntwo\nthree\nfour\nfive\nsix\n")
        self.commonChecks(nopen, pd, "^tee \\[DataWriter\\] <.+/input/simple1.txt >/dev/null$", isRe=True)

    def testOutArgNoOpen(self):
        "write to a pipe argument that doesn't get open"
        nopen = self.numOpenFiles()
        pd = ProcDag()
        dr = DataReader()
        pr = pd.create(("true", POut(dr)), stdin="/dev/null")
        pd.wait()
        self.commonChecks(nopen, pd, "true [DataWriter] </dev/null")

    def testInArgNoOpen(self):
        "read from a pipe argument that doesn't get open"
        nopen = self.numOpenFiles()
        pd = ProcDag()
        dw = DataWriter("one\ntwo\nthree\n")
        pr = pd.create(("true", PIn(dw)), stdin="/dev/null")
        pd.wait()
        self.commonChecks(nopen, pd, "true [DataWriter] </dev/null")

    def testStdoutToArg(self):
        "stdout to a pipe argument"
        nopen = self.numOpenFiles()
        pd = ProcDag()
        inf = self.getInputFile("simple1.txt")
        outf = self.getOutputFile(".out")
        io = Pipe()
        pd.create(("cat", inf), stdout=io)
        pd.create(("sort","-r", PIn(io)), stdin="/dev/null", stdout=outf)
        pd.wait()
        self.diffExpected(".out")
        self.commonChecks(nopen, pd, "^sort -r <\\(cat .+/input/simple1.txt\\) </dev/null >.+/output/ProcDagTests\\.ProcDagTests\\.testStdoutToArg\\.out$", isRe=True)

    def testArgToStdout(self):
        "pipe argument to stdout"
        nopen = self.numOpenFiles()
        pd = ProcDag()
        inf = self.getInputFile("simple1.txt")
        outf = self.getOutputFile(".out")
        io = Pipe()
        pd.create(("tee", POut(io)), stdin=inf, stdout="/dev/null")
        pd.create(("sort","-r"), stdin=io, stdout=outf)
        pd.wait()
        self.diffExpected(".out")
        self.commonChecks(nopen, pd, "^tee >\\(sort -r >.+/output/ProcDagTests\\.ProcDagTests\\.testArgToStdout\\.out\\) <.+/input/simple1\\.txt >/dev/null$", isRe=True)

    def testSplitPipe(self):
        "tee into two sorts"
        nopen = self.numOpenFiles()
        pd = ProcDag()
        inf = self.getInputFile("simple1.txt")
        outFwd = self.getOutputFile(".fwd.out")
        outRev = self.getOutputFile(".rev.out")
        fwd = Pipe()
        rev = Pipe()
        pd.create(("tee", POut(rev)), stdin=inf, stdout=POut(fwd))
        pd.create(("sort",), stdin=fwd, stdout=outFwd)
        pd.create(("sort","-r"), stdin=rev, stdout=outRev)
        pd.wait()
        self.diffExpected(".fwd.out")
        self.diffExpected(".rev.out")
        self.commonChecks(nopen, pd, "^tee >\\(sort -r >.+/output/ProcDagTests\\.ProcDagTests\\.testSplitPipe\\.rev\\.out\\) <.+/input/simple1\\.txt \\| sort >.+/output/ProcDagTests\\.ProcDagTests\\.testSplitPipe\\.fwd\\.out$", isRe=True)

    def testSplitPipe2(self):
        "tee into two pipeline of two sorts each "
        nopen = self.numOpenFiles()
        pd = ProcDag()
        inf = self.getInputFile("simple1.txt")
        outFwd = self.getOutputFile(".fwd.out")
        outRev = self.getOutputFile(".rev.out")
        fwd1 = Pipe()
        fwd2 = Pipe()
        rev1 = Pipe()
        rev2 = Pipe()
        # sort wrong direction, then sort again into final orders
        pd.create(("tee", POut(rev1)), stdin=inf, stdout=POut(fwd1))
        pd.create(("sort","-r"), stdin=fwd1, stdout=fwd2)
        pd.create(("sort",), stdin=fwd2, stdout=outFwd)
        pd.create(("sort",), stdin=rev1, stdout=rev2)
        pd.create(("sort","-r"), stdin=rev2, stdout=outRev)
        pd.wait()
        self.diffExpected(".fwd.out")
        self.diffExpected(".rev.out")
        self.commonChecks(nopen, pd, "^tee >\\(sort \\| sort -r >.+/output/ProcDagTests\\.ProcDagTests\\.testSplitPipe2\\.rev\\.out\\) <.+/input/simple1\\.txt \\| sort -r \\| sort >.+/output/ProcDagTests\\.ProcDagTests\\.testSplitPipe2\\.fwd\\.out$", isRe=True)

    def testJoinPipe2(self):
        "cat from two pipeline of two sorts each "
        nopen = self.numOpenFiles()
        pd = ProcDag()
        inf = self.getInputFile("simple1.txt")
        outf = self.getOutputFile(".out")
        fwd1 = Pipe()
        fwd2 = Pipe()
        rev1 = Pipe()
        rev2 = Pipe()
        # sort reverse direction, then sort again into final orders
        pd.create(("sort","-r"), stdin=inf, stdout=fwd1)
        pd.create(("sort",), stdin=fwd1, stdout=fwd2)
        pd.create(("sort",), stdin=inf, stdout=rev1)
        pd.create(("sort","-r"), stdin=rev1, stdout=rev2)
        pd.create(("cat", PIn(fwd2), PIn(rev2)), stdout=outf)
        pd.wait()
        self.diffExpected(".out")
        self.commonChecks(nopen, pd, "^cat <\\(sort -r <.+/input/simple1\\.txt \\| sort\\) <\\(sort <.+/input/simple1.txt \\| sort -r\\) >.+/output/ProcDagTests\\.ProcDagTests\\.testJoinPipe2\\.out$", isRe=True)

    def testJoinPipe2Uniq(self):
        "cat from two pipeline of two sorts each, results pipe to sort -u"
        nopen = self.numOpenFiles()
        pd = ProcDag()
        inf = self.getInputFile("simple1.txt")
        outf = self.getOutputFile(".out")
        fwd1 = Pipe()
        fwd2 = Pipe()
        rev1 = Pipe()
        rev2 = Pipe()
        sortu = Pipe()
        # sort reverse direction, then sort again into final orders
        pd.create(("sort","-r"), stdin=inf, stdout=fwd1)
        pd.create(("sort",), stdin=fwd1, stdout=fwd2)
        pd.create(("sort",), stdin=inf, stdout=rev1)
        pd.create(("sort","-r"), stdin=rev1, stdout=rev2)
        pd.create(("cat", PIn(fwd2), PIn(rev2)), stdout=sortu)
        pd.create(("sort","-u"), stdin=sortu, stdout=outf)
        pd.wait()
        self.diffExpected(".out")
        self.commonChecks(nopen, pd, "^cat <\\(sort -r <.+/tests/input/simple1.txt \\| sort\\) <\\(sort <.+/input/simple1.txt \\| sort -r\\) \\| sort -u >.+/output/ProcDagTests\\.ProcDagTests\\.testJoinPipe2Uniq\\.out$", isRe=True)

    def testArgToArg(self):
        "pipe argument to argument of another process"
        nopen = self.numOpenFiles()
        pd = ProcDag()
        inf = self.getInputFile("simple1.txt")
        outf = self.getOutputFile(".out")
        io = Pipe()
        pd.create(("tee", POut(io)), stdin=inf, stdout="/dev/null")
        pd.create(("cat", PIn(io)), stdin="/dev/null", stdout=outf)
        pd.wait()
        self.diffExpected(".out")
        # formatting fails here: (FIXME: improve??), FIXME: doesn't always produce same result,
        # so we check multiple posibilities
        self.commonChecks(nopen, pd,
                          "(^tee >\\(cat <\\(tee \\.\\.\\.\\) </dev/null >.+/output/ProcDagTests\\.ProcDagTests\\.testArgToArg\\.out\\) <.+/input/simple1\\.txt >/dev/null ; cat \\.\\.\\.$)" \
                              + "|(^tee >\\(cat <\\(tee \\.\\.\\.\\) </dev/null >.+/output/ProcDagTests\\.ProcDagTests\\.testArgToArg\\.out\\) <.+/input/simple1\\.txt >/dev/null ; cat \\.\\.\\.$)",
                          isRe=True)

    def testStdioCycleDetect(self):
        "stdio cycle detection"
        nopen = self.numOpenFiles()
        pd = ProcDag()
        inf = self.getInputFile("simple1.txt")
        p1 = Pipe()
        p2 = Pipe()
        p3 = Pipe()
        p4 = Pipe()
        pd.create(("cat",), stdin=inf, stdout=p1)
        pd.create(("cat", "/dev/stdin", PIn(p4)), stdin=p1, stdout=p3)
        pd.create(("cat",), stdin=p3, stdout=p4)
        ex = None
        try:
            pd.wait()
        except ProcDagException, ex:
             pass
        msg = str(ex)
        expect = "cycle detected: entering: cat /dev/stdin <([Pipe])"
        if msg != expect:
            self.fail("'"+ msg + "' != '"+ expect + "'")
        self.commonChecks(nopen, pd, "{CYCLE}: cat ; cat ; cat /dev/stdin <([Pipe])")

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(ProcDagTests))
    return suite

if __name__ == '__main__':
    unittest.main()
