# Copyright 2006-2012 Mark Diekhans
import unittest, sys
if __name__ == '__main__':
    sys.path.append("../../..")
from pm_pycbio.sys.Pipeline import *
from pm_pycbio.sys import procOps
from pm_pycbio.sys.TestCaseBase import TestCaseBase


class PipelineTests(TestCaseBase):
    def cpFileToPl(self, inName, pl):
        inf = self.getInputFile(inName)
        fh = open(inf)
        for line in fh:
            pl.write(line)
        fh.close()

    def testWrite(self):
        outf = self.getOutputFile(".out")
        outfGz = self.getOutputFile(".out.gz")

        pl = Pipeline(("gzip", "-1"), "w", otherEnd=outfGz)
        self.cpFileToPl("simple1.txt", pl)
        pl.wait()

        procOps.runProc(("zcat", outfGz), stdout=outf)
        self.diffExpected(".out")

    def testWriteMult(self):
        outf = self.getOutputFile(".wc")

        # grr, BSD wc adds an extract space, so just convert to tabs
        pl = Pipeline((("gzip", "-1"),
                       ("gzip", "-dc"),
                       ("wc",),
                       ("sed", "-e", "s/  */\t/g")),
                      "w", otherEnd=outf)
        self.cpFileToPl("simple1.txt", pl)
        pl.wait()

        self.diffExpected(".wc")

    def cpPlToFile(self, pl, outExt):
        outf = self.getOutputFile(outExt)
        fh = open(outf, "w")
        for line in pl:
            fh.write(line)
        fh.close()

    def testRead(self):
        inf = self.getInputFile("simple1.txt")
        infGz = self.getOutputFile(".txt.gz")
        procOps.runProc(("gzip", "-c", inf), stdout=infGz)

        pl = Pipeline(("gzip", "-dc"), "r", otherEnd=infGz)
        self.cpPlToFile(pl, ".out")
        pl.wait()

        self.diffExpected(".out")

    def testReadMult(self):
        inf = self.getInputFile("simple1.txt")

        pl = Pipeline((("gzip","-1c"),
                       ("gzip", "-dc"),
                       ("wc",),
                       ("sed", "-e", "s/  */\t/g")),
                      "r", otherEnd=inf)
        self.cpPlToFile(pl, ".wc")
        pl.wait()

        self.diffExpected(".wc")

    def XXtestPassRead(self):
        "using FIFO to pass pipe to another process for reading"
        # FIXME: should this be supported somehow
        inf = self.getInputFile("simple1.txt")
        infGz = self.getOutputFile(".txt.gz")
        cpOut = self.getOutputFile(".out")
        procOps.runProc(("gzip", "-c", inf), stdout=infGz)

        pl = Pipeline(("gzip", "-dc"), "r", otherEnd=infGz)
        procOps.runProc(["cat"],  stdin=pl.pipePath, stdout=cpOut)
        pl.wait()

        self.diffExpected(".out")

    def XXtestPassWrite(self):
        "using FIFO to pass pipe to another process for writing"
        # FIXME: should this be supported somehow
        inf = self.getInputFile("simple1.txt")
        outf = self.getOutputFile(".out")
        pipePath = self.getOutputFile(".fifo")

        pl = Pipeline(("sort", "-r"), "w", otherEnd=outf, pipePath=pipePath)
        procOps.runProc(["cat"],  stdin=inf, stdout=pl.pipePath)
        pl.wait()

        self.diffExpected(".out")

    def testExitCode(self):
        pl = Pipeline(("false",))
        e = None
        try:
            pl.wait()
        except ProcException as e:
            pass
        self.failUnless(e != None)
        # FIXME: should Procline keep an ordered list?
        for p in pl.procs:
            self.failUnless(p.returncode == 1)

    def testSigPipe(self):
        "test not reading all of pipe output"
        pl = Pipeline([("yes",), ("true",)], "r")
        pl.wait()

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(PipelineTests))
    return suite

if __name__ == '__main__':
    #unittest.main()
    # FIXME: tmp
    s = suite()
    alltests = unittest.TestSuite(suite())
    runner = unittest.TextTestRunner(sys.stdout, verbosity=2)
    # FIXME:
    #doTrace = True
    doTrace = False
    if doTrace:
        from pm_pycbio.sys.Trace import Trace
        ignoreMods = ("os", sys, "posixpath", unittest, "UserDict",
                      "threading", "stat", "traceback", "linecache",
                      "pm_pycbio.sys.TestCaseBase")
        tr = Trace("debug.log", ignoreMods=ignoreMods,
                   inclThread=False, inclPid=True)
        tr.enable()
    result = runner.run(alltests)
    if not result.wasSuccessful():
        sys.exit(1)
