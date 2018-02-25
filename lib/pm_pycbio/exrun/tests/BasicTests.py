# Copyright 2006-2012 Mark Diekhans
"tests of basic functionality"

import unittest, sys, os, time
if __name__ == '__main__':
    sys.path.append("../../..")
from pm_pycbio.sys import fileOps
from pm_pycbio.sys.TestCaseBase import TestCaseBase
from pm_pycbio.exrun import ExRunException, ExRun, File, Target, Rule, Production, Verb
from pm_pycbio.exrun.Graph import RuleState, ProdState
from pm_pycbio.exrun.tests import ExRunTestCaseBase

# change this for debugging:
verbFlags=set((Verb.error,))
#xverbFlags=set((Verb.error, Verb.trace, Verb.details, Verb.dumpStart))
#verbFlags=Verb.all

class ProdSet(object):
    "set of file productions and contents; deletes files if they exist"
    
    def __init__(self, exRun, tester, exts):
        self.tester = tester
        self.prods = []
        self.contents = {}
        for ext in exts:
            fp = exRun.getFile(self.tester.getOutputFile(ext))
            fileOps.rmFiles(fp.path)
            self.prods.append(fp)
            self.contents[fp] = ext

    def check(self):
        "verify files and contents"
        for fp in self.prods:
            self.tester.checkContents(fp, self.contents)

class TouchRule(Rule):
    "rule that creates files"
    def __init__(self, name, tester, pset, requires=None):
        Rule.__init__(self, name, requires=requires, produces=pset.prods)
        self.tester = tester
        self.pset = pset
        self.touchCnt = 0

    def _touch(self, fp):
        "create a file product"
        self.tester.failUnless(isinstance(fp, File))
        ext = os.path.splitext(fp.path)[1]
        fileOps.ensureFileDir(fp.getOutPath())
        fh = open(fp.getOutPath(), "w")
        try:
            fh.write(self.pset.contents.get(fp))
        finally:
            fh.close()
            self.touchCnt += 1

    def execute(self):
        "create file products"
        for fp in self.produces:
            self._touch(fp)

class TouchTests(ExRunTestCaseBase):
    def checkContents(self, fp, contents={}):
        "contents is a dict of optional values to write"
        ext = os.path.splitext(fp.path)[1]
        self.verifyOutputFile(ext, contents.get(fp))

    def testSimple(self):
        "rule with no requirements "

        # rule creates three files with known content
        er = ExRun(verbFlags=verbFlags)
        pset = ProdSet(er, self, (".out1", ".out2", ".out3"))
        rule = TouchRule("simple", self, pset)
        er.addRule(rule)
        # use default target
        er.run()
        self.failUnlessEqual(rule.touchCnt, 3)
        pset.check()
        self.checkGraphStates(er)

        # try again, nothing should be made this time
        rule.touchCnt = 0
        er.run()
        self.failUnlessEqual(rule.touchCnt, 0)
        pset.check()
        self.checkGraphStates(er)

    class TwoLevel(object):
        "container for two level objects"
        def __init__(self, topPset, topRule, topTar, low1Pset, low1Rule, low1Tar, low2Pset, low2Rule, low2Tar):
            self.topPset = topPset
            self.topRule = topRule
            self.topTar = topTar
            self.low1Pset = low1Pset
            self.low1Rule = low1Rule
            self.low1Tar = low1Tar
            self.low2Pset = low2Pset
            self.low2Rule = low2Rule
            self.low2Tar = low2Tar

    def twoLevelSetup(self, er, makeTargets=False):
        "setup for tests using two levels of rules" 
        # lower level, 2 productions and rules
        low1Pset = ProdSet(er, self, (".low1a", ".low1b", ".low1c"))
        low1Rule = TouchRule("low1Rule", self, low1Pset)
        er.addRule(low1Rule)
        low2Pset = ProdSet(er, self, (".low2a", ".low2b", ".low2c"))
        low2Rule = TouchRule("low2Rule", self, low2Pset)
        er.addRule(low2Rule)
        
        # top level, dependent on intermediates
        topPset = ProdSet(er, self, (".top1", ".top2", ".top3"))
        topRule = TouchRule("top", self, topPset, requires=low1Pset.prods+low2Pset.prods)
        er.addRule(topRule)
        
        if makeTargets:
            topTar = er.obtainTarget("topTar", topPset.prods)
            low1Tar = er.obtainTarget("low1Tar", low1Pset.prods)
            low2Tar = er.obtainTarget("low2Tar", low2Pset.prods)
        else:
            topTar = low1Tar = low2Tar = None

        return self.TwoLevel(topPset, topRule, topTar, low1Pset, low1Rule, low1Tar, low2Pset, low2Rule, low2Tar)

    def testTwoLevel(self):
        "two levels of requirements"
        er = ExRun(verbFlags=verbFlags)
        tl = self.twoLevelSetup(er)
        er.obtainDefaultTarget(tl.topPset.prods)
        er.run()
        self.failUnlessEqual(tl.low1Rule.touchCnt, 3)
        tl.low1Pset.check()
        self.failUnlessEqual(tl.low2Rule.touchCnt, 3)
        tl.low2Pset.check()
        self.failUnlessEqual(tl.topRule.touchCnt, 3)
        tl.topPset.check()
        self.checkGraphStates(er)

    def testTargetLowLevel1(self):
        "target runs one low level rule"
        er = ExRun(verbFlags=verbFlags)
        tl = self.twoLevelSetup(er, makeTargets=True)
        er.run(targets=tl.low1Tar)
        self.failUnlessEqual(tl.low1Rule.touchCnt, 3)
        tl.low1Pset.check()
        self.failUnlessEqual(tl.low2Rule.touchCnt, 0)
        self.failUnlessEqual(tl.topRule.touchCnt, 0)
        self.checkGraphStates(er,
                              ((tl.low2Pset.prods, ProdState.outdated),
                               (tl.low2Rule,       RuleState.outdated),
                               (tl.topPset.prods,  ProdState.outdated),
                               (tl.topRule,        RuleState.outdated)))

    def testTargetLowLevel12(self):
        "target runs two low level rules"
        er = ExRun(verbFlags=verbFlags)
        tl = self.twoLevelSetup(er, makeTargets=True)
        er.run(targets=[tl.low1Tar, tl.low2Tar.name])
        self.failUnlessEqual(tl.low1Rule.touchCnt, 3)
        tl.low1Pset.check()
        self.failUnlessEqual(tl.low2Rule.touchCnt, 3)
        tl.low2Pset.check()
        self.failUnlessEqual(tl.topRule.touchCnt, 0)
        self.checkGraphStates(er,
                              ((tl.topPset.prods, ProdState.outdated),
                               (tl.topRule,       RuleState.outdated)))

    def testTargetTop(self):
        "target runs two levels"
        er = ExRun(verbFlags=verbFlags)
        tl = self.twoLevelSetup(er, makeTargets=True)
        er.run(targets=tl.topTar.name)
        self.failUnlessEqual(tl.low1Rule.touchCnt, 3)
        tl.low1Pset.check()
        self.failUnlessEqual(tl.low2Rule.touchCnt, 3)
        tl.low2Pset.check()
        self.failUnlessEqual(tl.topRule.touchCnt, 3)
        tl.topPset.check()
        self.checkGraphStates(er)

class CurrentProd(Production):
    "production that is always up-to-date"
    def __init__(self, name):
        Production.__init__(self, name)
    def getLocalTime(self):
        "always returns current time"
        return time.time()

class NeverRunRule(Rule):
    "rule that generates an error if it's run"
    def __init__(self, name, requires=None, produces=None):
        Rule.__init__(self, name, requires=requires, produces=produces)

    def execute(self):
        raise ExRunException("rule should never have been run")

class MiscTests(ExRunTestCaseBase):
    def testUptodateProd(self):
        "check that a rule is not run for a current production"
        er = ExRun(verbFlags=verbFlags)
        prod = CurrentProd("neverRunProd")
        er.addProd(prod)
        rule = NeverRunRule("neverRunRule", produces=prod)
        er.obtainDefaultTarget(prod)
        er.addRule(rule)
        er.run()
        self.checkGraphStates(er)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TouchTests))
    suite.addTest(unittest.makeSuite(MiscTests))
    return suite

if __name__ == '__main__':
    unittest.main()
