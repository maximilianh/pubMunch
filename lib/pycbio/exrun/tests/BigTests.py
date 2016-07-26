# Copyright 2006-2012 Mark Diekhans
"tests of basic functionality"

import unittest, sys, os, time
if __name__ == '__main__':
    sys.path.append("../../..")
from pycbio.sys import typeOps
from pycbio.sys.TestCaseBase import TestCaseBase
from pycbio.exrun import ExRun, Target, Rule, Production, Verb
from pycbio.exrun.Graph import RuleState, ProdState, negInf
from pycbio.exrun.tests import ExRunTestCaseBase
import threading

# change this for debugging:
verbFlags=set((Verb.error,))
#verbFlags=set((Verb.error, Verb.trace, Verb.details, Verb.dumpStart))
#verbFlags=Verb.all
#verbFlags=set([Verb.dumpEnd])

class MemProd(Production):
    "production that is just in memory, track times"
    def __init__(self, name):
        Production.__init__(self, name)
        self.time = None

    def getLocalTime(self):
        return self.time

class MemRule(Rule):
    "rule for running in-memory tests"
    def __init__(self, name, requires=None, produces=None):
        Rule.__init__(self, name, requires=requires, produces=produces)

    def execute(self):
        for p in self.produces:
            time.sleep(0.00001) # allow other threads to run
            if (p.time != None) and (p.time > 0.0):
                raise Exception("production already done: " + p.name)
            p.time = time.time()


class RuleDef(object):
    """definition of a Rule either either symbolic name of productions or
    production objects"""
    def __init__(self, name, requires=None, produces=None):
        self.name = name
        self.requires = typeOps.mkset(requires)
        self.produces = typeOps.mkset(produces)

class ExprBuilder(object):
    """Build artificial experiments.  The graph is defined by a list of
    rule definition objects.  Productions are defined by lists
    of names. There is no other definition of the productions.
    """
    def __init__(self, ruleDefs, verbFlags=None, keepGoing=False):
        self.er = ExRun(verbFlags=verbFlags, keepGoing=keepGoing)
        self.graph = self.er.graph
        for rdef in ruleDefs:
            self.__addRuleDef(rdef)
        # set times leaf productions
        for p in self.graph.productions:
            if p.producedBy == None:
                p.time = time.time()

    def __addRuleDef(self, rdef):
        if rdef.name in self.graph.rulesByName:
            raise Exception("duplicate rule def: " + rdef.name)
        rule = self.er.addRule(MemRule(rdef.name))
        for pName in rdef.produces:
            self.__addProduces(rule, pName)
        for rName in rdef.requires:
            self.__addRequires(rule, rName)

    def __addProduces(self, rule, pName):
        prod = self.__obtainProd(pName)
        if prod.producedBy != None:
            raise Exception("duplicate production def: " + pName)
        rule.linkProduces(prod)
        
    def __addRequires(self, rule, rName):
        prod = self.__obtainProd(rName)
        rule.linkRequires(prod)
            
    def __obtainProd(self, pName):
        prod = self.graph.productionsByName.get(pName)
        if prod == None:
            prod = self.er.addProd(MemProd(pName))
        return prod

class BigTests(ExRunTestCaseBase):
    def testNoLeaves(self):
        "no leaf productions"
        eb = ExprBuilder([
                RuleDef("r1", (), ("p1.1","p1.2")),
                RuleDef("r2", ("p1.1","p1.2"), ("p2.1","p2.2")),
                ], verbFlags=verbFlags)
        eb.er.run()
        self.checkGraphStates(eb.er)

    def testLeaves(self):
        "leaf productions"
        eb = ExprBuilder([
                RuleDef("r1", ("p0.1","p0.2","p0.3"), ("p1.1","p1.2")),
                RuleDef("r2", ("p1.1","p1.2"), ("p2.1","p2.2")),
                ], verbFlags=verbFlags)
        eb.er.run()
        self.checkGraphStates(eb.er)

    def testDiamond(self):
        "graph with a diamond and crossed dependencies"
        eb = ExprBuilder([
                RuleDef("r1", ("p0.1","p0.2","p0.3"), ("p1.1","p1.2","p1.3")),
                RuleDef("r2.1", ("p1.1",), ("p2.1.1","p2.1.2")),
                RuleDef("r2.2", ("p1.2",), ("p2.2.1","p2.2.2")),
                RuleDef("r3.1", ("p2.1.1","p2.2.1"), ("p3.1.1","p3.1.2")),
                RuleDef("r3.2", ("p2.1.2","p2.2.2"), ("p3.2.1","p3.2.2")),
                RuleDef("r4", ("p3.1.1","p3.1.2", "p3.2.1","p3.2.2"), ("p4",))
                ], verbFlags=verbFlags)
        eb.er.run()
        self.checkGraphStates(eb.er)

    def mkRule(self, level, maxLevel, numLeaves, rname, rules):
        """recursively construct tree of rules, return output production name from
        previous level"""
        reqs = []
        if level != maxLevel:
            for ir in range(level+1):
                reqs.append(self.mkRule(level+1, maxLevel, numLeaves, rname + "#"+str(level) + "."+str(ir), rules))
        else:
            for ip in range(numLeaves):
                reqs.append(rname+"#leaf"+str(ip))
        prod = rname + "#prod"+str(level)
        rules.append(RuleDef(rname, reqs, prod))
        return prod
    
    def testMany(self):
        "graph with a many independent rules in a tree"
        rules =  []
        self.mkRule(0, 4, 5, "rule.0", rules)
        eb = ExprBuilder(rules, verbFlags=verbFlags)
        eb.er.run()
        self.checkGraphStates(eb.er)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(BigTests))
    return suite

if __name__ == '__main__':
    unittest.main()
