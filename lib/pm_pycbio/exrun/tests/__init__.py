# Copyright 2006-2012 Mark Diekhans
from pm_pycbio.sys.TestCaseBase import TestCaseBase
from pm_pycbio.exrun.Graph import ProdState,RuleState
from pm_pycbio.sys import typeOps

class ExRunTestCaseBase(TestCaseBase):
    "test bases class for unit tests that provides check of status "

    def __specsToStateTbl(self, notOkSpecs):
        "convert spec lists no tables"
        notOkStates = {}
        for spec in notOkSpecs:
            for n in typeOps.mkiter(spec[0]):
                notOkStates[n] = spec[1]
        return notOkStates

    def __checkStates(self, nodes, notOkStates, defaultState):
        "check states of one time of nodes"
        for n in nodes:
            expect = notOkStates.get(n)
            if expect == None:
                expect = defaultState
            if n.state != expect:
                self.fail("State of " + str(n.__class__) + " " +  n.name
                          + " state " + str(n.state)+ " != expected " + str(expect))
        
    def checkGraphStates(self, er, notOkSpecs=()):
        """check states of productions and rules nodes, ones that are
        expected to be non-ok should have entries in notOk specs.
        These are list in the forms ((nodes, state), ...); nodes can
        be a single node object to a list of them"""
        notOkStates = self.__specsToStateTbl(notOkSpecs)
        self.__checkStates(er.graph.productions, notOkStates, ProdState.current)
        self.__checkStates(er.graph.rules, notOkStates, RuleState.ok)
