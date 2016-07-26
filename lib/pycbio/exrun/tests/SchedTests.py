# Copyright 2006-2012 Mark Diekhans
"test of Sched class independent of rest of ExRun"

import unittest, sys, os, time
if __name__ == '__main__':
    sys.path.append("../../..")
from pycbio.sys import fileOps
from pycbio.sys.TestCaseBase import TestCaseBase
from pycbio.exrun.Sched import Sched

def mkGrpName(i):
    "group name from a number"
    return "grp"+str(i)

class TrivialTask(object):
    "just note that task ran"
    def __init__(self):
        self.ran = False

    def run(self, task):
        self.ran = True

class MoveGrpTask(object):
    "just note that task ran"
    def __init__(self, newGrp):
        self.newGrp = newGrp
        self.ran = False
        self.moved = False

    def run(self, task):
        self.ran = True
        task.moveGroup(self.newGrp)
        self.moved = (task.group.name == self.newGrp)

class SchedTests(TestCaseBase):
    def testSimple(self):
        "simple scheduling and running of tasks"
        sched = Sched()
        grp = sched.obtainLocalGroup()
        tasks = []
        for i in range(5):
            t = TrivialTask()
            tasks.append(t)
            sched.addTask(t.run, grp)
        sched.run()
        for t in tasks:
            self.failUnless(t.ran)

    def testSimpleGrps(self):
        "simple tasks in groups"
        sched = Sched()
        tasks = []
        for i in range(10):
            t = TrivialTask()
            tasks.append(t)
            grp = sched.obtainGroup(mkGrpName(i%2), 2)
            sched.addTask(t.run, grp)
        sched.run()
        for t in tasks:
            self.failUnless(t.ran)

    def testMoveGrps(self):
        "tasks moving between groups"
        sched = Sched()
        tasks = []
        for i in range(10):
            t = MoveGrpTask(mkGrpName(i+1))
            tasks.append(t)
            grp = sched.obtainGroup(mkGrpName(i))
            sched.addTask(t.run, grp)
        sched.run()
        for t in tasks:
            self.failUnless(t.ran)
            self.failUnless(t.moved)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(SchedTests))
    return suite

if __name__ == '__main__':
    unittest.main()
