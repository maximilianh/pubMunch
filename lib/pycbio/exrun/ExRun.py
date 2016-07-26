# Copyright 2006-2012 Mark Diekhans
"""Experiment running objects"""

import os.path,sys,socket,threading
from pycbio.exrun.Graph import *
from pycbio.sys import typeOps, strOps, PycbioException
from pycbio.exrun import ExRunException, Verb, Sched
from pycbio.exrun.CmdRule import CmdRule, Cmd, File, FileIn, FileOut

os.stat_float_times(True) # very, very gross

# FIXME: should rules/prods automatically be added? ExRun object to constructor
# would do the trick
# FIXME: Dir object that is just as a base for File objects.
# FIXME: error output is really hard to read, especially when executing a non-existent program
#        just get `OSError: [Errno 2] No such file or directory', not much help
# FIXME: need to improve graph dump
# FIXME: need tracing of what is out of date (dry run)


# NOTES:
#  - thread schedule should be based on load level; setup of a cluster job
#    adds a big load, running cluster jobs doesn't.


class _RuleTask(object):
    """Object that is associated with a task and runs a rule,"""
    def __init__(self, exrun, rule):
        assert(isinstance(rule, Rule))
        assert(rule.state == RuleState.outdated)
        # MUST FLAG AS RUNNING NOW
        rule.setState(RuleState.running)
        self.exrun = exrun
        self.verb = self.exrun.verb
        self.rule = rule

    def __preEvalRuleCheck(self):
        "Sanity check before a rule is run"
        for r in self.rule.requires:
            if r.getLocalTime() == None:
                raise ExRunException("require should have been built:" + str(r))

    def __postEvalRuleCheck(self):
        "check that a rule build it's productions"
        for p in self.rule.produces:
            if p.getLocalTime() == None:
                if (p.producedBy == None):
                    raise ExRunException("No rule to build production: " + str(p))
                else:
                    raise ExRunException("Production not built: " + str(p))
        if self.rule.isOutdated():
            raise ExRunException("rule didn't update all productions: " + Node.joinNames(self.rule.getOutdated()))

    def __finishSucceed(self):
        """finish up finish up requires/produces on success, failures here
        cause the rule to fail"""
        for p in self.rule.produces:
            p.finishSucceed()
        for r in self.rule.requires:
            r.finishRequire()

    def __finishFail(self):
        """finish up finish up requires/produces on failure, will log errors,
        but not fail so original error is not lost"""
        for p in self.rule.produces:
            try:
                p.finishFail()
            except Exception as ex:
                # FIXME: count these errors
                ex = ExRunException("Error in Production.finishFail() for "+p.name,
                                    cause=ex)
                self.verb.prall(str(ex))
                self.verb.pr(Verb.error, ex.format())
        for r in self.rule.requires:
            try:
                r.finishRequire()
            except Exception as ex:
                ex = ExRunException("Error in Production.finishRequire() for "+name,
                                    cause=ex)
                self.verb.prall(str(ex))
                self.verb.pr(Verb.error, +ex.format())
        
    def __evalRule(self):
        "evaluate a rule"
        assert(self.rule.state == RuleState.running)
        self.verb.enter(Verb.trace, "eval rule:", self.rule)
        try:
            self.__preEvalRuleCheck()
            self.verb.pr(Verb.details, "run:", self.rule)
            self.rule.execute()
            self.__finishSucceed()
            self.__postEvalRuleCheck()
            self.rule.setState(RuleState.ok)
            self.verb.leave(Verb.trace, "done rule:", self.rule)
        except Exception as ex:
            ex = ExRunException("rule error: "+str(self.rule), cause=ex)
            self.verb.pr((Verb.trace,Verb.error), str(ex))
            self.verb.pr(Verb.error, ex.format())
            self.rule.setState(RuleState.failed)
            self.__finishFail()
            self.verb.leave((Verb.trace,Verb.error), "failed rule:", self.rule)
            raise ex

    def run(self, task):
        "run method to start task"
        try:
            self.rule.task = task
            self.__evalRule()
        except Exception as ex:
            self.exrun.flagError(ex)
        self.rule.task = None
        self.exrun.scheduleReady()

class ExRun(object):
    """Run an experiment.
    """
    # name of default target
    defaultTargetName = "default"

    """Object that defines and runs an experiment.  """
    def __init__(self, verbFlags=None, keepGoing=False, maxLocalThreads=1):
        self.lock = threading.RLock()
        self.verb = Verb(flags=verbFlags)
        self.keepGoing = keepGoing
        self.graph = Graph(self, self.verb)
        self.hostName = socket.gethostname()
        self.uniqIdCnt = 0
        self.files = {}
        self.running = False
        self.failedCnt = 0
        self.targets = None # list of targets to execute
        self.sched = Sched.Sched()
        self.sched.obtainLocalGroup(maxLocalThreads)
        self.errors = [] # exceptions that occurred

    def setRemoteMaxThreads(self, host, maxThreads):
        """set the max number of threads on a remote host"""
        self.sched.obtainGroup(host).setMaxConcurrent(maxConcurrent)

    def __modGraphErr(self):
        "generate error on attempt to modify graph after started running"
        raise ExRunException("attempt to modify graph once running")

    def getUniqId(self):
        "get a unique id for generating file names"
        with self.lock:
            id = self.hostName + "." + str(os.getpid()) + "." + str(self.uniqIdCnt)
            self.uniqIdCnt += 1
        return id

    def getAtomicPath(self, path, namePrefix="tmp"):
        """generate a unique temporary file path from path, the file will be
        in the same directory.  The file extensions will be maintained, to
        allow recognition of file types, etc. """
        # FIXME: check for non-existence
        return os.path.join(os.path.dirname(path),
                            namePrefix + "." + self.getUniqId() + "." + os.path.basename(path))

    def addTarget(self, target):
        "add a new Target"
        assert(isinstance(target, Target))
        if self.running:
            self.__modGraphErr()
        self.graph.addTarget(target)
        return target

    def addRule(self, rule):
        "add a new rule"
        assert(isinstance(rule, Rule))
        if self.running:
            self.__modGraphErr()
        self.graph.addRule(rule)
        return rule

    def addProd(self, prod):
        "add a new Production"
        assert(isinstance(prod, Production))
        if self.running:
            self.__modGraphErr()
        self.graph.addProd(prod)
        return prod

    def getFile(self, path):
        """get a file production, creating if it doesn't exist, if path is already an
        instance of File instead of a string, just return it."""
        # doesn't require locking
        if isinstance(path, File):
            return path
        realPath = os.path.realpath(path)
        fprod = self.files.get(realPath)
        if fprod == None:
            if self.running:
                self.__modGraphErr()
            self.files[realPath] = fprod = File(path, realPath)
            self.addProd(fprod)
        return fprod

    def getFiles(self, paths):
        """like getFile(), only path can be a single path, or a list of paths,
        or a File object, or list of File objects.  Returns a list of File
        objects"""
        files = []
        for p in typeOps.mkiter(paths):
            files.append(self.getFile(p))
        return files

    def obtainTarget(self, name, requires=None):
        """get a target production, creating if it doesn't exist, optionally
        adding new requires"""
        n = self.graph.targetsByName.get(name)
        if n == None:
            n = self.addTarget(Target(name))
        if requires != None:
            n.linkRequires(requires)
        return n

    def obtainDefaultTarget(self, requires=None):
        """get the target production, creating if it doesn't exist, optionally
        adding new requires"""
        return self.obtainTarget(self.defaultTargetName, requires)

    def addCmd(self, cmd, name=None, requires=None, produces=None, stdin=None, stdout=None, stderr=None):
        """add a command rule with a single command or pipeline, this is a
        shortcut for addRule(CmdRule(Cmd(....),...)"""
        return self.addRule(CmdRule(Cmd(cmd, stdin=stdin, stdout=stdout, stderr=stderr), name=name, requires=requires, produces=produces))
    
    def flagError(self, err):
        "add an error from a rule to the list"
        with self.lock:
            self.errors.append(err)
        # FIXME abort other tasks

    def scheduleReady(self):
        # must synchronize until rule states are changed
        with self.lock:
            if len(self.errors) == 0:
                for rule in self.graph.getReady(self.targets):
                    self.sched.addTask(_RuleTask(self, rule).run,
                                       Sched.groupLocal)
            
    def getTarget(self, name):
        "find a target object by name, or an error"
        target = self.graph.targetsByName.get(name)
        if target == None:
            raise ExRunException("no target named: " + name)
        return target

    def __getRunTargets(self, targets=None):
        "get list of targets to run, return the default if none specified"
        ts = []
        for t in typeOps.mkiter(targets):
            if isinstance(t, Target):
                ts.append(t)
            else:
                ts.append(self.getTarget(t))
        if len(ts) == 0:
            ts.append(self.getTarget(self.defaultTargetName))
        return ts

    def __reportExprError(self, ex):
        self.verb.prall(strOps.dup(80,"=")+"\n")
        self.verb.prall(PycbioException.formatExcept(ex) + "\n")
        self.verb.prall(strOps.dup(80,"-")+"\n")

    def __reportExprErrors(self):
        "final error reporting at the end of a run"
        if self.verb.enabled(Verb.error):
            for ex in self.errors:
                self.__reportExprError(ex)
        raise ExRunException("Experiment failed: " + str(len(self.errors)) + " error(s) encountered")

    def __runTargets(self, targets):
        "run targets"
        try:
            self.targets = self.__getRunTargets(targets)
            self.scheduleReady()
            self.sched.run()
        finally:
            if self.verb.enabled(Verb.dumpEnd):
                self.dumpGraph("ending")
        if len(self.errors) > 0:
            self.__reportExprErrors()
        

    def run(self, targets=defaultTargetName, dryRun=False):
        """run the experiment, If targets are not specified, the default
        target is used.  Otherwise it can be a single or list of Target objects
        or names.
        """
        self.running = True
        if self.verb.enabled(Verb.dumpIn):
            self.dumpGraph("input")
        self.graph.complete(self.defaultTargetName)
        if self.verb.enabled(Verb.dumpStart):
            self.dumpGraph("starting")
        if not dryRun:
            self.__runTargets(targets=targets)
        self.running = False

    def __dumpTarget(self, target, fh=None):
        self.verb.prall("target:", str(target))
        self.verb.enter()
        pre = "req: "
        for r in target.requires:
            self.verb.prall(pre, str(r))
            pre = "     "
        self.verb.leave()
        
    def __dumpRule(self, rule, fh=None):
        self.verb.prall("Rule:", str(rule), "<"+str(rule.state)+">")
        self.verb.enter()
        pre = "prd: "
        for p in rule.produces:
            self.verb.prall(pre, str(p))
            pre = "     "
        pre = "req: "
        for r in rule.requires:
            self.verb.prall(pre, str(r))
            pre = "     "
        self.verb.leave()
        
    def __dumpProduction(self, prod):
        self.verb.prall("Production:", str(prod), "<"+ str(prod.state)+ ">", "["+str(prod.getLocalTime())+"]")
        self.verb.enter()
        self.verb.prall("producedBy:", str(prod.producedBy))
        self.verb.leave()
        
    def dumpGraph(self, msg, fh=None):
        if fh != None:  # FIXME: kind of hacky
            holdFh = self.verb.fh
            self.verb.fh = fh
        self.verb.prall(strOps.dup(70, "="))
        self.verb.prall("graph dump:", msg)
        self.verb.enter()
        for target in self.graph.targets:
            self.__dumpTarget(target)
        for node in self.graph.bfs():
            if isinstance(node, Rule):
                self.__dumpRule(node)
            elif isinstance(node, Production):
                self.__dumpProduction(node)
        self.verb.leave()
        self.verb.prall(strOps.dup(70, "^"))
        if fh != None:
            self.verb.fh = holdFh

__all__ = (ExRun.__name__)
