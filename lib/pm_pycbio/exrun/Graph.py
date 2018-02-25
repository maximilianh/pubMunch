# Copyright 2006-2012 Mark Diekhans
"""Graph (DAG) of productions and rules.  This also manages 
of production and rule state flags."""
from __future__ import with_statement
import os.path, threading
from pm_pycbio.sys import typeOps
from pm_pycbio.sys.Enumeration import Enumeration
from pm_pycbio.exrun import ExRunException, Verb
import sys # FIXME: debug

# FIXME: adding node has gotten too complex
# FIXME: state initialization is rather complex and redundent
# FIXME: look for unused methods
# FIXME: move states to be class methods of root/production
# FIXME: use of r/p vars confusing r == requres/rule, etc

posInf = float("inf")
negInf = float("-inf")
_emptySet = frozenset()

class CycleException(ExRunException):
    "Exception indicating a cycle has occured"
    def __init__(self, cycle):
        desc = []
        for n in  cycle:
            desc.append("  " + str(n) + " ->")
        ExRunException.__init__(self, "cycle detected:\n" + "\n".join(desc))

# state of a Production and valid transitions
#  - bad - doesn't exist and no rule to create
#  - failed - rule to create failed
#  - blocked - blocked because some requirement can't be built
ProdState = Enumeration("ProdState",
                        ("unknown", "outdated", "current", "failed", "blocked", "bad"))
_prodStateTbl = {}
_prodStateTbl[ProdState.unknown]  = frozenset([ProdState.outdated, ProdState.current, ProdState.blocked, ProdState.bad])
_prodStateTbl[ProdState.outdated] = frozenset([ProdState.current, ProdState.blocked, ProdState.failed, ProdState.bad])
_prodStateTbl[ProdState.current]  = _emptySet
_prodStateTbl[ProdState.failed]   = _emptySet
_prodStateTbl[ProdState.blocked]  = _emptySet
_prodStateTbl[ProdState.bad]      = _emptySet

# state of a Rule and valid transitions
#  - failed - rule failed
#  - blocked - blocked because some requirement can't be built
RuleState = Enumeration("RuleState",
                        ("unknown", "outdated", "running", "ok", "failed", "blocked"))
_ruleStateTbl = {}
_ruleStateTbl[RuleState.unknown]  = frozenset([RuleState.ok, RuleState.outdated, RuleState.blocked])
_ruleStateTbl[RuleState.outdated] = frozenset([RuleState.running, RuleState.blocked])
_ruleStateTbl[RuleState.running]  = frozenset([RuleState.ok, RuleState.failed])
_ruleStateTbl[RuleState.ok]       = _emptySet
_ruleStateTbl[RuleState.failed]   = _emptySet
_ruleStateTbl[RuleState.blocked]  = _emptySet

class Node(object):
    """Base object for all entries in graph. Derived object define generators
    prevNodes() and nextNodes() for traversing tree."""
    def __init__(self, name, shortName):
        assert(isinstance(name, str))
        self.name = name
        self.shortName = shortName if (shortName != None) else name
        self.graph = None # set when added to graph
        # these set when added to graph
        self.exrun = None
        self.verb = None

    def __str__(self):
        return self.shortName

    @staticmethod
    def joinNames(nodes):
        "join node names into a sorted, comma-separated list"
        nodes = list(nodes)
        nodes.sort()
        return ", ".join([n.name for n in nodes])
    
class Production(Node):
    """Base class for a production. It can only have one requires (produced by)
    link.  When a production is being create by a rule, it is single threaded.
    Once the rule is creating a production has finished, access to it is
    multi-threaded when used as a required."""
    def __init__(self, name, shortName=None):
        """Name must be unique, short name does not have to be unique and defaults to name."""
        Node.__init__(self, name, shortName);
        self.producedBy = None
        self.requiredBy = set()
        self.state = ProdState.unknown

    def _transition(self, newState):
        "transition to a new state, validating transition"
        if newState not in _prodStateTbl[self.state]:
            raise ExRunException("invalid Production transition: " + str(self.state) + " to " + str(newState) + " on " + self.name)
        self.state = newState

    def __stateFromRuleState(self, rs):
        "get state give state of producing rule"
        if rs == RuleState.outdated:
            return ProdState.outdated
        elif rs == RuleState.running:
            return ProdState.outdated
        elif rs == RuleState.ok:
            return ProdState.current
        elif rs == RuleState.failed:
            return ProdState.failed
        elif rs == RuleState.blocked:
            return ProdState.blocked
        else:
            raise ExRunException("BUG: can't update production from rule in state: " + str(rs))

    def computeState(self):
        "compute and update state from producing rule state"
        if self.producedBy == None:
            # no rule to produce, must exist
            if prod.getLocalTime() == None:
                prod._transition(ProdState.bad)
            else:
                prod._transition(ProdState.current)
        else:
            self._transition(self.__stateFromRuleState(self.producedBy.state))

    def nextNodes(self):
        "generator for traversing tree"
        if self.producedBy != None:
            yield self.producedBy

    def prevNodes(self):
        "generator for traversing tree"
        for r in self.requiredBy:
            yield r

    def linkProducedBy(self, producedBy):
        "set producedBy link, and link back to this object"
        if self.producedBy != None:
            raise ExRunException("Production " + str(self) + " producedBy link already set")
        if not isinstance(producedBy, Rule):
            raise ExRunException("Production " + str(self) + " producedBy can only be linked to a Rule, attempt to link to: " + str(producedBy))
        self.producedBy = producedBy
        producedBy.produces.add(self)

    def linkRequiredBy(self, requiredBy):
        """link to rules that require production, can be single rule, lists or
        sets of rules, reverse links are created"""
        for r in typeOps.mkiter(requiredBy):
            if not isinstance(r, Rule):
                raise ExRunException("Production " + str(self) + " requiredBy can only be linked to a Rule, attempt to link to: " + str(r))
            self.requiredBy.add(r)
            r.requires.add(self);

    def setState(self, state):
        "recursively set the state or this production and parents"
        with self.graph.lock:
            self.verb.enter(Verb.debug, "Production.setState: " + str(self.state), "=>", str(state), self.name) # FIXME
            self._transition(state)
            if (state == ProdState.failed) or (state == ProdState.blocked):
                for r in self.requiredBy:
                    r.setState(RuleState.blocked)
            # FIXME: could have a ready state
            self.verb.leave() # FIXME

    def finishSucceed(self):
        "called when the rule to create this production finishes successfully"
        pass

    def finishFail(self):
        "called when the rule to create this production fails"
        pass

    def finishRequire(self):
        """Called when the rule that requires this production finishes. A
        requirement should never be modified, however this is useful for
        cleaning up things like decompress pipelines.  Must be thread-safe."""
        pass

    def getLocalTime(self):
        """Get the modification time of this Production as a floating point number.
        If the object does not exist, return None.  This is not recurisve (local), it is
        simply the time associated with the production.   Must be thread-safe."""
        raise ExRunException("getLocalTime() not implemented for Production " + str(type(self)))

    def getUpdateTime(self):
        """Recursively get the update time of this Products as a floating point
        number.  This is the oldest time recursive times of any of the requirements.
        If the production does not exist, returns None. """
        pt = self.getLocalTime()
        if (pt == None) or (self.producedBy == None):
            return None # short-circuit
        else:
            return min(pt, self.producedBy.getUpdateTime())

class Rule(Node):
    """Base class for a rule.  A rule is single threaded."""
    def __init__(self, name, requires=None, produces=None, shortName=None):
        """requires/produces can be a single Production or list of productions.
        name must be unique, short name does not have to be unique and defaults to name"""
        Node.__init__(self, name, shortName);
        self.state = RuleState.unknown
        self.requires = set()
        self.produces = set()
        if requires != None:
            self.linkRequires(requires)
        if produces != None:
            self.linkProduces(produces)
        # set when task is associated
        self.task = None

    def __str__(self):
        return self.name

    def _transition(self, newState):
        "transition to a new state, validating transition"
        if newState not in _ruleStateTbl[self.state]:
            raise ExRunException("invalid Rule transition: " + str(self.state) + " to " + str(newState) + " on " + self.name)
        self.state = newState

    def __getRequiresStates(self):
        "get a set of all of the states of requires"
        states = set()
        for r in self.requires:
            states.add(r.state)
        return states

    def __getRequiresTime(self):
        """get the newest requires local time, +inf if any don't exist, None if no
        requires"""
        if len(self.requires) == 0:
            return None
        rtime = negInf
        for r in self.requires:
            rt = r.getLocalTime()
            if rt == None:
                return posInf
            elif rt > rtime:
                rtime = rt
        return rtime

    def __getProducesTime(self):
        """get the oldest produces local time, -inf if any don't exist, None if no
        produces"""
        if len(self.produces) == 0:
            return None
        ptime = posInf
        for p in self.produces:
            pt = p.getLocalTime()
            if pt == None:
                return negInf
            elif pt < ptime:
                ptime = pt
        return ptime

    def __stateFromRequiresState(self, reqStates):
        "compute state from require states, return None if times must be compared"
        if ProdState.unknown in reqStates:
            raise ExRunException("BUG: can't update rule from production in state: " + str(ProdState.unknown))
        elif ProdState.failed in reqStates:
            return RuleState.failed
        elif (ProdState.bad in reqStates) or (ProdState.blocked in reqStates):
            return RuleState.blocked
        else:
            return None

    def __stateFromReqProdTimes(self):
        "compute state by comparing times of requires and produces"
        # can only look at times of produces, not state, as state is set
        # bottom up
        rtime = self.__getRequiresTime()  # or None or +Inf
        ptime = self.__getProducesTime()  # or None or -Inf
        if rtime == None:
            # no requires
            if ptime == None:
                return RuleState.failed  # should never happen
            elif ptime < 0:
                return RuleState.outdated
            else:
                return RuleState.ok  # exists
        elif (ptime == None) or (ptime < rtime):
            return RuleState.outdated
        else:
            return RuleState.ok

    def computeState(self):
        """Compute and update state from requires and produces Productions"""
        newState = self.__stateFromRequiresState(self.__getRequiresStates())
        if newState == None:
            newState = self.__stateFromReqProdTimes()
        self._transition(newState)

    def setState(self, state):
        "recursively set the state or this rule and parents"
        with self.graph.lock:
            self.verb.enter(Verb.debug, "Rule.setState: " + str(self.state), "=>", str(state), self.name) # FIXME
            self._transition(state)
            if state == RuleState.running:
                pstate = None
            elif state == RuleState.ok:
                pstate = ProdState.current
            elif state == RuleState.failed:
                pstate = ProdState.failed
            elif state == RuleState.blocked:
                pstate = ProdState.blocked
            else:
                raise ExRunException("BUG: Rule.setState() invalid state:" + str(state))
            if pstate != None:
                for p in self.produces:
                    p.setState(pstate)
            self.verb.leave() # FIXME

    def nextNodes(self):
        "generator for traversing tree"
        for r in self.requires:
            yield r

    def prevNodes(self):
        "generator for traversing tree"
        for p in self.produces:
            yield p

    def linkRequires(self, requires):
        """link in productions required by this node. Can be single nodes, or
        lists or sets of nodes, reverse links are created"""
        for r in typeOps.mkiter(requires):
            if not isinstance(r, Production):
                raise ExRunException("Rule " + str(self) + " requires can only be linked to a Production, attempt to link to: " + str(r))
            r.linkRequiredBy(self)
            
    def linkProduces(self, produces):
        """link in productions produced by this node. Can be single nodes, or
        lists or sets of nodes, reverse links are created"""
        for p in typeOps.mkiter(produces):
            if not isinstance(p, Production):
                raise ExRunException("Rule " + str(self) + " produces can only be linked to a Production, attempt to link to: " + str(p))
            p.linkProducedBy(self)

    def execute(self):
        """Execute the rule, must be implemented by derived class.  ExRun will
        call the approriate finish*() methods on the produces and requires,
        they should not be run by the derived rule class"""
        raise ExRunException("execute() not implemented for " + str(type(self)))

    def getUpdateTime(self):
        """Recursively get the update time of this Rule as a floating point
        number.  This is the oldest recursive times of any of the requirements.
        If a requried do not exist, returns None. """
        # FIXME: bad name, needed??
        if len(self.returns) == 0:
            return None
        rt = posInf
        for r in self.requires:
            pt = r.getUpdateTime()
            if pt == None:
                return None # short-circuit
            rt = min(rt, pt)
        return rt
    
    def __isProdOutdated(self, rtime, prod):
        """is production outdated relative to requirement time, rtime is None
        if no requires, +Inf one doesn exist """
        # FIXME: needed
        pt = prod.getLocalTime()
        return (pt == None) or ((rtime != None) and ((pt < rtime) or (rtime < 0)))

    def getOutdated(self):
        """Get list of productions that are out of date relative to requires"""
        # FIXME: needed
        outdated = []
        rtime = self.__getRequiresTime()
        for p in self.produces:
            if self.__isProdOutdated(rtime, p):
                outdated.append(p)
        return outdated

    def isOutdated(self):
        """Determine if this rule is outdated and needs to be run.  It is
        outdated if any of the requires are newer than any of the produces.  If
        there are no requires, it is outdated if any of the produces don't
        exist.  This is not recursive."""
        # FIXME: needed
        rtime = self.__getRequiresTime()
        for p in self.produces:
            if self.__isProdOutdated(rtime, p):
                return True
        return False

    def isReady(self):
        "is this rule ready to be run?" 
        if self.state != RuleState.outdated:
            return False
        for r in self.requires:
            if r.state != ProdState.current:
                return False
        return True

class Target(Node):
    """A target is an explicit entry point into a graph.  It can have multiple
    productions or other targets as children, but will not be listed as their
    parents.  Thus multiple Targets can refer to the same production.  A graph
    must have at least one target.
    """
    def __init__(self, name, requires=None, shortName=None):
        """requires can be Productions or Targets, Name must be unique, short
        name does not have to be unique and defaults to name."""
        Node.__init__(self, name, shortName)
        self.requires = set()
        if requires != None:
            self.linkRequires(requires)
        
    def linkRequires(self, requires):
        """link in productions or targets required by this node. Can be single nodes,
        or lists or sets of nodes"""
        for r in typeOps.mkiter(requires):
            if not (isinstance(r, Production) or isinstance(r, Target)):
                raise ExRunException("Target " + str(self) + " requires can only be linked to a Production or a Target, attempt to link to: " + str(r))
            self.requires.add(r)
            
    def nextNodes(self):
        "generator for traversing tree"
        for r in self.requires:
            yield r

class Graph(object):
    """Graph of productions and rules. Assumes that topology modifications
    are single threaded"""
    def __init__(self, exRun, verb):
        """Construct graph call complete() after adding nodes and edges."""
        self.exrun = exRun
        self.verb = verb
        self.nodes = set()
        self.productions = set()
        self.productionsByName = {}
        self.entryProds = set()
        self.rules = set()
        self.rulesByName = {}
        self.targets = set()
        self.targetsByName = {}
        self.lock = threading.RLock()

    def __addNode(self, desc, node, nodeList, nodeIdx):
        "common operations on adding a node"
        self.nodes.add(node)
        node.graph = self
        if node.name in nodeIdx:
            raise ExRunException("duplicate " + desc + " name: " + node.name)
        nodeList.add(node)
        nodeIdx[node.name] = node
        node.exrun = self.exrun
        node.verb = self.verb
        
    def addTarget(self, target):
        "add a target to the graph"
        assert(isinstance(target, Target))
        with self.lock:
            self.__addNode("Target", target, self.targets, self.targetsByName)

    def getEntryProductions(self):
        """Get entry productions of the graph; that is those having no requireBy"""
        entries = [p for p in self.productions if (len(p.requiredBy) == 0)]
        # sorted by names so that test produce consistent results
        entries.sort(cmp=lambda a,b:cmp(a.name,b.name))
        return entries

    def addRule(self, rule):
        "add a rule to the graph"
        assert(isinstance(rule, Rule))
        with self.lock:
            self.__addNode("Rule", rule, self.rules, self.rulesByName)

    def addProd(self, prod):
        "add a production to the graph"
        assert(isinstance(prod, Production))
        with self.lock:
            self.__addNode("Production", prod, self.productions, self.productionsByName)

    def __findEntryProductions(self):
        """Get entry productions of the graph; that is those no requiredBy,
        and thus only reachable from targets"""
        with self.lock:
            entries = [p for p in self.productions if (len(p.requiredBy) == 0)]
            # sorted by names so that test produce consistent results
            entries.sort(lambda a,b:cmp(a.name,b.name))
            return entries

    def __entrySetup(self, defaultTargetName=None):
        "setup entry points info"
        self.entryProds = self.__findEntryProductions()
        if (len(self.targets) == 0) and (len(self.entryProds) > 0):
            if (defaultTargetName != None):
                self.addTarget(Target(defaultTargetName, self.entryProds))
            else:
                raise ExRunException("graph does not contain any targets")

    def __cycleCheckNode(self, visited, allVisited, node):
        """check for cycles, once a visited node is found, return list of
        nodes in cycle, throwing an exception when returns to start of
        cycle.
        """
        if node in visited:
            return [node]  # cycle detected
        visited.add(node)
        allVisited.add(node)

        for n in node.nextNodes():
            cycle = self.__cycleCheckNode(visited, allVisited, n)
            if cycle != None:
                if node == cycle[0]:
                    # back at start of cycle
                    cycle.reverse()
                    raise CycleException(cycle)
                else:
                    cycle.append(node)
                visited.remove(node)
                return cycle
        visited.remove(node)
        return None

    def __cycleCheck(self, root, allVisited):
        """check for cycles, starting at a root, update set of all visited"""
        cycle = self.__cycleCheckNode(set(), allVisited, root)
        if cycle != None:
            raise CycleException(cycle) # should never happen

    def __getCheckEntries(self):
        """get entries for cycle check. If all is well, there should be a least one
        target, but handle incorrect cases so that cycles of all nodes get
        reported by cycle check code."""
        if len(self.targets) > 0:
            return self.targets
        elif len(self.entryProds) > 0:
            return self.entryProds
        elif len(self.nodes) > 0:
            return self.nodes
        else:
            return []

    def __connectivityCheck(self):
        """check for cycles and a full connected graph"""
        reachable = set()
        for entry in self.__getCheckEntries():
            self.__cycleCheck(entry, reachable)
        if len(reachable) < len(self.nodes):
            raise ExRunException("Nodes not reachable from any Target: "
                                 + Node.joinNames(self.nodes-reachable))
        elif len(reachable) > len(self.nodes):
            raise ExRunException("Invalid graph, nodes not added through API: "
                                 + Node.joinNames(reachable-self.nodes))

    def __initRuleStates(self, rule):
        "recursively initial rules states"
        for prod in rule.requires:
            if prod.state == ProdState.unknown:
                self.__initProdStates(prod)
        rule.computeState()

    def __initProdStates(self, prod):
        "recursively initial production states"
        if prod.producedBy == None:
            # no rule to produce, must exist
            if prod.getLocalTime() == None:
                prod._transition(ProdState.bad)
            else:
                prod._transition(ProdState.current)
        else:
            # recursively update, 
            if prod.producedBy.state == RuleState.unknown:
                self.__initRuleStates(prod.producedBy)
            prod.computeState()

    def __initStates(self):
        "initialize state information"
        for prod in self.entryProds:
            if prod.state == ProdState.unknown:
                self.__initProdStates(prod)

    def __getBadProds(self):
        "get list of productions flags as bad, or None"
        l = None
        for p in self.productions:
            if p.state == ProdState.bad:
                l = typeOps.listAppend(l, p)
        return l

    def __stateCheck(self):
        "check for various things after state initialized"
        bad = self.__getBadProds()
        if bad != None:
            raise ExRunException("No rule to build production(s): " + Node.joinNames(bad))
        
    def complete(self, defaultTargetName=None):
        """complete construction of graph, validating and setting initial states.
        If there are no targets, then a default one can be created linking to
        all defaultTargetName"""
        with self.lock:
            self.__entrySetup(defaultTargetName)
            self.__connectivityCheck()
            self.__initStates()
            self.__stateCheck()

    def getTargets(self, targetNames):
        "get targets for given names"
        targets = []
        for tn in targetNames:
            t = self.targetsByName.get(tn)
            if t == None:
                raise ExRunException("no Target named: " + tn)
            targets.append(t)
        return targets

    def bfs(self):
      "BFS generator for graph"
      with self.lock:
          # initialize queue
          queue = self.__findEntryProductions()
          visited = set(queue)
          while len(queue) > 0:
              node = queue.pop()
              yield node
              for n in node.nextNodes():
                  if not n in visited:
                      visited.add(n)
                      queue.append(n)

    def __getReady(self, root, ready):
        assert(isinstance(root, Node))
        if isinstance(root, Target):
            for r in root.requires:
                self.__getReady(r, ready)
        elif isinstance(root, Production):
            if root.producedBy != None:
                self.__getReady(root.producedBy, ready)
        elif isinstance(root, Rule):
            if root.isReady():
                ready.add(root)
            elif root.state == RuleState.outdated:
                for r in root.requires:
                    self.__getReady(r, ready)
                      
    def getReady(self, roots):
        """get list of rules that are ready to run, starting at the
        given list of nodes."""
        with self.lock:
            ready = set()
            for r in roots:
                self.__getReady(r, ready)
            return ready

__all__ = (CycleException.__name__, Target.__name__, Production.__name__, Rule.__name__, Graph.__name__, "RuleState", Node.__name__)
