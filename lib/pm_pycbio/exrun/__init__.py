# Copyright 2006-2012 Mark Diekhans
"""Experiment running module.


"""

import sys,traceback,types
from pm_pycbio.sys import typeOps, PycbioException
                                
class ExRunException(PycbioException):
    "Exceptions thrown bye exrun module derive from this object" 
    def __init__(self, msg, cause=None):
        PycbioException.__init__(self, msg, cause)

# FIXME: make this a sys object:
class Verb(object):
    """Verbose tracing, bases on a set of flags.  The str() of
    any object passed to print routines with the following exceptions:
      traceback - stack is formatted
    """
    
    # flag values
    error = sys.intern("error")     # output errors
    trace = sys.intern("trace")     # basic tracing
    details = sys.intern("details") # detailed tracing
    debug = sys.intern("debug")     # debugging
    dumpIn = sys.intern("dumpIn")   # dump graph input graph before complete()
    dumpStart = sys.intern("dumpStart")   # dump graph at start
    dumpEnd = sys.intern("dumpEnd")     # dump graph after finish
    all = set([error, trace, details, debug, dumpIn, dumpStart, dumpEnd])

    def __init__(self, flags=None, fh=sys.stderr):
        self.fh = fh
        self.flags = flags
        if self.flags == None:
            self.flags = set([Verb.error, Verb.trace])
        else:
            self.flags = set(flags)
        self.indent = 0

    def enabled(self, flag):
        """determine if tracing is enabled for the specified flag, flag can be
        either a single flag or sequence of flags"""
        if typeOps.isListLike(flag):
            for f in flag:
                if f in self.flags:
                    return True
            return False
        else:
            return (flag in self.flags)

    def __prIndent(self, msg):
        ind = ("%*s" % (2*self.indent, ""))
        self.fh.write(ind)
        sep = ""
        for m in msg:
            self.fh.write(sep)
            sep = " "
            if isinstance(m, types.TracebackType):
                self.fh.write(ind.join(traceback.format_tb(m)))
            else:
                self.fh.write(str(m))
        self.fh.write("\n")
        self.fh.flush()

    def prall(self, *msg):
        "unconditionally print a message with indentation"
        self.__prIndent(msg)

    def pr(self, flag, *msg):
        "print a message with indentation if flag indicates enabled"
        if self.enabled(flag):
            self.__prIndent(msg)

    def enter(self, flag=None, *msg):
        "increment indent count, first optionally output a trace message"
        if self.enabled(flag) and (len(msg) > 0):
            self.__prIndent(msg)
        self.indent += 1

    def leave(self, flag=None, *msg):
        "decrement indent count, then optionally outputing a trace message "
        self.indent -= 1
        if self.enabled(flag) and (len(msg) > 0):
            self.__prIndent(msg)

# make classes commonly used externally part of top module
from pm_pycbio.exrun.Graph import Target, Production, Rule
from pm_pycbio.exrun.CmdRule import CmdRule, Cmd, FileOut, FileIn, FileOut, File
from pm_pycbio.exrun.ExRun import ExRun


__all__ = (ExRunException.__name__, Verb.__name__, Production.__name__,
           Rule.__name__, CmdRule.__name__, Cmd.__name__, 
           File.__name__, FileIn.__name__, FileOut.__name__, ExRun.__name__)
