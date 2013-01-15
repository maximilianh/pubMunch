# Copyright 2006-2012 Mark Diekhans
"array and matrix classed, indexed by Enumerations"

from pycbio.sys.Enumeration import Enumeration

class EnumArray(list):
    """array indexed by an Enumeration."""

    def __init__(self, enum, initVal=None):
        for i in xrange(enum.maxNumValue+1):
            self.append(initVal)

    def __getitem__(self, eval):
        return list.__getitem__(self, eval.numValue)

    def __setitem__(self, eval, val):
        return list.__setitem__(self, eval.numValue, val)

class EnumMatrix(EnumArray):
    """matrix indexed by Enumerations."""

    def __init__(self, rowEnum, colEnum, initVal=None):
        assert(isinstance(rowEnum, Enumeration))
        assert(isinstance(colEnum, Enumeration))
        EnumArray.__init__(self, rowEnum)
        self.rowEnum = rowEnum
        self.colEnum = colEnum
        for val in self.rowEnum.values:
            self[val] = EnumArray(self.colEnum, initVal)
