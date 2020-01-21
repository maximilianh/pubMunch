# Copyright 2006-2012 Mark Diekhans
"""
Base class used to define immutable objects
"""

# FIXME: r886 used a __new__ and had __init__ set immutable flag, however this
# doesn't worth with cPickle format < 2, however other transMap code didn't
# work with format == 2, so take this out for now.


# FIXME: some other ideas:
# http://code.activestate.com/recipes/576527-freeze-make-any-object-immutable/
# http://code.activestate.com/recipes/577207-immutable-objectsubclass/

class Immutable(object):
    """Base class to make an object instance immutable.  Call 
    Immutable.__init__(self) after construction to make immutable"""

    __immAttr = "_Immutable__immutable"
    __slots__ = (__immAttr,)

    def __init__(self):
        "constructor"
        object.__setattr__(self, Immutable.__immAttr, False)

    def mkImmutable(self):
        "set immutable flag in object"
        object.__setattr__(self, Immutable.__immAttr, True)

    def __setattr__(self, attr, value):
        if self.__immutable:
            raise TypeError("immutable object", self)
        object.__setattr__(self, attr, value)

    def __delattr__(self, attr):
        if self.__immutable:
            raise TypeError("immutable object", self)
        object.__delattr__(self, attr)

