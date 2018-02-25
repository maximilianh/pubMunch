# Copyright 2006-2012 Mark Diekhans

class AutoDict(dict):
    """Dictionary where entries can be automatically allocated.  Normally used
    for dict of dicts or other contained"""

    def __init__(self, entryFactory):
        """construct, entryFactory is either the class of the contained
        values or function to allocate the object"""
        self.entryFactory = entryFactory

    def obtain(self, key):
        """get the value for key, creating an empty entry if key is not in
        object"""
        if self.has_key(key):
            val = dict.__getitem__(self, key)
        else:
            val = self[key] = self.entryFactory()
        return val

    def add(self, key, val):
        """Add a new entry"""
        self.obtain(key).append(val)

    def __getitem__(self, key):
        return self.obtain(key)

