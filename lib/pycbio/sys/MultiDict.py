# Copyright 2006-2012 Mark Diekhans

# FIXME: need to other dict methods.


class MultiDict(dict):
    """Dictionary which allows multiple values"""

    def obtain(self, key):
        """get the values for key, creating an empty entry if key is not in
        object"""
        if key in self:
            vals = self[key]
        else:
            vals = []
            dict.__setitem__(self, key,  vals)
        return vals

    def add(self, key, val):
        """Add a new entry"""
        self.obtain(key).append(val)

    def __setitem__(self, key, val):
        self.add(key, val)

    def itervalues(self, key=None):
        """get iter over all values, or values for a key"""
        if key != None:
            vals = self.get(key)
            if vals != None:
                for val in vals:
                    yield val
        else:
            for key in self.iterkeys():
                vals = self.get(key)
                for val in vals:
                    yield val

    def iterentries(self):
        """get iter over entries for each key """
        for key in self.iterkeys():
            yield self.get(key)
