# Copyright 2006-2012 Mark Diekhans
"""Implementation of interval skip lists. 

See:
The Interval Skip List: A Data Structure for Finding All Intervals That Overlap a Point
Eric N. Hanson and Theodore Johnson
tr92-016, June 1992
ftp://ftp.cis.ufl.edu/pub/tech-reports/tr92/tr92-016.ps.Z
see also
http://www-pub.cise.ufl.edu/~hanson/IS-lists/
"""

from pycbio.sys.Immutable import Immutable
from pycbio.sys import typeOps

class Entry(Immutable):
    "An entry for an interval and associated value. This is immutable"

    __slots__ = ("start", "end", "val")

    def __init__(self, start, end, val):
        Immutable.__init__(self)
        self.start = start
        self.end = end
        self.val = val
        self.mkImmutable()

class Node(object):
    """a node in the skip list.

    Fields:
        pos - key of node
        forward - array of forward links
        markers - array of sets of markers
        owners -  intervals that have an endpoint equal to key
        eqMarkers - a set of markers for intervals that have a a marker on an
                    edge that ends on this node,
    """
    def __init__(self, pos):
        self.pos = pos
        self.forward = []
        self.markers = []
        self.owners = set()
        self.eqMarkers = set()


class IntervalSkipList(object):
    "skip list object addressed by interval"
    def __init__(self, maxLevel):
        self.maxLevel = maxLevel
        self.head = Node(None)

    def _insert(self, key):
        pass

    def _adjustMarkersOnInsert(self, x, update):
        pass

    def _adjustMarkersOnDelete(self, x, update):
        pass

    def remove(self, I):
        pass

    def _remove(self, x, update):
        pass

    def _search(self, searchKey, update):
        pass

    def _findIntervals(self, searchKey):
        "return list of intervals overlapping v"
        x = self.head
        vals = set()
        # Step down to bottom level
        for i in range(self.maxLevel-1, -1, -1):
            # Search forward on current level as far as possible.
            while (x.forward[i] != None) and (x.forward[i].start < start):
                x = x.forward[i]
                
            # Pick up interval markers on edge when dropping down a level.
            vals.add(x.markers[i])


        # Scan forward on bottom level to find location where search key will
        # lie.
        while (x.forward[0] != None) and (x.forward[0].start < start):
            x = x.forward[0]

        # If K is not in list, pick up interval markers on edge, otherwise
        # pick up markers on node with value = K.
        if (x.forward[0] == None) or (x.forward[0].start != start):
            val |= x.markers[0]
        else: 
            val |= x.forward[0].eqMarkers

        return vals

    def insert(self, I):
        pass

    def _placeMarkers(self, left, right, I):
        pass

    def _removeMarkers(self, left, I):
        pass

    def _removeMarkFromLevel(self, m, i, l, r):
        pass

    def normalizedRandom(self):
        pass

    def randomLevel(self):
        pass

 
__all__ = (Entry.__name__, IntervalSkipList.__name__)
