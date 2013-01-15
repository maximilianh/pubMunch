# Copyright 2006-2012 Mark Diekhans
"Generate Venn diagram set intersection statistics"

from pycbio.stats.Subsets import Subsets
from pycbio.sys import fileOps, setOps

# FIXME: setName vs items can get confusing, more doc.

class SetDict(dict):
    "Dictionary of sets"

    def __init__(self, subsets=None):
        """sets can be pre-defined to allow for empty sets or shared subsets
        objects"""
        if subsets != None:
            for ss in subsets:
                self[ss] = set()

    def add(self, key, val):
        "add a value to a set"
        if not key in self:
            self[key] = set()
        self[key].add(val)

class Venn(object):
    """Generate Venn diagram set intersections.  Each set has 
    list of ids associated with it that are shared between sets.
    """

    def __init__(self, subsets=None, isInclusive=False):
        """create new Venn.  If subsets is None, a private Subsets object is
        created,"""

        # is this a standard venn or inclusive?
        self.isInclusive = isInclusive
        self.subsets = subsets
        if self.subsets == None:
            self.subsets = Subsets()

        # Tables mappings set name to items and items to set names
        self.nameToItems = SetDict()
        self.itemToNames = SetDict()
        
        # Venn table, dict index by name, of items (lazy build)
        self.venn = None

    def addItem(self, setName, item):
        "add a single item from a named set"
        self.subsets.add(setName)
        self.nameToItems.add(setName, item)
        self.itemToNames.add(item, setName)
        self.venn = None

    def addItems(self, setName, items):
        "add items from a named set"
        self.subsets.add(setName)
        for item in items:
            self.nameToItems.add(setName, item)
            self.itemToNames.add(item, setName)
        self.venn = None

    def getNumItems(self):
        return len(self.itemToNames)

    def __buildVenn(self):
        "build Venn table"
        self.venn = SetDict(self.subsets.getSubsets())

        for item in self.itemToNames.iterkeys():
            nameSet = frozenset(self.itemToNames[item])
            self.venn.add(nameSet, item)

    def __buildInclusive(self):
        "build as inclusive subsets"
        self.venn = SetDict(self.subsets.getSubsets())

        for item in self.itemNames.iterkeys():
            setName = self.itemNames[item]
            for iss in self.subsets.getInclusiveSubsets(setName):
                self.venn.add(iss, item)

    def __update(self):
        "build venn or inclusive venn, if it doesn't exists"
        if self.venn == None:
            if self.isInclusive:
                self.__buildInclusive()
            else:
                self.__buildVenn()

    def getSubsetIds(self, subset):
        "get ids for the specified subset"
        self.__update()
        ids = self.venn.get(subset)
        if ids == None:
            ids = []
        return ids

    def getSubsetCounts(self, subset):
        "get counts for the specified subset"
        return len(self.getSubsetIds(subset))

    def getTotalCounts(self):
        "get total of counts for all subsets (meaningless on inclusive)"
        t = 0
        for subset in self.subsets.getSubsets():
            t += self.getSubsetCounts(subset)
        return t

    def writeCounts(self, fh, subsetNameSeparator=" "):
        "write TSV of subset counts to an open file"
        fileOps.prRowv(fh, "subset", "count")
        for subset in self.subsets.getSubsets():
            fileOps.prRowv(fh, subsetNameSeparator.join(subset), self.getSubsetCounts(subset))
        
    def writeSets(self, fh, subsetNameSeparator=" "):
        "write TSV of subsets and ids to an open file"
        fileOps.prRowv(fh, "subset", "ids")
        for subset in self.subsets.getSubsets():
            fileOps.prRowv(fh, subsetNameSeparator.join(subset), self.getSubsetCounts(subset))
        
