# Copyright 2006-2012 Mark Diekhans

class Subsets(object):
    "Generate possible subsets for a set of elements"

    def __init__(self, elements=None):
        """initialize set, optionally defining elements from a list or set of elements"""
        if elements != None:
            self.elements = set(elements)
        else:
            self.elements = set()

        # lazy cache of subsets
        self.subsets = None

        # dict of inclusive subsets for each subset, built in a lazy manner
        self.inclusiveSubsets = None

    def add(self, element):
        "Add an element to the set, ignore ones that already exist"
        self.elements.add(element)
        self.subsets = None
        self.inclusiveSubsets = None

    @staticmethod
    def __subListCmp(sub1, sub2):
        "compare two subsets for sorting"
        # first by length
        diff = len(sub1).__cmp__(len(sub2))
        if diff == 0:
            # same length, compare elements lexically, first convert sets to
            # lists
            l1 = list(sub1)
            l2 = list(sub2)
            i = 0
            while (i < len(l1)) and (diff == 0):
                if l1[i] < l2[i]:
                    diff = -1
                elif l1[i] > l2[i]:
                    diff = 1
                i += 1
        return diff

    def __makeSubset(self, bitSet, elements):
        "generated a subset for a bit set of the elements in list"
        iBit = 0
        bits = bitSet
        subset = list()
        while (bits != 0):
            if (bits & 1):
                subset.append(elements[iBit])
            bits = bits >> 1
            iBit += 1
        return frozenset(subset)

    def __makeSubsets(self, elements):
        "Build list of all of the possible subsets of a set using binary counting."
        # convert set input, as elements must be indexable for this algorithm
        if isinstance(elements, set) or isinstance(elements, frozenset):
            elements = list(elements)

        # build as lists for sorting
        nSubsets = (1 << len(elements))-1
        subsets = list()
        for bitSet in range(1, nSubsets+1):
            subsets.append(self.__makeSubset(bitSet, elements))
        # sort and constructs sets
        subsets.sort(cmp=Subsets.__subListCmp)
        return tuple(subsets)

    def getSubsets(self):
        "get the subsets, building if needed"
        if self.subsets == None:
            self.subsets = self.__makeSubsets(self.elements)
        return self.subsets

    def getSubset(self, wantSet):
        "search for the specified subset object, error if it doesn't exist"
        if self.subsets == None:
            self.subsets = self.__makeSubsets(self.elements)
        for ss in self.subsets:
            if ss == wantSet:
                return ss
        raise Exception("not a valid subset: " + str(wantSet))

    def __makeInclusiveSubset(self, subset):
        "make an inclusive subset list for a subset"
        inclSubsets = []
        for iss in self.__makeSubsets(subset):
            inclSubsets.append(iss)
        inclSubsets.sort(cmp=Subsets.__subListCmp)
        return tuple(inclSubsets)


    # get the inclusive subsets for particular subset; that is all subsets
    # that contain all of the specified sets.
    def getInclusiveSubsets(self, subset):
        if self.inclusiveSubsets == None:
            self.inclusiveSubsets = dict()
        inclSubsets = self.inclusiveSubsets.get(subset)
        if inclSubsets == None:
            inclSubsets = self.__makeInclusiveSubset(subset)
            self.inclusiveSubsets[subset] = inclSubsets
        return inclSubsets
