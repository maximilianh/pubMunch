# Copyright 2006-2012 Mark Diekhans
import math
from pycbio.sys.fileOps import prLine, prRowv, iterRows
from pycbio.sys.typeOps import isListLike

# FIXME: computed histo should be an object, not just a list
# FIXME: binnins doesn't work for values in the range [-1.0, 1.0]

class Bin(object):
    "A bin in the histogram"
    def __init__(self, histo, idx, binMin, binSize):
        self.histo = histo
        self.idx = idx
        self.binMin = binMin
        self.binSize = binSize
        self.cnt = 0
        self.freq = 0.0

    def getCenter(self):
        "return center point of bin"
        return self.binMin+(self.binSize/2.0)

    def __str__(self):
        return "bin: "+ str(self.idx) + " min: " + str(self.binMin) + " size: " + str(self.binSize) + " cnt: " + str(self.cnt) + " freq: " + str(self.freq)
        
class NumData(list):
    "data consisting of individual numbers"
    def __init__(self):
        self.min = None
        self.max = None
        self.total = None

    def compute(self):
        "compute data range and total"
        if len(self) > 0:
            self.min = min(self)
            self.max = max(self)
            self.total = sum(self)

class NumCntData(list):
    "data consisting of tuples of numbers and counts"
    def __init__(self):
        self.min = None
        self.max = None
        self.total = None

    def compute(self):
        "compute data range and total"
        self.min = self.max = self[0][0]
        self.total = 0
        for item in self:
            val = item[0]
            if val < self.min:
                self.min = val
            if val > self.max:
                self.max = val
            self.total += item[1]*val

class Histogram(object):
    """Bin data into a histogram for ploting or other purposes.
    Data items can either be single numbers (floats or ints), or
    tuples of (value, count).
    """

    def __init__(self, data=None, isTupleData=False,
                 truncMin = False, truncMax = False,
                 binMin = None, binMax = None,
                 binSize = None, numBins = None):
        "create histogram, optionally adding data"
        # parameters controling histogram
        self.truncMin = truncMin
        self.truncMax = truncMax
        self.binMin = binMin
        self.binMax = binMax
        self.binSize = binSize
        self.numBins = numBins

        self.isTupleData = isTupleData
        if isTupleData:
            self.data = NumCntData()
        else:
            self.data = NumData()

        # these are computed automatically if the value is not specified
        # above.  This allows changing the above values and rebinning
        self.binMinUse = None
        self.binMaxUse = None
        self.binSizeUse = None
        self.numBinsUse = None
        self.binFloorUse = None  # min to actually use for indexing
        self.binCeilUse = None   # max to actually use for indexing
        self.bins = None         # computed by build()
        if (data != None):
            self.data.extend(data)

    def addItem(self, item):
        "add a single data item of the approriate type"
        self.data.append(item)

    def addData(self, data):
        "add a sequence of data items"
        self.data.extend(data)

    def addFile(self, fname, type=int, valCol=0):
        "add from a tab separated file of values of the specfied type"
        assert(not self.isTupleData)
        for row in iterRows(fname):
            self.data.append(type(row[valCol]))

    def addTupleFile(self, fname, type=int, valCol=0, cntCol=1):
        "add from a tab separated file of values of the specfied type and counts"
        assert(self.isTupleData)
        for row in iterRows(fname):
            self.data.append((type(row[valCol]), int(row[cntCol])))

    def __calcParams(self):
        "Calculate binning paramters"
        self.data.compute()
        self.binMinUse = self.binMin
        if self.binMinUse == None:
            self.binMinUse = self.data.min

        self.binMaxUse = self.binMax
        if self.binMaxUse == None:
            self.binMaxUse = self.data.max

        self.numBinsUse = self.numBins
        self.binSizeUse = self.binSize
        if (self.numBinsUse == None) and (self.binSizeUse == None):
            # default num bins and compute bin size from it below
            self.numBinsUse = 10

        if self.binMinUse == None:
            self.binSizeUse = self.binFloorUse = self.binCeilUse = 0
        elif self.binSizeUse == None:
            # compute bin size from num bins
            estBinSize = float(self.binMaxUse-self.binMinUse)/float(self.numBinsUse-1)
            self.binSizeUse = float(self.binMaxUse-self.binMinUse+estBinSize)/float(self.numBinsUse)
            self.binFloorUse = self.binMinUse - (self.binSizeUse/2.0)
            self.binCeilUse = self.binFloorUse + (self.numBinsUse*self.binSizeUse)
        else:
            # compute num bins from bin size
            raise Exception("doesn't work")
            self.numBinsUse = int(float(self.binMaxUse-self.binMinUse)/float(self.binSizeUse))
            self.binFloorUse = self.binMinUse
            self.binCeilUse = self.binMaxUse

    def __getBinIdx(self, val):
        "Get the integer bin number for a value, or None to ignore"
        if self.binSizeUse == 0.0:
            return 0
        elif val < self.binMinUse:
            return None if self.truncMin else 0
        elif val > self.binMaxUse:
            return None if self.truncMax else self.numBinsUse - 1
        else:
            return int((val-self.binFloorUse)/float(self.binSizeUse))

    def __mkBins(self):
        self.bins = []
        for i in xrange(self.numBinsUse):
            self.bins.append(Bin(self, i, self.binFloorUse+(i*self.binSizeUse), self.binSizeUse))

    def __binTupleData(self):
        for item in self.data:
            iBin = self.__getBinIdx(item[0])
            if iBin != None:
                self.bins[iBin].cnt += item[1]

    def __binScalarData(self):
        for item in self.data:
            iBin = self.__getBinIdx(item)
            if iBin != None:
                self.bins[iBin].cnt += 1

    def __computeFreqs(self):
        ndata = float(len(self.data))
        for bin in self.bins:
            if ndata != 0.0:
                bin.freq = bin.cnt / ndata
            else:
                bin.freq = 0.0
                
    def build(self):
        "construct histogram from data, return list of bins (also in bins field)"
        self.__calcParams()
        self.__mkBins()
        if self.isTupleData:
            self.__binTupleData()
        else:
            self.__binScalarData()

        # compute frequencies
        if self.data.total != 0.0:
            self.__computeFreqs()

        return self.bins

    def dump(self, fh, desc=None):
        self.__calcParams()
        if desc != None:
            prLine(fh, desc)
        prLine(fh, "  data:  len: ", len(self.data), "  min: ", self.data.min, "  max: ", self.data.max)
        prLine(fh, "  bins:  num: ", self.numBins, "  size: ", self.binSize, "  min: ", self.binMin, "  max: ", self.binMax)
        prLine(fh, "  use:  num: ", self.numBinsUse, "  size: ", self.binSizeUse, "  min: ", self.binMinUse, "  max: ", self.binMaxUse,
               "  floor: ", self.binFloorUse, "  ceil: ", self.binCeilUse)
        
