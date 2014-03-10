# create a bed based on exons that is extending exons up to the midspace between two neighboring ones
#

from collections import defaultdict
import sys
import array, logging

# comment the following line if you don't have numpy installed
import numpy # numpy is not really needed, you can uncomment some lines below to use native python code

def writeFeats(chrom, feats, outf, nameRewriteDict):
    for feat in feats:
        row = (str(x) for x in feat)

        # make sure we don't produce illegal coords
        start, end = feat[:2]
        start = int(start)
        end = int(end)
        #if (start>=end):
            #print feat
        assert(start<end)

        name = feat[2]
        if nameRewriteDict:
            name = nameRewriteDict[name]
            row = list(row)
            row[2] = name
        outf.write(chrom+"\t")
        outf.write("\t".join(row))
        outf.write("\n")

def writeDictFeats(chromFeats, outf):
    for chrom, feats in chromFeats.iteritems():
            for feat in feats:
                row = (str(x) for x in feat)
                outf.write(chrom+"\t")
                outf.write("\t".join(row))
                outf.write("\n")

def removeOverlaps(chromFeats):
    """ 
    remove overlaps from chrom -> list of (start, end, name) features 

    if some features overlap, flatten them, assign space by length of features, shortest first

    >>> fts = {'chrV': [[8165120, 8568326, 'NR_101742'], [8494873, 8502139, 'NM_072787'], [8495278, 8495363, 'NR_069077'], [8495279, 8495362, 'NR_051802'], [8495280, 8495363, 'NR_070083'], [8495283, 8495362, 'NR_050340'], (250000000, 250000010, "maxchr1")]}
    >>> removeOverlaps(fts)
    """
    logging.info("Flattening overlapping features")
    fts = {}
    for chrom, featList in chromFeats.iteritems():
        logging.debug("flattening chrom %s" % chrom)
        assert(len(featList)<65536)

        # sort features by length
        featList.sort(key=lambda x: x[1]-x[0], reverse=True)

        # determine maximum pos on chromosome for array size
        arrSize = 0
        for f in featList:
            fEnd = f[1]
            arrSize = max(arrSize, fEnd)

        # create array of unsigned ints
        # this array will hold the one-baed index of the feature with the shortest length 
        # for each basepair and one beyond, the index 0 means unassigned
        # version without numpy
        #levels = array.array("I", (arrSize+1)*[0])
        # version with numpy
        levels = numpy.zeros(arrSize, dtype=numpy.uint16)

        # fill array with index+1 of features for each covered bp
        for featIdx, f in enumerate(featList):
            start, end, name = f
            for pos in range(start, end):
                levels[pos]=featIdx+1

        # get all positions that are different from 0 as an array
        # this is a lot faster than iterating over the whole array
        # version without numpy
        #nonZeros = [i for i in range(0, arrSize) if levels[i]!=0]
        # version with numpy
        nonZeros = levels.nonzero()[0]

        # convert array back to features
        flatFeats = []
        start = 0
        for pos in nonZeros:
            fi = levels[pos]
            leftFi = levels[pos-1]
            if fi!=leftFi:
                if start!=0 and leftFi!=0:
                    name = featList[leftFi-1][2]
                    flatFeats.append( (start, pos+1, name) )
                start = pos
        # handle last stretch
        if start!=0:
            name = featList[leftFi-1][2]
            flatFeats.append( (start, pos+1, name) )
        fts[chrom] = flatFeats
    return fts


def parseBedMids(lines):
    """ 
    Input are bed lines with exons
    returns a dict with chrom -> list of (midpoint of feature, feature name) 
    for included features, split into three parts.
    for overlapping features, split the overlap halfways
    
    """
    # parse into dict chrom -> (start, end, name)
    chromFts = defaultdict(list)
    lastChrom = None
    lastStart = None
    for l in lines:
        l = l.strip("\n")
        chrom, start, end, name = l.split("\t")[:4]
        start, end = int(start), int(end)
        if lastChrom==chrom and lastStart > start:
            raise Exception("error: bed file is not sorted, violating feature: %s:%d" % (chrom, start))
        chromFts[chrom].append( (start, end, name) )
        lastChrom = chrom
        lastStart = start

    # flatten overlapping and included features
    chromFts = removeOverlaps(chromFts)
    #writeDictFeats(chromFts, open("flat.bed", "w"))

    ftMids = defaultdict(list)
    for chrom, featList in chromFts.iteritems():
        for start, end, name in featList:
            mid = start + (end-start)/2
            ftMids[chrom].append( (mid, name) )
    return ftMids

def slurpdict(fname):
    """ read in tab delimited file as dict key -> value (integer) """
    dict = {}
    for l in open(fname, "r"):
        l = l.strip("\n")
        fs = l.split("\t")
	if not len(fs)>1:
            continue
        key = fs[0]
        val = int(fs[1])
        if key not in dict:
            dict[key] = val
        else:
            sys.stderr.write("info: hit key %s two times: %s -> %s\n" %(key, key, val))
    return dict

def joinSameName(feats):
    " join neighboring features if they have the same name "
    newFeats = []
    i = 0
    while i < len(feats):
        start, end, name = feats[i]
        # go forward until name changes
        for j in range(i+1, len(feats)):
            if feats[j][2]==name:
                end = feats[j][1]
                i+=1
            else:
                break
        newFeats.append( (start, end, name) )
        i+=1
    return newFeats

def outputLoci(mids, chromSizes, outf, flankSize=100000, nameRewriteDict=None):
    """
    output ranges around midpoints
    nameRewriteDict can change names to other identifiers if needed
    """
    flankSize = 100000
    # iterate over all midpoints per chrom
    for chrom, midTuples in mids.iteritems():
        feats = []
        # special case for first feature
        firstMid, firstName = midTuples[0]
        feats.append((max(0, firstMid-flankSize), firstMid, firstName))
        for i in range(0, len(midTuples)-1):
            left, leftName  = midTuples[i]
            right, rightName = midTuples[i+1]
            # point between two midpoints
            midmid = left + (right-left)/2
            feats.append((left, midmid, leftName))
            feats.append((midmid+1, right, rightName))
        # last feature special case
        lastStart, lastName = midTuples[-1]
        feats.append((lastStart, min(chromSizes[chrom], lastStart+flankSize), lastName))
        feats = joinSameName(feats)
        writeFeats(chrom, feats, outf, nameRewriteDict)

if __name__=="__main__":
    import doctest
    doctest.testmod()

