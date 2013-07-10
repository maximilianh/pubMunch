# create a bed based on exons that is extending exons up to the midspace between two neighboring ones
#

from collections import defaultdict
import sys

def writeFeats(chrom, feats, outf):
    for feat in feats:
        row = (str(x) for x in feat)
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
    """
    fts2 = {}
    for chrom, featList in chromFeats.iteritems():
        cFts = []
        i = 1
        lStart, lEnd, lName = featList[0]
        while i < len(featList)-1:
            rStart, rEnd, rName = featList[i]
            if (lStart == rStart) and (lEnd==rEnd):
                # left feature == right feature: just skip
                lStart, lEnd, lName = featList[i]
                i+=1
                continue
            if (lStart <= rStart <= rEnd < lEnd):
                # right feature is included within left:
                #       lllllllllllllll
                #           rrrrrr
                # write llll
                # write     rrrrrr
                # left            lllll
                cFts.append ((lStart, rStart, lName))
                cFts.append ((rStart, rEnd, rName))
                #cFts.append ((rEnd, lEnd, lName))
                lStart = rEnd
                i+=1
                continue
            elif (rStart < lEnd):
                # right feature overlaps left:
                #       lllllllllllll
                #              rrrrrrrrrrrrr
                # write lllllll   
                # left         rrrrrrrrrrrrr
                cFts.append( (lStart, rStart, lName) )
                lStart = rStart
                lEnd = rEnd
                lName = rName
                i+=1
                continue
            cFts.append( (lStart, lEnd, lName) )
            i += 1
            lStart, lEnd, lName = featList[i]
        # try to append last feature
        if i < len(featList):
            cFts.append( featList[i] )
        fts2[chrom] = cFts
    return fts2

def parseBedMids(lines):
    """ 
    returns a dict with chrom -> list of (midpoint of feature, feature name) 
    for included features, split into three.
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
            sys.stderr.write("error: bed file is not sorted, violating feature: %s:%d" % (chrom, start))
            sys.exit(1)
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

def outputLoci(mids, chromSizes, outf, flankSize=100000):
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
        # last feature spec case
        lastStart, lastName = midTuples[-1]
        feats.append((lastStart, min(chromSizes[chrom], lastStart+flankSize), lastName))
        feats = joinSameName(feats)
        writeFeats(chrom, feats, outf)
