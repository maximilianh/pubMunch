# Copyright 2006-2012 Mark Diekhans

class PslMap(object):
    """Object for mapping coordinates using PSL alignments.
    Can map from either query-to-target or target-to-query coordinates.
    Takes an object that has the following callback functions that are
    called on each contiguous range that is mapped:

    - cb.mapBlock(psl, blk, qRngStart, qRngEnd, tRngStart, tRngEnd)
      Called for every aligned block in the requested range.
    - cb.mapGap(psl, prevBlk, nextBlk, qRngStart, qRngEnd, tRngStart, tRngEnd)
      Called for every gap in the requested range.  Either the qRng or
      tRng arguments will be None. prevBlk or nextBlk maybe none if at
      beginning or end of alignment.

    Blocks are traversed in order, reverse complement PSL to traverse in
    opposite order.
    """
    def __init__(self, callback):
        "initialize with callback object"
        self.cb = callback

    def __t2qProcessGap(self, psl, prevBlk, nextBlk, tRngNext, tRngEnd):
        """analyze gap before blk, return updated tRngNext"""
        gapTStart = prevBlk.tEnd
        gapTEnd = nextBlk.tStart
        if tRngNext >= gapTEnd:
            return tRngNext  # gap before range
        assert(tRngNext >= gapTStart)
        tRngNextNext = tRngEnd if (tRngEnd < gapTEnd) else gapTEnd
        self.cb.mapGap(psl, prevBlk, nextBlk, None, None, tRngNext, tRngNextNext)
        return tRngNextNext

    def __t2qProcessBlk(self, psl, blk, tRngNext, tRngEnd):
        """Analyze next block overlapping range.  Return updated tRngNext"""
        if tRngNext >= blk.tEnd:
            return tRngNext  # block before range
        assert(tRngNext >= blk.tStart)

        # in this block, find corresponding query range
        qRngNext = blk.qStart + (tRngNext - blk.tStart)
        if tRngEnd < blk.tEnd:
            # ends in this block
            qRngNextNext = qRngNext + (tRngEnd - tRngNext)
            tRngNextNext = tRngEnd
        else:
            # continues after block
            left = blk.tEnd - tRngNext
            qRngNextNext = qRngNext + left
            tRngNextNext = tRngNext + left
        self.cb.mapBlock(psl, blk, qRngNext, qRngNextNext, tRngNext, tRngNextNext)
        return tRngNextNext

    def targetToQueryMap(self, psl, tRngStart, tRngEnd):
        """Map a target range to query ranges using a PSL. Target range must
        be in PSL block-specific coordinates (positive or negative strand)"""
        # deal with gap at beginning
        tRngNext = tRngStart
        if tRngNext < psl.blocks[0].tStart:
            self.cb.mapGap(psl, None, psl.blocks[0], None, None, tRngNext, psl.blocks[0].tStart)
            tRngNext = psl.blocks[0].tStart

        # process blocks and gaps
        prevBlk = None
        for blk in psl.blocks:
            if  tRngNext >= tRngEnd:
                break
            if prevBlk != None:
                tRngNext = self.__t2qProcessGap(psl, prevBlk, blk, tRngNext, tRngEnd)
            if tRngNext < tRngEnd:
                tRngNext = self.__t2qProcessBlk(psl, blk, tRngNext, tRngEnd)
            prevBlk = blk

        # deal with gap at end
        lastBlk = psl.blocks[psl.blockCount-1]
        if tRngEnd > lastBlk.tEnd:
            self.cb.mapGap(psl, lastBlk, None, None, None, lastBlk.tEnd, tRngEnd)

    def __q2tProcessGap(self, psl, prevBlk, nextBlk, qRngNext, qRngEnd):
        """analyze gap before blk, return updated qRngNext"""
        gapQStart = prevBlk.qEnd
        gapQEnd = nextBlk.qStart
        if qRngNext >= gapQEnd:
            return qRngNext  # gap before range
        assert(qRngNext >= gapQStart)
        qRngNextNext = qRngEnd if (qRngEnd < gapQEnd) else gapQEnd
        self.cb.mapGap(psl, prevBlk, nextBlk, qRngNext, qRngNextNext, None, None)
        return qRngNextNext

    def __q2tProcessBlk(self, psl, blk, qRngNext, qRngEnd):
        """Analyze next block overlapping range.  Return updated qRngNext"""
        if qRngNext >= blk.qEnd:
            return qRngNext  # block before range
        assert(qRngNext >= blk.qStart)

        # in this block, find corresponding target range
        tRngNext = blk.tStart + (qRngNext - blk.qStart)
        if qRngEnd < blk.qEnd:
            # ends in this block
            tRngNextNext = tRngNext + (qRngEnd - qRngNext)
            qRngNextNext = qRngEnd
        else:
            # continues after block
            left = blk.qEnd - qRngNext
            tRngNextNext = tRngNext + left
            qRngNextNext = qRngNext + left
        self.cb.mapBlock(psl, blk, qRngNext, qRngNextNext, tRngNext, tRngNextNext)
        return qRngNextNext

    def queryToTargetMap(self, psl, qRngStart, qRngEnd):
        """Map a query range to target ranges using a PSL.  Query range must
        be in PSL block-specific coordinates (positive or negative strand)"""
        # refuse protein PSLs
        assert(not psl.isProtein()) # run psl.protToNa() on map psls first
        
        # deal with gap at beginning
        qRngNext = qRngStart
        if qRngNext < psl.blocks[0].qStart:
            self.cb.mapGap(psl, None, psl.blocks[0], qRngNext, psl.blocks[0].qStart, None, None)
            qRngNext = psl.blocks[0].qStart

        # process blocks and gaps
        prevBlk = None
        for blk in psl.blocks:
            if  qRngNext >= qRngEnd:
                break
            if prevBlk != None:
                qRngNext = self.__q2tProcessGap(psl, prevBlk, blk, qRngNext, qRngEnd)
            if qRngNext < qRngEnd:
                qRngNext = self.__q2tProcessBlk(psl, blk, qRngNext, qRngEnd)
            prevBlk = blk

        # deal with gap at end
        lastBlk = psl.blocks[psl.blockCount-1]
        if qRngEnd > lastBlk.qEnd:
            self.cb.mapGap(psl, lastBlk, None, lastBlk.qEnd, qRngEnd, None, None)

