import maxCommon, re, sys, logging

def hasCdr3Len(seq):
    return len(seq)>7 and len(seq)<26 

# top 90% cdr3 prefixes according to adaptive TCR list of 2mil seqs
prefixes = set("ASS ASR SAR AST ASG AIS ATS AWS ASK ASN".split(" "))

cdr3Regex = re.compile("C?(ASS|ASR|SAR|AST|ASG|AIS|ATS|AWS|ASK|ASN)")

blackList = set("CASREACT CASSCFCASSCF CLASTALW CASTGRID CPMASNMR CASSCFAMBER CNMATSCNMRT CASSCFICMRCI CPATSAEMBRAPA CATSAEBAA".split(" "))

def hasCdr3Prefix(seq):
    """
    make sure sequence starts with A or C and has an important tag at the start
    """
    #if not seq.startswith("C") and not seq.startswith("A") and not seq.startswith("SAR"):
        #return False

    #prefixSeq = seq[:7]
    #for prefix in prefixes:
        #if prefix in prefixSeq:

    if cdr3Regex.match(seq):
        return True
    else:
        return False

def splitAndKeep(text, regex):
    """ split on string but keep the strings in the parts 
    >>> splitAndKeep("CASSactyhacthCSARACTGACT", cdr3Regex)
    ['CASSactyhacth', 'CSARACTGACT']
    """
    parts = []
    lastStart = 0
    for m in regex.finditer(text):
        start = m.start()
        if start==0:
            continue
        parts.append(text[lastStart:start])
        lastStart = start
    parts.append(text[lastStart:])
    return parts

def iterCdr3Rows(fname):
    for row in maxCommon.iterTsvRows(fname):
        seq = row.seq
        logging.debug("seq %s" % seq)

        if not (row.prefixFilterAccept=="Y" and row.suffixFilterAccept=="Y"):
            logging.debug("didn't pass prefix or suffix filter")
            continue

        if "CLASS" in seq:
            logging.debug("contains CLASS")
            continue

        if seq in blackList:
            logging.debug("blacklisted")
            continue

        if not hasCdr3Prefix(seq):
            logging.debug("prefix not OK")
            continue

        if hasCdr3Len(seq):
            logging.debug("and length OK")
            yield row
        else:
            # trying to split cdr3s that got fused into separate seqs again
            # not that this makes the annotation ID longer: it adds three additional digits for the sub-parts
            logging.debug("Length not OK, trying to split")
            parts = splitAndKeep(row.seq, cdr3Regex)

            okParts = []
            for p in parts:
                if hasCdr3Prefix(p) and hasCdr3Len(p):
                    okParts.append(p)

            if len(parts) - len(okParts)< len(parts)/3: # we tolerate a few bad pieces
                for num, p in enumerate(okParts):
                    numStr = "%03d" % num
                    newRow = row._replace(annotId=row.annotId+numStr, seq=p)
                    yield newRow
                    #ofh.write("\t".join())
                    #ofh.write("\n")

if __name__=="__main__":
    import doctest
    doctest.testmod()

