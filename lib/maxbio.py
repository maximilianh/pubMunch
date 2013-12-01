# Routines for handling fasta sequences and tab sep files

# std packages
import sys, textwrap, operator, types, doctest,logging, gzip, struct, cPickle, gc, itertools, math
from collections import defaultdict
from types import *
from os.path import basename, splitext

# external packages
try:
    import namedtuple 
except:
    pass

try:
    import dist, math
except:
    pass

# --- CONVENIENCE ---
def openFile(fname, mode="r"):
    """ opens file, recognizing stdout and stdin and none"""
    if hasattr(fname, "read") or hasattr(fname, "write"):
        return fname
    elif fname.endswith(".gz"):
        fh = gzip.open(fname, mode)
    elif fname=="stdout":
        fh = sys.stdout
    elif fname=="stdin":
        fh = sys.stdin
    elif fname==None or fname.lower()=="none":
        fh = None
    else:
        fh = open(fname, mode)
    return fh

def flattenValues(dict):
    """ return all values in dictionary (key -> list) as one long flat list """
    list = []
    for value in dict.values():
        list.extend(value)
    return list

def writeToTsv(fileObj, rec):
    """ writes a namedtuple to a file as a tab-sep line """
    if rec:
        rec = [x.encode("utf-8") for x in rec]
        string = "\t".join(rec)
        fileObj.write(string+"\n")

# --- FASTA FILES ---
def parseFastaAsDict(fname, inDict=None):
    if inDict==None:
        inDict = {}
    logging.info("Parsing %s" % fname)
    fr = FastaReader(fname)
    for (id, seq) in fr.parse():
        if id in inDict:
            print inDict
            print inDict[id]
            raise Exception("%s already seen before" % id)
        inDict[id]=seq
    return inDict

def parseFasta(fname):
    fr = FastaReader(fname)
    for (id, seq) in fr.parse():
        yield id, seq

class FastaReader:
    """ a class to parse a fasta file 
    Example:
        fr = maxbio.FastaReader(filename)
        for (id, seq) in fr.parse():
            print id,seq """

    def __init__(self, fname):
        if hasattr(fname, 'read'):
            self.f = fname
        elif fname=="stdin":
            self.f=sys.stdin
        elif fname.endswith(".gz"):
            self.f=gzip.open(fname)
        else:
            self.f=open(fname)
        self.lastId=None

    def parse(self):
      """ Generator: returns sequences as tuple (id, sequence) """
      lines = []

      for line in self.f:
              if line.startswith("\n") or line.startswith("#"):
                  continue
              elif not line.startswith(">"):
                 lines.append(line.replace(" ","").strip())
                 continue
              else:
                 if len(lines)!=0: # on first >, seq is empty
                       faseq = (self.lastId, "".join(lines))
                       self.lastId=line.strip(">").strip()
                       lines = []
                       yield faseq
                 else:
                       if self.lastId!=None:
                           sys.stderr.write("warning: when reading fasta file: empty sequence, id: %s\n" % line)
                       self.lastId=line.strip(">").strip()
                       lines=[]

      # if it's the last sequence in a file, loop will end on the last line
      if len(lines)!=0:
          faseq = (self.lastId, "".join(lines))
          yield faseq
      else:
          yield (None, None)

def outputFasta(id, seq, fh=sys.stdout, width=80):
    """ output fasta seq to file object, break to 80 char width """
    fh.write(">"+id+"\n")
    #fh.write("\n".join(textwrap.wrap(seq, width=width)))
    if len(seq)>width:
        last = 0
        for l in range(width,len(seq),width):
            fh.write(seq[last:l])
            fh.write("\n")
            last = l
        fh.write(seq[last:len(seq)])
    else:
        fh.write(seq)
    fh.write("\n")

def outputFastaFile(id, seq, fname, width=80):
    """ output fasta seq to fname and close file, break to 80 char width """
    fh = openFile(fname)
    outputFasta(id, seq, fh, width=80)
    fh.close()

### functions for handling lists of tuples

def _makeGetter(var):
    """ returns the right getter, depending on the type of var """
    if type(var)==types.StringType:
        getter = operator.attrgetter(var) # to get elements from records with named fields
    else:
        getter = operator.itemgetter(var) # to get elements from lists
    return getter

def sortList(list, field, reverse=True, key=None):
    """ sort list of tuples by a given field """
    if not key:
        key = _makeGetter(field)
    list.sort(key=key, reverse=reverse)
    return list

def bestIdentifiers(scoredList):
    """
    given a list of tuples with a numeric last field and an id field, return the id fields with 
    the highest last field.
    >>> bestIdentifiers ([("clinton", 1), ("obama", 3), ("washington", 10), ("lincoln", 10)])
    ['washington', 'lincoln']
    """
    scoredList.sort(key=operator.itemgetter(-1), reverse=True)
    bestScore = scoredList[0][-1]
    bestElements = [e[0] for e in scoredList if e[-1] >= bestScore]
    return bestElements

def bestScoreElements(list, scoreField):
    """ return only those tuples in a list that contain a score >= the best score in the list 
    >>> import namedtuple
    >>> tuple = namedtuple.namedtuple("test", "f1, f2")
    >>> tuples = [tuple(1, 6), tuple(4, 7), tuple(2, 7)]
    >>> print bestScoreElements(tuples, scoreField="f2")
    [test(f1=4, f2=7), test(f1=2, f2=7)]
    """
    scoreGetter = _makeGetter(scoreField)
    sortList(list, scoreField, reverse=True, key=scoreGetter)
    bestScore = scoreGetter(list[0])
    bestElements = [e for e in list if scoreGetter(e) >= bestScore]
    return bestElements

def indexByField(list, field):
    """ index by a given field: convert list of tuples to dict of tuples """
    map = {}
    indexGetter = _makeGetter(field)
    for tuple in list:
        map.setdefault(indexGetter(tuple), []).append(tuple)
    return map

def bestTuples(list, idField, scoreField):
    """ Index a list of a key-value-tuples, keep only the best tuples per value and return their keys. 
    
    >>> import namedtuple
    >>> tuple = namedtuple.namedtuple("test", "f1, f2")
    >>> tuples = [tuple(1, 6), tuple(1, 3), tuple(2, 7), tuple(2,1000)]
    >>> print bestTuples(tuples, idField="f1", scoreField="f2")
    [test(f1=1, f2=6), test(f1=2, f2=1000)]

    """
    map = indexByField(list, idField)
    filteredList = []
    for id, idList in map.iteritems():
        bestElements = bestScoreElements(idList, scoreField)
        filteredList.extend(bestElements)
    return filteredList

def removeBigSets(predDict, limit):
    """ given a dict with string -> set , remove elements where len(set) >= than limit """
    result = {}
    for key, predSet in predDict:
        if len(predSet)<limit:
            result[key] = predSet
    return result


# return types for benchmark()
BenchmarkResult = namedtuple.namedtuple("BenchResultRec", "TP, FN, FP, Prec, Recall, F, errList, objCount")
ErrorDetails    = namedtuple.namedtuple("ErrorDetails", "id, expected, predicted")

def benchmark(predDict, refDict):
    """ returns a class with attributes for TP, FN, FP and various other counts and information about prediction errors 
    >>> benchmark({"a" : set([1,2,3]), "b" : set([3,4,5])}, {"a":set([1]), "b":set([4])})
    BenchResultRec(TP=2, FN=0, FP=4, Prec=0.3333333333333333, Recall=1.0, F=0.5, errList=[ErrorDetails(id='a', expected=set([1]), predicted=set([1, 2, 3])), ErrorDetails(id='b', expected=set([4]), predicted=set([3, 4, 5]))], objCount=2)
    """
    OBJECTNAME="documents"

    TP, FN, FP = 0, 0, 0
    objCount = 0
    atLeastOneHit = 0

    errDetails = []
    completeMatch = 0
    completeMismatch = 0 
    tooManyPred = 0
    notEnoughPred = 0
    limitPassed = 0
    predCount = 0 

    # iterate over objects and update counters
    for obj, predSet in predDict.iteritems():
        if obj not in refDict:
            logging.debug("%s not in reference, skipping" % obj)
            continue

        refSet = refDict[obj]
        objCount+=1
        
        perfectMatch=False
        partialMatch=False


        predCount += len(predSet)

        tpSet = predSet.intersection(refSet) # true positives: are in pred and in reference
        fnSet = refSet.difference(predSet)  # false negatives: are in reference but not in prediction
        fpSet = predSet.difference(refSet)  # false positives: are in prediction but not in refernce

        TP += len (tpSet)
        FN += len (fnSet) 
        FP += len (fpSet) 
        if len(tpSet)>0:
            atLeastOneHit+=1
            partialMatch=True
        if len(tpSet)==len(predSet)==len(refSet):
            completeMatch+=1
            perfectMatch=True # set flag to avoid checking several times below
        if len(tpSet)==0:
            completeMismatch+=1
        if len(predSet)>len(refSet):
            tooManyPred+=1
        if len(predSet)<len(refSet):
            notEnoughPred+=1

        if not perfectMatch:
            errDetails.append(ErrorDetails(id=obj, expected=refSet, predicted=predSet))

    if objCount==0:
        logging.debug("number of %s in common between prediction and reference is zero" % OBJECTNAME)
        return None

    if TP+FP > 0:
        Prec    = float(TP) / (TP + FP)
    else:
        print "Warning: Cannot calculate Prec because TP+FP = 0"
        Prec = 0

    if TP+FN > 0:
        Recall  = float(TP) / (TP + FN)
    else:
        print "Warning: Cannot calculate Recall because TP+FN = 0"
        Recall = 0
        
    if Recall>0 and Prec>0:
        F       = 2 * (Prec * Recall) / (Prec + Recall)
    else:
        print "Warning: Cannot calculate F because Recall and Prec = 0"
        F = 0

    return BenchmarkResult(TP=TP, FN=FN, FP=FP, Prec=Prec, Recall=Recall, F=F, errList=errDetails, objCount=objCount)

def allToString(list):
    """ converts all members to a list to strings.
    numbers -> string, lists/sets -> comma-sep strings """
    newList = []
    s = set()
    for e in list:
        if type(e)==types.ListType or type(e)==type(s):
            newList.append(",".join(e))
        else:
            newList.append(str(e))
    return newList

def prettyPrintDict(dict):
    """ print dict to stdout """
    for key, value in dict.iteritems():
        print key, value

def calcBinomScore(background, foreground, genes, backgroundProb):
    TP = len(genes.intersection(foreground))
    binomProb = dist.pbinom(TP, len(genes), backgroundProb)
    binomScore = -math.log10(binomProb)
    return binomScore

def packCoord(start, end):
    " pack start, end into 8 bytes "
    return struct.pack("<ll", int(start), int(end))
def unpackCoord(start, end):
    " undo packCoord "
    start, end = struct.unpack("<ll", arr)
    return start, end

def packChromCoord(chrom, start, end):
    """ pack chrom,start,end into 9 little-endian bytes, return a byte string
    >>> s = packChromCoord("chr21", 1233,123232299)
    >>> unpackChromCoord(s)
    ('chr21', 1233, 123232299)
    >>> unpackChromCoord(packChromCoord("chrM", 1233,123232299))
    ('chrM', 1233, 123232299)
    >>> packChromCoord("chr6_hap", 1,2)
    >>> len(packChromCoord("chr6", 1,2))
    9
    """
    if "_gl" in chrom or "hap" in chrom:
        return None
    chrom = chrom.replace("chr", "")
    if chrom in ["M","X","Y"]:
        chromInt = ord(chrom)
    else:
        chromInt = int(chrom)
    return struct.pack("<bll", chromInt, int(start), int(end))

def unpackChromCoord(arr):
    " undo packCoord "
    chrom, start, end = struct.unpack("<bll", arr)
    if(chrom)>22:
        chrom = "chr"+chr(chrom)
    else:
        chrom = "chr"+str(chrom)
    return chrom, start, end, 

def revComp(seq):
    table = { "a":"t", "A":"T", "t" :"a", "T":"A", "c":"g", "C":"G", "g":"c", "G":"C", "N":"N", "n":"n", 
            "Y":"R", "R" : "Y", "M" : "K", "K" : "M", "W":"W", "S":"S",
            "H":"D", "B":"V", "V":"B", "D":"H", "y":"r", "r":"y","m":"k",
            "k":"m","w":"w","s":"s","h":"d","b":"v","d":"h","v":"b","y":"r","r":"y" }
    newseq = []
    for nucl in reversed(seq):
       newseq += table[nucl]
    return "".join(newseq)

# ----- 
if __name__ == "__main__":
    import doctest
    doctest.testmod()

