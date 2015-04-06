# a searcher for <GENESYM>/<GENESYM> or <GENESYM>-<GENESYM

# we need the regular expressions module to split text into words
# (unicode-awareness) and gzip
import re, gzip, logging, itertools, collections
from os.path import *
from nltk.tokenize import PunktSentenceTokenizer

# FRAMEWORK
# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
headers = ["start", "end", "trigger", "matchType", "word1", "word2", "recogSym1", "recogSym2", "sym1", "sym2", "markerId", "hgncIds1", "hgncIds2"]

# SEARCH 

# any word, for trigger word search
wordRe = re.compile("[a-zA-z]+")

# these words have to occur somewhere in the text
triggerWords = set(["fusion", "fusions", "translocations", "translocation", "inversion", "inversions","chimeric", "oncoprotein", "oncogene", "oncofusion"])
# removed: "deletion", "hybrid", 

# rough description of what a fusion gene description looks like, note the lookahead at the end to accomodate lists
# ex: " PITX2/OTX-1 "
slashFusion = re.compile(r'[\s().,;]([a-zA-Z0-9-]+)[/]([a-zA-Z0-9-]+)(?=[ (),;.])')
# ex: " PITX2-OTX1 "
dashFusion = re.compile(r'[\s().,;]([A-Z][a-zA-Z0-9/-]+)[-]([A-Z][a-zA-Z0-9/]+)(?=[ (),;.])')
# ex: " PITX2:OTX1 "
colonFusion = re.compile(r'[\s().,;]([A-Z][a-zA-Z0-9/-]+)[:]([A-Z][a-zA-Z0-9/]+)(?=[ (),;.])')

# ex. Pitx-1/2 
slashNumRe = re.compile("([A-Za-z-]{2,6})([0-9]{1,2})/([0-9]{1,2})")
# ex Hox-3a/b
slashLetRe = re.compile("([A-Za-z]+[-]*[0-9]+)/([a-z]+)")

# ex PITX1 splits into PITX and 1
stemRe = re.compile(r'^([A-Z]+)([0-9]+)$')

fusionReDict = {"slash": slashFusion, "dash" : dashFusion, "colon" : colonFusion}

# the path to hugo.tab, list of hugo symbols, synonomys and previous symbols
dataFname = abspath(join(dirname(__file__), "data", "hugo2.tab.gz"))

# global variable, holds the mapping symbol => official Symbol
hugoDict = {}

# holds mapping synonym/previous symbol -> set of official symbolss
symToOffSym = collections.defaultdict(set)

# holds mapping of stem symbol -> official symbol
# used to check if id2 is not a vague synonym of id1
# if synonym is PITX2 then the "stem" symbol is PITX
stemToOffSym = collections.defaultdict(set)

# pathway names or other crap, cannot be fusion genes
blackList = set([("JAK","STAT"), ("PI3K", "AKT"),("MAPK", "ERK"),("AKT","PKB"),("RH", "HR"),("SAPK", "JNK"), ("CAD", "CAM"), ("SD", "OCT"), ("IVF", "ET"), ("PTEN", "AKT"), ("CD", "ROM"), ("JUN", "FOS")])

# never recognize these as genes
notGenes = ["OK", "II", "KO", "CD[0-9]+", "C[0-9]", "CT", "MS", "MRI", "H[0-9]", "ZIP", "WAF", "CIP", "OCT", "APR", "SEP", "NOV", "DEC", "JAN", "FEB", "TOP", "FOP", "FLASH"]
notGeneRes = [re.compile(s) for s in notGenes]

def stem(string):
    " get stem of symbol  "
    #assert(string!='' and string!=" ")
    match = stemRe.match(string)
    if match!=None:
        stem, num = match.groups()
        #logging.debug("%s is in stem/number format, stem %s, num %s" % (string, stem, num))
        return stem
    else:
        #logging.debug("%s is not in stem/number format" % string)
        return string

def addStem(stemToOffSym, syn, symbol):
    """ add PTC->PTC3 for syn='PTC2'/symbol=NOAH to stemToOffSym
    """
    match = stemRe.match(syn)
    if match==None:
        stemToOffSym[syn].add(symbol)
    else:
        stem, num = match.groups()
        stemToOffSym[stem].add(symbol)

# this method is called ONCE on each cluster node, when the article chunk
# is opened, it fills the hugoDict variable
def startup(paramDict):
    """ parse HUGO file into dict 
    dict format is:
    symbol -> set of IDs
    """
    logging.info("Reading %s" % dataFname)
    for line in gzip.open(dataFname):
        hugoId, symbol, prevSyms, synSyms = line.strip("\n").split("\t")
        prevSyms = prevSyms.split(",")
        synSyms = synSyms.split(",")
        
        symToOffSym[symbol].add(symbol) # ?
        hugoDict.setdefault(symbol, set()).add(hugoId)
        for syn in prevSyms:
            hugoDict.setdefault(syn, set()).add(hugoId)
            symToOffSym[syn].add(symbol)
            stemToOffSym[stem(syn)].add(symbol)
        for syn in synSyms:
            if syn=="ABL": # why do we ignore ABL?
                continue
            hugoDict.setdefault(syn, set()).add(hugoId)
            symToOffSym[syn].add(symbol)
            addStem(stemToOffSym, syn, symbol)

    for sym, ids in hugoDict.iteritems():
        assert(len(ids)!=0)

def findMatches(reDict, text):
    for reType, reObj in reDict.iteritems():
        for match in reObj.finditer(text):
            yield reType, match

def tooSimilar(s1, s2):
    " if same len and only last char different, return true "
    logging.debug("Checking off symbols %s and %s" % (s1, s2))
    if s1==s2:
        ret = True
    elif len(s1) == len(s2) and s1[-1]!=s2[-1] and s1[:-1]==s2[:-1]:
        ret = True
    else:
        ret = False
    logging.debug("Are official symbols too similar? -> %s" % ret)
    return ret
    # special case: if two genes are longer than two chars and 
    # the first letters are different, 
    #if len(s1)>2 and s1[0]==s2[0] and s1[1]==s2[1]:
    #return False
    #hammDist = sum(ch1 != ch2 for ch1, ch2 in zip(s1, s2))
    #if hammDist>1:
    #return False
    #else:
    #return True
    
def matchesAny(string, reList):
    for reObj in reList:
        if reObj.match(string):
            return True
    return False

def removeGreek(string):
    " replace greek letters with latin letters "
    greekTable = {"alpha" : "a", "beta":"b", "gamma":"g", "delta":"d"}
    for greek, latin in greekTable.iteritems():
        if greek in string:
            string = string.replace(greek, latin)
    return string

def resolveSlash(string):
    """ resolve PITX1/2 or Pitx-1/2 or Hox3a/b to a list [PITX1, PITX2]
    >>> resolveSlash("PITX1/2")
    ['PITX1', 'PITX2']
    """
    if "/" not in string:
        return [string]
    match = slashNumRe.match(string)
    genes = []
    if match!=None:
        gene, startNum, endNum = match.groups()
        startNum = int(startNum)
        endNum = int(endNum)
        if endNum-startNum > 5:
            logging.debug("list too long, using only first gene in list")
            genes.append(gene+str(startNum))
        else:
            for i in range(startNum, endNum+1):
                genes.append(gene+str(i))
    else:
        match = slashLetRe.match(string)
        if match==None:
            logging.debug("%s is neither slash-letter nor slash-number" % string)
            return [string]
        gene, startLet, endLet = match.groups()
        startOrd = ord(startLet)
        endOrd = ord(endLet)
        if endOrd-startOrd > 5:
            logging.debug("list too long, using only first gene in list")
            return [gene+startLet]
        for i in range(startOrd, endOrd):
            genes.append(gene+chr(i))
    return genes

    #logging.debug("doesn't match slashNumRe")
    #return [string]
    #print gene, modifier

def offSymbolsOk(offSyms1, offSyms2):
    " check if there are any problems in two lists of official symbols "
    logging.debug("checking if %s and %s are different enough" % (offSyms1, offSyms2))
    for (offSym1, offSym2) in itertools.product(offSyms1, offSyms2):
        if tooSimilar(offSym1, offSym2):
            logging.debug("skipping %s,%s: official symbols too similar" % (offSym1, offSym2))
            return False
        if (offSym1, offSym2) in blackList:
            logging.debug("skipping %s,%s: blacklisted" % (sym1, sym2))
            return False
    return True

def findFusions(text, triggers):
    # go over putative fusion gene descriptions
    rows = []
    for matchType, match in findMatches(fusionReDict, text):
        logging.debug("Found match %s" % match.group(0))
        word1 = match.group(1)
        word2 = match.group(2)
        sym1 = word1.upper()
        sym2 = word2.upper()
        if len(sym1)<=2 or len(sym2)<=2:
            logging.debug("stop: symbols not long enough")
            continue
        if matchesAny(sym1, notGeneRes) or matchesAny(sym2, notGeneRes):
            logging.debug("stop: one symbol is not a gene")
            continue
        if (sym1, sym2) in blackList:
            logging.debug("stop: blacklisted gene")
            continue

        if "-" in sym1:
            sym1 = sym1.replace("-", "")
        if "-" in sym2:
            sym2 = sym2.replace("-", "")
        sym1 = removeGreek(sym1)
        sym2 = removeGreek(sym2)

        # convert to two lists
        logging.debug("Got symbols %s, %s" % (sym1, sym2))
        if matchType=="slash":
            logging.debug("Slash type")
            syms1 = [sym1]
            syms2 = [sym2]
        elif matchType in ["dash","colon"]:
            logging.debug("Dash or colon type")
            syms1 = resolveSlash(sym1)
            syms2 = resolveSlash(sym2)
        else:
            assert(False)

        for sym1, sym2 in itertools.product(syms1, syms2):
            # eliminate cases like C1/C2
            if len(sym2)<=2:
                logging.debug("stop: symbols not long enough")
                continue
            logging.debug("Checking %s,%s against symbol db" % (sym1, sym2))
            # eliminate cases HOX1/HOX4 or HOX1-HOX4
            if tooSimilar(sym1, sym2):
                logging.debug("skipping %s,%s: symbols too similar" % (sym1, sym2))
                continue
            # check if stem of 2nd symbol is official symbol of sym1
            stem2 = stem(sym2)
            if sym1 in stemToOffSym[stem2]:
                logging.debug("Skipping: stem of %s is %s, is a synonym of %s" % \
                    (sym2, stem2, sym1))
                break

            # check if the results are valid symbols
            if sym1 in hugoDict and sym2 in hugoDict and sym1!=sym2:
                hgncSet1 = hugoDict[sym1]
                hgncSet2 = hugoDict[sym2]
                hgncIds1 = ",".join(hgncSet1)
                hgncIds2 = ",".join(hgncSet2)
                if hgncIds1==hgncIds2:
                    logging.debug("skipping %s,%s: same HGNC ids" % (sym1, sym2))
                    continue
                offSyms1 = symToOffSym[sym1]
                offSyms2 = symToOffSym[sym2]
                logging.debug("Official syms1: %s, official syms2: %s" % (offSyms1, offSyms2))

                if offSymbolsOk(offSyms1, offSyms2):
                    offSym1 = list(offSyms1)[0] # we always take the first off symbol. good idea?
                    offSym2 = list(offSyms2)[0]
                    sortPair = [offSym1, offSym2]
                    sortPair.sort()
                    result = [ match.start(), match.end(), ",".join(triggers), matchType, word1, word2, \
                        sym1, sym2, offSym1, offSym2, "/".join(sortPair), hgncIds1, hgncIds2]
                    rows.append(result)

    if len(rows)>10: # we skip sentences with more than 10 genes 
        logging.warn("More than 10 hits in a single sentence, dropping all")
        return []
    return rows

# this method is called for each FILE. one article can have many files
# (html, pdf, suppl files, etc). Article data is passed in the object 
# article, file data is in "file". For a list of all attributes in these 
# objects see the file ../lib/pubStore.py, search for "DATA FIELDS"
def annotateFile(article, file):
    """ go over words of text and check if they are in dict 
    >>> annotateFile(None, " AML-1/ETO ")

    """
    # for debugging
    if isinstance(file, basestring):
        text = file
    else:
        text = file.content

    rows = []
    for start, end in PunktSentenceTokenizer().span_tokenize(text):
        if end-start > 700:
            logging.debug("Too long sentence, skipping")
            continue
        sent = text[start:end]
        # make sure that one of the triggerwords occur
        words =  set([w.lower() for w in wordRe.findall(sent)])
        sentTriggers = words.intersection(triggerWords)
        if len(sentTriggers) == 0:
            #logging.debug("stop: no triggerword")
            continue

        for row in findFusions(sent, sentTriggers):
            row[0] += start
            row[1] += start
            rows.append(row)

    if len(rows)>100:
        logging.warn("More than 100 rows per document, skipping all output")
        return None
    return rows


if __name__=="__main__":
    import doctest
    doctest.testmod()

