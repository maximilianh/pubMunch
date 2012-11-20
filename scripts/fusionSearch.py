# a searcher for <GENESYM>/<GENESYM> or <GENESYM>-<GENESYM

# we need the regular expressions module to split text into words
# (unicode-awareness) and gzip
import re, gzip
from os.path import *

# FRAMEWORK
# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
headers = ["start", "end", "matchType", "word1", "word2", "symbol1", "symbol2", "officialSym1", "officialSym2", "sortedOffSyms", "hgncIds1", "hgncIds2"]

# SEARCH 

# any word, for trigger word search
wordRe = re.compile("[a-zA-z]+")

# these words have to occur somewhere in the text
triggerWords = set(["cancer", "fusion", "fusions", "translocations", "translocation", "hybrid", "deletion", "deletions", "inversion", "inversions","chimeric", "oncoprotein", "oncogene", "oncofusion"])

# rough description of what a fusion gene description looks like
# ex: " PITX2/OTX-1 "
slashFusion = re.compile("[\s().,;]([A-Z][a-zA-Z0-9-]+)[/]([A-Z][a-zA-Z0-9-]+)[\s(),;.]")
# ex: " PITX2-OTX1 "
dashFusion = re.compile("[\s().,;]([A-Z][a-zA-Z0-9]+)[-]([A-Z][a-zA-Z0-9]+)[\s(),;.]")

fusionReDict = {"slash": slashFusion, "dash" : dashFusion}

# the path to hugo.tab, list of hugo symbols, synonomys and previous symbols
dataFname = join(dirname(__file__), "data", "hugo2.tab.gz")

# global variable, holds the mapping symbol => official Symbol
hugoDict = {}

# holds mapping alternative symbol -> one of the (possibly several) main symbols
symToOffSym = {}

# pathway names or other crap, cannot be fusion genes
blackList = set([("JAK","STAT"), ("PI3K", "AKT"),("MAPK", "ERK"),("AKT","PKB"),("RH", "HR"),("SAPK", "JNK"), ("CAD", "CAM"), ("SD", "OCT"), ("IVF", "ET"), ("PTEN", "AKT"), ("CD", "ROM")])

# never recognize these
notGenes = ["OK", "II", "KO", "CD[0-9]+", "C[0-9]", "CT", "MS", "MRI", "H[0-9]", "ZIP", "WAF", "CIP", "OCT", "APR"]
notGeneRes = [re.compile(s) for s in notGenes]

# this method is called ONCE on each cluster node, when the article chunk
# is opened, it fills the hugoDict variable
def startup(paramDict):
    """ parse HUGO file into dict 
    dict format is:
    symbol -> set of IDs
    """
    for line in gzip.open(dataFname):
        hugoId, symbol, prevSyms, synSyms = line.strip("\n").split("\t")
        prevSyms = prevSyms.split(",")
        synSyms = synSyms.split(",")
        
        symToOffSym[symbol] = symbol
        hugoDict.setdefault(symbol, set()).add(hugoId)
        for sym in synSyms:
            hugoDict.setdefault(sym, set()).add(hugoId)
            symToOffSym[sym] = symbol
        for sym in prevSyms:
            hugoDict.setdefault(sym, set()).add(hugoId)
            symToOffSym[sym] = symbol

    for sym, ids in hugoDict.iteritems():
        assert(len(ids)!=0)

def findMatches(reDict, text):
    for reType, reObj in reDict.iteritems():
        for match in reObj.finditer(text):
            yield reType, match

def tooSimilar(s1, s2):
    " if same len and only last char different, return true "
    if len(s1) != len(s2):
        return False
    if s1[-1]!=s2[-1] and s1[:-1]==s2[:-1]:
        return True
    else:
        return False
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

# this method is called for each FILE. one article can have many files
# (html, pdf, suppl files, etc). Article data is passed in the object 
# article, file data is in "file". For a list of all attributes in these 
# objects please see the file ../lib/pubStore.py, search for "DATA FIELDS"
def annotateFile(article, file):
    " go over words of text and check if they are in dict "
    resultRows = []
    text = file.content

    # make sure that one of the triggerwords occur
    words =  set([w.lower() for w in wordRe.findall(text)])
    if len(words.intersection(triggerWords)) == 0:
        return
        
    # go over putative fusion gene descriptions
    for matchType, match in findMatches(fusionReDict, text):
        word1 = match.group(1)
        word2 = match.group(2)
        sym1 = word1.upper()
        sym2 = word2.upper()
        if len(sym1)<=2 and len(sym2)<=2:
            continue
        if matchesAny(sym1, notGeneRes) or matchesAny(sym2, notGeneRes):
            continue
        if (sym1, sym2) in blackList:
            continue
        # remove internal dashes
        if "-" in sym1:
            sym1 = sym1.replace("-", "")
        if "-" in sym2:
            sym2 = sym2.replace("-", "")
        # eliminate cases like C1/C2 or HOX1/HOX4
        if tooSimilar(sym1, sym2):
            continue
        # check if the result is a valid symbol
        if sym1 in hugoDict and sym2 in hugoDict and sym1!=sym2:
            hgncSet1 = hugoDict[sym1]
            hgncSet2 = hugoDict[sym2]
            hgncIds1 = ",".join(hgncSet1)
            hgncIds2 = ",".join(hgncSet2)
            if hgncIds1==hgncIds2:
                continue
            offSym1 = symToOffSym[sym1]
            offSym2 = symToOffSym[sym2]
            if tooSimilar(offSym1, offSym2):
                continue
            if (offSym1, offSym2) in blackList:
                continue
            sortPair = [offSym1, offSym2]
            sortPair.sort()
            result = [ match.start(), match.end(), matchType, word1, word2, \
                sym1, sym2, offSym1, offSym2, sortPair, hgncIds1, hgncIds2]
            resultRows.append(result)

    if len(resultRows)>1000: # we skip files with more than 1000 genes 
        return None
    return resultRows
