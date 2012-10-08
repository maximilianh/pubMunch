# example file for pubtools

# this file searches articles for genetic markers:
# - band names
# - snp identifiers
# - genbank accession numbers
# - ucsc genome code + coordinate identifiers

# each marker type has its own regular expression

# marker names are defined by dicts in 
# e.g. DICTDIR/band.dict.gz
# format 
# <identifier><tab> <syn>|<syn2>|...

# some identifiers have synonyms that can be resolved using dictionaries.

# it returns the fields 'recogId' with the recognized synonym
# and the field 'id' with the final resolved identifier 

# standard python libraries for regex
import re, sys, logging, os.path, gzip, pubConf, glob, doctest
import fastFind
from os.path import *

# skip articles with more annotations than this
MAXCOUNT=150

# parseKwDicts will read dictionaries and bed files from this directory
DICTDIR=pubConf.markerDbDir

# words that are usually not gene names
stopWords = set(['NHS', 'SDS', 'VIP', 'NSF', 'PDF', 'CD8', 'CD4'])

# Regular expressions NEED TO DEFINE a group named "id"
# see python re engine doc: instead of (bla) -> (?P<id>bla)
genbankRe = re.compile("""[ ,.()](?P<id>(([A-Z]{1}\d{5})|([A-Z]{2}\d{6})|([A-Z]{4}\d{8,9})|([A-Z]{5}\d{7}))(\.[0-9]{1,})?)[ ,.()]""")
snpRsRe = re.compile("""[ ,.()](?P<id>rs[0-9]{4,10})[ ,.()]""")
snpSsRe = re.compile("""[ ,.()](?P<id>ss[0-9]{4,16})[ ,.()]""")
#coordRe = re.compile(" chr[0-9]+:[0-9,]+[ ]*[-][ ]*[0-9,]+ ")
bandRe = re.compile("""[ ,.()](?P<id>(X|Y|[1-9][0-9]?)(p|q)[0-9]+(\.[0-9]+)?)[ ,.()]""")
symbolRe = re.compile("""[ ,.()-](?P<id>[A-Z]+[a-zA-z0-9]*)[ ,.()-]""") # an uppercase word

reDict = {"genbank": genbankRe, \
          "snp": snpRsRe, \
          "snpSs": snpSsRe, \
          "band": bandRe, \
          "symbol": symbolRe, \
          }

# == CODE COMMON FOR ANNOTATOR AND MAP TASK 
def parseKwDicts(markerTypes):
    """ parse id<tab>word1|word2|word3-style files and return as list with
    tuples of (markerType, regex, dictionary word -> list of ids)"""
    kwDictList = []
    for markerType in markerTypes:
        markerRe = reDict[markerType]
        markerFname = os.path.join(DICTDIR, markerType+".dict.tab.gz")
        if not isfile(markerFname):
            logging.info("Not found file %s, using only regular expression for %s" % (markerFname, markerType))
            markerDict = None
        else:
            markerDict = {}
            logging.info("Reading %s" % markerFname)
            for id, nameString in fastFind._lexIter(gzip.open(markerFname)):
                markerDict.setdefault(nameString, []).append((id))
            logging.info("Finished reading")
        kwDictList.append((markerType, markerRe, markerDict))
    return kwDictList

def findMarkers(markerDictList, text):
    """ search text for occurences of regular expression + check against dictionary
        yield tuples (start, end, typeOfWord, recognizedId) 
    >>> b=parseKwDicts(["band"])
    >>> list(findMarkers(b, "(Yq11.221,"))
    [[1, 9, 'band', 'Yq11.221']]
    >>> g=parseKwDicts(["symbol"])
    >>> list(findMarkers(g, "(PITX2,"))
    [[1, 6, 'gene', 'PITX2']]
    >>> list(findMarkers(g, "(P53,"))
    []
    >>> list(findMarkers(g, "(NHS,"))
    []
    """
    global stopWords
    for markerType, isId, markerRe, markerDict in markerDictList:
        if markerType=="symbol":
            textLower = text.lower()
            if not "gene" in text or "protein" in text or "locus" in text:
                continue

        for match in markerRe.finditer(text):
            word = match.group("id")
            if word in stopWords:
                continue
            #print markerType, word
            if markerDict==None:
                idList = [word]
            else:
                idList = markerDict.get(word, None)
            if idList != None:
                #print idList
                start = match.start("id")
                end = match.end("id")
                result = [ start, end, markerType, ",".join(idList), "" ]

                yield result

def getSearchTypes(paramDict):
    """ get searchType from paramDict, if not set, return all possible types,
        possible types are defined by all second parts of bedfiles in markerDir
    """
    bedFiles = glob.glob(join(DICTDIR, "*.bed"))
    logging.info("Found bedfiles in %s: %s" % (pubConf.markerDbDir, bedFiles))
    allTypes = [splitext(basename(x))[1] for x in bedFiles]
    paramTypes = paramDict.get("searchType", "").split(",")
    if paramTypes==[""]:
        searchTypes = allTypes
    else:
        searchTypes = paramTypes
    logging.info("Searching for: %s" % searchTypes)
    return set(searchTypes)

# === ANNTOATOR ====
class MarkerAnnotate:
    def __init__(self):
        # this variable has to be defined, otherwise the jobs will not run.
        # The framework will use this for the headers in table output file
        self.headers = ["start", "end", "type", "recogId", "id"]

        # let's ignore files with more than 1000 matches
        self.MAXCOUNT = 100

        # we want sections in our tables
        self.sectioning = True
        # we don't want to run on both pdf and xml
        self.bestMain = True

    # this method is called ONCE on each cluster node, when the article chunk
    # is opened, it fills the kwDict variable
    def startup(self, paramDict):
        """ parse dictioary of keywords """
        self.searchTypes = getSearchTypes(paramDict)
        self.kwDictList = parseKwDicts(self.searchTypes)

    # this method is called for each FILE. one article can have many files
    # (html, pdf, suppl files, etc). article data is passed in the object 
    # article, file data is in "file". For a list of all attributes in these 
    # objects please see the file ../lib/pubStore.py, search for "DATA FIELDS"
    def annotateFile(self, article, file):
        " go over words of text and check if they are in dict "
        #if article.year!="2010":
            #return None
        #else:
            #resultRows.append(["0", "1", "GOODYEAR", "GOODYEAR"])
        text = file.content
        count = 0
        annots = list(findMarkers(self.kwDictList, text))
        if len(annots)>MAXCOUNT:
            logging.info("more than %d annotations (excel table, list of genes, etc), skipping file" % MAXCOUNT)
            return None
        return annots
            
# === MAP/REDUCE TASK ====
class FilterKeywords:
    def __init__(self):
       self.headers = ["keyword"] 

    def startup(self, paramDict, resultDict):
        self.maxCount = paramDict["maxCount"]
        kwFilename = paramDict["keywords"]
        self.searchTypes = getSearchTypes(paramDict)
        self.kwDict = parseKwDicts(self.searchTypes)

    def map(self, article, file, text, resultDict):
        matches = list(findMarkers(self.kwDict, text))
        for start, end, type, word in matches:
            resultDict.setdefault(word, set()).add(file.fileId)

    # this is called after all jobs are finished, on the main machine
    # it is called once for each key
    def reduce(self, word, fileIds):
        if len(fileIds) < self.maxCount:
            return word, len(fileIds)

if __name__=="__main__":
    import doctest
    doctest.testmod()


