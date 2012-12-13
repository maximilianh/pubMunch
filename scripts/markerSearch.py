# example file for pubtools

# this file searches articles for genetic markers:
# - band names
# - snp identifiers
# - ensembl gene identifier
# - genbank accession 
# - ucsc genome code + coordinate identifiers
# - PDB accession 
# - uniprot accession 
# - gene symbol (hugo/HGNC)

# each marker type has its own regular expression

# marker names *can* be restricted by textfiles in 
# e.g. DICTDIR/band.dict.gz
# format 
# <identifier><tab> <syn>|<syn2>|...
# Some identifiers have synonyms that can be resolved using dictionaries.
# Some identifier formats are so general that they need a dictionary to find
# the right ones (e.g. uniprot)

# The main function returns the fields 'recogId' with the recognized synonym
# and the field 'markerId' with the final resolved identifier 

# can be restricted to search only for certain markers with the parameter
# 'searchType' (comma-sep), e.g. searchType="snp,genbank"

# standard python libraries for regex
import re, sys, logging, os.path, gzip, glob, doctest
import fastFind, pubConf
from os.path import *

# skip articles with more markers than this
MAXCOUNT=150

# skip genbank lists like A1234-A1240 with more identifiers than this
MAXGBLISTCOUNT=50

# ignore articles with more than X markers found
MAXROWS = 100

# parseKwDicts will read dictionaries and bed files from this directory
DICTDIR=pubConf.markerDbDir

# words that are usually not gene names, rather used for cell lines or pathways or other stuff
stopWords = set(['NHS', 'SDS', 'VIP', 'NSF', 'PDF', 'CD8', 'CD4','JAK','STAT','CD','ROM','CAD','CAM','RH', 'HR','CT','MRI','ZIP','WAF','CIP','APR','OK','II','KO','CD80','H9', 'SMS'])

# Some identifiers are so general that we want to restrict our search
# to documents that contain some keyword
# the neededWordDict hash sets up the lists of keywords in the document
# that are required for certain identifiers
genbankKeywords = ["genbank", "accession", " embl", "ddbj", "insdc", " ena ", "european nucleotide", " acc. "]
neededWordDict = {
    "genbank" :     genbankKeywords,
    "genbankList" : genbankKeywords,
    "symbol" : ["gene", "protein", "locus"],
    "pdb" : ["pdb", "rcsb", "protein data bank"],
    "hg18" : ["hg18"],
    "hg19" : ["hg19"],
    "hg17" : ["hg17"],
    "flybase": ["flybase", "drosophila", "melanogaster"]
}

# separators before or after the regular expressions below
endSep = "(?=[\s:,.()])"
endSepDash = "(?=[\s:,.()-])"
startSep = r'[ ,.();:=]'
startSepDash = r'[ ,.()-;:-=]'

# Regular expressions NEED TO DEFINE a group named "id"
# see python re engine doc: instead of (bla) -> (?P<id>bla)

# received regex by email from Guy Cochrane, EBI
genbankRe = re.compile("""[ ;,.()](?P<id>(([A-Z]{1}\d{5})|([A-Z]{2}\d{6})|([A-Z]{4}\d{8,9})|([A-Z]{5}\d{7}))(\.[0-9]{1,})?)%s""" % endSep)
genbankListRe = re.compile("""[ ;,.()](?P<id1>(([A-Z]{1}\d{5})|([A-Z]{2}\d{6})|([A-Z]{4}\d{8,9})|([A-Z]{5}\d{7}))(\.[0-9]{1,})?)-(?P<id2>(([A-Z]{1}\d{5})|([A-Z]{2}\d{6})|([A-Z]{4}\d{8,9})|([A-Z]{5}\d{7}))(\.[0-9]{1,})?)%s""" % (endSep))

snpRsRe = re.compile("""[ ;,.()](?P<id>rs[0-9]{4,10})%s""" % (endSep))
snpSsRe = re.compile("""[ ;,.()](?P<id>ss[0-9]{4,16})%s""" % (endSep))
coordRe = re.compile("%s(?P<id>(chr|chrom|chromosome)[ ]*[0-9XY]{1,2}:[0-9,]{4,12}[ ]*-[ ]*[0-9,]{4,12})%s" % (startSep, endSep))
bandRe = re.compile("""[ ,.()](?P<id>(X|Y|[1-9][0-9]?)(p|q)[0-9]+(\.[0-9]+)?)%s""" % (endSep))
symbolRe = re.compile("""[ ;,.()-](?P<id>[A-Z]+[a-zA-z0-9]*)%s""" % (endSepDash))

# http://flybase.org/static_pages/docs/nomenclature/nomenclature3.html#2.
flybaseRe = re.compile("""[ ;,.()-](?P<id>(CG|CR)[0-9]{4,5})%s""" % (endSepDash))
# http://flybase.org/static_pages/docs/refman/refman-F.html
flybase2Re = re.compile("""[ ;,.()-](?P<id>FB(ab|al|ba|cl|gn|im|mc|ms|pp|rf|st|ti|tp|tr)[0-9]{7})%s""" % (endSepDash))

# http://www.uniprot.org/manual/accession_numbers
# letter + number + 3 alphas + number,eg A0AAA0
uniprotRe = re.compile(r'[\s;,.()-](?P<id>[A-NR-ZOPQ][0-9][A-Z0-9][A-Z0-9][A-Z0-9][0-9])%s' % (endSepDash))

# http://pdbwiki.org/wiki/PDB_code
pdbRe = re.compile(r'%s(?P<id>[0-9][a-zA-Z][a-zA-Z][a-zA-Z])%s' % (startSepDash, endSepDash)) # number with three letters

# http://www.ncbi.nlm.nih.gov/RefSeq/key.html#accession
refseqRe = re.compile(r'%s(?P<id>[XYNAZ][MPR]_[0-9]{4,11})%s' % (startSepDash, endSepDash))
ensemblRe = re.compile(r'%s(?P<id>ENS([A-Z]{3})?[GPT][0-9]{9,14})%s' % (startSepDash, endSepDash))

reDict = {"genbank": genbankRe, \
          "genbankList": genbankListRe, \
          "snp": snpRsRe, \
          "snpSs": snpSsRe, \
          "band": bandRe, \
          "symbol": symbolRe, \
          "uniprot": uniprotRe, \
          "pdb": pdbRe,
          "refseq" : refseqRe,
          "ensembl" : ensemblRe,
          "hg17" : coordRe,
          "hg18" : coordRe,
          "hg19" : coordRe,
          "flybase" : flybaseRe
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

def splitGenbankAcc(acc):
    """ split a string like AY1234 into letter-number tuple, e.g. (AY, 1234)
    >>> splitGenbankAcc("AY1234")
    ('AY', 1234, 4)
    """
    match = re.match("([A-Z]+)([0-9]+)", acc)
    if match==None:
        return None

    letters, numbers = match.groups()
    return (letters, int(numbers), len(numbers))

def iterGenbankRows(markerRe, markerType, text):
    """ generate match rows for a list like <id1>-<id2> 
    >>> list(iterGenbankRows(genbankListRe, "genbankList", "    JN011487-JN011488 "))
    [[3, 12, 'genbank', ' JN011487-JN011488', 'JN011487'], [3, 12, 'genbank', ' JN011487-JN011488', 'JN011488']]
    >>> list(iterGenbankRows(genbankListRe, "gbl", " JN011487-AP011488 "))
    []
    """
    markerType = markerType.replace("List", "")
    for match in markerRe.finditer(text):
        word = match.group()
        id1  = match.group("id1")
        id2  = match.group("id2")

        let1, num1, digits1 = splitGenbankAcc(id1)
        let2, num2, digits2 = splitGenbankAcc(id2)
        if let1!=let2 or digits1!=digits2:
            continue
        if (num2-num1) > MAXGBLISTCOUNT:
            continue
        for num in range(num1, num2+1):
            numFmt = "%%0%sd" % digits1
            acc = let1+(numFmt % num)
            start = match.start(0)
            end = match.end(1)
            yield [ start, end, markerType, word, acc ]

def textContainsAny(text, keywords):
    for keyword in keywords:
        if keyword in text:
            return True
    return False

def findMarkers(markerDictList, text):
    """ search text for occurences of regular expression + check against dictionary
        yield tuples (start, end, typeOfWord, recognizedId) 
    >>> b=parseKwDicts(["band"])
    >>> list(findMarkers(b, "(Yq11.221,"))
    [[1, 9, 'band', '', 'Yq11.221']]
    >>> g=parseKwDicts(["symbol"])
    >>> list(findMarkers(g, "gene (PITX2,"))
    [[6, 11, 'symbol', '', 'PITX2']]
    >>> list(findMarkers(g, "gene (P53,"))
    []
    >>> list(findMarkers(g, "(NHS,"))
    []
    >>> p=parseKwDicts(["pdb"])
    >>> list(findMarkers(p, " 1abz protein data bank"))
    [[1, 5, 'pdb', '', '1abz']]
    >>> list(findMarkers(p, " 1ABZ PDB"))
    [[1, 5, 'pdb', '', '1abz']]
    >>> u=parseKwDicts(["uniprot"])
    >>> list(findMarkers(u, " B7ZGX9 P12345 "))
    [[1, 7, 'uniprot', '', 'B7ZGX9']]
    >>> g=parseKwDicts(["genbank"])
    >>> list(findMarkers(g, " L76943 ena "))
    [[1, 7, 'genbank', '', 'L76943']]
    >>> list(findMarkers(g, " L76943"))
    []
    >>> e=parseKwDicts(["ensembl"])
    >>> list(findMarkers(e, " ENSG001230434 "))
    [[1, 14, 'ensembl', '', 'ENSG001230434']]
    >>> c=parseKwDicts(["hg19"])
    >>> list(findMarkers(c, " chr1:123,220-123334234 hg19"))
    [[1, 23, 'hg19', '', 'chr1:123220-123334234']]
    """
    global stopWords

    rows = []
    for markerType, markerRe, markerDict in markerDictList:
        textLower = text.lower()
        #if markerType in ["genbankList", "genbank"]:
            #keywords = 
            #if not textContainsAny(textLower, keywords):
                #continue
            
        if markerType=="genbankList":
            for row in iterGenbankRows(markerRe, markerType, text):
                #yield row
                rows.append(row)
            continue

        if markerType in neededWordDict:
            keywords = neededWordDict[markerType]
            if not textContainsAny(textLower, keywords):
                continue

        for match in markerRe.finditer(text):
            word = match.group("id")
            if word in stopWords:
                continue


            if markerType=="pdb":
                word = word.lower()

            if markerType in ["hg17", "hg18", "hg19"]:
                word = word.replace(",", "").replace(" ", "")

            if markerDict==None:
                idList = [word]
            else:
                idList = markerDict.get(word, None)

            if idList != None:
                start = match.start("id")
                end = match.end("id")
                for recogId in idList:
                    if word==recogId:
                        word=""
                    row = [ start, end, markerType, word, recogId]
                #yield result
                rows.append(row)

    if len(rows)<MAXROWS:
        return rows
    else:
        return []

def getSearchTypes(paramDict):
    """ get searchType from paramDict, if not set, return all possible types,
        possible types are defined by all second parts of bedfiles in markerDir
    """
    bedFiles = glob.glob(join(DICTDIR, "*.bed"))
    logging.info("Found bedfiles in %s: %s" % (pubConf.markerDbDir, bedFiles))
    #allTypes = [basename(x).split(".")[1] for x in bedFiles]
    paramTypes = paramDict.get("searchType", "").split(",")
    if paramTypes==[""]:
        searchTypes = reDict.keys() # search for all types
    else:
        searchTypes = paramTypes
    logging.info("Searching for: %s" % searchTypes)
    return set(searchTypes)

# === ANNTOATOR ====
class Annotate:
    def __init__(self):
        # this variable has to be defined, otherwise the jobs will not run.
        # The framework will use this for the headers in table output file
        self.headers = ["start", "end", "type", "recogId", "markerId"]

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


