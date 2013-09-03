# Create medline fingerprints

# Try to find PMID for an article by comparing fingerprints of article data against medline
# fingerprints. 

import logging, optparse, sys, os, marshal, unicodedata, gdbm, unidecode, gzip
import pubGeneric, pubConf, maxCommon, pubStore
from os.path import isfile, join

noIssuePage = 0
noIssn = 0

def remove_accents(input_str):
    #nkfd_form = unicodedata.normalize('NFKD', unicode(input_str))
    #return "".join([c for c in nkfd_form if not unicodedata.combining(c)])
    return unidecode.unidecode(input_str)

def getFingerprint0(row):
    " the first fingerprint is just the DOI "
    if row.doi=="":
        return None
    else:
        return remove_accents(row.doi)

def getFingerprint1(row, withEIssn = False):
    " return fingerprint1 as a string printIssn|vol|issue|page "
    global noIssuePage
    global noIssn

    if row.page=="" or (row.page!="" and (row.vol=="" or row.issue=="")):
        noIssuePage +=1
        logging.debug("No issue and no page: %s" % str(row))
        return None

    issn = row.printIssn
    if issn=="" or withEIssn:
        issn = row.eIssn

    if issn=="":
        if not withEIssn:
            logging.debug("No issn: %s" % str(row))
        noIssn += 1
        return None

    issue=row.issue
    if len(issue)!=0:
        issue = issue.split()[0].split("-")[0]
    page = row.page.split()[0].split("-")[0]
    fprint = "|".join((issn, row.vol, issue, page))
    logging.debug("fingerprint1: %s" % fprint)
    return remove_accents(fprint)

def removePrefixes(famNameStr):
    " remove these annoying prefixes from a family name"
    for p in ["van ", "von ", "de ", "al ", "el ", "van der ", "de la", "des ", "du ", "van de ", \
              "Van ", "Von ", "De ", "Al ", "El ", "Van der ", "De la", "Des ", "Du ", "Van de "]:
        if p in famNameStr:
            famNameStr = famNameStr.replace(p, "")
            break
    return famNameStr

def getFingerprint2(row):
    """ fp2 is: first five authors, year, first and last word of title, separated by | 
    returns None if authors or title is too short or no year.
    """
    if row.year=="" or row.title==None:
        return None
    authors = row.authors.split(";")
    authors = [x.strip() for x in authors]
    famNames = [a.split(",")[0].lower() for a in authors][:5]
    famNames = [removePrefixes(a) for a in famNames]
    famNameStr = "-".join(famNames).lower()
    famNameStr = unidecode.unidecode(famNameStr)
    if len(famNameStr)<5:
        return None
    #famNameStr = remove_accents(famNameStr).lower()
    #page = row.page.split()[0].split("-")[0].split(",")[0]
    title1 = row.title.strip("[]()\n ").split(".")[0]
    titleWords = title1.strip().strip(".").split()
    if len(titleWords) <= 3:
        return None
    title = (titleWords[0]+"-"+titleWords[-1]).lower()
    fprint = "|".join((famNameStr, row.year, title))
    logging.debug("fingerprint2: %s" % fprint)
    return remove_accents(fprint)

def addFprint(data, fprint, articleId):
    " add fingerprint to dictionary "
    #if fprint!=None and fprint not in skipPrints:
        #if fprint in data:
            #del data[fprint]
            #skipPrints.add(fprint)
        #else:
    # do not filter for duplicates anymore
    # we can have duplicates on both sides (medline and elsevier)
    # just get one match
    if fprint!=None:
        data[fprint] = articleId

def lookupFprint(fprint, artMap, artIds):
    " lookup fprint in artMap dictionary, optionally join on artIds "
    if fprint==None:
        logging.debug("fingerprint is empty")
        return None
    # does not work with gdmb:
    # artId = artMap.get(fprint, None) 
    # gdbm does not implement __contains__ so might be better to use has_key() instead of "in"
    #if artMap.has_key(fprint):
    artId = artMap.get(fprint, None)
    if artId==None:
        logging.debug("No match for %s" % fprint)
        return None

    if artIds==None:
        logging.debug("Match for fingerprint found: %s" % str(artId))
        return artId
    else:
        match = artIds[artId]
        extId, doi, pmid = match
        ret = (fprint, artId, extId, doi, str(pmid))
        logging.debug("Match for fingerprint found: %s" % (ret))
        return ret

def createFingerprints(inDir, updateIds=None):
    map0 = {}   # doi -> articleId
    map1 = {}   # issn/vol/page -> articleId
    map2 = {}   # author, title, year -> articleId
    artIds = {} # articleId -> (extId, doi, pmid)

    global noIssuePage
    noIssuePage = 0
    global noIssn
    noIssn = 0

    logging.info("Indexing %s" % inDir)
    count = 0
    for row in pubStore.iterArticleDataDir(inDir, type="articles", updateIds = updateIds):
        articleId = int(row.articleId)

        fprint0 = getFingerprint0(row)
        addFprint(map0, fprint0, articleId)

        fprint1 = getFingerprint1(row)
        addFprint(map1, fprint1, articleId)

        fprint2 = getFingerprint2(row)
        addFprint(map2, fprint2, articleId)

        artIds[articleId] = (row.externalId, row.doi, int(row.pmid))
        count += 1
    return artIds, map0, map1, map2, noIssn, noIssuePage

def writeDicts(mapList, outFname, articleIds):
    """ mapList is a list of dictionaries fingerprint -> id and articleIds is a
    dict id -> pmid. Write/Append a table fingerprint -> pmid to outFname and return offset
    where we started to write. """
    if isfile(outFname):
        logging.info("Appending to %s" % outFname)
        ofh = gzip.open(outFname, "a")
        offset = ofh.tell()
    else:
        logging.info("Creating new file %s" % outFname)
        ofh = gzip.open(outFname, "w")
        ofh.write("#fingerprint\tpmid\n")
        offset = 0

    fprintType = 0
    typeDesc = {0:"doi", 1:"issn|vol|issue|page", 2:"author|year|titlewords"}
    for fprints in mapList:
        
        logging.info("Writing %d fingerprints, type %s" % (len(fprints), typeDesc[fprintType]))
        pm = maxCommon.ProgressMeter(len(fprints))
        for fprint, artId in fprints.iteritems():
            artData = articleIds[int(artId)]
            pmid = str(artData[-1])
            ofh.write("%s\t%s\n" % (fprint, pmid))
            pm.taskCompleted()
        fprintType+=1
    logging.info("Wrote %s" % outFname)
    ofh.close()
    return offset

def addDictsToDbms(mapList, dbmList, articleIds):
    " write fingerprints to tab sep file as fingerprint -> pmid "
    assert(len(mapList)==len(dbmList))
    fprintType = 0
    for fprints, dbm in zip(mapList, dbmList):
        logging.info("Writing fingerprint %d (0=doi, 1=issn/vol/page, 2=author/year/title)" % fprintType)
        pm = maxCommon.ProgressMeter(len(fprints))
        for fprint, artId in fprints.iteritems():
            artData = articleIds[int(artId)]
            pmid = str(artData[-1])
            dbm[str(fprint)] = pmid
            pm.taskCompleted()
        fprintType+=1

def openDbms(outDir, mode):
    #fnames = []
    #fnames.append( join(outDir, "doi2pmid.gdbm"))
    #fnames.append( join(outDir, "issnVolPage2pmid.gdbm"))
    #fnames.append( join(outDir, "authorTitle2pmid.gdbm"))
    #logging.info("Opening DBM files: %s" % str(fnames))
    #if not isfile(fnames[0]):
        #logging.error("File not found: %s, PMID LOOKUP DEACTIVATED" % fnames[0])
        #return None

    #dbms = []
    #for fname in fnames:
        #dbms.append(gdbm.open(fname, mode))
    fname = join(outDir, "fingerprints.tab.gz")
    db = pubGeneric.getKeyValDb(fname)
    return db

def closeDbms(dbms):
    logging.info("Closing DBM files")
    for d in dbms:
        d.close()

def writeAsMarshal(map0, map1, map2, artIds, mapStoreFname):
    """ marshal three dicts into a file. If the file exists,
      merge them into the existing dicts first.
    """
    if isfile(mapStoreFname):
        logging.info("Found %s, merging new fingerprints into old ones" % mapStoreFname)
        logging.info("Loading %s" % mapStoreFname)
        oldTuples = marshal.load(open(mapStoreFname))
        # support the old files just in case
        if len(oldTuples)==4:
            oldmap0, oldmap1, oldmap2, oldArtIds = oldTuples
        else:
           oldmap1, oldmap2, oldArtIds = oldTuples
           oldmap0 = {}

        logging.info("Merging")
        oldmap0.update(map0)
        oldmap1.update(map1)
        oldmap2.update(map2)
        oldArtIds.update(artIds)
        map0, map1, map2 = oldmap0, oldmap1, oldmap2
        artIds = oldArtIds

    logging.info("Writing fingerprints to %s" % mapStoreFname)
    ofh = open(mapStoreFname, "w")
    data = (map0, map1, map2, artIds)
    marshal.dump(data, ofh)
    ofh.close()

def saveMergeFingerprints(artIds, map0, map1, map2, outDir):
    " merge all fingerprints into existing out files in outDir "
    # update the dbm files
    #db = openDbms(outDir, "wfu")
    outFname = join(outDir, "fingerprints.tab.gz")
    #addDictsToDbms([map0, map1, map2], dbms, artIds)
    offset = writeDicts([map0, map1, map2], outFname, artIds)
    #closeDbms(dbms)
    pubGeneric.indexKvFile(outFname, offset)

    # update the marshal file
    mapStoreFname = join(outDir, "fingerprints.marshal")
    writeAsMarshal(map0, map1, map2, artIds, mapStoreFname)

def createWriteFingerprints(textDir, updateIds=[]):
    " create fingerprints for all articles in textDir with given updateIds and save to this directory "
    artIds, map0, map1, map2, noIssn, noIssuePage =  \
            createFingerprints(textDir, updateIds=updateIds)
    logging.info("Processed %d articles" % len(artIds))
    logging.info("No Issn = %d, no issue or page = %d" % (noIssn, noIssuePage))
    saveMergeFingerprints(artIds, map0, map1, map2, textDir)

def lookupArtIds(articleData, map0, map1, map2, artIds, noPrints, noMatches):
    """ 
    Resolve articleData to articleIds by mapping through fingerprints in the map0-2 dicts.

    if artIds is not None:
        Return a tuple with fingerprintUsed, artId, extId, doi, pmid 
    if artIds is None:
        Return just the value in the maps (PMID)

    Will try the three fingerprints against map0, map1, map2 to find matches for articleData.
    None if not found.

    Appends to noPrints and noMatches with the articles where no fingerprint was found or
    no match was found, for debugging.
    """
    # first try a lookup with just the DOI
    logging.debug("Trying DOI lookup")
    fprint0   = getFingerprint0(articleData)
    matchData = lookupFprint(fprint0, map0, artIds)
    if matchData!=None:
        return matchData

    # then try a lookup with fingerprint1
    logging.debug("Trying issn/vol/page")
    fprint1 = getFingerprint1(articleData)
    matchData = lookupFprint(fprint1, map1, artIds)
    if matchData!=None:
        return matchData

    # retry fingerprint1 with eIssn instead of printIssn
    logging.debug("Trying eissn/vol/page")
    fprint1b = getFingerprint1(articleData, withEIssn=True)
    matchData = lookupFprint(fprint1b, map1, artIds)
    if matchData!=None:
        return matchData

    # if still no match (strange page numbers etc), try fingerprint2 
    logging.debug("Trying authors,title,year")
    fprint2 = getFingerprint2(articleData)
    if fprint1==None and fprint1b==None and fprint2==None:
        logging.debug("All fingerprints failed: %s" % str(articleData))
        noPrints.append(articleData)
        return None

    matchData = lookupFprint(fprint2, map2, artIds)
    if matchData==None:
            noMatches.append(articleData)
            logging.log(5, "No match at all: %s" % str(articleData))
            logging.debug("fingerprints: %s, %s, %s, %s" % (fprint0, fprint1, fprint1b, fprint2))

    return matchData

class PmidFinder:
    def __init__(self):
        textDir = pubConf.resolveTextDir("medline")
        fname = join(textDir, "fingerprints.tab.gz")
        self.db = pubGeneric.getKeyValDb(fname)
        self.noPrints = []
        self.noMatches = []
        
    def lookupPmid(self, articleDict):
        """ lookup the pmid on-disk using fingerprints in dbm files 
        """
        if self.db==None:
            logging.debug("no db, not returning any pmid")
            return ""
        artTuple = pubStore.articleDictToTuple(articleDict)
        pmid = lookupArtIds(artTuple, self.db, self.db, self.db, None, \
            self.noPrints, self.noMatches)
        if pmid==None:
            pmid = ""
        return pmid

    def __exit__(self):
        self.db.close()

    def close(self):
        self.db.close()

    
