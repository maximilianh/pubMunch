# functions to update the crawling PMIDs given a parsed medline update

from collections import defaultdict
import os, logging, random, shutil
import maxCommon, pubStore, unidecode, pubConf, pubCrawlLib
from os.path import *

def getIssnPmidDict(medlineDir, updateIds, minYear):
    """ go over medline articles and collect a printIssn -> pmidList dictionary 
    return to dicts: issn -> set of pmids, issn -> journal name
    
    """
    issnToPmid = defaultdict(set)
    issnToJournal = {}
    pmidCount = 0
    noIssnPmidCount = 0
    noMinYearCount = 0
    issnToJournal = {}
    logging.info("Reading ISSN/PMID assignment from directory %s" % medlineDir)
    for artData in pubStore.iterArticleDataDir(medlineDir, updateIds=updateIds):
        issn = artData.printIssn
        if issn=="":
            issn = artData.eIssn
        if issn=="":
            #oIssnCount.add(artData.pmid)
            logging.debug("PMID %s has not Issn" % artData.pmid)
            noIssnPmidCount += 1
            continue
        if minYear!=None and artData.year.isdigit() and not int(artData.year) >= minYear:
            logging.debug("PMID %s is too early" % artData.pmid)
            noMinYearCount += 1
            continue
                
        issnToPmid[issn].add(int(artData.pmid))
        issnToJournal[issn] = unidecode.unidecode(artData.journal)
        #pmids.add(artData.pmid)
        pmidCount += 1
    logging.info("Got %d PMIDs for %d ISSNs" % (pmidCount, len(issnToPmid)))
    logging.info("No info for %d PMIDs, %d PMIDs did not fulfill the minYear" % \
        (noIssnPmidCount, noMinYearCount))
    return issnToPmid, issnToJournal

def writeIssnPmids(issnToPmid, issnToJournal, outFname):
    " only for debugging: write pmid -> issn assignment to file "
    logging.info("Writing ISSN -> PMID assignment to %s" % outFname)

    ofh = open(outFname, "w")
    ofh.write("#issn\tjournal\tpmids\n")
    for issn, pmidList in issnToPmid.iteritems():
        journal = issnToJournal[issn]
        pmidList = [str(x) for x in pmidList]
        ofh.write("%s\t%s\t%s\n" % (issn, journal, ",".join(pmidList)))
    ofh.close()

def getSubdirs(dir):
   " get all direct subdirs of a dir at depth=1"
   return [name for name in os.listdir(dir) if isdir(join(dir, name))]

def getEIssnToPIssn(journalFname):
    """ return a dict that maps from eIssn to pIssn """
    logging.info("Parsing %s to get eIssn -> pIssn mapping" % journalFname)
    ret = {}
    for row in maxCommon.iterTsvRows(journalFname):
        eStr = row.journalEIssns
        pStr = row.journalIssns
        if eStr=="" or pStr=="":
            continue
        eIssns = eStr.split("|")
        pIssns = pStr.split("|")
        assert(len(eIssns)==len(pIssns))
        for eIs, pIs in zip(eIssns, pIssns):
            if eIs!="" and pIs!="":
                ret[eIs] = pIs
    return ret
        
def getPmidsForIssn(con, cur, issn, fieldName):
    " retrieve PMIDs for an ISSN, use fieldName as the ISSN field  "
    pmids = []
    query = "SELECT pmid FROM articles WHERE %s=?" % fieldName
    for row in cur.execute(query, (issn,)):
        pmids.append(row[0])
    return pmids

def updatePmids(medlineDir, crawlDir, updateIds, minYear=None):
    """ go over subdirs of crawlDir, for each: read the ISSNs, and add new
    PMIDs we have in medlineDir to subdir/pmids.txt

    We never remove a PMID from pmids.txt.
    """ 
    logging.info("Now updating crawler directories with the new PMIDs")
    eIssnToPIssn = getEIssnToPIssn(pubConf.publisherIssnTable)
    con, cur = pubStore.openArticleDb("medline", mustOpen=True)
    #issnToPmid, issnToJournal = getIssnPmidDict(medlineDir, updateIds, minYear)
    for subdir in getSubdirs(crawlDir):
        subPath = join(crawlDir, subdir)
        logging.debug("Processing subdirectory %s" % subPath)
        if pubCrawlLib.containsLockFile(subPath):
            logging.warn("Ongoing crawling in %s, skipping" % subPath)
            continue

        pmidFname = join(crawlDir, subdir, "pmids.txt")
        issnFname = join(crawlDir, subdir, "issns.tab")
        if not isfile(issnFname) or not isfile(pmidFname):
            logging.info("Skipping %s, ISSN or docId file not found" % subPath)
            continue
        logging.debug("reading subdir %s: %s and %s" % (subdir, pmidFname, issnFname))
        issns = [row.issn.strip() for row in maxCommon.iterTsvRows(issnFname)]
        logging.debug("ISSNs: %s" % ",".join(issns))
        # read old pmids
        oldPmids = set([int(line.rstrip()) for line in open(pmidFname)])
        newPmids = set()
        # add new pmids, for each issn
        for issn in issns:
            logging.debug("Processing ISSN %s" % issn)

            issnPmids = getPmidsForIssn(con, cur, issn, "printIssn")
            if len(issnPmids)==0:
                issnPmids = getPmidsForIssn(con, cur, issn, "eIssn")

            if len(issnPmids)==0:
                logging.warn("No Pmids for ISSN %s" % issn)
                continue

            logging.debug("%d PMIDs" % (issn, len(issnPmids)))
            newPmids.update(issnPmids)
        # get some counts and output to user
        logging.info("Subdirectory %s: %d ISSNs, %d old PMIDs, %d new PMIDs" % \
            (subPath, len(issns), len(oldPmids), len(newPmids)))
        oldCount = len(oldPmids)
        updateCount = len(newPmids)
        oldPmids.update(newPmids) # faster to add new to old set than old to new set
        pmids = oldPmids
        newCount = len(pmids)
        addCount = newCount - oldCount
        logging.info("crawl dir %s: old PMID count %d, update has %d, new total %d, added %d" % \
            (subdir, oldCount, updateCount, newCount, addCount))

        # write new pmids
        pmids = [str(x) for x in pmids]
        # randomize order, to distribute errors
        random.shuffle(pmids)

        # write all pmids to a tmp file
        pmidTmpFname = pmidFname+".new"
        pmidFh = open(pmidTmpFname, "w")
        pmidFh.write("\n".join(pmids))
        pmidFh.close()

        # keep a copy of the original pmid file
        shutil.copy(pmidFname, pmidFname+".bak")
        # atomic rename  the tmp file to the original file
        # to make sure that an intact pmid file always exists
        os.rename(pmidTmpFname, pmidFname)
        
    #outFname = join(outDir, "issnToPmid.tab")
    #writeIssnPmids(issnToPmid, issnToJournal, outFname)
    #sys.exit(0)

