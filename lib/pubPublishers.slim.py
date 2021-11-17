#!/usr/bin/env python

# this script collects the data about publishers from medline and from what we have on disk
# it's terrible but it's only used here at UCSC

# TODO: add index by pISSN to "here" 

from __future__ import print_function
from os.path import *
import os, sys, optparse, logging, marshal, zlib, unicodedata, gc, cPickle, shutil, random, urllib2
import glob, json, operator
progFile = os.path.abspath(sys.argv[0])
progDir  = os.path.dirname(progFile)
pubToolsLibDir = os.path.join(progDir, "..", "lib")
sys.path.insert(0, pubToolsLibDir)
import maxCommon, collections, pubGeneric, pubConf, pubResolvePublishers
import sqlite3 as s

from collections import defaultdict

# === FUNCTIONS =====
def parseUidToCounts(fname):
    res = {}
    for row in maxCommon.iterTsvRows(fname):
        total = int(row.total)
        geneProtCount = int(row.geneProtCount)

        res[row.uid] = (total, geneProtCount)
    logging.info('Found "gene/protein"-counts for %d journals in %s' % (len(res), fname))
    return res

def getTargetJournals(journalFname):
    " get english journals with eIssn "
    logging.info("Parsing %s" % journalFname)
    data = {}
    #issnToUid = {}
    for row in maxCommon.iterTsvRows(journalFname):
        if not row.source.startswith("NLM") or row.uniqueId=="":
            continue
        if row.language=="eng" and row.eIssn!="":
            #data.add(row.uniqueId)
            data[row.uniqueId] = row
        #if row.uniqueId!="":
            #issnToUid[row.pIssn] = row.uniqueId
            #issnToUid[row.eIssn] = row.uniqueId
    logging.info("In NLM Catalog, found %d journals with eIssn , english and with UID" % len(data))
    #return data, issnToUid
    return data
    
def parsePermissions(LICENSETABLE):
    " return dict with publisher name lower cased -> permission color (green or red) "
    pubToPermission = {}
    for row in maxCommon.iterTsvRows(LICENSETABLE):
        pubName = row.pubName.lower()
        if int(row.havePermission)==1:
            pubToPermission[pubName] = "green"
        else:
            pubToPermission[pubName] = "red"

    for row in maxCommon.iterTsvRows(OATABLE):
            pubToPermission[row.pubName.lower()] = "blue"
            #print pubName.lower()
    return pubToPermission
        
def parseMembers(outFname):
    " download and convert the crossref member table "
    for i in range(0, 8000, 1000):
        outTmp = "/tmp/crossrefMembers"+str(i)+".json"
        if isfile(outTmp):
            continue
        ofh = open(outTmp, "w")
        url = "http://api.crossref.org/members?rows=1000&offset="+str(i)
        logging.info("Downloading %s to %s" % (url, outTmp))
        ofh.write(urllib2.urlopen(url).read())

    headers = ["primaryName", "totalCount", "backfileCount", "currentCount", "prefixes", "prefixInfo","altNames", "location"]

    rows = []
    for fname in glob.glob("/tmp/crossrefMembers*.json"):
        logging.info("Parsing %s" % fname)
        if os.path.getsize(fname)==0:
            continue
        d = json.load(open(fname))

        for member in d["message"]["items"]:
            row = []
            row.append(member["primary-name"])
            row.append(member["counts"]["total-dois"])
            row.append(member["counts"]["backfile-dois"])
            row.append(member["counts"]["current-dois"])
            row.append("|".join(member["prefixes"]))

            prefixInfos = []
            if member["prefix"] is not None:
                for prefix in member["prefix"]:
                    prefixInfos.append("%s=%s" % (prefix["value"], prefix["name"]))
            row.append("|".join(prefixInfos))

            row.append("|".join(member["names"]))
            row.append(member["location"])
                
            rows.append(row)

    rows.sort(key=operator.itemgetter(1), reverse=True)

    ofh = open(outFname, "w")
    ofh.write( "\t".join(headers))
    ofh.write( "\n")
    for row in rows:
            row = [unicode(x).encode("utf8") for x in row]
            ofh.write("\t".join(row))
            ofh.write("\n")
    logging.info("Wrote %s" % ofh.name)
    ofh.close()

# === COMMAND LINE ====
parser = optparse.OptionParser("""usage: %prog [options] <step> - guess ISSNs for each publisher, count relevant articles per ISSN per year and create overview tables of publishers, their journals, and % of articles on disk

steps are:
"journals - create a table publisher -> journals by guessing the "real" publisher for all
              journals in the NLM catalog and other lists (Highwire, Wiley)
              that we got via email from publishers. Write these to pubConf.publisherIssnTable
              This step is essential for pubPrepCrawl
"crossref" - parse crossref member table to tab-sep file

These steps are only needed at UCSC to compared the list of PMIDs we got with the list of PMIDs 
we expect:
"articles" - create a table with journalUid -> articleCount from medline, to pubCounts.tab
"here" - determine which documents of publishers we have here, in the form of pmids.txt

"pubs" - create a table with publishers, their post-2000 article counts and how many we have on disk
- Starts from list of journals. Uses the journalId -> pmid list from the "articles" step.
- filters list of publishers to english/eIssn/more than x articles/more than x% of articles with "gene"
- retrieves permission info from two tables with license information

""")

parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages") 
parser.add_option("-p", "--pmidFile", dest="pmidFile", action="store", help="instead of reporting as the base number the number the number of PMIDs in medline, use the PMIDs from a file. Adds a colum 'filterPercent' to publisher table with total number relative to all pmids in filter file") 
#parser.add_option("", "--parse", dest="parse", action="store_true", help="for debugging, just parse one single xml file", default=None) 
(options, args) = parser.parse_args()
pubGeneric.setupLogging(__file__, options)

if len(args)==0:
    parser.print_help()
    sys.exit(1)

steps = args

publisherFname = pubConf.publisherIssnTable
journalFname = pubConf.journalTable

# # dir with permission info
# licDir = "/cluster/home/max/public_html/mining/"
# # table with pubName and havePermission field for each publisher
# LICENSETABLE = licDir+"licenseTable.tab"
# # table with list of pubNames that are open access
# OATABLE = licDir+"oaPublishers.tab"

# MEDLINEDIR = pubConf.resolveTextDir("medline")

# # MEDLINE: table with journal UID -> count of articles in Medline
# COUNTFNAME = join(pubConf.inventoryDir, "mlJournalCounts.tab")

# # MEDLINE: sqlite db with a table journal uid -> pmids
# PMIDFNAME = join(pubConf.inventoryDir,"mlJournalPmids.db")

# outDir = pubConf.inventoryDir
# if options.pmidFile:
#     outDir = "./pubPublishersOut"
#     if not isdir(outDir):
#         logging.info("Creating %s" % outDir)
#         os.makedirs(outDir)

# # datasets: just the count of all articles in all datasets
# articleCountFname = join(outDir, "articleCount.txt")
# # datasets: count of all articles, by eIssn and articleType
# issnCountFname = join(outDir, "issnCounts.marshal")

# # table with information on journals, coverage, eISSN, etc.
# journalCoverageFname = join(outDir, "journalCoverage.tab")

# # table with info on publishers, coverage, etc
# finalCountFname = join(outDir, "pubCounts.tab")

# # datasets to collect PMIDs for
# datasets = "elsevier,crawler,pmc,springer"

# # conditions when collecting PMIDs
# minYear = 2000

# # conditions on journals
# minCount = 50 # minimum number of articles in any journal
# minGeneProtCount = 50 # minimum number of articles with gene or protein in the abstract
# minGeneProtRatio = 0.01 # minimum number of articles of a journal that mention "gene" or "protein"

# # a publisher has to fullfill certain conditions to be taken into consideration
# minPubCount = 1000 # minimum number of articles per publisher
# minPubGeneCount = 200 # minimum number of articles with gene/prot in abstract per publisher
# minPubGeneProtRatio = 0.01 # minimum number of articles of a publisher that mention "gene" or "protein"

# # file with all pmids we have here on disk
# herePmidFname = join(pubConf.inventoryDir, "herePmids.txt")

if len(steps)==0:
    print("You need to specify a step to run")
    sys.exit(0)

# create list with publisher -> journals
if "journals" in steps:
    inDir = pubConf.journalListDir
    pubResolvePublishers.initJournalDir(inDir, None, journalFname, publisherFname)

# elif "crossref" in steps:
#     outFname = join(pubConf.journalInfoDir, "crossrefMembers.tab")
#     parseMembers(outFname)

# # process medline:
# # make table with journalId -> number of articles
# # and db with uid -> list of pmids
# elif "articles" in steps:
#     if not isdir(pubConf.inventoryDir):
#         logging.info("Creating %s" % pubConf.inventoryDir)
#         os.makedirs(pubConf.inventoryDir)

#     #cmd = "mv %s pubTable/counts.tab.old; mv %s pubTable/pmids.db.old; mkdir -p pubTable" % (COUNTFNAME, PMIDFNAME)
#     #os.system(cmd)

#     counts = {}
#     names = {}
#     pmids = defaultdict(list)
#     issnCounts = defaultdict(int)
#     count = 0
#     noYear = 0
#     noAuthor = 0
#     noAbstract = 0
#     recCount = 0
#     for row in maxCommon.iterTsvDir(MEDLINEDIR, ext="articles.gz"):
#         counts.setdefault(row.journalUniqueId, collections.defaultdict(int))
#         recCount += 1

#         if row.year=='' or int(row.year)<minYear:
#             noYear +=1
#             continue
#         if row.authors=='':
#             noAuthor +=1
#             continue
#         if len(row.abstract)<= 40:
#             noAbstract +=1
#             continue

#         counts[row.journalUniqueId]["total"] += 1
#         names[row.journalUniqueId] = row.journal
#         pmids[row.journalUniqueId].append(int(row.pmid))
#         if row.eIssn!="":
#             issnCounts[row.eIssn] +=1
#         abs = row.abstract.lower()
#         if " gene " in abs or " protein " in abs:
#             counts[row.journalUniqueId]["geneProt"] += 1
#         count += 1

#     logging.info("Total number of records was %d" % (recCount))
#     logging.info("Ignored: No year %d, no author %d, no abstract %d" % (noYear, noAuthor, noAbstract))
#     logging.info("Read %d pubmed records from %d journals" % (count, len(pmids)))
#     logging.info("Writing PMIDs to sqlite DB")

#     # writing a table with uniqueId -> PMIDs to sqlite database
#     con = s.connect(PMIDFNAME+".new", isolation_level=None)
#     cur = con.cursor()
#     cur.execute("PRAGMA synchronous=OFF") # recommended by
#     cur.execute("PRAGMA count_changes=OFF") # http://blog.quibb.org/2010/08/fast-bulk-inserts-into-sql
#     cur.execute("PRAGMA cache_size=800000") # http://web.utk.edu/~jplyon/sqlite/SQLite_optimization_FA
#     cur.execute("PRAGMA journal_mode=OFF") # http://www.sqlite.org/pragma.html#pragma_journal_mode
#     cur.execute("PRAGMA temp_store=memory") 
#     con.commit()

#     cur.execute("create table pmids (uniqueId text, pmids blob);")
#     for uniqueId, uidPmids in pmids.iteritems():
#         pmidStr = ",".join([str(x) for x in uidPmids])
#         pmidStr = buffer(zlib.compress(pmidStr))
#         row = (uniqueId, pmidStr)
#         cur.execute("INSERT INTO pmids Values (?, ?)", row)
#     con.commit()
#     cur.execute("CREATE INDEX uidIdx ON pmids(uniqueId);")
#     con.commit()

#     # writing table with uid -> counts to tab sep file
#     logging.info("Writing journal PMID counts from medline")
#     ofh = open(COUNTFNAME+".new", "w")
#     ofh.write("uid\tname\ttotal\tgeneProtCount\n")
#     for uniqueId, dataDict in counts.iteritems():
#         if uniqueId not in names:
#             # journal has no article with year > 1990
#             continue
#         name = names[uniqueId]
#         row = [uniqueId, name, str(dataDict["total"]), str(dataDict["geneProt"])]
#         line = "\t".join(row)+"\n"
#         line = line.encode("utf8")
#         ofh.write(line)

#     shutil.move(COUNTFNAME+".new", COUNTFNAME)
#     shutil.move(PMIDFNAME+".new", PMIDFNAME)

# elif "here" in steps:
#     dataDirs = pubConf.resolveTextDirs(datasets)
#     pmids = []
#     articleCount = 0
#     issnCounts = {}

#     for dataDir in dataDirs:
#         logging.info("Reading PMIDs from %s, by ISSN" % dataDir)
#         for row in maxCommon.iterTsvDir(dataDir, ext=".articles.gz"):
#             pmids.append(row.pmid)
#             articleCount +=1
#             if row.printIssn!="":
#                 issnCounts.setdefault(row.printIssn, {})
#                 issnCounts[row.printIssn].setdefault(row.articleType, 0)
#                 issnCounts[row.printIssn][row.articleType] += 1
#                 if row.pmid=="":
#                     issnCounts[row.printIssn].setdefault("noPmidUrls", []).append(row.fulltextUrl)
#                 else:
#                     issnCounts[row.printIssn].setdefault("herePmids", []).append(row.pmid)
#             # DEBUG
#             #if articleCount ==10000:
#                 #break
#         #if articleCount ==10000:
#             #break

#     # keep only 10 random urls / PMIDs
#     for issn, counts in issnCounts.iteritems():
#         if "noPmidUrls" not in counts:
#             counts["noPmidUrls"] = []
#         else:
#             urls = counts["noPmidUrls"]
#             random.shuffle(urls)
#             counts["noPmidUrls"] = urls[:10]

#         if "herePmids" not in counts:
#             counts["herePmids"] = []
#         else:
#             issnPmids = counts["herePmids"]
#             random.shuffle(issnPmids)
#             counts["herePmids"] = issnPmids[:10]

#     pmids = set(pmids)
#     ofh = open(herePmidFname+".new", "w")
#     for pmid in pmids:
#         ofh.write("%s\n" % pmid)

#     ofh = open(articleCountFname+".new", "w")
#     ofh.write("%d" % articleCount)
#     ofh.close()

#     #cPickle.dump(eIssnCounts, issnCountFname)
#     marshal.dump(issnCounts, open(issnCountFname, "w"))

#     shutil.move(herePmidFname+".new", herePmidFname)
#     shutil.move(articleCountFname+".new", articleCountFname)
#     logging.info("Created %s and %s and %s" % (herePmidFname, articleCountFname, issnCountFname))

# # get english journals with more than x gene/protein abstracts

# elif "pubs" in steps:
#     # create table with number of post-minYear articles per publisher
#     # and only for NLM journals that are english and have eIssn

#     journalCounts = parseUidToCounts(COUNTFNAME)
#     #targetIds, issnToUid = getTargetJournals(journalFname)
#     targetIds = getTargetJournals(journalFname)
#     pubToPermissionColor = parsePermissions(LICENSETABLE)

#     # reduce to PMIDs in filter file
#     filterPmids = None
#     if options.pmidFile:
#         filterPmids = set([int(x.strip()) for x in open(options.pmidFile).readlines()])
#         logging.info("Restricting PMIDs to the ones in %s: found %d PMIDs" % (options.pmidFile, len(filterPmids)))

#     totalArtCount = 0
#     filtArtCount = 0

#     logging.info("Parsing PMIDs we have here from %s" % herePmidFname)
#     herePmids = set([int(x.strip()) for x in open(herePmidFname).readlines() if len(x)>3])

#     con = s.connect(PMIDFNAME)
#     cur = con.cursor()
#     # open journal info file
#     jfh = open(journalCoverageFname+".new", "w")
#     headers = ["pubName", "relevant", "journal", "publisher", "uid", "pIssn", "eIssn", "language", "country", "pmidCount", "hereCount", "notHerePmids"]
#     jfh.write("\t".join(headers)+"\n")

#     logging.info("iterating over publishers, counting how many articles in medline they have")
#     removedUids = []
#     removedPublishers = []
#     noUidIssns = []
#     outRows = []
#     allPmids = []
#     # PMIDs that we have permission for (blue=OA, green=OK, red=no permission)
#     greenBluePmidCount = 0
#     # total number of PMIDs we have here
#     totalHereCount = 0
#     for row in maxCommon.iterTsvRows(publisherFname):
#         pubName = row.pubName
#         #if not row.pubName.startswith("NLM"):
#             #continue
#         if row.pubName.startswith("NLM"):
#             languages = set(row.languages.split("|"))
#             if "eng" not in languages:
#                 logging.debug("%s: No single english journal for this publisher" % row.pubName)
#                 removedPublishers.append(pubName)
#                 continue

#         #if row.uid=="":
#             # if this is not NLM data, need to lookup NLM UID list first
#             #uids = set()
#             #for issn in row.journalIssns.split("|"):
#                 #issn = issn.strip()
#                 #if issn in issnToUid:
#                     #uids.add(issnToUid[issn])
#                 #else:
#                     #noUidIssns.append(issn)
#         #else:
#         uids = set(row.uid.split("|"))
#         pubCount = 0
#         pubGeneProtCount = 0
#         filteredJournalCount = 0
#         pubUids = []

#         pubPmids = []


#         #sanePub = unicodedata.normalize('NFKD', pubName).encode('ascii','ignore').replace(" ", "_").replace("NLM_","").replace("/","-")
#         #jfh = open("pubTable/journals/"+sanePub, "w")
#         #logging.info("%s" % jfh.name)

#         noPmidCount = 0
#         for uid in uids:
#             relevant = True
#             if uid not in journalCounts:
#                 logging.debug("No pmids for uid %s (no eIssn or not english)" % uid)
#                 relevant = False
#                 noPmidCount +=1
#                 continue
#             if uid not in targetIds:
#                 logging.debug("Uid %s is not english/has no eIssn" % uid)
#                 removedUids.append(uid)
#                 relevant = False
#                 continue

#             logging.debug("UID %s" % uid)
#             jTotal, jGeneProt = journalCounts[uid]
#             pubUids.append(uid)
#             if relevant:
#                 if float(jGeneProt)/float(jTotal) > minGeneProtRatio and \
#                     jTotal > minCount and jGeneProt > minGeneProtCount:
#                     passedFilter = True
#                     pubCount += jTotal
#                     pubGeneProtCount += jGeneProt
#                     totalArtCount += jTotal
#                 else:
#                     filteredJournalCount += 1
#                     passedFilter = False

#             # get pmids for this uid in medline
#             pmidCur = cur.execute("select pmids from pmids where uniqueId=:uid",locals())
#             pmidStrRow = pmidCur.fetchone()
#             if pmidStrRow!=None:
#                 pmidStr = pmidStrRow[0]
#                 jPmids = [int(x) for x in zlib.decompress(pmidStr).split(",")]
#                 pubPmids.extend(jPmids)
#             else:
#                 logging.warn("No pmids in medline for uid %s" % uid)

#             # write row to journal file
#             hereCount = len(herePmids.intersection(jPmids))
#             jInfo = targetIds[uid]
#             notHerePmids = list(set(jPmids).difference(herePmids))[:10]
#             notHerePmidStr = ",".join([str(x) for x in notHerePmids])

#             jRow = [pubName, str(passedFilter), jInfo.title, jInfo.publisher, uid, \
#                 jInfo.pIssn, jInfo.eIssn, jInfo.language, jInfo.country, len(jPmids), \
#                 hereCount, notHerePmidStr]
#             jRow = [unicode(x) for x in jRow]
#             jfh.write(u"\t".join(jRow).encode("utf8")+"\n")
    
#         if pubCount < minPubCount:
#             logging.debug( "Removing publisher %s : count %d too low" % (pubName, pubCount))
#             removedPublishers.append(pubName)
#             continue

#         #if pubGeneProtCount < minPubGeneCount:
#             #logging.debug( "Removing publisher %s : gene/protein count too low" % pubName)
#             #removedPublishers.append(pubName)
#             #continue

#         # count how many we have here by intersect medline's with our PMIDs
#         pubPmids = set(pubPmids)

#         # optionally filter down to some predefined set of PMIDs
#         filterPercent = ""
#         if filterPmids:
#             pubPmids = pubPmids.intersection(filterPmids)
#         allPmids.extend(pubPmids)

#         pubHerePmidCount = len(herePmids.intersection(pubPmids))
#         totalHereCount += pubHerePmidCount

#         geneProtRatio = float(pubGeneProtCount) / float(pubCount)
#         if geneProtRatio < minPubGeneProtRatio:
#             logging.debug( "Removing %s : gene/prot ratio too low: count %d, gene count %d" % (pubName, pubCount, pubGeneProtCount))
#             removedPublishers.append(pubName)
#             continue

#         pubPmidCount = len(pubPmids)
#         filtArtCount += pubPmidCount

#         geneProtRatioStr = "%02.2f" % geneProtRatio
#         uidStr= ",".join(pubUids)
#         eIssnStr = row.journalEIssns
#         if eIssnStr=="|":
#             eIssnStr = ""
#         #pubName = pubName.replace("NLM ", "")
#         permColor = pubToPermissionColor.get(pubName.replace("NLM ", "").lower(), "yellow")
#         if permColor in ["green", "blue"]:
#             greenBluePmidCount+=len(pubPmids)
#         row = [pubName, permColor, str(pubPmidCount), str(pubGeneProtCount), geneProtRatioStr, str(len(pubPmids)), str(pubHerePmidCount), uidStr, row.journalEIssns]
#         outRows.append(row)

#     # write publisher info file, adding percentages
#     ofh = open(join(pubConf.TEMPDIR, "pubCounts.tab"), "w")
#     headers = ["publisher", "permColor", "articleCount", "geneProtArticleCount", "genePercent", "medlinePmidCount", "herePmidCount", "journalUids", "journalEIssns", "filterPercent"]
#     ofh.write("\t".join(headers)+"\n")
#     for row in outRows:
#         allCount = len(set(allPmids))
#         if filterPmids:
#             filterPercent = "%2.2f" % (100*float(int(row[2]))/allCount)
#         else:
#             filterPercent = ""
#         row.append(filterPercent)
#         ofh.write(u"\t".join(row).encode("utf8")+"\n")
#     ofh.close()
#     logging.info("Total PMIDs across all publishers that passed filters: %d" % allCount)
#     logging.info("Total PMIDs across all publishers that we have here: %d" % totalHereCount)

#     logging.info("Total PMIDs across all publishers with green or blue permission color: %d" % greenBluePmidCount)
#     #logging.info("Could not resolve ISSN -> UID for %d ISSNs" % len(set(noUidIssns)))
#     logging.info("No PMID for %d UIDs (not English? no fulltext?) " % noPmidCount)
#     logging.info("min count of publications for publishers: %d" % minPubCount)
#     logging.info("min ratio of gene/protein containing articles for publishers: %f" % minPubGeneProtRatio)
#     logging.info("Removed %d publishers because of too few journals or too few genes" % len(removedPublishers))
#     logging.info("Removed %d journals because not English/no eIssn" % len(removedUids))
#     logging.info("Removed %d journals because not enough articles, not enough genes" % filteredJournalCount)
#     logging.info("Total articles post-%s: %d" % (minYear, totalArtCount))
#     logging.info("Total articles after filtering: %d" % filtArtCount)

#     cmd = "cp %s %s" % (ofh.name, finalCountFname)
#     os.system(cmd)

#     shutil.move(journalCoverageFname+".new", journalCoverageFname)
#     logging.info("Wrote results to %s and %s" % (finalCountFname, journalCoverageFname))

# else:
#     assert("No valid step-command specified")
