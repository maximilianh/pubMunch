#!/usr/bin/env python

# first load the standard libraries from python
# we require at least python 2.5
#from sys import *
import sys

# load default python packages
import logging, optparse, os, collections, tarfile, mimetypes
from os.path import *

# add <scriptDir>/lib/ to package search path
progFile = os.path.abspath(sys.argv[0])
progDir  = os.path.dirname(progFile)
pubToolsLibDir = os.path.join(progDir, "lib")
sys.path.insert(0, pubToolsLibDir)

# now load our own libraries
import pubGeneric, pubStore, pubConf, maxCommon, pubPubmed
from pubXml import *

# === COMMAND LINE INTERFACE, OPTIONS AND HELP ===
parser = optparse.OptionParser("""usage: %prog [options] <inFile> <outDir> - convert Yale Image Finder Dump to pubtools format

Example:
    %prog /hive/data/outside/pubs/yif/ocrtext yif
""")

parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages") 
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="show more debug messages") 
#parser.add_option("", "--minId", dest="minId", action="store", help="minimum numerical ID, default %s", default=pubConf.identifierStart["yif"])
(options, args) = parser.parse_args()

# ==== FUNCTIONs =====

def createIndex(inDir, outDir, minId):
    " get all PMIDs from dir and create index file in outDir "
    files =  os.listdir(inDir)
    logging.info("Reading input dir %s" % inDir)

    # create dict pmid -> set of filenames
    idFiles = {}
    for fname in files:
        fileId = basename(fname).split(".")[0]
        idFiles.setdefault(fileId, set()).add(fname)
    logging.info("Found %d files with %d article identifiers" % (len(files), len(idFiles)))

    indexFname = join(outDir, "index.tab")
    indexFile = open(indexFname, "w")
    logging.info("Writing index file %s" % indexFname)

    # write index file
    headers = ["chunkId", "articleId", "externalId", "mainFile", "suppFiles"]
    indexFile.write("\t".join(headers)+"\n")
    articleId = minId
    for extId, files in idFiles.iteritems():
        chunkId = "00000"
        mainFile = extId+".pdf"
        files.remove(mainFile)
        row = [chunkId, str(articleId), extId, mainFile, ",".join(files)]
        indexFile.write("\t".join(row)+"\n")
        articleId += 1
    indexFile.close()
    return indexFname

def convertFiles(inDir, outDir, minId):
    " parse yif format and write to outDir "
    chunkCount = 0
    yifFname = inDir
    chunkSize = 3000

    con, cur = pubStore.openArticleDb("pmc")

    # sort yif by pmc id
    sortFname = join(dirname(yifFname), "ocrtext.sorted")
    logging.info("Sorting input file")
    cmd = "sort -t/ -k1 %s > %s" % (yifFname, sortFname)
    ret = os.system(cmd)
    assert(ret==0)

    writer = None
    lastPmcId = None
    artData = None
    artCount = 0
    noInfo = 0
    donePmcIds = set()
    pm = maxCommon.ProgressMeter(os.path.getsize(sortFname))

    for line in open(sortFname):
        # parse the rather weird yif format
        pm.taskCompleted(len(line))
        line = line.decode("utf8")
        fields = line.split(" ")
        idField = fields[0]
        content  = " ".join(fields[1:])
        pmcId, figId = idField.split("/")
        pmcId = pmcId.replace("PMC", "")

        chunkId = "0_%05d" % chunkCount

        if artData==None and pmcId==lastPmcId:
            continue

        # get and write article data if needed
        if pmcId != lastPmcId or lastPmcId==None and pmcId not in donePmcIds:
            artData = pubStore.lookupArticle(con, cur, "pmcId", pmcId)

            # skip articles for which we have no data here
            if artData==None:
                logging.warn("No info locally for id %s" % pmcId)
                noInfo+=1
                continue

            artData["source"] = "yif"
            # if we have written enough articles, open a new file
            if writer==None or artCount % chunkSize == 0:
                if writer!=None:
                    writer.close()
                chunkCount += 1
                chunkId = "0_%05d" % chunkCount
                print "making new writer"
                writer = pubStore.PubWriterFile(join(outDir, chunkId))
                
            articleId = int(artData["articleId"]) - pubConf.identifierStart["pmc"] + minId
            #print "writing %s" % artData
            #articleId = int(artData["articleId"])
            writer.writeArticle(articleId, artData)
            donePmcIds.add(pmcId)
            fileCount = 0
            artCount += 1
            lastPmcId = pmcId

        # create a new file row and write
        fileData = pubStore.createEmptyFileDict()
        fileData["desc"] = "Figure %d" % (fileCount+1)
        fileData["externalId"] = "PMC"+pmcId
        fileData["fileType"] = "fig"
        #url = "http://www.ncbi.nlm.nih.gov/core/lw/2.0/html/tileshop_pmc/tileshop_pmc_inline.html?title=UCSCGenomeBrowserRedirect&p=PMC3&id=%s.jpg" % figId
        #url = "PMCFIG://%s" % figId
        url = "http://krauthammerlab.med.yale.edu/imagefinder/ImageDownloadService.svc?articleid=%s&file=%s&size=LARGE" % (pmcId, figId)
        fileData["url"] = url
        fileData["mimeType"] = "image/jpeg"
        fileData["content"] = content
        fileData = pubStore.dictToUtf8Escape(fileData)

        # we add 500 to fileId to avoid overlaps
        fileId   = ((10**pubConf.FILEDIGITS)*int(articleId))+fileCount+500
        writer.writeFile(articleId, fileId, fileData, externalId=pmcId)
        fileCount += 1

    print "Articles processed: %d" % len(donePmcIds)
    print "Articles not found: %d" % noInfo
    writer.close()
    
def main(args, options):
    inDir, outDataset = args
    maxCommon.mustExist(inDir)
    #minId = options.minId
    minId = pubConf.identifierStart["yif"]

    pubGeneric.setupLogging(progFile, options)

    outDir = pubConf.resolveTextDir(outDataset)
    maxCommon.mustExistDir(outDir)
    maxCommon.mustBeEmptyDir(outDir)
    convertFiles(inDir, outDir, minId)
# ----------- MAIN --------------
if args==[]:
    parser.print_help()
    exit(1)

main(args, options)
