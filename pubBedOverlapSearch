#!/usr/bin/env python

# first load the standard libraries from python
# we require at least python 2.5
#from sys import *
import sys
if sys.version_info[0]==2 and not sys.version_info[1]>=7:
    print "Sorry, this program requires at least python 2.7"
    print "You can download a more current python version from python.org and compile it"
    print "into your homedir with 'configure --prefix ~/python'; make;"
    print "then run this program by specifying your own python executable like this: "
    print "   ~/python/bin/python ~/pubtools/pubtools"
    print "or add python/bin to your PATH before /usr/bin, then run pubtools itself"
    exit(1)

# load default python packages
import logging, optparse, os, tempfile
from os.path import *

# add <scriptDir>/lib/ to package search path
progFile = os.path.abspath(sys.argv[0])
progDir  = os.path.dirname(progFile)
pubToolsLibDir = os.path.join(progDir, "lib")
sys.path.insert(0, pubToolsLibDir)

# now load our own libraries
import pubGeneric, pubStore, pubConf, maxCommon, pubPubmed, util, tabfile, pubAlg, maxTables
from pubXml import *

# === COMMAND LINE INTERFACE, OPTIONS AND HELP ===
parser = optparse.OptionParser("""usage: %prog [options] <blatBaseDir> <db> <keywordsCommaSep> <bedfile1> <bedfile2> ... outFile - search pubBlat matches that overlap a given bed (with unique names) file for list of keywords (comma-sep), return numbers of bed features that are overlapped by features with this keyword for each input file

""")

parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages") 
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="show more debug messages") 
parser.add_option("-b", "--pubBed", dest="pubBed", action="store", help="use this bed file instead of blatBaseDir/tables/<db>.bed") 
parser.add_option("-r", "--removeBed", dest="removeBed", action="store", help="do an overlapSelect -noOverlap against this file at each step") 
parser.add_option("-o", "--outFname", dest="outFname", action="store", help="outfile file, default is stdout", default="stdout") 
parser.add_option("-a", "--onlyAbstracts", dest="onlyAbstracts", action="store_true", help="search only abstracts for keywords", default=False)
(options, args) = parser.parse_args()

# ==== FUNCTIONs =====

def overlapSelect_MergeId(selectBed, inBed, removeBed=None):
    """ 
    overlap two bed files (using blocks! not ranges), 
    return dictionary inBedName-name -> set of selectBed names 
    """

    logging.info("Running overlapSelect (input bed needs to have unique names for all features)")
    tempOut = tempfile.NamedTemporaryFile(suffix=".bed", prefix="overlapSelectOutputId")
    tempOutName = tempOut.name
    if removeBed==None:
        removeBedCmd = ""
    else:
        removeBedCmd = "overlapSelect -nonOverlapping -inFmt=bed %s stdin stdout | " % removeBed

    cmd = "cut -f1-12 %(selectBed)s | %(removeBedCmd)s overlapSelect stdin -idOutput %(inBed)s -selectFmt=bed %(tempOutName)s" % locals()
    util.execCmdLine(cmd)

    idDict = tabfile.slurpdictset(tempOutName)
    return idDict

def dispIdToArticleId(overlapIdMap, dispToArt, targetArticleIds):
    """ convert all elements in values of overlapIdMap (dispIds) to 
    articleIds using the authors.tab file and keep those also in targetArticleIds"""

    allArticleIds = []
    filteredArticleIds = []
    demoCount = 0

    convDict = {}
    for id1, dispIdSet in overlapIdMap.iteritems():
        artIdList = []
        for dispId in dispIdSet:
            if not dispId in dispToArt:
                logging.warn("%s not found in authors.tab" % dispId)
                continue
            artId = dispToArt[dispId]
            artIdList.append(artId)

        filteredSet = set(artIdList).intersection(targetArticleIds)
        if len(filteredSet)>0:
            convDict[id1] = filteredSet
            if demoCount<10:
                logging.debug(id1)
                demoCount+=1

        allArticleIds.extend(artIdList)
        filteredArticleIds.extend(filteredSet)

    logging.info("Overlapped articleIds %d" % len(set(allArticleIds)))
    logging.info("Keyword-containing articleIds %d" % len(set(filteredArticleIds)))
    return convDict

def getAllValues(dict):
    " given a dict key -> list, get all list members "
    vals = []
    for key, valList in dict.iteritems():
        vals.extend(valList)
    return set(vals)

#def overlapArticleIds(inBed, pubBed, blatBaseDir, db, dispToArt, targetArticleIds, removeBed):
    #overlapIdMap    = overlapSelectOutputId(pubBed, inBed, removeBed)
    #bedArticleIds   = dispIdToArticleId(overlapIdMap, dispToArt, targetArticleIds)
    #articleIds      = getAllValues(bedArticleIds)
    #return bedArticleIds

def searchForKeywords(textDir, keywords, onlyAbstracts):
    " search textDir for keywords, search either abstracts of fulltext "
    keywordDict = {}
    keywordFname = ""
    for keyword in keywords:
        keyword = keyword.replace("_", " ")
        keywordDict[keyword]=keyword
        logging.debug("Got search keyword %s" % keyword)
        keywordFname += keyword.replace(" ","_")
    if onlyAbstracts:
        keywordFname += ".abstracts"
    keywordFname += ".articleIds"

    paramDict = {"keywordDict" : keywordDict, "onlyAbstract": onlyAbstracts}

    if not isfile(keywordFname):
        logging.info("File %s not found, regenerating it" % keywordFname)
        pubAlg.mapReduce("keywordSearch.py:FilterKeywords", textDir, paramDict, keywordFname, deleteDir=False)
    else:
        logging.info("Found file %s with articleIds for these keywords" % keywordFname)
        logging.info("Delete this file to regenerate it")

    targetArticleIds = set(maxTables.TableParser(open(keywordFname)).column("articleId"))
    logging.info("Got %d articleIds with keywords" % len(targetArticleIds))
    return targetArticleIds

def filterPubBed(inBed, targetArticleIds, dispToArticleId):
    """ keep only features from bed that come from targetArticleIds, use dispToArtId to resolve IDs
    returns temporary filename, has to be deleted afterwards
    """
    logging.debug("Reading and filtering %s" % inBed)
    passedLines = []
    for line in open(inBed):
        fields = line.split("\t")
        bedName = fields[3]
        articleId = dispToArticleId[bedName]
        if articleId in targetArticleIds:
            passedLines.append(line)

    tmpFname = tempfile.mktemp(prefix="filterPubBed", suffix=".bed")
    logging.debug("Writing %s" % tmpFname)
    fh = open(tmpFname, "w")
    for line in passedLines:
        fh.write(line)
    fh.close
    return tmpFname
        
def showBedFeatures(bedNameDict, filteredPubBed):
    logging.debug("Debug mode: showing input BEDs that are overlapped")
    #print bedNameDict
    for b in maxTables.openBed(filteredPubBed, fileType="bed4"):
        #print b
        if b.name in bedNameDict:
            print "%s:%d-%d %s %s" % (b.chrom, b.start, b.end, b.name, bedNameDict[b.name])
            #print "\t".join(b)

# ----------- MAIN --------------
if args==[]:
    parser.print_help()
    exit(1)

pubGeneric.setupLogging(progFile, options)

blatBaseDir, db, keywordString = args[:3]
inBedFiles = args[3:]

pubBed = options.pubBed
removeBed = options.removeBed
outFname = options.outFname

if not isdir(blatBaseDir):
    blatBaseDir = join(pubConf.pubBlatBaseDir, blatBaseDir)

textDir   = open(join(blatBaseDir, "textDir.conf")).read().strip("\n")
keywords = keywordString.split(",")
targetArticleIds = searchForKeywords(textDir, keywords, options.onlyAbstracts)

# read mapping dispId -> articleId
dispIdFname = join(blatBaseDir, "authors.tab")
dispToArt = tabfile.slurpdict(dispIdFname, keyField=1, valField=0)

if pubBed==None:
    pubBed = join(blatBaseDir, "tables", db+".bed")

outFile = maxTables.openFile(outFname, "w")

filteredPubBed = filterPubBed(pubBed, targetArticleIds, dispToArt)

for inBed in inBedFiles:
    bedNameDict = overlapSelect_MergeId(filteredPubBed, inBed, removeBed=removeBed)
    if options.debug:
        showBedFeatures(bedNameDict, inBed)
    #bedToArticleIds = overlapArticleIds(inBed, pubBed, blatBaseDir, db, dispToArt, targetArticleIds, removeBed)
    outFile.write("overlapCount %s %d\n" %(inBed, len(bedNameDict)))

if not options.debug:
    os.remove(filteredPubBed)
