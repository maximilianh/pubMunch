#!/usr/bin/env python

# script to run the various steps of the text classification pipeline

# first load the standard libraries from python
# we require at least python 2.5
#from sys import *
import sys
if sys.version_info[0]==2 and not sys.version_info[1]>=7:
    print "Sorry, this program requires at least python 2.7"
    sys.exit(1)

# load default python packages
import logging, optparse, os, glob, zipfile, types, gzip, shutil, subprocess, \
    itertools, operator, gc, random, codecs
import marshal
from os.path import *
from collections import defaultdict, Counter
from datetime import datetime

# add <scriptDir>/lib/ to package search path
progFile = os.path.abspath(sys.argv[0])
progDir  = os.path.dirname(progFile)
pubToolsLibDir = os.path.join(progDir, "lib")
sys.path.insert(0, pubToolsLibDir)

# now load our own libraries
import pubGeneric, maxRun, pubConf, maxCommon, pubExpMatrix, html, pubStore, pubAlg

# === CONSTANTS & GLOBALS ===================================

# do not delete output dirs when starting a new step
leaveDirs = False

# === COMMAND LINE INTERFACE, OPTIONS AND HELP ===
parser = optparse.OptionParser("""usage: %prog [options] <datasetList> <step> - script to run the various steps of the article classification pipeline

steps:
bestWords = create the list of best words for each database
wordCount = create document-frequency list of words, needs to be filtered with 
            google/BNC to be useful
            writes to data/wordList/wordList.raw.txt
tmatrix   = create training matrix from document collection (map/reduce)
            reads pmid lists from data/classify/pmids
            writes to <pubBase>/classify/tmatrix.tab
            rewrite training matrix to one file per biological database
            and train svmlight models
            writes to data/classify/models
dmatrix   = create big document matrix from document collection, by default
            runs only on 
            reads from article datasets
            writes to <pubBase>/classify/docMatrix.svml 
classify  = run models onto docMatrix and write to <pubBase>/classify/docClasses.tab
html      = generate html pages for databases and write to html directory

example:
pubFilter pmc,elsevier,crawler dmatrix

""")

parser.add_option("", "--skipMap", dest="skipMap", action="store_true", help="skip all map steps")
parser.add_option("", "--onlyDbs", dest="onlyDbs", action="store", help="run only on a given db, not on all")
parser.add_option("-t", "--test", dest="test", action="store_true", help="only run the test, nothing else")
parser.add_option("-g", "--twoGrams", dest="twoGrams", action="store_true", help="also use twograms", default=False)
parser.add_option("-w", "--bestWordCount", dest="bestWordCount", action="store", type="int", help="how many best words to use? default %default", default=2000)
parser.add_option("", "--noMeta", dest="noMeta", action="store_true", help="do not add separate features for title/abstract/lastAuthor", default=False)
parser.add_option("-l", "--leaveDirs", dest="leaveDirs", action="store_true", help="do not delete output dirs before starting a new step, saves times but can corrupt data", default=False)
parser = pubGeneric.addGeneralOptions(parser)
(options, args) = parser.parse_args()
pubGeneric.setupLogging(__file__, options)

# ==== FILE NAMES ====

# set up two main dirs: one for static data from UCSC and one for local data
dataDir = pubConf.getStaticDataDir()
statClassDir = join(dataDir, "classify") # static data, part of code repo: foreground PMIDs, one list per DB
varDir       = join(pubConf.pubsDataDir, "classify") # pipeline data, like temporary files and output
localDir     = join(pubConf.localHeadDir, "classify")

rawWordListFname = join(statClassDir, "wordList.raw.txt")
wordListFname    = join(statClassDir, "wordList.txt.gz")
pmidListDir      = join(statClassDir, "trainPmids")
# like above, but ALL pmids in database
pmidListDirOrig  = join(statClassDir, "trainPmids/orig")
svmlBinDir      = pubConf.svmlBinDir

def defDirectories(baseDir, locBaseDir, datasets, options):
    class D:
        pass

    maxWordRank = options.bestWordCount
    if not isdir(baseDir):
        os.makedirs(baseDir)

    gramType = "1grams"
    if options.twoGrams:
        gramType = "2grams"

    sepType = ".withMeta"
    if options.noMeta:
        sepType = ""

    dirs = D()
    dirs.wordCountDir     = join(baseDir, "wordListOutput%s.%s" % (sepType, gramType))
    #dirs.concatDir        = join(baseDir, "wordListByDb")
    dirs.concatDir        = join(locBaseDir, "wordListByDb%s.%s" % (sepType, gramType))
    dirs.corpusDir        = join(locBaseDir, "corpus")
    dirs.wordCountFname   = join(baseDir, "wordCounts.marshal")
    dirs.wordPmidDir      = join(baseDir, "wordPmidDicts.%s" % (gramType))
    dirs.textDirs         = pubConf.resolveTextDirs(datasets)

    dirs.rankedWordsDir     = join(baseDir, "rankedWords.%s.%d" % (gramType, maxWordRank))
    dirs.bestWordsDir     = join(baseDir, "bestWords.%s.%d" % (gramType, maxWordRank))
    dirs.tMatrixFname     = join(baseDir, "allTrain.pmidsvml")
    dirs.clusterSvmlDir   = join(baseDir, "svmlJobOutput.%s.%d" % (gramType, maxWordRank))
    dirs.svmlDir          = join(baseDir, "svmlInput.%s.%d" % (gramType, maxWordRank))
    dirs.trainSvmlDir     = join(baseDir, "svmlTrain.%s.%d" % (gramType, maxWordRank))
    dirs.testSvmlDir      = join(baseDir, "svmlTest.%s.%d" % (gramType, maxWordRank))
    dirs.benchTable       = join(baseDir, "bench.%s.%d.tab" % (gramType, maxWordRank))

    dirs.modelDir         = join(baseDir, "svmlModels.%s.%d" % (gramType, maxWordRank))
    dirs.alphaDir         = join(baseDir, "svmlAlphas.%s.%d" % (gramType, maxWordRank))
    dirs.dMatrixFname     = join(baseDir, "docs.svml")
    dirs.dIdFname         = join(baseDir, "docs.docIds")
    dirs.categoryFname    = join(baseDir, "docClasses.tab")
    dirs.predClassDir     = dirs.testSvmlDir
    return dirs

# ==== FUNCTIONs =====
def parsePmidsDbs(inDir):
    " return training PMIDs in format pmid -> list of DBs "
    pmids, dbList, dbPmids = parsePmids(inDir)
    res = defaultdict(list)
    for pmid, classDbs in dbPmids.iteritems():
        dbList = []
        for docClass, db in classDbs:
            dbList.append(db)
        res[pmid]=dbList
    return res

def parsePmidClasses(inDir, db):
    """ return training PMIDs in format pmid -> class (1 or -1)"""
    logging.info("Parsing class assignments for db %s" % db)
    overlapPmids = set()
    res = {}
    for className, classId in [("pos", 1), ("neg", -1)]:
        inPmids = set(open(join(inDir, className+"."+db+".txt")).read().splitlines())
        for pmid in inPmids:
            pmid = int(pmid)
            if pmid in res:
                #raise Exception("PMID %d is in both classes" % pmid)
                overlapPmids.add(pmid)
            res[pmid] = classId
    logging.warn("%d PMIDs in both classes" % len(overlapPmids))
    return res

def parsePosPmids(db, inDir):
    " parse pos pmids of db"
    fname = join(inDir, "pos.%s.txt" % db)
    pmids = open(fname).read().splitlines()
    pmids = [int(x) for x in pmids]
    return set(pmids)

def parsePmids(inDir):
    """ parse pmids from dir and return as 
        - a set of all PMIDs,  
        - a list of DBS and - 
        - as a dict pmid -> list of (class, db) tuples, class is either "+1" or "-1"

        all PMIDs are integers
    """
    logging.info("Parsing PMIDs")
    allPmids = set()
    pmidDbs = {}
    dbs = set()
    inFnames = glob.glob(join(inDir, "*.txt"))
    for inFname in inFnames:
        base = basename(inFname)
        if not (base.startswith("pos") or base.startswith("neg")):
            continue
        pmidClass, db = base.split(".")[:2]
        dbs.add(db)
        if pmidClass=="pos":
            svmlClass = "+1"
        elif pmidClass=="neg":
            svmlClass = "-1"
        else:
            assert(False)

        logging.debug("Parsing %s" % inFname)
        for line in open(inFname):
            pmid = line.strip()
            pmid = int(pmid)
            allPmids.add(pmid)
            pmidDbs.setdefault(pmid, []).append( (svmlClass, db) )
    logging.info("Read %d PMIDs from %s" % (len(allPmids), inDir))

    #for pmid,  in pmidDbs.iteritems():
        #logging.info("Got %d pmids for DB %s" % (len(pmids), db))
    assert(len(pmidDbs)!=0)
    assert(len(allPmids)!=0)

    return allPmids, dbs, pmidDbs

def mkEmptyDir(dir, doNotDelete=False):
    " make sure dir exists and is empty "
    logging.info("Making/Cleaning %s" % dir)
    if leaveDirs:
        doNotDelete=True
    if isdir(dir):
        if not doNotDelete:
            shutil.rmtree(dir)
            #cmd = "mv %(dir)s %(dir)s.old; rm -rf %(dir)s.old &" % locals()
            #print cmd
            #os.system(cmd)

    if not isdir(dir):
        os.makedirs(dir)

def splitSvml(tMatrixFname, dbs, pmidDbs, svmlDir):
    """ create one svml output file in svmlDir per db in dbPmids and distribute the svml lines from
        tMatrixFname to the right files in svmlDir
    """
    dbOfh = {}
    fnames = []
    mkEmptyDir(svmlDir)

    for db in dbs:
        ofname = join(svmlDir, db+".svml")
        dbOfh[db] = open(ofname, "w")
        fnames.append(ofname)

    logging.debug("Rewriting %s" % tMatrixFname)
    for line in open(tMatrixFname):
        docId, featVec = line.split(" ", 1)
        pmid = int(docId.split("/")[2])
        for svmlClass, db in pmidDbs[pmid]:
            dbOfh[db].write(svmlClass+" ")
            dbOfh[db].write(featVec)

    logging.info("Wrote SVML files: %s" % " ".join(fnames))

def svmlLearn(svmlBinDir, svmlDir, modelDir, alphaDir, dbList):
    " run svml_learn on all .svml files in svmlDir "
    binPath = join(svmlBinDir, "svm_learn")
    if not isfile(binPath):
        raise Exception("%s does not exist" % binPath)
    mkEmptyDir(modelDir)
    mkEmptyDir(alphaDir)

    logging.info("Using SVML files in dir %s" % svmlDir)
    for svmlFname in glob.glob(join(svmlDir, "*.svml")):
        db = splitext(basename(svmlFname))[0]
        if dbList!=None and db not in dbList:
            continue
        modelFname = join(modelDir, db+".model")
        alphaFname = join(alphaDir, db+".alpha")
        logging.info("Running SVMlight for db %s" % db)
        cmd = [binPath, svmlFname, modelFname, "-a", alphaFname]
        subprocess.check_call(cmd)
    logging.info("alphaput written to %s and %s" % (modelDir, alphaDir))

def svmlClassify(svmlBinDir, svmlDir, modelDir, classDir, dbList):
    " run svml on all models from modelDir "
    #mkEmptyDir(classDir)
    binPath = join(svmlBinDir, "svm_classify")
    if not isfile(binPath):
        raise Exception("%s does not exist" % binPath)

    runner = pubGeneric.makeClusterRunner(__file__, algName="svmlClassify", headNode='localhost')
    for modelFname in glob.glob(join(modelDir, "*.model")):
        db = splitext(basename(modelFname))[0]
        if dbList!=None and db not in dbList:
            continue
        svmlFname = join(svmlDir, db+".svml")
        #logging.info("Classifying with SVMLight, feature file %s" % svmlFname)
        outFname = join(classDir, db+".predClasses")
        logging.info("Running on %s and %s" % (svmlFname, modelFname))
        cmd = [binPath, svmlFname, modelFname, "{check out line+ %s}" % outFname]
        cmd = " ".join(cmd)
        #logging.debug("command is %s" % cmd)
        runner.submit(cmd)
    runner.finish(wait=True)

def parseDocClasses(fname):
    " return dict db -> list of (articleId (INT!), score) "
    logging.info("Parsing %s" % fname)
    res = defaultdict(list)
    for row in maxCommon.iterTsvRows(fname):
        artId = int(row.articleId)
        classes = row.classes.split(',')
        scores = row.scores.split(',')
        for db, score in zip(classes, scores):
            res[db].append((artId, float(score)))
    return res

def makeHtmlTest(dbList, datasets, svmlDir, outDir):
    " "
    mkEmptyDir(outDir)
    logging.info("Writing html to %s" % outDir)
    for db in dbList:
        pmidFn = join(svmlDir, db+".pmids")
        pmids = open(pmidFn).read().splitlines()

        predFn = join(svmlDir, db+".predClasses")
        predScores = open(predFn).read().splitlines()

        realFn = join(svmlDir, db+".classes")
        realClasses = open(realFn).read().splitlines()
        logging.info("Reading %s, %s and %s" % (pmidFn, predFn, realFn))

        outfname = join(outDir, db+".html")
        errDict = defaultdict(list)
        for pmid, predScore, realClass in zip(pmids, predScores, realClasses):
            predScore = float(predScore)
            realClass = int(realClass)
            if predScore>0 and realClass==1:
                err = "tp"
            elif predScore<0 and realClass==-1:
                err = "tn"
            elif predScore>0 and realClass==-1:
                err = "fp"
            elif predScore<0 and realClass==1:
                err = "fn"
            errDict[err].append((pmid, predScore, realClass))
            

        logging.info("DB %s, filename %s" % (db, outfname))
        h = html.htmlWriter(outfname)
        desc = {"tp" : "in database and predicted", \
                "tn" : "not in database and predicted", \
                "fp" : "not in database, but predicted to be", \
                "fn" : "in database, but not predicted"
                }
        h.head("Database: %s" % db)
        h.h4("Database: %s" % db)
        h.p()
        h.writeLn("<ul>")
        for errCode, pmidList in errDict.iteritems():
            h.li("Class: %s (%s): %d documents" % (errCode, desc[errCode], len(pmidList)))
        h.writeLn("</ul>")
        #h.hr()

        for errType, errTuples in errDict.iteritems():
            if errType in ["tp", "tn"]:
                continue
            h.h4("Class: %s / %s" % (errType, desc[errType]))
            #pm = maxCommon.ProgressMeter(pmidCount, stepCount=100)
            for pmid, predScore, realClass in errTuples:
                art = pubStore.lookupArticleByPmid(datasets, pmid)
                if art==None:
                    logging.warn("No info on PMID %s?" % pmid)
                    continue
                h.link(art["fulltextUrl"], art["title"])
                h.br()
                ref = ["score: "+str(predScore), art["journal"], art["year"], art["authors"]]
                h.writeLn(", ".join(ref))
                #h.hr()
                h.p()
                #pm.taskCompleted()
        h.endHtml()
    
def readWordPmids3(inDir):
    " merge together all those word -> pmid dictionatries and return it as one big dict "
    gc.disable()
    fnames = glob.glob(join(inDir, "*.marshal"))
    logging.info("Found %d files in %s" % (len(fnames), inDir))

    duplPmidCount = 0
    donePmids = set()
    wordPmids = {}
    pm = maxCommon.ProgressMeter(len(fnames))
    for fname in fnames:
        logging.debug("Reading %s" % fname)
        if os.path.getsize(fname)==0:
            logging.error("Empty file %s, skipping" % fname)
            continue
        f = open(fname)
        fileWordPmids = marshal.load(f)
        f.close()
        for word, pmids in fileWordPmids.iteritems():
            wordPmids.setdefault(word, []).extend(pmids)
        pm.taskCompleted()
    logging.info("wordCount: %d" % len(wordPmids))
    gc.enable()
    return wordPmids

def readWordPmids(inDir, pmids, onlyOneGrams=False):
    " parse wordCounts, return as dict word -> pmids "
    #fnames = glob.glob(join(inDir, "*.tab.gz"))
    logging.info("Found %d files in %s" % (len(fnames), inDir))
    logging.info("Reading wordcounts for %d PMIDs" % len(pmids))
    logging.info("Time: %s" % str(datetime.now()))
    pm = maxCommon.ProgressMeter(len(fnames))

    donePmids = set()
    duplPmidCount = 0
    res = {}
    for fname in fnames:
        logging.debug("Parsing %s")
        for row in maxCommon.iterTsvRows(fname):
            gc.disable()
            pmid = int(row.pmid)
            if pmid not in pmids:
                logging.debug("Ignoring %s, not in target pmids" % pmid)
                continue
            if pmid in donePmids:
                logging.debug("duplicated pmid %d" % pmid)
                duplPmidCount += 1
                continue
            donePmids.add(pmid)
            wordStr = row.wordCounts
            words = [w.split("=")[0] for w in wordStr.split(",")]
            for word in words:
                if onlyOneGrams and "_" in word:
                    continue
                word = str(word) # bytestrings take 1/2 memory
                res.setdefault(word, []).append(pmid)
            gc.enable()
        pm.taskCompleted()
    logging.info("Featurecount: %d" % len(res))
    logging.info("input PMID count: %d" % len(pmids))
    logging.info("found PMID count: %d" % len(donePmids))
    logging.info("Duplicated PMIDs: %d" % duplPmidCount)
    return res, donePmids
    
def filterPmids(inDir, pmids, dbList, outDir):
    " parse all wordCounts, write to one single file per DB "
    maxCommon.mustExistDir(outDir, makeDir=True)
    fnames = glob.glob(join(inDir, "*.tab.gz"))
    logging.info("Found %d files in %s" % (len(fnames), inDir))
    logging.info("Reading wordcounts for %d PMIDs" % len(pmids))
    logging.info("Time: %s" % str(datetime.now()))
    pm = maxCommon.ProgressMeter(len(fnames))

    # open filehandles
    headerLine = gzip.open(fnames[0]).readline()
    ofhs = {}
    for db in dbList:
        ofhs[db] = gzip.open(join(outDir, db+".tab.gz"), "w")
        ofhs[db].write(headerLine)

    # go over input files and write lines
    donePmids = set()
    duplPmidCount = 0
    res = {}
    for fname in fnames:
        logging.debug("Parsing %s")
        for row, line in maxCommon.fastIterTsvRows(fname):
            pmid = int(row.pmid)
            if pmid not in pmids:
                logging.debug("Ignoring %s, not in target pmids" % pmid)
                continue
            if pmid in donePmids:
                logging.debug("duplicated pmid %d" % pmid)
                duplPmidCount += 1
                continue
            donePmids.add(pmid)

            dbList = pmids[pmid]
            for db in dbList:
                ofhs[db].write(line)
                ofhs[db].write("\n")
        pm.taskCompleted()

    logging.info("input PMID count: %d" % len(pmids))
    logging.info("found PMID count: %d" % len(donePmids))
    logging.info("Duplicated PMIDs: %d" % duplPmidCount)
    logging.info("Output directory: %s" % outDir)

def calcChiSq(allCount, posCount, negCount, posOvl, negOvl):
    expOvl = int((float(negOvl) / negCount) * posCount)
    if expOvl==0:
        expOvl=1
    chiSq = (posOvl-expOvl)**2 / float(expOvl)
    return chiSq

def makeBestWords(dbList, wordCountFname, pmidDir, rankedWordsDir, maxRank, bestWordsDir):
    """ read word counts from dataset completely into memory, for each db, use 
      pmidList to separate into pos/neg and do a chi-square test to find best 
      words 
    """
    mkEmptyDir(bestWordsDir)
    mkEmptyDir(rankedWordsDir)
    logging.info("Loading word->doc table %s" % wordCountFname)
    wordPmids = marshal.load(open(wordCountFname))
    for db in dbList:
        logging.info("Finding best words for db %s" % db)
        # parse pmids
        posPmids = set([int(l.strip()) for l in open(join(pmidDir, "pos."+db+".txt"))])
        negPmids = set([int(l.strip()) for l in open(join(pmidDir, "neg."+db+".txt"))])
        logging.debug("%d positive documents, %d background documents" % (len(posPmids), len(negPmids)))

        gc.disable()
        wordScores = []
        for word, pmidList in wordPmids.iteritems():
            wordPmids = set(pmidList)
            obsOvl = len(wordPmids.intersection(posPmids))
            if obsOvl < 10:
                continue
            negOvl = len(wordPmids.intersection(negPmids))
            expOvl = int(float(negOvl) * len(posPmids))
            logging.debug("word %s: overlap with positives %d, overlap with background %d, expected overlap with positives %d" % (word, obsOvl, negOvl, expOvl))
            if expOvl==0:
                expOvl=1
            chiSq = (obsOvl-expOvl)**2 / float(expOvl)
            wordScores.append( (word, chiSq) )
        wordScores.sort(key=operator.itemgetter(1))
        gc.enable()

        # write words + score
        ofname = join(rankedWordsDir, "%s.tab" % (db))
        ofh = open(ofname, "w")
        ofh.write("word\tchi2\n")
        for word, score in wordScores:
            ofh.write("%s\t%f\n" % (word, score))
        logging.info("Wrote chi2 scores to %s" % ofname)

        # write only top x words
        ofname = join(bestWordsDir, "%s.txt" % (db))
        ofh = open(ofname, "w")
        for word, score in wordScores[:maxRank]:
            ofh.write("%s\n" % (word))
        logging.info("Wrote top %d words to %s" % (maxRank, ofname))

def filterWords(wordCounts, posMin, posMax, negMin, negMax):
    logging.info("Basic filter: Keeping only words with count: %d<pos<%d, %d<neg<%d" % (posMin, posMax, negMin, negMax))
    #logging.info("Time: %s" % str(datetime.now()))
    gc.disable()
    newDict = {}
    for key, valList in wordCounts.iteritems():
        count = len(valList)
        if count>posMin and count<posMax and \
            count>negMin and count<negMax:
            newDict[key] = valList
    gc.enable()
    logging.info("Word count reduced from %d to %d after basic filtering" % (len(wordCounts), len(newDict)))
    return newDict

def writeWordPmids(wordPmids, wordCountFname):
    logging.info("Writing words")
    f = open(wordCountFname, "w")
    marshal.dump(wordPmids, f)
    f.close()
    logging.info("%s: %3d MB" % (wordCountFname, os.path.getsize(wordCountFname)/1000000))

def getDbList(pmidDir):
    " create a list of all possible DBs in pmidDir, those with a pos. and a neg.<db>.txt file "
    fnames = os.listdir(pmidDir)
    posNames = [fn.split(".")[1] for fn in fnames if fn.startswith("pos.") and fn.endswith(".txt")]
    negNames = [fn.split(".")[1] for fn in fnames if fn.startswith("neg.") and fn.endswith(".txt")]
    dbNames = set(posNames).intersection(negNames)
    return dbNames

def submitToSvmlJobs(textDirs, outDir, wordFname, addMeta, addTwoGrams):
    paramDict = { "dbWords" : dbWords, "addMeta":addMeta, "addTwoGrams" : addTwoGrams }
    algName = "wordList.py:SvmlWriter"
    runner = pubGeneric.makeClusterRunner(__file__, algName="toSvml")
    pubAlg.submitAnnotateWrite("wordList", textDirs, paramDict, outDir, runner=runner)
    runner.finish()

def submitWordCountJobs(pmids, datasets, textDirs, outDir, addMeta, addTwoGrams):
    """ 
    creates a big marshalled dict with word -> list of pmids
    """
    maxCommon.mustExistDir(outDir, makeDir=True)
    mkEmptyDir(outDir)
    runner = pubGeneric.makeClusterRunner(__file__, algName="countWords")
    paramDict = {"addMeta" : addMeta, "addTwoGrams" : addTwoGrams, "pmids":pmids}
    pubAlg.submitAnnotateWrite(runner, "wordList", textDirs, paramDict, outDir)
    logging.info("Output written to %s" % outDir)
    runner.finish()

def submitWordCountJobs2(pmidListDir, datasets, textDirs, outDir, addMeta, addTwoGrams):
    """ 
    creates a big marshalled dict with word -> list of pmids
    """
    #maxCommon.mustExistDir(outDir, makeDir=True)
    #mkEmptyDir(outDir)
    pmids, dbs, dbPmids = parsePmids(pmidListDir)
    runner = pubGeneric.makeClusterRunner(__file__, algName="countWords")
    paramDict = {
        "addMeta" : addMeta,
        "addTwoGrams" : addTwoGrams,
        "outFname" : "big.marshal",
        "pmids" : pmids
        }
    pubAlg.mapReduce("wordList:WordPmids", textDirs, paramDict, None, runner=runner, runTest=True)
    logging.info("Output written to %s" % outDir)
    runner.finish()

def wordCountToSvml(dbList, pmidListDir, concatDir, bestWordsDir, svmlDir):
    " convert our internal word=count format to svmlight "
    mkEmptyDir(svmlDir)
    for db in dbList:
        # setup files
        pmidClasses = parsePmidClasses(pmidListDir, db)
        #inFname = join(concatDir, db+".tab.gz")
        inFname = join(concatDir, db+".tab")
        svmlOutFname = join(svmlDir, db+".svml")
        pmidOutFname = join(svmlDir, db+".pmids")
        logging.info("Writing to %s and %s" % (svmlOutFname, pmidOutFname))
        svmlFh = open(svmlOutFname, "w")
        pmidFh = open(pmidOutFname, "w")
        wordFname = join(bestWordsDir, db+".txt")
        logging.info("Reading bestwords from %s" % wordFname)

        # setup dict word -> integer
        bestWordList = open(wordFname).read().splitlines()
        wordToIdx = dict((y,x+1) for x,y in enumerate(bestWordList))

        # parse infile and convert to svml
        logging.info("Parsing %s" % inFname)
        gc.disable()
        badCount = 0
        rowCount = 0
        for row, line in maxCommon.fastIterTsvRows(inFname):
            pmid = int(row.pmid)
            classId = pmidClasses[pmid]
            # create tuples (word, count)
            tuples = [tuple(tpl.split("=")) for tpl in row.wordCounts.split(",")]
            #if pmid==14593087:
                #print list(tuples)
            # create list of (word, count) for all good words 
            items = [(wordToIdx[t[0]], int(t[1])) for t in tuples if t[0] in wordToIdx]
            #if pmid==14593087:
                #print list(countTuples)
            # convert to sorted list (wordIdx, count) and join to string
            if len(items)==0:
                if classId==1:
                    logging.warn("PMID %d, count %d: positive class but no single good word?" % \
                        (badCount, pmid))
                    badCount += 1
                continue
            items.sort(key=operator.itemgetter(0))
            items = ("%d:%d" % (x,y) for x,y in items)
            svmlStr = " ".join(items)
            svmlFh.write("%+d %s\n" % (classId, svmlStr))
            pmidFh.write("%d\n" % pmid)
            rowCount +=1
            if rowCount % 1000 == 0:
                print rowCount
        gc.enable()

def getText(textDirs, dbList, pmidToDb, corpusDir):
    mkEmptyDir(corpusDir)

    # open outfiles
    outFiles = {}
    for db in dbList:
        outFname = join(corpusDir, db+".txt")
        logging.info("Opening %s" % outFname)
        outFiles[db] = open(outFname, "w")
        
    # iterate over input and spread over output
    logging.debug("Reading text from %s" % textDirs)
    for textDir in textDirs:
        #ar = pubStore.PubReaderFile(textDir)
        print textDir
        dataIter = pubStore.iterArticleDirList(textDir, onlyMain=True, preferPdf=True)
        #for article, fileList in ar.iterArticlesFileList(onlyMain=True, onlyBestMain=True):
        for article, fileList in dataIter:
            f = fileList[0]
            text = f.content
            if article.pmid=="":
                continue
            pmid = int(article.pmid)
            if pmid!="" and pmid in pmidToDb:
                for db in pmidToDb[pmid]:
                    outFiles[db].write("%s\t%s\n" % (pmid, text.encode("utf8")))

def dictToSvml (wordToPmids, bestWords, posPmids, negPmids, svmlDir, db):
    pmidToWords = defaultdict(list)
    for wordIdx, word in enumerate(bestWords):
        wordPmids = wordToPmids[word]
        for pmid in set(wordPmids):
            pmidToWords[pmid].append(wordIdx)
    svmlOutFname = join(svmlDir, db+".svml")
    pmidOutFname = join(svmlDir, db+".pmids")
    classOutFname = join(svmlDir, db+".classes")
    logging.info("Writing to %s and %s" % (svmlOutFname, pmidOutFname))
    svmlFh = open(svmlOutFname, "w")
    pmidFh = open(pmidOutFname, "w")
    classIdFh = open(classOutFname, "w")

    allPmids = posPmids.union(negPmids)
    for pmid in allPmids:
        wordIdxList = pmidToWords.get(pmid, [])
        if pmid in posPmids:
            classId = 1
        else:
            classId = -1

        wordVec = ["1:1"]
        # svml is 1-based
        # AND we reserve feature 1 to indicate if any best word matched
        wordVec.extend(["%d:1" % (wordIdx+2) for wordIdx in wordIdxList])

        # if no word matched, set feature 1 to 0
        if len(wordVec)==1:
            wordVec = ["1:0"]

        wordStr = " ".join(wordVec)
        svmlFh.write("%+d %s\n" % (classId, wordStr))
        pmidFh.write("%d\n" % pmid)
        classIdFh.write("%d\n" % classId)

    svmlFh.close()
    pmidFh.close()
    classIdFh.close()
    return svmlOutFname

def marshalToSvml(dbList, pmidListDir, dataCountFname, bestWordsDir, svmlDir):
    mkEmptyDir(svmlDir)
    gc.disable()
    logging.info("Reading word pmids")
    wordToPmids = marshal.load(open(dataCountFname))
    for db in dbList:
        logging.info("creating pmid -> word lists for db %s" % db)
        pmidToWords = defaultdict(list)
        wordFname = join(bestWordsDir, db+".txt")
        pmidClasses = parsePmidClasses(pmidListDir, db)
        logging.info("Getting best words from %s" % wordFname)
        bestWordList = open(wordFname).read().splitlines()
        #wordToIdx = dict((y,x+1) for x,y in enumerate(bestWordList))
        for wordIdx, word in enumerate(bestWordList):
            wordPmids = wordToPmids[word]
            dbWordPmids = set(wordPmids).intersection(pmidClasses)
            #wordIdx = wordToIdx[word]
            for pmid in dbWordPmids:
                pmidToWords[pmid].append(wordIdx)
            #print pmidToWords

        svmlOutFname = join(svmlDir, db+".svml")
        pmidOutFname = join(svmlDir, db+".pmids")
        classOutFname = join(svmlDir, db+".classes")
        logging.info("Writing to %s and %s" % (svmlOutFname, pmidOutFname))
        svmlFh = open(svmlOutFname, "w")
        pmidFh = open(pmidOutFname, "w")
        classIdFh = open(classOutFname, "w")

        for pmid, wordIdxList in pmidToWords.iteritems():
                classId = pmidClasses[pmid]
                wordVec = ["%d:1" % (wordIdx+1) for wordIdx in wordIdxList]
                wordStr = " ".join(wordVec)
                svmlFh.write("%+d %s\n" % (classId, wordStr))
                pmidFh.write("%d\n" % pmid)
                classIdFh.write("%d\n" % classId)
    gc.enable()
        
def splitFile(inFname, ratio, outDir1, outDir2):
    " split text file and also the .pmid / .classes files "
    base   = basename(inFname)
    baseNoExt = splitext(base)[0]
    noExt = splitext(inFname)[0]

    outFname1 = join(outDir1, base)
    outFname2 = join(outDir2, base)
    pmidInFn  = noExt+".pmids"
    classInFn = noExt+".classes"

    logging.info("Splitting %s" % inFname)
    ofhs = []
    pmidFhs = []
    classFhs = []

    pmidFhs.append(open(join(outDir1, baseNoExt+".pmids"), "w"))
    pmidFhs.append(open(join(outDir2, baseNoExt+".pmids"), "w"))

    classFhs.append(open(join(outDir1, baseNoExt+".classes"), "w"))
    classFhs.append(open(join(outDir2, baseNoExt+".classes"), "w"))

    ofhs.append( open(outFname1, "w"))
    ofhs.append( open(outFname2, "w"))

    pmids = open(pmidInFn).read().splitlines()
    classes = open(classInFn).read().splitlines()

    for lineIdx, line in enumerate(open(inFname)):
        rnd = random.random()
        if rnd < ratio:
            ofhIdx = 0
        else:
            ofhIdx = 1
        ofhs[ofhIdx].write(line)
        pmidFhs[ofhIdx].write(pmids[lineIdx]+"\n")
        classFhs[ofhIdx].write(classes[lineIdx]+"\n")

def parseBestWords(inDir, dbList):
    " parse best words and return as dict db -> list of words "
    logging.info("Reading best words from %s" % inDir)
    res = {}
    for fname in glob.glob(join(inDir, "*.txt")):
        db = splitext(basename(fname))[0]
        if not db in dbList:
            continue
        bestWords = open(fname).read().splitlines()
        res[db] = bestWords
    assert(len(res)!=0)
    return res
        
def textToSvml(textDirs, dbWords, svmlDir, addMeta, addTwoGrams, modelDir):
    """create subdirs in svmlDir, one per db and write svml files into it 
    use only words in bestWords and add +1/-1 classes according to dbPmids, 
    a dict with pmid -> list of (db, class)
    """
    maxCommon.mustExistDir(svmlDir, makeDir=True)
    mkEmptyDir(svmlDir)
    paramDict = {
        "addMeta" : addMeta,
        "addTwoGrams" : addTwoGrams,
        "dbWords" : dbWords,
        "svmlBinDir" : svmlBinDir,
        "modelDir" : modelDir
        }
    runner = pubGeneric.makeClusterRunner(__file__, algName="toSvml")
    pubAlg.submitAnnotateWrite(runner, "wordList:SvmlWriter", textDirs, paramDict, svmlDir)
    runner.finish()
    logging.info("Output written to %s" % svmlDir)

def parsePmidPosNeg(db, pmidListDir):
    # open list of pmids in pos and neg set as set of integers
    posPmids = set([int(x) for x in open(join(pmidListDir, "pos."+db+".txt")).read().splitlines()])
    negPmids = set([int(x) for x in open(join(pmidListDir, "neg."+db+".txt")).read().splitlines()])
    return posPmids, negPmids

def writeWordCounts(counts, posPmids, negPmids, outFname, bestWordCount, bestWordFname):
    """ 
    write counts of pmids in foreground / background to tab file for each word 
    counts is a dict word -> pmids
    
    """
    posCount = len(posPmids)
    negCount = len(negPmids)
    rows = []
    for word, pmidList in counts.iteritems():
        pmids = set(pmidList)
        posOvl = len(pmids.intersection(posPmids))
        negOvl = len(pmids.intersection(negPmids))
        chiSq = calcChiSq(len(pmids), posCount, negCount, posOvl, negOvl)
        row = [word, len(pmids), posOvl, negOvl, chiSq]
        rows.append(row)

    # write best words
    rows.sort(key=operator.itemgetter(-1), reverse=True)
    bestWords = [row[0] for row in rows[:bestWordCount]]
    ofh = open(bestWordFname, "w")
    ofh.write("\n".join(bestWords))
    ofh.write("\n")
    ofh.close()
    logging.info("Wrote %s" % bestWordFname)

    ofh = open(outFname, "w")
    ofh.write("# posPmids=%d, negPmids=%d\n" % (posCount, negCount))
    ofh.write("\t".join(["word", "count", "posOvl", "negOvl", "chiSq"])+"\n")
    for row in rows:
        row = [str(s) for s in row]
        ofh.write("\t".join(row)+"\n")
    ofh.close()
    logging.info("Wrote %s" % outFname)

    return bestWords
                
def summarizeResults(dbList, svmlDir, categoryFname):
    """ combine svml output and article ids and write categories in 
    a format that is easier to parse: docId<tab>dbs (comma-sep) 
    """
    # open outfile
    ofh = open(categoryFname, "w")
    ofh.write("articleId\texternalId\tpmid\tclasses\tscores\n")

    logging.info("Reading directory %s" % svmlDir)
    fnames = os.listdir(svmlDir)
    chunkIds = set([basename(fn).split(".")[0] for fn in fnames])
    logging.info("Parsing %d chunks" % len(chunkIds))

    dbCounts = Counter()
    pm = maxCommon.ProgressMeter(len(chunkIds))
    for chunkId in chunkIds:
        logging.debug("%s" % chunkId)
        docClasses = defaultdict(list)
        docIdRows = list(maxCommon.iterTsvRows(join(svmlDir, chunkId+".docIds")))
        for db in dbList:
            classFname = join(svmlDir, chunkId+".%s.classes" % db)
            logging.debug("Reading class assignment from %s" % classFname)
            classScores = [float(l.strip()) for l in open(classFname).readlines()]
            assert(len(classScores)==len(docIdRows))
            for docIdRow, classValue in itertools.izip(docIdRows, classScores):
                if classValue>0.0:
                    docClasses[docIdRow].append((db, classValue))
                    dbCounts[db] += 1
        logging.debug("processed %d articles" % len(docClasses))

        for docIdRow, catScores in docClasses.iteritems():
            artId, extId, pmid =  docIdRow
            catScores.sort(key=operator.itemgetter(1), reverse=True)
            classes, scores = zip(*catScores) # weird python magic
            scores = [str(s) for s in scores]
            ofh.write("%s\t%s\t%s\t%s\t%s\n" % (artId, extId, pmid, ",".join(classes), ",".join(scores)))
        pm.taskCompleted()
    ofh.close()
    for db, dbCount in dbCounts.iteritems():
        logging.info("%s: %d assigned documents" % (db, dbCount))
    logging.info("Wrote class info to %s" % categoryFname)

def makeHtmlDir(dbList, outDir):
    outfname = join(outDir, "index.html")
    h = html.htmlWriter(outfname)
    title = "Genocoding document categories recognized by Support Vector Machine"
    h.head(title)
    h.h4(title)
    h.startUl()
    for db in dbList:
        desc = pubConf.classDescriptions[db]
        h.li('<a href="%s.html">%s</a> (<a href="%s.ids.txt">article IDs</a>)' % (db, desc, db))
    h.endUl()
    h.endHtml()

def makeHtml(dbList, pmidDir, catFname, outDir):
    " "
    docClasses = parseDocClasses(catFname)
    mkEmptyDir(outDir)
    logging.info("Writing html to %s" % outDir)
    for db in dbList:
        docIdFname = join(outDir, db+".ids.txt")
        docFh = codecs.open(docIdFname, "w", encoding="utf8")
        docFh.write("#PMID\tDOI\n")

        outfname = join(outDir, db+".html")

        artScores = docClasses[db]

        logging.info("DB %s, filename %s" % (db, outfname))
        h = html.htmlWriter(outfname)
        h.head("Database: %s" % db)
        desc = pubConf.classDescriptions.get(db, "")
        h.h4("Documents in category <i>'%s'</i> (%s)" % (desc, db))
        h.writeLn("Source database of training documents: <b>%s</b><br>" % db)
        h.writeLn("Number of articles shown: %d<br>" % len(artScores))
        h.writeLn('Articles already in the database are not shown<br>')
        h.writeLn('<a href="%s.ids.txt">Download document IDs</a> in tab-sep format' % (db))
        h.p()
        h.writeLn('<hr>')

        artScores.sort(key=operator.itemgetter(1), reverse=True)

        pm = maxCommon.ProgressMeter(len(artScores), stepCount=100)
        skippedPmids = 0
        dbPmids = parsePosPmids(db, pmidDir)
        for artId, score in artScores:
            art = pubStore.lookupArticleByArtId(artId)
            if art==None:
                logging.warn("No info on artId %s?" % pmid)
                continue
            # don't show articles that are already in DB
            if art["pmid"]!="" and int(art["pmid"]) in dbPmids:
                skippedPmids += 1
                continue
            h.link(art["fulltextUrl"], art["title"])
            h.br()
            authors = art["authors"]
            if len(authors)>60:
                authors = authors.split(";")[0]+" et. al. "
            ref = ["score: "+str(score), art["journal"], art["year"], authors]
            h.writeLn(", ".join(ref))
            #h.hr()
            h.p()

            docFh.write("%s\t%s\n" % (art["pmid"], art["doi"]))
            pm.taskCompleted()
        logging.info("Skipped %d PMIDs as they are already in DB" % skippedPmids)
        h.endHtml()
        docFh.close()
    
def compSvmlClasses(testSvmlDir, db):
    classFname = join(testSvmlDir, db+".predClasses")
    svmlFname  = join(testSvmlDir, db+".classes")
    print classFname, svmlFname
    classes = open(classFname).read().splitlines()
    svmls = open(svmlFname).read().splitlines()
    TP, FN, FP, TN = 0, 0, 0, 0
    realPosCount = 0
    realNegCount = 0
    for classLine, svmlLine in zip(classes, svmls):
        predVal = float(classLine)
        realVal = float(svmlLine.split()[0])
        if realVal > 0:
            realPosCount += 1
        else:
            realNegCount += 1
        print realVal, predVal
        if predVal<0 and realVal<0:
            print "tn"
            TN+=1
        if predVal>0 and realVal>0:
            print "tp"
            TP+=1
        if predVal<0 and realVal>0:
            print "fp"
            FP+=1
        if predVal>0 and realVal<0:
            print "fn"
            FN+=1
    prec = float(TP) / float(TP+FP)
    rec =  float(TP) / float(TP+FN)
    return realPosCount, realNegCount, TP, TN, FP, FN, prec, rec
    
def benchmark(dbList, pmidDir, pmidOrigDir, testSvmlDir, benchTable):
    " calc prec and recall and write to tab output file "
    ofh = open(benchTable, "w")
    headers = ["db", "pmidCount", "countAtUcsc", "hereRatio", "testPos", "testNeg", "tp", "fp", "tn", "fn", "prec", "recall"]
    ofh.write("\t".join(headers))
    ofh.write("\n")
    for db in dbList:
        origPmid = len(parsePosPmids(db, pmidOrigDir))
        herePmid = len(parsePosPmids(db, pmidDir))
        hereRatio = float(herePmid)*100 / origPmid
        posPmids, negPmids, tp, fp, tn, fn, prec, recall = compSvmlClasses(testSvmlDir, db)
        row = [db, origPmid, herePmid, "%2.2f%%" % hereRatio, posPmids, negPmids, tp, fp, tn, fn, "%2.2f" % prec, "%2.2f" % recall]
        row = [str(x) for x in row]
        ofh.write("\t".join(row)+"\n")
    logging.info("Table written to %s" % benchTable)
    ofh.close()


def main(args, options):
    datasets, stepsString = args
    steps = stepsString.split(',')
    dbList = getDbList(pmidListDir)
    dataId = datasets.replace(",", "-")
    global leaveDirs
    leaveDirs = options.leaveDirs # for mkEmptyDir()

    baseDir = join(varDir, dataId)
    localBaseDir = join(localDir, dataId)
    dirs = defDirectories(baseDir, localBaseDir, datasets, options)
    addMeta = not options.noMeta
    addTwoGrams = options.twoGrams
    bestWordCount = options.bestWordCount

    logging.info("DBs defined: %s" % dbList)

    if options.onlyDbs:
        dbList = options.onlyDbs.split(",")
        assert(len(set(dbList).intersection(dbList))==len(dbList)) # dbs have to exist in pmidListDir
    
    # NOT USED ANYMORE
    if "wordCount" in steps:
        runner = pubGeneric.makeClusterRunner(__file__, "pubClassify-wordCount")
        pubExpMatrix.buildWordList(runner, datasets, options.skipMap, wordListFname)
        logging.info("raw word list created, use your own command now to reduce it to something smaller")
        logging.info("e.g. cat %s | gawk '($2<50000) && ($2>100)' | cut -f1 | lstOp remove stdin /hive/data/outside/pubs/wordFrequency/google-ngrams/fiction/top100k.tab  | lstOp remove stdin /hive/data/outside/pubs/wordFrequency/bnc/bnc.txt > %s" % (rawWordListFname, wordListFname))

    # not used
    if "corpus" in steps:
        pmidToDb  = parsePmidsDbs(pmidListDir)
        getText(dirs.textDirs, dbList, pmidToDb, dirs.corpusDir)

    if "trainCounts" in steps:
        pmids, dbs, dbPmids = parsePmids(pmidListDir)
        submitWordCountJobs(pmids, datasets, dirs.textDirs, dirs.wordCountDir, addMeta, options.twoGrams)

        wordPmids = readWordPmids3(dirs.wordCountDir)
        mkEmptyDir(dirs.wordPmidDir, options.onlyDbs)
        logging.info("Across all DBs, found %d words" % len(wordPmids))
        #writeWordPmids(wordPmids, join(dirs.wordPmidDir, db+".marshal"))
        wordPmids = filterWords(wordPmids, 20, 999999999, 20, 999999999)
        writeWordPmids(wordPmids, dirs.wordCountFname)

    if "rankWords" in steps:
        logging.info("Reading word -> pmid dict %s" % dirs.wordCountFname)
        wordPmids = marshal.load(open(dirs.wordCountFname))
        mkEmptyDir(dirs.svmlDir)
        mkEmptyDir(dirs.trainSvmlDir)
        mkEmptyDir(dirs.testSvmlDir)
        mkEmptyDir(dirs.alphaDir)
        for db in dbList:
            logging.info("Filtering most common words for DB %s" % db)
            posPmids, negPmids = parsePmidPosNeg(db, pmidListDir)
            posMin = 0.001 * len(posPmids) # remove uncommon words
            posMax = len(posPmids) # keep words that appear in all foreground
            negMin = 0.005 * len(negPmids) # remove uncommon words
            negMax = 0.90 * len(negPmids) # remove common English words
            dbWordPmids = filterWords(wordPmids, posMin, posMax, negMin, negMax)

            outFname = join(dirs.wordPmidDir, db+".counts.tab")
            bestWordFname = join(dirs.bestWordsDir, db+".txt")
            bestWords = writeWordCounts(dbWordPmids, posPmids, negPmids, outFname, \
                bestWordCount, bestWordFname)

            svmlFname = dictToSvml (dbWordPmids, bestWords, posPmids, negPmids, dirs.svmlDir, db)
            splitFile(svmlFname, 0.8, dirs.trainSvmlDir, dirs.testSvmlDir)

        svmlLearn(svmlBinDir, dirs.trainSvmlDir, dirs.modelDir, dirs.alphaDir, dbList)
        svmlClassify(svmlBinDir, dirs.testSvmlDir, dirs.modelDir, dirs.testSvmlDir, dbList)

    # for inspection
    if "wordCounts" in steps:
        for db in dbList:
            countFname = join(dirs.wordPmidDir, db+".marshal")
            counts = marshal.load(open(countFname))
            posPmids, negPmids = parsePmidPosNeg(db, pmidListDir)
            outFname = join(dirs, dirs.wordPmidDir, db+".counts.tab")
            writeWordCounts(counts, pmidPmids, negPmids, outFname)

    # NO
    if "trainCounts2" in steps:
        submitWordCountJobs2(pmidListDir, datasets, dirs.textDirs, dirs.wordCountDir+".tmp", addMeta, options.twoGrams)

    #if "splitCounts" in steps:
        #pmidToDb  = parsePmidsDbs(pmidListDir)
        #filterPmids(dirs.wordCountDir, pmidToDb, dbList, dirs.concatDir)
    # NO
    # debugging only
    if "catCounts" in steps:
        #pubAlg.concatFiles(dirs.wordCountDir, join(dirs.concatDir, "flybase.tab.gz"))
        pass

    # NO
    if "wordPmids" in steps:
        pmids, dbs, dbPmids = parsePmids(pmidListDir)
        wordPmids, donePmids = readWordPmids2(dirs.wordCountDir, pmids)
        #wordPmids, donePmids = readWordPmids(dirs.wordCountDir, pmids)
        writeWordPmids(wordPmids, "temp.marshal")
        minCount = 0.01 * len(donePmids)
        maxCount = 0.5 * len(donePmids)
        wordPmids = removeWords(wordPmids, minCount, maxCount)
        writeWordPmids(wordPmids, dirs.dataCountFname)

    # for debugging
    if "getCounts" in steps:
        ofh = open("counts.tsv", "w")
        d = marshal.load(open(dirs.dataCountFname))
        for word, pmids in d.iteritems():
            ofh.write("%s\t%d\n" % (word, len(pmids)))

    if "rankWords2" in steps:
        makeBestWords(dbList, dirs.dataCountFname, pmidListDir, dirs.rankedWordsDir, \
            options.bestWordCount, dirs.bestWordsDir)

    if "svml" in steps:
        #pmids, dbs, dbPmids = parsePmids(pmidListDir)
        #marshalToSvml(dbList, pmidListDir, dirs.dataCountFname, dirs.bestWordsDir, dirs.svmlDir)
        bestWords = parseBestWords(dirs.bestWordsDir, dbList)
        #dbs = list(dbs)
        textToSvml(dirs.textDirs, bestWords, dirs.clusterSvmlDir, addMeta, addTwoGrams, dirs.modelDir)

        #if "splitSvml" in steps:
        #inFnames = glob.glob(join(dirs.svmlDir, "*.svml"))
        #mkEmptyDir(dirs.trainSvmlDir)
        #mkEmptyDir(dirs.testSvmlDir)
        #for inFname in inFnames:
            #splitFile(inFname, 0.5, dirs.trainSvmlDir, dirs.testSvmlDir)

    #if "tmatrix" in steps:
        #runner = pubGeneric.makeClusterRunner(__file__, "pubClassify-tMatrix")
        #textDirs = pubConf.resolveTextDirs(datasets)
        #pubExpMatrix.runMatrixJobs(dirs.tMatrixFname, textDirs, dirs.wordListFname, None, None, \
                #options.skipMap, "pmidsvml", options.test, posPmids=pmids, negPmids=[])
        #wordCountToSvml(dbList, pmidListDir, dirs.concatDir, dirs.bestWordsDir, dirs.svmlDir)
        #logging.info("output matrix written to %s" % dirs.svmlDir)
        #splitSvml(tMatrixFname, dbs, dbPmids, svmlDir)

    # YES
    if "concat" in steps:
        summarizeResults(dbList, dirs.clusterSvmlDir, dirs.categoryFname)

    if "testHtml" in steps:
        makeHtmlTest(dbList, dirs.textDirs, dirs.testSvmlDir, pubConf.testOutHtmlDir)
        
    if "fullSvml" in steps:
        for db in dbList:
            dbBestWords = parseBestWords(dirs.bestWordsDir)
            submitToSvmlJobs(dirs.textDirs, dirs.fullSvmlDir, dbBestWords, addMeta, addTwoGrams)
        #pubExpMatrix.runMatrixJobs(fullSvmlName, dirs.textDirs, wordListFname, None, None, \
                #options.skipMap, "svml", docIdFname, options.test, docIdFname)

    if "classify" in steps:
        assert(len(textDirs)==1)
        textDir = textDirs[0]
        svmlClassify(svmlBinDir, dirs.fullSvmlDir, dirs.modelDir, dirs.classDir, dbList)
        convertSvmlResults(dIdFname, dirs.classDir, categoryFname, dbList)

    if "html" in steps:
        makeHtml(dbList, pmidListDirOrig, dirs.categoryFname, pubConf.classOutHtmlDir)
        makeHtmlDir(dbList, pubConf.classOutHtmlDir)

    if "dir" in steps:
        makeHtmlDir(dbList, pubConf.classOutHtmlDir)

    if "benchmark" in steps:
        benchmark(dbList, pmidListDir, pmidListDirOrig, dirs.testSvmlDir, dirs.benchTable)

# ----------- MAIN --------------
if args==[]:
    parser.print_help()
    exit(1)

main(args, options)
