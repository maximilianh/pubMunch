import sys, logging, optparse, os, collections, tempfile,\
    shutil, glob, array, codecs, string, re, gzip, time, socket

import maxRun, pubStore, pubConf, pubGeneric, maxCommon, bigBlat, pubAlg, unidecode
import maxbio, tabfile, maxMysql, maxTables, util
from collections import defaultdict
from os.path import *

progFile = os.path.abspath(sys.argv[0])

# name of marker counts file
MARKERCOUNTSBASE = "markerCounts.tab"
MARKERDIR = "markerBeds"

# pseudo class, for data storage
class C:
    pass

# a bed12 feature with one added field
BedxClass = collections.namedtuple("bedx", \
    ["chrom", "start", "end", "articleId", "score", "strand", "thickStart",
    "thickEnd", "itemRgb", "blockCount", "blockSizes", "blockStarts", "tSeqTypes"])

# these are needed almost everywhere
publisher = None # e.g. pmc
baseDir = None # e.g. /hive/data/inside/pubs/pmc

def appendBatchProgress(d, updateIds, step):
    " add a new line to steps.tab "
    if not isfile(d.stepProgressFname):
        batchFh = open(d.stepProgressFname, "w")
        headers = "batchId,updateIds,step,date".split(",")
        batchFh.write("\t".join(headers)+"\n")
    else:
        batchFh = open(d.stepProgressFname, "a")

    row = [d.batchId, ",".join(d.updateIds), step, time.asctime()]
    batchFh.write("\t".join(row)+"\n")

def findUnprocBatch(stepFname, allSteps):
    """ return a batchId that is not yet at the tables step and
    as tuple (batchId, missingSteps).
    return (None, None) if all batches are at tables
    """
    # parse steps.tab
    batchIds = []
    batchSteps = {}

    if not isfile(stepFname):
        logging.debug("File %s does not exist, batchId = 0" % stepFname)
        return None, []

    # parse batch info into dict batchId -> set of steps
    tp = maxTables.TableParser(stepFname)
    for row in tp.lines():
        batchSteps.setdefault(row.batchId, set()).add(row.step)

    # find batches without tables steps
    incompBatchIds = []
    for batchId, doneSteps in batchSteps.iteritems():
        if "tables" not in doneSteps:
            incompBatchIds.append(batchId)
    assert(len(incompBatchIds)<=1)

    if len(incompBatchIds)==0:
        return None, None

    incompBatchId = incompBatchIds[0]
    doneSteps = batchSteps[incompBatchId]
    for step in allSteps:
        if step not in doneSteps:
            todoSteps = allSteps[allSteps.find(step):]
            logging.debug("incomplete batch ids: %s, todoSteps %s" % (incompBatchIds, todoSteps))
            return incompBatchIds, todoSteps
    
def findUnprocUpdateIds(stepProgressFname, textDir, baseDir):
    """ 
    compare batches and <baseDir>/updates/ to find 
    all updateIds that have not been processed by the
    'annot' step in any batch.

    """
    # parse tracking file and get all updateIds
    logging.debug("Parsing tracking file %s and getting all updateIds" % stepProgressFname)
    doneUpdateIds = set()
    if isfile(stepProgressFname):
        tp = maxTables.TableParser(stepProgressFname)
        for row in tp.lines():
            if row.step=="annot":
                doneUpdateIds = row.updateIds.split(",")

    # parse
    allUpdateIds = pubStore.listAllUpdateIds(textDir)
    unprocIds = allUpdateIds.difference(doneUpdateIds)
    logging.info("Updates that have not been annotated yet: %s" % unprocIds)
    return unprocIds

def findProcessedBatches(mainBaseDir, step, currentBatchId=None):
    " get batchIds of batches that have completed a given step, removing current batchId "
    stepProgressFname = join(mainBaseDir, "steps.tab")
    oldBatchIds = set()
    for row in maxCommon.iterTsvRows(stepProgressFname):
        if step==None or row.step==step:
            oldBatchIds.add(row.batchId)

    if currentBatchId!=None and currentBatchId in oldBatchIds:
        oldBatchIds.remove(currentBatchId)
    logging.debug("Read batchIds from %s with '%s' completed: %s" % (stepProgressFname, step, oldBatchIds))
    return oldBatchIds

    #def findUnprocessedBatches(mainBaseDir, step1, step2):
    #""" get all annotated batchIds from steps.tab that 
    #had step1 run on them but not step2 """
    #stepProgressFname = join(mainBaseDir, "steps.tab")
    #step1Ids = set()
    #step2Ids = set()
    #for row in maxCommon.iterTsvRows(stepProgressFname):
        #updateIds = row.updateIds.split(",")
        #if row.step=="step1":
            #step1Ids.update(updateIds)
        #if row.step=="load":
            #step2Ids.update(updateIds)
    #
    #unprocIds = step1Ids.difference(step2Ids)
    #return unprocIds

def countUpcaseWords(baseDir, wordCountBase, textDir, updateIds):
    " submit map-reduce-style job to count uppercase words "
    mapTmpDir = join(baseDir, "mapReduceTmp")
    if isdir(mapTmpDir):
        logging.info("Deleting old directory %s" % mapTmpDir)
        shutil.rmtree(mapTmpDir)

    wordFile = join(baseDir, wordCountBase) # updates use the baseline word file
    if not isfile(wordFile): # if baseline has no wordfile, recreate it
        logging.info("Counting upcase words for protein search to %s" % wordFile)
        pubAlg.mapReduce("upcaseCount", textDir, {}, wordFile, \
            tmpDir=mapTmpDir, updateIds=updateIds, runTest=False)
    else:
        logging.info("Not counting words, file %s found" % wordFile)

    return wordFile

def runStepRange(d, allSteps, fromStep, toStep, args, options):
    """ run a range of steps, from-to, given the list of all steps
        
        parses d.baseDir/steps.tab and will only run steps that have not been 
        run before.
    """

    if fromStep not in allSteps:
        logging.error("%s is not a valid command" % fromStep)
        sys.exit(0)
    if toStep not in allSteps:
        logging.error("%s is not a valid command" % toStep)
        sys.exit(0)

    startIdx = allSteps.index(fromStep)
    endIdx   = allSteps.index(toStep)

    for stepName in allSteps[startIdx:endIdx+1]:
        logging.info("=== RUNNING STEP %s ===" % stepName)
        runStep(d.publisher, stepName, d)
        appendBatchProgress(d, d.updateIds, stepName)


def parseSteps(command):
    " parse the 'steps' command "
    stepFrom = "annot"
    stepTo   = "tables"
    if ":" in command:
        fromTo = command.split(":")[1]
        if fromTo=="all":
            stepFrom, stepTo = "annot", "tables"
        else:
            stepFrom, stepTo = fromTo.split("-")
    return stepFrom, stepTo

def lastBatchUpdateIds(stepProgressFname, textDir, baseDir):
    """ parse last line from step file and return batchId and updateIds as tuple
    batchId = ID of run of pipeline (e.g. once per week)
    updateIds = IDs of data updates in text dir (e.g. one new one every day)
    """
    lastRow = None
    if isfile(stepProgressFname):
        for row in maxCommon.iterTsvRows(stepProgressFname):
            lastRow = row
        logging.debug("stepProgressFile %s found, batchId %s, updateIds %s" % (stepProgressFname, lastRow.batchId, lastRow.updateIds))
        return lastRow.batchId, lastRow.updateIds.split(",")
    else:
        updateIds = findUnprocUpdateIds(stepProgressFname, textDir, baseDir)
        logging.debug("stepProgressFile %s not found, batchId -1, updateIds %s" % (stepProgressFname, updateIds))
        return "-1", updateIds

def initDirs(publisher, pipelineSteps):
    """ define all directories that are relevant for jobs and the updateIds 
        that need processing

        return them as an one object, d, that has attributes for all 
        batch-relevant directories and updateIds. This is something like
        a global runtime config object.
    """
    dbList = pubConf.alignGenomeOrder
    textDir = pubConf.resolveTextDir(publisher)
    if textDir==None:
        raise Exception("publisher %s can not be resolved to a directory" % publisher)

    maxCommon.mustExistDir(pubConf.pubBlatBaseDir, makeDir=True)

    d = C() # pseudo object, blame Guido, not me

    d = defineBatchDirectories(d, pubConf.pubBlatBaseDir, publisher, textDir)
    d.pipelineSteps = pipelineSteps
    d.textDir = textDir
    # get last used batchId and updatedIds or init them 
    return d

def appendAsFasta(inFilename, outObjects, maxSizes, seqLenCutoff, forceDbs=None):
    """ create <db>.<long|short>.fa files in faDir and fill them with data from
    tab-sep inFile (output file from pubRun)

    if forceDbs is a comma-sep string: do not try to infer target dbs from "dbs" field of
    seq table but instead search all sequences against all dbs in the list
    of dbs
    """
    #fileIdCnt = collections.Counter()
    #fileIdCnt[fileId]+=1
    #logging.debug("Parsing sequences from %s" % inFilename)

    for row in maxCommon.iterTsvRows(inFilename):
        if forceDbs!=None:
            dbs = forceDbs
        else:
            dbs = row.dbs
            if dbs=="":
                dbs = pubConf.defaultGenomes
            else:
                dbs = dbs.split(',')

        for db in dbs:
            annotId = int(row.annotId)
            #fileId = annotId / 100000
            fileId = annotId / (10**pubConf.ANNOTDIGITS)
            #articleId = fileId / 1000
            articleId = fileId / (10**pubConf.FILEDIGITS)
            seq = row.seq

            if len(seq)<seqLenCutoff:
                dbType = "short"
            else:
                dbType = "long"
            maxSize = maxSizes[dbType]
            outObj = outObjects[db][dbType]
            # if exceeded maxSize and at fileId-boundary
            # start a new output file
            if outObj.nuclCount > maxSize and articleId!=outObj.lastArticleId:
                outObj.file.close()
                outObj.count+=1
                newFname = join(outObj.dir, db+".%.2d.fa" % outObj.count)
                logging.debug("max size reached for %s, creating new file %s" % (db, newFname))
                outObj.file = open(newFname, "w")
                outObj.nuclCount=0
            faId = str(annotId)
            outObj.file.write(">%s\n%s\n"% (faId, seq))
            outObj.nuclCount+=len(seq)
            outObj.lastArticleId=articleId
    
def closeOutFiles(outDict):
    for typeDict in outDict.values():
        for fileObject in typeDict.values():
            logging.debug("Closing %s" % fileObject.file.name)
            fileObject.file.close()

class Object():
    pass

def createOutFiles(faDir, dbList, maxSizes):
    """ create one output file per db
        return dict[db] => dict[dbType => Object with attributes:
        file, count, dir, nuclCount
    """
    outDict = {}
    for db in dbList:
        for dbType in maxSizes:
            dbDir = join(faDir, dbType)
            if not isdir(dbDir):
                os.makedirs(dbDir)
            filename = join(dbDir, db+".00.fa")
            fh = open(filename, "w")
            logging.debug("Created file %s" % filename)
            outDict.setdefault(db, {})
            dbOut = Object()
            dbOut.dir = dbDir
            dbOut.count = 0
            dbOut.nuclCount = 0
            dbOut.file = fh
            dbOut.lastArticleId = 0
            outDict[db][dbType]= dbOut
    return outDict
        
def pubToFasta(inDir, outDir, dbList, maxSizes, seqLenCutoff, forceDbs=None):
    " convert sequences from tab format to fasta"
    maxCommon.mustBeEmptyDir(outDir, makeDir=True)
    logging.info("Converting tab files in %s to fasta in %s" % (inDir, outDir))
    inFiles = glob.glob(join(inDir, "*.tab"))
    outFileObjects = createOutFiles(outDir, dbList, maxSizes)
    pm = maxCommon.ProgressMeter(len(inFiles))
    logging.debug("Running on %d input files" % len(inFiles))
    for count, inFile in enumerate(inFiles):
        logging.debug("parsing %d of %d input files" % (count, len(inFiles)))
        appendAsFasta(inFile, outFileObjects, maxSizes, seqLenCutoff, forceDbs=forceDbs)
        pm.taskCompleted()
    closeOutFiles(outFileObjects)

def indexFilesByTypeDb(faDir, blatOptions):
    """ do a find on dir and sort files by type and db into double-dict 
    e.g. dbFaFiles["short"]["hg19"] = list of hg19 fa files 
    """
    dbFaFiles = {}
    for seqType in blatOptions:
        seqTypeDir = join(faDir, seqType)
        faFiles = glob.glob(join(seqTypeDir, "*.fa"))
        logging.debug("%d fa files found in dir %s" % (len(faFiles), seqTypeDir))
        dbFaFiles[seqType]={}
        for faName in faFiles:
            if getsize(faName)==0:
                continue
            db = basename(faName).split(".")[0]
            dbFaFiles[seqType].setdefault(db, [])
            dbFaFiles[seqType][db].append(faName)
    return dbFaFiles

def getRunner():
    " configure the cluster batch submission system and return an runner object "
    batchDir = join(baseDir, "parasol")
    if not isdir(batchDir):
        logging.info("Creating %s for parasol" % batchDir)
        os.makedirs(batchDir)
    return maxRun.Runner(batchDir=batchDir)

def submitBlatJobs(faDir, pslDir, cdnaDir=None, blatOptions=pubConf.seqTypeOptions, noOocFile=False):
    """ read .fa files from faDir and submit blat jobs that write to pslDir 
        dbs are taken from pubConf, but can be overwritten with onlyDbs 
    """
    #maxCommon.makedirs(pslDir)
    maxCommon.mustBeEmptyDir(pslDir, makeDir=True)
    splitParams = pubConf.genomeSplitParams

    # shred genomes/cdna and blat fasta onto these
    dbFaFiles = indexFilesByTypeDb(faDir, blatOptions)
    runner = getRunner()
    for seqType, dbFiles in dbFaFiles.iteritems():
        for db, faNames in dbFiles.iteritems():
            logging.debug("seqtype %s, db %s, query file file count %d" % (seqType, db, len(faNames)))
            blatOpt, filterOpt = blatOptions[seqType]
            pslTypeDir = maxCommon.joinMkdir(pslDir, seqType, db)
            logging.info("creating blat jobs: db %s, query count %d, output to %s" \
                % (db, len(faNames), pslDir))
            if cdnaDir:
                targetMask =join(cdnaDir, db, "*.2bit")
                targets = glob.glob(targetMask)
                logging.info("Found %s files matching %s" % (len(targets), targetMask))
                if len(targets)==0:
                    logging.warn("Skipping db %s, no target cdna file found" % (db))
                    continue
                splitTarget = False
            else:
                targets = [db]
                splitTarget = True
            jobLines = list(bigBlat.getJoblines(targets, faNames, pslTypeDir, splitParams, blatOpt, filterOpt, splitTarget=splitTarget, noOocFile=noOocFile))
            logging.info("Scheduling %d jobs" % len(jobLines))
            for line in jobLines:
                runner.submit(line)
    runner.finish(wait=True)

def clusterCmdLine(method, inFname, outFname, checkIn=True, checkOut=True):
    """ generate a cmdLine for batch system that calls this module
    with the given parameters
    """
    if checkIn:
        inFname = "{check in exists %s}" % inFname
    if checkOut:
        outFname = "{check out exists %s}" % inFname

    cmd = "%s %s %s %s %s" % (sys.executable, __file__, method, inFname, outFname)
    return cmd

def submitSortPslJobs(sortCmd, inDir, outDir, dbList, addDirs=None):
    """ submit jobs to sort psl files, one for each db"""
    #maxCommon.mustBeEmptyDir(outDir)
    maxCommon.makedirs(outDir, quiet=True)
    if addDirs:
        for addDir in addDirs:
            if not isdir(addDir):
                logging.warn("Directory %s does not exist so this is the first pass")
                logging.warn("Make sure to run 'sort' again once you have run sortCdna")
            else:
                logging.info("Adding data from directory %s, this seems to be the 2nd pass")
                inDir +=","+addDir

    runner = getRunner()
    for db in dbList:
        dbOutDir = join(outDir, db)
        maxCommon.makedirs(dbOutDir, quiet=True)
        dbOutFile = join(dbOutDir, db+".psl")
        cmd = clusterCmdLine(sortCmd, inDir, dbOutFile, checkIn=False)
        #cmd = "%s %s %s %s {check out line %s}" % (sys.executable, progFile, sortCmd, inDir, dbOutFile)
        runner.submit(cmd)
    runner.finish(wait=True)
    logging.info("If batch went through: output can be found in %s" % dbOutFile)
        
def sortDb(pslBaseDir, pslOutFile, tSeqType=None, pslMap=False):
    """ 
        pslBaseDir can be a comma-sep string with two dirs, second one will be added
        "as is". Otherwise, pslBaseDir will be resolved to <pslBaseDir>/{short,long}/<db>

        Sort psl files from inDir/genomeBlat/{short,long}/<db> to outFile
        Prefix TSeq field of psl with name of the db (e.g. "hg19,chr1")

        if liftCdna is True: use cdnaDir/<db> to lift from mrna coordinates to genome coords
    """ 

    db = splitext(basename(pslOutFile))[0]
    cdnaDir = None
    genomeLevel = False
    if "," in pslBaseDir:
        pslParts   = pslBaseDir.split(",")
        pslBaseDir = pslParts[0]
        cdnaDir    = pslParts[1]
        genomeLevel= True

    allPslInDirs = []
    for seqType in ["short", "long"]:
        pslInDir    = join(pslBaseDir, seqType, db)
        dirFileList = pubGeneric.findFiles(pslInDir, ".psl")
        pslSubDirs  = set([join(pslInDir, relDir) for relDir,fname in dirFileList])
        logging.debug("Found %d subdirs with psl files in %s" % (len(pslSubDirs), pslInDir))
        allPslInDirs.extend(pslSubDirs)

    if cdnaDir!=None:
        cdnaDbDir = join(cdnaDir, db)
        logging.debug("Adding cdna data dir %s to psl dirs" % cdnaDbDir)
        allPslInDirs.append(cdnaDbDir)
    logging.debug("Found psl dirs: %s" % allPslInDirs)
    pslInDirStr = " ".join(allPslInDirs)
    tmpDir = pubConf.getTempDir()
    tmpDir = join(tmpDir, "pubBlat-pslSort-"+db)
    if isdir(tmpDir):
        shutil.rmtree(tmpDir)
    os.makedirs(tmpDir)

    addCommand = ""
    # we're on cdna level, need to map to genome coords from mrna coords
    if pslMap:
        cdnaTargetBaseDir = pubConf.cdnaDir
        pslMapFile = join(cdnaTargetBaseDir, db, "cdna.psl")
        if not isfile(pslMapFile):
            logging.warn("Cannot find pslMap file %s, not doing any sorting" % pslMapFile)
            open(pslOutFile, "w").write("") # for parasol
            return
        addCommand += "| pslMap stdin %s stdout" % (pslMapFile)
    # add the db in front of the tName field in psl
    addCommand += """| gawk '{OFS="\\t"; if (length($0)!=0) {$14="%s,"$14",%s"; print}}' """ % (db, tSeqType)

    cmd = """pslSort dirs -nohead stdout %(tmpDir)s %(pslInDirStr)s | pslCDnaFilter stdin stdout -globalNearBest=0 -filterWeirdOverlapped -ignoreIntrons %(addCommand)s | uniq > %(pslOutFile)s """ % (locals())
    maxCommon.runCommand(cmd)
    shutil.rmtree(tmpDir)
    logging.info("Output written to %s" % pslOutFile)

def makeBlockSizes(pslList):
    """ generate bed block sizes for a bed from 
    potentially overlapping psls. Uses a sort of bitmask.
    """
    # generate bitmask of occupied positions
    pslList.sort(key=lambda f: f.tStart) # sort fts by start pos
    minStart = min([f.tStart for f in pslList])
    maxEnd = max([f.tEnd for f in pslList])
    logging.debug("Creating blockSizes for %d psls, length %d" % (len(pslList), maxEnd-minStart))
    for psl in pslList:
        logging.debug(" - %s" % str(psl))
    mask = array.array("b", [0]*(maxEnd-minStart))

    for psl in pslList:
        starts = psl.tStarts.strip(",").split(",")
        sizes = psl.blockSizes.strip(",").split(",")
        for start, size in zip(starts, sizes):
            size = int(size)
            start = int(start) - minStart
            for pos in range(start, start+size):
                mask[pos] = 1

    blockStarts = []
    blockSizes = []
    # search for consec stretches of 1s
    lastStart=None
    wasZero=True
    for i in range(0, len(mask)):
        if mask[i]==1 and wasZero:
            blockStarts.append(i)
            wasZero=False
            lastStart=i
        if mask[i]==0 and not wasZero:
            blockSizes.append(i-lastStart)
            wasZero=True
            lastStart=None
    if lastStart!=None:
        blockSizes.append(len(mask)-lastStart)
    assert(mask[len(mask)-1]==1)
    blockStarts = [str(x) for x in blockStarts]
    blockSizes = [str(x) for x in blockSizes]
    blockSizesCount = mask.count(1)
    return blockStarts, blockSizes, blockSizesCount

def pslListToBedx(chain, minCover):
    """ create bedx feature and check if chain is long enough """
    logging.debug("Converting chain with %d psls to bedx" % (len(chain)))
    blockStarts, blockSizes, blockSizeSum = makeBlockSizes(chain)
    if blockSizeSum>=minCover:
        bedNames = []
        chrom = None
        start, end = 99999999999, 0
        tSeqTypes = set()
        for psl in chain:
            name = "%s:%d-%d" % (psl.qName, psl.qStart, psl.qEnd)
            bedNames.append(name)
            db, chrom, tSeqType = psl.tName.split(",")
            tSeqTypes.add(tSeqType)
            start = min(start, psl.tStart)
            end = max(end, psl.tEnd)
        bedName = ",".join(bedNames)
        bedx = BedxClass(chrom, start, end, bedName, blockSizeSum, "+", start, end, "128,128,128", len(blockSizes), ",".join(blockSizes), ",".join(blockStarts), ",".join(tSeqTypes))
        logging.debug("final chain %s" % str(bedx))
        return bedx
    else:
        logging.debug("chain not long enough, skipping featureList, blockSizeSum is %d" % (blockSizeSum))
        for psl in chain:
            logging.debug("%s" % str(psl))
        return None

def indexByDbChrom(pslList):
    " given a list of psls, return a dict (db, chrom) -> pslList "
    pslDict = {}
    for psl in pslList:
        target = psl.tName
        db, chrom, seqType = target.split(",")
        pslDict.setdefault( (db, chrom), [] )
        pslDict[ (db, chrom) ].append(psl)
    return pslDict


def chainPsls(pslList, maxDistDict):
    """ chain features if same chrom, same articleId and closer than maxDist

        chains a query sequence only once and ignores all matches for the same chain
        return a dict chainId -> seqId -> list of psls
    """
    logging.debug("%d unchained genome hits" % len(pslList))
    chromPsls = indexByDbChrom(pslList)

    chains = {}
    for dbChrom, chromPslList in chromPsls.iteritems():
        db, chrom = dbChrom
        logging.debug("db %s, chrom %s, %d features" % (db, chrom, len(chromPslList)))
        if "_hap" in chrom:
            logging.debug("haplotype chromosome, skipping all features")
            continue
        chromPslList = maxbio.sortList(chromPslList, "tStart", reverse=False)
        chain = []
        lastEnd = None
        alreadyChained = {}
        maxDist = maxDistDict.get(db, maxDistDict["default"])
        # chain features
        for psl in chromPslList:
            if psl.qName in alreadyChained:
                oldPsl = alreadyChained[psl.qName]
                if psl.tStart==oldPsl.tStart and psl.tEnd==oldPsl.tEnd and \
                    psl.blockSizes==oldPsl.blockSizes and psl.tName!=oldPsl.tName:
                    logging.debug("same match, but different tSequenceType (cdna, prot, genome), keeping hit")
                else:
                    logging.debug("weird match, q-sequence already in this chain, skipping %s" % str(psl))
                    continue
            if len(chain)>0 and abs(int(psl.tStart) - lastEnd) > maxDist:
                chainId = chain[0].tName + "-" + str(chain[0].tStart)
                chains[chainId]=chain
                alreadyChained = {}
                chain = []
            logging.debug("Adding feature %s to chain" % str(psl))
            chain.append(psl)
            alreadyChained[psl.qName] = psl
            lastEnd = psl.tEnd
        chainId = db + "," + chrom + "-" + str(chain[0].tStart)
        chains[chainId]=chain

    # index all chains by qName to create a nested dict chainId -> seqId -> pslList
    # chainId looks like hg19,chr1,123456
    idxChains = {}
    for chainId, pslList in chains.iteritems():
        pslDict = {}
        for psl in pslList:
            pslDict.setdefault(psl.qName, []).append(psl)
        idxChains[chainId] = pslDict

    return idxChains

#def indexByDb(pslList):
    #""" index psl by db (in target name) and return as dict[db] -> list of psls
        #remove the db from the psl tName field """
    #pslByDb = {}
    #for psl in pslList:
        #db, chrom = psl.tName.split(",")
        #psl = psl._replace(tName=chrom)
        #pslByDb.setdefault(db, []).append(psl)
    #return pslByDb

def getBestElements(dict):
    """ given a dict with name -> score, keep only the ones with the highest score,
        return them as a list
    """ 
    maxScore = max(dict.values())
    result = []
    for key, value in dict.iteritems():
        if value == maxScore:
            result.append(key)
    return result

def onlyLongestChains(chains): 
    """ given a dict chainId -> annotId -> list of psls,
    return a filtered list where members are 
    mapped only to those chains with the most members. 

    e.g. we have four sequences blatted onto genomes.
    These are joined into three chains of hits.
    Chains are specified by their ids and members:
    chain1 -> (s1, s3)
    chain2 -> (s1, s2, s3)
    chain3 -> (s1, s3)
    chain4 -> (s1, s4)

    can be rewritten as
    s1 -> (chain1, chain2, chain3)
    s2 -> (chain1, chain2)
    s3 -> (chain2, chain3)
    s4 -> (chain4, chain2)

    then the weights for the chains are:
    chain1: 2
    chain2: 3
    chain3: 2
    chain4: 2

    so chain2 is kept, its sequences removed from all other chains and the process repeats
    until there are no chains left.
    """

    bestChains = {}
    while len(chains)!=0:
        # create score for chains: number of qNames, e.g. chain1->1, chain2->3
        logging.debug("Starting chain balancing with %d chains" % len(chains))
        chainScores = {}
        for chainId, qNameDict in chains.iteritems():
            chainScores[chainId] = len(qNameDict.keys())
        logging.debug("chainScores are: %s" % chainScores)

        # keep only chains with best scores, create list with their chainIds
        # and create list with  qNames of all their members
        bestChainIds = getBestElements(chainScores)
        logging.debug("Best chainIds are: %s" % str(bestChainIds))
        chainQNames = set()
        for bestChainId in bestChainIds:
            db = bestChainId.split(",")[0]
            bestChain = chains[bestChainId]
            bestChains.setdefault(db, []).append(maxbio.flattenValues(bestChain))
            for pslList in bestChain.values():
                for psl in pslList:
                    chainQNames.add(psl.qName)
        logging.debug("Best chain contains %d sequences, removing these from other chains" % len(chainQNames))

        # keep only psls with names not in chainQNames 
        newChains = {}
        for chainId, chainDict in chains.iteritems():
            newChainDict = {}
            for qName, pslList in chainDict.iteritems():
                if qName not in chainQNames:
                    newChainDict[qName]=pslList
            if len(newChainDict)!=0:
                newChains[chainId] = newChainDict
        chains = newChains
    return bestChains

def chainsToBeds(chains):
    """ convert psl chains to lists of bed features 
    Return None if too many features on any db
    Return dict db -> tuple (list of chain-beds, list of all psls for beds) otherwise
    """
    dbBeds = {}
    for db, chains in chains.iteritems():
        logging.debug("Converting %d chains on db %s to bedx" % (len(chains), db))
        dbPsls = []
        # convert all chains to bedx, filtering out chains with too many features
        bedxList = []
        for pslList in chains:
            bedx = pslListToBedx(pslList, pubConf.minChainCoverage)
            if bedx==None:
                continue
            if bedx.end - bedx.start > pubConf.maxChainLength:
                logging.debug("Chain %s is too long, >%d" % (bedx, pubConf.maxChainLength))
                continue
            bedxList.append(bedx)
            dbPsls.extend(pslList)

        if len(bedxList)==0:
            logging.debug("No bedx for db %s" % db)
            continue
        elif len(bedxList) > pubConf.maxFeatures:
            logging.warn("Too many features on db %s, skipping this article" % db)
            return None
        else:
            dbBeds[db] = (bedxList, dbPsls)
    return dbBeds


def writePslsFuseOverlaps(pslList, outFh):
    """ index psls to their seqTypes. Remove identical psls (due to genome+cdna blatting)
    and replace with one feature with seqTypes added as an additional psl field no. 22
    Add an additional psl field no 23 as the articleId.
    Write to outFh.
    """
    pslSeqTypes = {}
    for psl in pslList:
        psl = [str(p) for p in psl]
        tName = psl[13]
        chrom,db,tSeqType = tName.split(",")
        psl[13] = db
        pslLine = "\t".join(psl)
        pslSeqTypes.setdefault(pslLine, set())
        pslSeqTypes[pslLine].add(tSeqType)

    for pslLine, seqTypes in pslSeqTypes.iteritems():
        psl = pslLine.split("\t")
        psl.append("".join(seqTypes))
        #articleId = psl[9][:pubConf.ARTICLEDIGITS]
        #psl.append(articleId)
        outFh.write("\t".join(psl))
        outFh.write("\n")

def chainPslToBed(tmpPslFname, oneOutFile, dbList, maxDist, tmpDir):
    """ read psls, chain and convert to bed 
    output is spread over many files, one per db, at basename(oneOutFile).<db>.bed

    filtering out:
    - features on db with too many features for one article
    - too long chain
    """

    outBaseName = splitext(splitext(oneOutFile)[0])[0]
    logging.info("Parsing %s" % tmpPslFname)
    groupIterator = maxCommon.iterTsvGroups(open(tmpPslFname), format="psl", groupFieldNumber=9, useChars=10)
    outFiles = {} # cache file handles for speed

    for articleId, pslList in groupIterator:
        logging.info("articleId %s, %d matches" % (articleId, len(pslList)))
        chainDict = chainPsls(pslList, maxDist)
        chains    = onlyLongestChains(chainDict)
        dbBeds    = chainsToBeds(chains)

        if dbBeds!=None:
            for db, bedPslPair in dbBeds.iteritems():
                bedxList, pslList = bedPslPair
                for bedx in bedxList:
                    # lazily open file here, to avoid 0-len files
                    if db not in outFiles:
                        fname = "%s.%s.bed" % (outBaseName, db)
                        pslFname = "%s.%s.psl" % (outBaseName, db)
                        logging.info("db %s, creating file %s and %s" % (db, fname, pslFname))
                        outFiles[db] = open(fname, "w")
                        outFiles[db+"/psl"] = open(pslFname, "w")
                    outFile = outFiles[db]

                    # write all bed features
                    logging.debug("%d chained matches" % len(bedx))
                    strList = [str(x) for x in bedx]
                    outFile.write("\t".join(strList))
                    outFile.write("\n")
                outPslFile = outFiles[db+"/psl"]
                writePslsFuseOverlaps(pslList, outPslFile)

    # when no data was found on hg19, then the file was not created and parasol thinks
    # that the job hash crashed. Make Parasol happy by creating a zero-byte outfile 
    if not isfile(oneOutFile):
        logging.info("Creating empty file %s for parasol" % oneOutFile)
        open(oneOutFile, "w").write("")
            
def removeEmptyDirs(dirList):
    """ go over dirs and remove those with only empty files, return filtered list """
    filteredList = []
    for dir in dirList:
        fileList = os.listdir(dir)
        isEmpty=True
        for file in fileList:
            path = join(dir, file)
            if os.path.getsize(path) > 0:
                isEmpty = False
                break
        if not isEmpty:
            filteredList.append(dir)
    return filteredList
            
def mergeSplitChain(textDir, inDir, splitDir, bedDir, maxDbMatchCount, dbList, updateIds, addDirs=None):
    " join all psl files from each db into one big PSL for all dbs, keep best matches and re-split "
    maxCommon.mustBeEmptyDir(bedDir, makeDir=True)
    maxCommon.mustBeEmptyDir(splitDir, makeDir=True)
    inDirs = glob.glob(join(inDir, "*"))
    if addDirs:
        inDirs.extend(addDirs)

    filteredDirs = removeEmptyDirs(inDirs)
    if len(filteredDirs)==0:
        return

    mergedPslFilename = mergeFilterPsls(filteredDirs)
    articleToChunk = pubGeneric.readArticleChunkAssignment(textDir, updateIds)
    splitPsls(mergedPslFilename, splitDir, articleToChunk, maxDbMatchCount)
    os.remove(mergedPslFilename)
    submitChainFileJobs(splitDir, bedDir, dbList)

def mergeFilterPsls(inDirs):
    """ merge/sort/filter all psls (separated by db) in inDir into a temp file with all
    psls for all dbs, split into chunked pieces and write them to outDir 
    """
    tmpFile, tmpPslFname = maxCommon.makeTempFile(tmpDir=pubConf.getTempDir(), ext=".psl", prefix = "pubBlat_split")
    tmpFile.close()
    logging.debug("Merging into tmp file %s" % tmpPslFname)
    #tmpFile, tmpPslFname = open("temp.psl", "w"), "temp.psl"
    pslSortTmpDir = join(pubConf.getTempDir(), "pubBlat-sortSplitPsls")
    if isdir(pslSortTmpDir):
        shutil.rmtree(pslSortTmpDir)
    os.makedirs(pslSortTmpDir)
    logging.info("Sorting psls in %s to temp file %s" % (str(inDirs), pslSortTmpDir))
    #inDirs = glob.glob(join(inDir, "*"))
    inDirString = " ".join(inDirs)
    cmd = "pslSort dirs -nohead stdout %(pslSortTmpDir)s %(inDirString)s | pslCDnaFilter stdin %(tmpPslFname)s -minAlnSize=19 -globalNearBest=0" % locals()
    maxCommon.runCommand(cmd)
    shutil.rmtree(pslSortTmpDir)
    return tmpPslFname

def splitPsls(inPslFile, outDir, articleToChunk, maxDbMatchCount):
    " splitting psls according to articleToChunk, ignore articles with > maxDbMatchCount psls "
    logging.info("SPLIT PSL - Reading %s, splitting to directory %s" % (inPslFile, outDir))
    articleDigits = pubConf.ARTICLEDIGITS
    groupIterator = maxCommon.iterTsvGroups(inPslFile, format="psl", groupFieldNumber=9, useChars=articleDigits)

    chunkFiles = {} 
    for articleId, pslList in groupIterator:
        articleId = int(articleId)
        chunkId = articleToChunk[articleId]
        logging.debug("articleId %s, %d matches" % (articleId, len(pslList)))
        if len(pslList) >= maxDbMatchCount:
            logging.debug("Skipping %s: too many total matches" % str(pslList[0].qName))
            continue

        chunkId  = "%.5d" % (articleToChunk[int(articleId)]/pubConf.chunkDivider)
        if not chunkId in chunkFiles:
            chunkFname = join(outDir, chunkId+".psl")
            outFile = open(chunkFname, "w")
            chunkFiles[chunkId]= outFile
        else:
            outFile = chunkFiles[chunkId]

        for psl in pslList:
            pslString = "\t".join([str(x) for x in psl])+"\n"
            outFile.write(pslString)
    logging.info("Finished writing to %d files in directory %s" % (len(chunkFiles), outDir))

def submitChainFileJobs(pslDir, bedDir, dbList):
    """ submit jobs, one for each psl file in pslDir, to chain psls and convert to bed """
    maxCommon.makedirs(bedDir, quiet=True)
    pslFiles = glob.glob(join(pslDir, "*.psl"))
    logging.debug("Found psl files: %s" % str(pslFiles))
    runner = getRunner()
    for pslFname in pslFiles:
        chunkId = splitext(basename(pslFname))[0]
        # we can only check out one single output file
        # the chainFile command will write the others
        outFile = join(bedDir, chunkId+"."+dbList[0]+".bed")
        cmd = clusterCmdLine("chainFile", pslFname, outFile)
        #cmd = "%s %s job:chainFile {check in exists %s} {check out line %s}" % \
            #(sys.executable, progFile, pslFname, outFile)
        runner.submit(cmd)
    runner.finish(wait=True)
    logging.info("if batch ok: results written to %s" % bedDir)

def makeRefString(articleData):
    """ prepare a string that describes the citation: 
    vol, issue, page, etc of journal 
    """
    refParts = [articleData.journal]
    if articleData.year!="":
        refParts[0] += (" "+articleData.year)
    if articleData.vol!="":
        refParts.append("Vol "+articleData.vol)
    if articleData.issue!="":
        refParts.append("Issue "+articleData.issue)
    if articleData.page!="":
        refParts.append("Page "+articleData.page)
    return ", ".join(refParts)

def readKeyValFile(fname, inverse=False):
    """ parse key-value tab-sep text file, return as dict integer => string """
    logging.info("Reading %s" % fname)
    fh = open(fname)
    fh.readline()
    dict = {}
    for line in fh:
        fields = line.strip().split("\t")
        if len(fields)>1:
            key, value = fields
        else:
            key = fields[0]
            value = ""
        if inverse:
            key, value = value, key
        dict[int(key)] = value
    return dict

def splitAnnoIdString(annotIdString):
    """ split annot as a string into three parts 
    >>> splitAnnotId("200616640112350013")
    (2006166401, 123, 50013)
    """
    fileDigits = pubConf.FILEDIGITS
    annotDigits = pubConf.ANNOTDIGITS
    articleDigits = pubConf.ARTICLEDIGITS

    articleId = annotIdString[:articleDigits]
    fileId = annotIdString[articleDigits:articleDigits+fileDigits]
    annotId = annotIdString[articleDigits+fileDigits:]
    return articleId, fileId, annotId

def splitAnnotId(annotId):
    """
    split the 64bit-annotId into packs of 10/3/5 digits and return all
    >>> splitAnnotId(200616640112350013)
    (2006166401, 123, 50013)
    """
    fileDigits = pubConf.FILEDIGITS
    annotDigits = pubConf.ANNOTDIGITS
    articleDigits = pubConf.ARTICLEDIGITS

    annotIdInt = int(annotId)
    articleId  = annotIdInt / 10**(fileDigits+annotDigits)
    fileAnnotId= annotIdInt % 10**(fileDigits+annotDigits)
    fileId     = fileAnnotId / 10**(annotDigits)
    annotId    = fileAnnotId % 10**(annotDigits)
    return articleId, fileId, annotId

def constructArticleFileId(articleId, fileId):
    " given two integers, articleId and fileId, construct the full fileId (articleId & fileId) "
    articleFileId = (articleId*(10**(pubConf.FILEDIGITS)))+fileId
    return articleFileId

def writeSeqTables(articleDbs, seqDirs, tableDir, dbList, fileDescs, annotLinks):
    """  
        write sequences to a <tableDir>/hgFixed.sequences.tab file
        articleDbs is a dict articleId(int) -> list of dbs
        fileDescs is a dict fileId(int) -> description
    """
    # setup output files, write headeres
    logging.info("- Formatting sequence tables to genome browser format")
    dbSeqFiles = {}

    seqTableFname = join(tableDir, "hgFixed.sequenceAnnot.tab")
    seqFh = codecs.open(seqTableFname, "w", encoding="latin1")

    # iterate over seq files, find out dbs for each one and write to output file
    seqFiles = []
    for seqDir in seqDirs:
        dirSeqFiles = glob.glob(join(seqDir, "*.tab"))
        seqFiles.extend(dirSeqFiles)

    logging.info("Filtering %d files from %s to %d files in %s" % \
        (len(seqFiles), str(seqDirs), len(dbSeqFiles), tableDir))
    artWithSeqs = set()
    outRowCount = 0
    inRowCount = 0
    meter = maxCommon.ProgressMeter(len(seqFiles))
    noDescCount = 0

    for fname in seqFiles:
        for annot in maxCommon.iterTsvRows(fname):
            articleId, fileId, seqId = splitAnnotId(annot.annotId)
            dbs = articleDbs.get(articleId, None)
            if not dbs:
                logging.debug("article %d is not mapped to any genome, not writing any sequence" % articleId)
                continue
            artWithSeqs.add(articleId)
            inRowCount += 1

            # lookup file description
            annotId = int(annot.annotId)
            articleFileId = constructArticleFileId(articleId, fileId)
            fileDesc = fileDescs.get(str(articleFileId), "")

            # prep data for output table
            annotLinkList   = annotLinks.get(annotId, None)
            if annotLinkList==None:
                annotLinkString=""
            else:
                annotLinkString = ",".join(annotLinkList)

            snippet = pubStore.prepSqlString(annot.snippet)
            outRowCount+=1
            if fileDesc == "" or fileDesc==None:
                logging.debug("Cannot find file description for file id %d" % articleFileId)
                noDescCount += 1
            newRow = [ unicode(articleId), unicode(fileId), unicode(seqId), annot.annotId, pubStore.prepSqlString(fileDesc), annot.seq, snippet, annotLinkString]

            # write new sequence row
            seqFh.write(string.join(newRow, "\t"))
            seqFh.write('\n')
        meter.taskCompleted()

    logging.info("Could not find file description for %d sequences" % noDescCount)
    logging.info("%d articles have mapped sequences" % len(artWithSeqs))
    logging.info("Got %d sequences" % inRowCount)
    logging.info("Wrote %d sequences" % outRowCount)
    return artWithSeqs
            
def annotToArticleId(annotId):
    """ map from annotation ID to article Id """
    # number to convert from annotation to article IDs id that are NOT article IDs
    # need to divide by this number to get article Id from annotation ID
    articleDivider = 10**(pubConf.FILEDIGITS+pubConf.ANNOTDIGITS)
    return int(annotId) / articleDivider

def addHumanForMarkers(articleDbs, markerArticleFname):
    " add a human genome entry to articleId -> db dict, for all article Ids in file "
    humanDb = (pubConf.humanDb)
    articleCount = 0
    for articleId in open(markerArticleFname):
        articleId = articleId.strip()
        articleDbs[int(articleId)] = [humanDb]
        articleCount += 1
    logging.info("Found %d articles with markers in %s" % (articleCount, markerArticleFname))
    return articleDbs

def sanitizeYear(yearStr):
    """ make sure that the year is really a number:
    split on space, take last element, remove all non-digits, return "0" if no digit found """
    nonNumber = re.compile("\D")
    lastWord = yearStr.split(" ")[-1]
    yearStrClean = nonNumber.sub("", lastWord)
    if yearStrClean=="":
        return "0"
    try:
        year = int(yearStrClean)
    except:
        logging.warn("%s does not look like a year, cleaned string is %s" % (yearStr, yearStrClean))
        year = 0
    return str(year)

def firstAuthor(string):
    " get first author family name and remove all special chars from it"
    string = string.split(" ")[0].split(",")[0].split(";")[0]
    string = "\n".join(string.splitlines()) # get rid of crazy unicode linebreaks
    string = string.replace("\m", "") # old mac text files
    string = string.replace("\n", "")
    string = unidecode.unidecode(string)
    return string

def writeArticleTables(articleDbs, textDir, tableDir, dbList, updateIds):
    """ 
        create the articles table based on articleDbs, display Ids and 
        the zipped text file directory.

        also create processArticles.tab for JSON elsevier script to distinguish between
        processed and no-sequence articles.

    """

    logging.info("- Formatting article information to genome browser format")
    # prepare output files
    artFiles = {}

    articleFname    = join(tableDir, "hgFixed.article.tab")
    articleFh       = codecs.open(articleFname, "w", encoding="utf8")
    extIdFh         = open(join(tableDir, "publications.processedArticles.tab"), "w")

    logging.info("Writing article titles, abstracts, authors")
    articleDict = {}
    articleCount = 0
    for articleData in pubStore.iterArticleDataDir(textDir, updateIds=updateIds):
        artId = int(articleData.articleId)
        extIdFh.write(articleData.articleId+"\t"+articleData.externalId+"\t"+articleData.doi+"\n")
        artDbs = articleDbs.get(artId, None)
        if not artDbs:
            continue
        logging.debug("article %d has dbs %s" % (artId, str(artDbs)))

        dbString = ",".join(artDbs)
        refString = makeRefString(articleData)
        pmid = str(articleData.pmid)
        if pmid=="" or pmid=="NONE":
            pmid = 0
        
        eIssn = articleData.eIssn
        if eIssn=="":
            eIssn = articleData.printIssn

        articleRow =  (str(artId), articleData.externalId, \
                       str(pmid), str(articleData.doi), str(articleData.source), \
                       pubStore.prepSqlString(refString), \
                       pubStore.prepSqlString(articleData.journal), \
                       pubStore.prepSqlString(eIssn), \
                       pubStore.prepSqlString(articleData.vol), \
                       pubStore.prepSqlString(articleData.issue), \
                       pubStore.prepSqlString(articleData.page), \
                       sanitizeYear(articleData.year), \
                       pubStore.prepSqlString(articleData.title), \
                       pubStore.prepSqlString(articleData.authors), \
                       firstAuthor(articleData.authors), \
                       pubStore.prepSqlString(articleData.abstract), \
                       articleData.fulltextUrl, dbString)
        articleFh.write(u'\t'.join(articleRow))
        articleFh.write(u'\n')
        articleCount+=1
    logging.info("Written info on %d articles to %s" % (articleCount, tableDir))

def parseBeds(bedDirs):
    """ open all bedFiles in dir, parse out the article Ids from field no 13 (12 zero based),
        return dictionary articleId -> set of dbs and a list of all occuring annotation IDs
        return dictionary annotationId -> list of coordStrings, like hg19/chr1:2000-3000
    """
    logging.info("- Creating a dictionary articleId -> db from bed files")
    bedFiles = []
    for bedDir in bedDirs:
        dirBeds = glob.glob(join(bedDir, "*.bed"))
        bedFiles.extend(dirBeds)
        logging.info("Found %d bed files in directory %s" % (len(dirBeds), bedDir))

    logging.info("Parsing %d bed files" % len(bedFiles))
    articleDbs = {}
    dbPointers = {}
    annotToCoord = {}
    pm = maxCommon.ProgressMeter(len(bedFiles))
    for bedFname in bedFiles:
        db = splitext(basename(bedFname))[0].split(".")[0]
        dbPointers.setdefault(db, db) # this should save some memory
        db = dbPointers.get(db)       # by getting a pointer to a string instead of new object
        for line in open(bedFname):
            fields = line.split()
            chrom, start, end = fields[:3]
            coordString = "%s/%s:%s-%s" % (db, chrom, start, end)
            articleIdStr = fields[3]
            articleId = int(articleIdStr)

            annotString = fields[12]
            annotStrings = annotString.split(",")
            annotIds = [int(x) for x in annotStrings]
            for annotId in annotIds:
                annotToCoord.setdefault(annotId, [])
                annotToCoord[annotId].append(coordString)
            #articleId = annotToArticleId(annotIds[0])
            articleDbs.setdefault(articleId, set()).add(db)
        pm.taskCompleted()
    logging.info("Found %d articles with sequences mapped to any genome" % len(articleDbs))
    return articleDbs, annotToCoord

def parseAnnotationIds(pslDir):
    " read all bed files and parse out their annotation IDs "
    #dummy, annotIdDict = parseBeds([bedDir])
    #annotIds = set(annotIdDict.keys())
    pslDirs = glob.glob(join(pslDir, "*"))
    pslFiles = []
    for pslDir in pslDirs:
        pslFiles.extend(glob.glob(join(pslDir, "*")))

    tm = maxCommon.ProgressMeter(len(pslFiles))
    qNames = set()
    for pslDir in pslDirs:
        pslFiles = glob.glob(join(pslDir, "*"))
        for pslFname in pslFiles:
            logging.debug("reading qNames from %s" % pslFname)
            fileQNames = []
            for line in open(pslFname):
                qName = line.split("\t")[9]
                fileQNames.append(qName)
            qNames.update(fileQNames)
            tm.taskCompleted()
    return qNames

def stripArticleIds(hitListString):
    """ remove articleIds from hitList string
        input: a string like 01234565789001000000:23-34,01234567890010000001:45-23 
        return: ("0123456789001000000,01234567890010000001", "23-34,45-23")
    """
    artChars = pubConf.ARTICLEDIGITS
    articleId = hitListString[:artChars]
    seqIds = []
    matchRanges = []
    for matchStr in hitListString.split(","):
        parts = matchStr.split(":")
        seqIds.append(parts[0])
        matchRanges.append(parts[1])

    return articleId, ",".join(seqIds), ",".join(matchRanges)

def findBedPslFiles(bedDirs):
    " find all pairs of bed and psl files with same basename in input dirs and return list of basenames "
    # get all input filenames
    basenames   = []
    for bedDir in bedDirs:
        bedFiles = glob.glob(join(bedDir, "*.bed"))
        pslFiles = glob.glob(join(bedDir, "*.psl"))
        assert(len(bedFiles)==len(pslFiles))
        logging.info("Found %d files in dir %s" % (len(bedFiles), str(bedDir)))
        for bedName in bedFiles:
            basenames.append( splitext(bedName)[0] )

    logging.info("Total: %d bed/psl files %d input directories" % (len(basenames), \
        len(bedDirs)))
    return basenames

def readReformatBed(bedFname):
    " read bed, return as dict, indexed by articleId "
    bedLines = {}
    for line in open(bedFname):
        fields = line.strip("\n").split("\t")
        bedName   = fields[3]
        articleId, seqIdsField, seqRangesField = stripArticleIds(bedName)
        fields[3] = articleId
        fields.append(seqIdsField)
        fields.append(seqRangesField)
        fields[5] = "" # remove strand 
        articleIdInt = int(articleId)
        bedLine = "\t".join(fields) 
        bedLines.setdefault(articleIdInt, []).append(bedLine)
    return bedLines


def openBedPslOutFiles(basenames, dbList, tableDir):
    """ open two file handles for each basename like /path/path2/0000.hg19.bed
    and return as two dict db -> filehandle """
    outBed = {}
    outPsl = {}
    for db in dbList:
        outFname    = join(tableDir, db+".blat.bed")
        outBed[db] = open(outFname, "w")
        outPslFname = join(tableDir, db+".blatPsl.psl")
        outPsl[db] = open(outPslFname, "w")
    return outBed, outPsl

def appendPslsWithArticleId(pslFname, articleIds, outFile):
    " append all psls in pslFname that have a qName in articleIds to outFile, append a field"
    for line in open(pslFname):
        psl = line.strip().split("\t")
        qName = psl[9]
        articleId = annotToArticleId(qName)
        if articleId in articleIds:
            articleIdStr = psl[9][:pubConf.ARTICLEDIGITS]
            psl.append(articleIdStr)
            outFile.write("\t".join(psl))
            outFile.write("\n")

def rewriteFilterBedFiles(bedDirs, tableDir, dbList):
    """ add extended column with annotationIds
    """
    logging.info("- Formatting bed files for genome browser")
    basenames = findBedPslFiles(bedDirs)
    outBed, outPsl = openBedPslOutFiles(basenames, dbList, tableDir)
        
    featCounts    = {}
    dropCounts    = {}
    dropArtCounts = {}
    logging.info("Concatenating and reformating bed and psl files to bed+/psl+")
    pm = maxCommon.ProgressMeter(len(basenames))
    for inBase in basenames:
        bedFname = inBase+".bed"
        pslFname = inBase+".psl"
        bedBase = basename(bedFname)
        db      = bedBase.split(".")[1]
        outName = join(tableDir, bedBase)
        featCounts.setdefault(db, 0)
        dropCounts.setdefault(db, 0)
        dropArtCounts.setdefault(db, 0)

        logging.debug("Reformatting %s to %s" % (bedFname, outName))

        bedLines = readReformatBed(bedFname)
        articleIds = set()
        outFh   = outBed[db]
        for articleId, bedLines in bedLines.iteritems():
            for lineNew in bedLines:
                outFh.write(lineNew)
                outFh.write("\n")
                featCounts[db] += 1
            articleIds.add(articleId)

        appendPslsWithArticleId(pslFname, articleIds, outPsl[db])
        pm.taskCompleted()

    logging.info("features that were retained")
    for db, count in featCounts.iteritems():
        logging.info("Db %s: %d features kept, %d feats (%d articles) dropped" % (db, count, dropCounts[db], dropArtCounts[db]))
    logging.info("bed output written to directory %s" % (tableDir))
    
def mustLoadTable(db, tableName, tabFname, sqlName, append=False):
    if append:
        appendOpt = "-oldTable "
    else:
        appendOpt = ""

    if isfile(tabFname):
        cmd = "hgLoadSqlTab %s %s %s %s %s" % (db, tableName, sqlName, tabFname, appendOpt)
        maxCommon.runCommand(cmd, verbose=False)
    else:
        logging.warn("file %s not found" % tabFname)

def mustLoadBed(db, tableName, bedFname, sqlTable=None, append=False):
    if isfile(bedFname):
        opts = "-tab"
        if append:
            opts = opts+ " -oldTable "

        if sqlTable:
            opts = opts + " -sqlTable=%s -renameSqlTable" % sqlTable
        cmd = "hgLoadBed %s %s %s %s -tab" % (db, tableName, bedFname, opts)
        maxCommon.runCommand(cmd, verbose=False)
    else:
        logging.error("file %s not found" % bedFname)
        sys.exit(0)

def loadTable(db, tableName, fname, sqlName, fileType, tableSuffix, appendMode):
    """ load tab sql table or bed file, append tableSuffix if not append """
    if not isfile(fname):
        logging.warn("File %s not found, skipping" % fname)
        return 
    if getsize(fname)==0:
        logging.warn("File %s has zero file size, skipping" % fname)
        return

    # seq match bed file
    if not appendMode:
        tableName = tableName+tableSuffix

    if fileType=="bed":
        mustLoadBed(db, tableName, fname, sqlName, appendMode)
    else:
        mustLoadTable(db, tableName, fname, sqlName, appendMode)

    return tableName

def addCounts(countDict, fname):
    " parse line of file with format <id>tab<count>, add counts to dict, return dict "
    logging.debug("Parsing %s" % fname)
    for line in open(fname):
        line = line.strip()
        id, count = line.split("\t")
        count = int(count)
        countDict[id]+=count
    return countDict

def upcaseFirstLetter(string):
    return string[0].upper() + string[1:] 

def filterBedAddCounts(oldBed, newBed, counts):
    " write bed from oldBed to newBed, keeping only features name in counts, add one field with count "
    logging.info("Filtering bed %s to %s" % (oldBed, newBed))
    ofh = open(newBed, "w")
    readCount = 0
    writeCount = 0
    for line in open(oldBed):
        fields = line.strip().split("\t")
        name = fields[3]
        count = counts.get(name, 0)
        readCount += 1
        if count==0:
            continue
        fields.append("%d" % count)
        ofh.write("\t".join(fields))
        ofh.write("\n")
        writeCount += 1
    logging.info("Kept %d features out of %d features" % (writeCount, readCount))
    
def countMarkers(baseDirs, markerCountFname):
    """ go over all base dirs and all batches therein and count how often a marker appears 
    uses markerCountFname, a table with <marker>tab<count> created by the 'tables' step
    """
    counts = defaultdict(int)
    for baseDir in baseDirs:
        logging.info("Reading counts from %s" % baseDir)
        # names of marker files
        markerCountNames = []
        batchIds = findProcessedBatches(baseDir, "tables")
        for batchId in batchIds:
            fname = join(baseDir, "batches", batchId, markerCountFname)
            if isfile(fname):
                logging.debug("Found %s" % fname)
                markerCountNames.append(fname)
            else:
                logging.warn("Not found: %s" % fname)

        # parse marker count files
        for markerCountName in markerCountNames:
            counts = addCounts(counts, markerCountName) # e.g. {"rs123231":13, "TP53":5000}
    return counts

def findRewriteMarkerBeds(db, baseDirs, markerCountFname, markerDbDir, markerOutDir, skipSnps, tableSuffix=""):
    """
    search baseDir and updates for markerCounts.tab to get a list of counts for each
    marker. Use this dictionary to filter the bed files in markerDbDir, add the counts an
    extended bed field, write beds to <baseName>/markerBeds/<db>.marker<type>.bed 
    
    return fileDict as ["marker"<type>]["hgFixed"] -> file name
    """
    logging.info("Writing marker bed files to %s, adding counts of matching articles" % markerOutDir)
    if not isdir(markerOutDir):
        os.mkdir(markerOutDir)
    else:
        if len(os.listdir(markerOutDir))!=0:
            logging.info("Deleting all files in %s" % markerOutDir)
            shutil.rmtree(markerOutDir)
            os.mkdir(markerOutDir)

    counts = countMarkers(baseDirs, markerCountFname)

    markerBeds = glob.glob(join(markerDbDir, "*.bed"))
    markerTypes = [splitext(basename(x))[0] for x in markerBeds] # e.g. ["band", "snp"]

    fileDict = {}
    for markerType in markerTypes:
        if skipSnps and markerType=="snp":
            logging.info("Skipping SNPs to gain speed")
            continue
        upMarkerType = upcaseFirstLetter(markerType) # e.g. Band
        oldBed = join(markerDbDir, markerType+".bed") 
        newBed = join(markerOutDir, db+".marker%s.bed" % upMarkerType)
        filterBedAddCounts(oldBed, newBed, counts)

        tableName = "marker"+upMarkerType # e.g. markerBand
        fileDict[(tableName, "bed")] = {}
        fileDict[(tableName, "bed")][db] = [newBed]
    return fileDict

def findUpdates(baseDir, updateId):
    " search baseDir for possible update directories, if updateId !=0 otherwise return just updateId "
    updates = []
    updateDir = join(baseDir, "updates")
    if updateId!=None:
        logging.debug("Baseline loading, only loading updateId %s" % updateId)
        updates = [updateId]
    elif updateId==None and isdir(updateDir):
        updates = os.listdir(updateDir)
        logging.debug("Baseline loading, also running on these updates: %s" % updates)
    return updates

#def cleanupDb(prefix, db):
    #" drop temporary pubs loading tables "
    #maxMysql.dropTablesExpr(db, prefix+"%New")
    #maxMysql.dropTablesExpr(db, prefix+"%Old")
    
#def safeRenameTables(newTableNames, suffix, tmpSuffix):
    #" Rename all tables "
    #finalTableNames = [t.replace(suffix, "") for t in newTableNames]
    #oldTableNames = [t+"Old" for t in finalTableNames]
    #logging.debug("Safe Renaming: new names: %s" % (newTableNames))
    #logging.debug("Safe Renaming: old names: %s" % (oldTableNames))
    #logging.debug("Safe Renaming: final names: %s" % (finalTableNames))

    #maxMysql.renameTables("hg19", finalTableNames, oldTableNames, checkExists=True)
    #maxMysql.renameTables("hg19", newTableNames, finalTableNames)
    #maxMysql.dropTables("hg19", oldTableNames)

def loadTableFiles(dbTablePrefix, fileDict, dbList, sqlDir, appendMode, suffix="", dropFirst=False):
    """ load all article and seq tables for a list of batchIds 
    return list of loaded tables in format: <db>.<tableName> 
    """
    logging.debug("Loading tables from %s for %s, append Mode %s" % (fileDict, dbList, appendMode))

    #for db in dbList:
        #cleanupDb(dbTablePrefix, db)
    #cleanupDb(dbTablePrefix, "hgFixed")

    # dropFirst: remove all tables before loading them
    if dropFirst:
        logging.info("Before appending to marker bed tracks, dropping the old ones first")
        dropTables = {}
        for (tableBaseName, fileType), dbFnames in fileDict.iteritems():
            upTableBase = upcaseFirstLetter(tableBaseName)
            for db, fnames in dbFnames.iteritems():
                dbTableName = dbTablePrefix + upTableBase
                dropTables.setdefault(db, set()).add(dbTableName)
        for db, tableNames in dropTables.iteritems():
            maxMysql.dropTables(db, tableNames )

    sqlFilePrefix = "pubs"
    dbTables = set()
    for (tableBaseName, fileType), dbFnames in fileDict.iteritems():
        upTableBase = upcaseFirstLetter(tableBaseName)
        for db, fnames in dbFnames.iteritems():
            for fname in fnames:
                # find the right .sql file
                if tableBaseName.startswith("marker") and not tableBaseName.startswith("markerAnnot"):
                    sqlName = join(sqlDir, sqlFilePrefix+"Marker.sql")
                else:
                    sqlName = join(sqlDir, sqlFilePrefix+upTableBase+".sql")

                dbTableName = dbTablePrefix + upTableBase
                loadedName = loadTable(db, dbTableName, fname, sqlName, fileType, suffix, appendMode)
                if loadedName!=None:
                    dbTables.add(db+"."+loadedName)

    logging.debug("Loaded these tables: %s" % dbTables)
    return dbTables
        
def findTableFiles(baseDir, batchIds, ignoreFilenames):
    """ find all bed/tab/psl files for blat matches. add to fileDict as (tableName, fileExt) -> 
        dict of db -> list of files, then return fileDict. 

    >>> findTableFiles("/hive/data/inside/literature/blat/miniEls", ["0"])
    """
    fileDict = {}
    logging.debug("Searching for all table files in %s" % baseDir)
    for batchId in batchIds:
        tableDir = join(baseDir, "batches", batchId, "tables")
        for tableFname in os.listdir(tableDir):
            tablePath = join(tableDir, tableFname)
            if tablePath in ignoreFilenames:
                logging.debug("file %s has already been loaded, skipping" % tablePath)
                continue
            if getsize(tablePath)==0:
                logging.debug("file %s has 0 size, skipping" % tablePath)
                continue
            fields = tableFname.split(".")
            if len(fields)!=3:
                logging.debug("file %s has wrong file format (not db.table.ext), skipping " % tablePath)
                continue
            db, table, ext = fields
            fileDict.setdefault((table, ext), {})
            fileDict[(table, ext)].setdefault(db, [])
            fileDict[(table, ext)][db].append(tablePath)

    logging.debug("Found these files: %s" % fileDict)
    return fileDict

def queryLoadedFnames(db, table):
    """ connect to mysql db and read loaded filenames from table pubsLoadedFile, 
        return as dict fname => (fsize (int) , time) """
    logging.debug("Loading already loaded filenames from table %s" % table)
    rows = maxMysql.hgGetAllRows(db, table, pubConf.TEMPDIR)
    data = {}
    for row in rows:
        fname, fsize, time = row
        if fname in data:
            raise Exception("fname %s appears twice in table %s" % (fname, table))
        data[fname] = (int(fsize), time)
    return data

def createLoadedFileTable(sqlDir, procDb, tableName):
    " create pubsLoadedFile table "
    logging.debug("Creating new table %s" % tableName)
    sqlFname = join(sqlDir, "pubsLoadedFile.sql")
    #cmd = 'hgsql %s < %s' % (procDb, sqlFname)
    #maxCommon.runCommand(cmd)
    maxMysql.execSqlCreateTableFromFile(procDb, sqlFname, tableName)

def getLoadedFiles(procDb, procTable):
    " read already loaded files from mysql tracking trable or create an empty one "
    sqlDir = pubConf.sqlDir

    if maxMysql.tableExists(procDb, procTable):
        alreadyLoadedFnames = queryLoadedFnames(procDb, procTable)
        logging.debug("These files have already been loaded: %s" % alreadyLoadedFnames)
    else:
        createLoadedFileTable(sqlDir, procDb, procTable)
        alreadyLoadedFnames = []
    return alreadyLoadedFnames

def appendFilenamesToSqlTable(fileDicts, trackDb, trackingTable, ignoreDir):
    " given dict (table, ext) -> list of filenames, write filenames/size to mysql table "
    # example fileDicts: 
    # [{(u'blatPsl', u'psl'): {u'xenTro2': [u'/hive/data/inside/literature
    # /blat/pmc/batches/1_0/tables/xenTro2.blatPsl.psl']}}]
    logging.debug("FileDicts is %s, appending these to tracking table %s" % (fileDicts, trackingTable))
    for fileDict in fileDicts:
        for dbFnames in fileDict.values():
            for db, fileNameList in dbFnames.iteritems():
                for fname in fileNameList:
                    if ignoreDir in fname:
                        logging.debug("not appending %s, is in temporary dir" % fname)
                        continue
                    fileSize = os.path.getsize(fname)
                    maxMysql.insertInto(trackDb, trackingTable, ["fileName","size"], [fname, fileSize])

def checkBaseDirsExist(baseDirs):
    " check if all baseDirs exist, is they don't exist, try <pubBlatBaseDir>/<baseDir> "
    if len(baseDirs)==0:
        logging.error("You have not specified a single baseDir to load data from")
        sys.exit(1)
        
    newDirs = []
    for baseDir in baseDirs:
        if not isdir(baseDir):
            baseDir2 = baseDir
            baseDir  = join(pubConf.pubBlatBaseDir, baseDir)
            if not isdir(baseDir):
                raise Exception("Cannot find directory %s nor %s" % (baseDir, baseDir2))
        newDirs.append(baseDir)
    logging.debug("BaseDirs for input tables are %s" % newDirs)
    return newDirs


def loadTables(baseDirs, dbList, markerCountBasename, markerOutDir, userTablePrefix, skipSnps):
    """ 
        Loads files that are NOT yet in hgFixed.pubLoadedFile

        This script is not using the rename+load scheme of Markd's genbank pipeline.
        If anything fails, the tables have to be all loaded again.
    """
    tablePrefix = "pubs"
    tablePrefix = tablePrefix + userTablePrefix

    sqlDir          = pubConf.sqlDir
    markerDbDir     = pubConf.markerDbDir
    trackingTable   = tablePrefix+"LoadedFiles"
    trackingDb      = "hgFixed"
    loadedFilenames = getLoadedFiles(trackingDb, trackingTable)
    append          = (len(loadedFilenames) != 0) # only append if there is already old data
    baseDirs        = checkBaseDirsExist(baseDirs)

    # first create the marker bed files (for all basedirs) and load them
    # this is separate because we pre-calculate the counts for all marker beds
    # instead of doing this in hgTracks on the fly
    tempMarkerDir = "markerBedTemp"
    markerFileDict = findRewriteMarkerBeds("hg19", baseDirs, markerCountBasename, \
        markerDbDir, tempMarkerDir, skipSnps)
    markerTables = loadTableFiles(tablePrefix, markerFileDict, dbList, sqlDir, append, dropFirst=True)

    fileDicts = [markerFileDict]
    tableNames = set(markerTables)
    # now load non-marker data from each basedir
    for baseDir in baseDirs:
        # find name of table files
        batchIds = findProcessedBatches(baseDir, "tables")
        fileDict = findTableFiles(baseDir, batchIds, loadedFilenames)
        fileDicts.append(fileDict)

        # load tables into mysql
        dirTableNames = loadTableFiles(tablePrefix, fileDict, dbList, sqlDir, append)
        tableNames.update(dirTableNames)
        append = True # the second baseDir must always append to the tables

    # update tracking table with filenames
    appendFilenamesToSqlTable(fileDicts, trackingDb, trackingTable, tempMarkerDir)

def submitFilterJobs(inDirs, chunkNames, outDirs):
    """ submit jobs to clear annotation file from duplicate sequences"""
    logging.info("Filtering sequences: Removing duplicates and short sequences")
    for outDir in outDirs:
        maxCommon.mustBeEmptyDir(outDir, makeDir=True)

    runner = getRunner()
    for inDir, outDir in zip(inDirs, outDirs):
        logging.info("Reading from %s, writing to %s" % (inDir, outDir))
        #logging.debug("Found %d files in %s" % (len(inFiles), inDir))
        for chunkName in chunkNames:
            inFname = join(inDir, chunkName, ".tab.gz")
            #inBase = basename(inFname).replace(".gz","")
            outFname = join(outDir, chunkName, ".tab")
            #cmd = "%s %s job:filterSeqFile {check in exists %s} {check out exists %s}" % (sys.executable, progFile, inFname, outFname)
            cmd = clusterCmdLine("filterSeqFile", inFname, outFname)
            runner.submit(cmd)
    runner.finish(wait=True)
        
def filterSeqFile(inFname, outFname):
    " skip annotation lines if sequence has been seen for same article "
    alreadySeenSeq = {} # to ignore duplicated sequences
    outFh = codecs.open(outFname, "w", encoding="utf8")

    headerLine = gzip.open(inFname).readline()
    outFh.write(headerLine)

    logging.debug("Filtering file %s" % inFname)
    for row in maxCommon.iterTsvRows(inFname, encoding="utf8"):
        articleId, dummy1, dummy2 = splitAnnotId(row.annotId)
        alreadySeenSeq.setdefault(articleId, set())
        if row.seq in alreadySeenSeq[articleId]:
            continue
        if len(row.seq) < pubConf.minSeqLen:
            continue
        alreadySeenSeq[articleId].add(row.seq)
        outFh.write(u"\t".join(row))
        outFh.write("\n")
    outFh.close()

def writeUnmappedSeqs(annotIds, inDir, outDir):
    """ read all tab files in seqDir, skip all seqs with annotIds, 
    write all others to unmapDir """
    logging.info("Writing sequences that do not match genome to cdna files")
    maxCommon.mustBeEmptyDir(outDir, makeDir=True)
    inFiles = glob.glob(join(inDir, "*.tab"))
    logging.info("Found %d .tab files in %s" % (len(inFiles), inDir))
    pm = maxCommon.ProgressMeter(len(inFiles))

    for inFname in inFiles:
        logging.debug("Filtering sequence file %s" % inFname)
        inBase = basename(inFname)
        outFname = join(outDir, inBase)
        outFh = codecs.open(outFname, "w", encoding="utf8")
        headerLine = open(inFname).readline()
        outFh.write(headerLine)
        for row in maxCommon.iterTsvRows(inFname):
            annotId = int(row.annotId)
            if annotId in annotIds:
                continue
            else:
                outFh.write("\t".join(row))
                outFh.write("\n")
        outFh.close
        pm.taskCompleted()
    
def liftCdna(inDir, outDir):
    " lift cdna psl files to genome coord psls"
    dbList  = pubConf.alignGenomeOrder
    cdnaDir = pubConf.cdnaDir
    maxCommon.mustBeEmptyDir(outDir, makeDir=True)
    for db in dbList:
        logging.info("Lifting CDna of db %s" % db)
        pslFile = join(inDir, db+".psl")
        outFile = join(outDir, db+".psl")

        mapMask = join(cdnaDir, db, "*.psl")
        mapPsls = glob.glob(mapMask)
        if len(mapPsls)==0:
            logging.warn("File %s not found, skipping organism" % mapMask)
            continue
        mapPsl = mapPsls[0]
        
        if not isfile(pslFile) or not isfile(mapPsl):
            logging.warn("File %s not found, skipping organism")
            continue

        assert(len(mapPsls)<=1)
        
        cmd = "pslMap %s %s %s" % (pslFile, mapPsl, outFile)
        maxCommon.runCommand(cmd)

    
    
def rewriteMarkerAnnots(markerAnnotDir, db, tableDir, fileDescs, markerArticleFile, markerCountFile):
    " reformat marker annot tables for mysql, write articleIds to file "
    # open outfiles
    idFh = open(markerArticleFile, "w")
    markerCountFh = open(markerCountFile, "w")
    outFname = join(tableDir, db+".markerAnnot.tab")
    tmpFname = tempfile.mktemp(dir=pubConf.TEMPDIR, prefix=db+".markerAnnot", suffix=".unsorted.tab")
    logging.info("Rewriting marker tables from %s to %s, articleIds to %s" \
        % (markerAnnotDir, tmpFname, markerArticleFile))
    outFile = codecs.open(tmpFname, "w", encoding="utf8")

    # init vars
    fnames = glob.glob(join(markerAnnotDir, "*.tab.gz"))
    meter = maxCommon.ProgressMeter(len(fnames))
    outRowCount = 0
    markerCounts = defaultdict(int) # store the count of articles for each marker

    for fname in fnames:
        fileMarkerArticles = defaultdict(set) # the list of article Ids for each marker in current file
        for row in maxCommon.iterTsvRows(fname):
            articleId, fileId, annotId = splitAnnoIdString(row.annotId)
            fullFileId = articleId+fileId
            snippet = pubStore.prepSqlString(row.snippet)
            markerCounts
            newRow = [articleId, fileId, annotId, unicode(fileDescs.get(fullFileId, "")), \
                row.type, row.id, row.section, unicode(snippet)]
            fileMarkerArticles[row.id].add(articleId)

            outFile.write(u'\t'.join(newRow))
            outFile.write('\n')
            outRowCount+=1

        articleIds = set()
        for markerId, articleIdSet in fileMarkerArticles.iteritems():
            markerCounts[markerId]+= len(articleIdSet)
            articleIds.update(articleIdSet)

        for articleId in articleIds:
            idFh.write(articleId+"\n")
        meter.taskCompleted()
    logging.info("Wrote %d rows to %s for %d markers" % (outRowCount, tmpFname, len(markerCounts)))

    # sort table by markerId = field 6 
    util.sortTable(tmpFname, outFname, 6)
    os.remove(tmpFname)

    logging.info("Writing marker counts")
    for markerId, count in markerCounts.iteritems():
        markerCountFh.write("%s\t%d\n" % (markerId, count))
        
def writeChunkNames(chunkNames, path):
    " write list of chunknames to file "
    logging.info("Writing names of %d chunks to %s" % (len(chunkNames), path))
    chunkNameFh = open(path, "w")
    chunkNameFh.write("chunkName\n")
    for chunkName in chunkNames:
        chunkNameFh.write(basename(chunkName)+"\n")

def iterChunkNames(path):
    " yield chunknames from path "
    for row in maxCommon.iterTsvRows(path):
        yield row.chunkName

def defineBatchDirectories(d, pubBlatBase, publisher, textDir, newBatchId=None):
    " add attributes for all input and output directories to object d"

    " DEFINE DIRECTORIES "

    # base dir for publisher
    d.baseDir = join(pubBlatBase, publisher)
    global baseDir
    baseDir = d.baseDir

    # pipeline progress table file
    d.batchBasename = "steps.tab"
    d.stepProgressFname = join(d.baseDir, d.batchBasename)

    # read current batch id from progress file if not supplied
    if newBatchId==None:
        d.batchId, d.updateIds = lastBatchUpdateIds(d.stepProgressFname, textDir, d.baseDir)
    else:
        d.updateIds = findUnprocUpdateIds(d.stepProgressFname, textDir, baseDir)
        d.batchId = newBatchId

    # define batch dir
    batchDir = join(d.baseDir, "batches", d.batchId)
    logging.info("batchDir directory is %s" % batchDir)
    d.batchDir = batchDir
    d.publisher = publisher

    # * now define all other directories relative to batchId

    # list of textfiles that were processed in batch
    d.chunkListFname = join(batchDir, "annotatedTextChunks.tab")

    # non-blat directories
    d.dnaAnnotDir    = join(pubConf.annotDir, "dna", publisher) # all sequences on all articles, includes tiny seqs&duplicates
    d.protAnnotDir   = join(pubConf.annotDir, "prot", publisher) # same for proteins
    d.markerAnnotDir = join(pubConf.annotDir, "markers", publisher) # same for markers

    d.tableDir     = join(batchDir, "tables") # tables for genome browser

    # non-blat files
    d.fileDescFname      = join(batchDir, "files.tab") # file descriptions for browser tables
    #d.displayIdFname     = join(batchDir, "authors.tab") # unique author IDs
    d.markerArticleFile  = join(batchDir, "markerArticles.tab") # articleIds associated to any marker
    d.markerCountFile    = join(batchDir, MARKERCOUNTSBASE) # number of articles per marker, for base and all updates
    d.markerDir          = join(batchDir, MARKERDIR) # filtered marker beds, annotated with article count

    d.textConfigFname = join(batchDir, "textDir.conf") # directory where text files are stored

    # genome blat directories
    genomeBlatDir = "blatGenome"
    d.seqDir         = join(batchDir, genomeBlatDir, "seq") # unique sequences per article, dups removed
    d.fastaDir       = join(batchDir, genomeBlatDir, "fasta") # like seq, but in fa format
    d.pslDir         = join(batchDir, genomeBlatDir, "psl") # blat output
    d.pslSortedDir   = join(batchDir, genomeBlatDir, "sortedPsl") # sorted blat output
    d.pslSplitDir    = join(batchDir, genomeBlatDir, "splitSortedPsl") # split blat output, for chaining
    d.bedDir         = join(batchDir, genomeBlatDir, "bed") # chained sorted blat output

    # cdna blat directories
    cdnaBlatDir    = "blatCdna"
    d.cdnaSeqDir       = join(batchDir, cdnaBlatDir, "seq") # unique sequences per article, dups removed
    d.cdnaFastaDir     = join(batchDir, cdnaBlatDir, "fasta") # like seq, but in fa format
    d.cdnaPslDir       = join(batchDir, cdnaBlatDir, "psl") # blat output
    d.cdnaPslSortedDir = join(batchDir, cdnaBlatDir, "cdnaSortedPsl") # sorted blat output

    # protein blat directories
    protBlatDir    = "blatProt"
    d.protSeqDir       = join(batchDir, protBlatDir, "seq") # unique sequences per article, dups removed
    d.protFastaDir     = join(batchDir, protBlatDir, "fasta") # like seq, but in fa format
    d.protPslDir       = join(batchDir, protBlatDir, "psl") # blat output
    d.protPslSortedDir = join(batchDir, protBlatDir, "sortedPsl") # sorted blat output
    d.protPslSplitDir  = join(batchDir, protBlatDir, "splitSortedPsl") # sorted blat output
    d.protBedDir       = join(batchDir, protBlatDir, "protBed") # chained output

    return d

def switchOver():
    """ For all databases: drop all pubsBakX, rename pubsX to pubsBakX, rename pubsDevX to pubsX
    """
    dbs = pubConf.alignGenomeOrder
    dbs.insert(0, "hgFixed")
    prodTables = []
    devTables = []
    bakTables = []
    for db in dbs:
        maxMysql.dropTablesExpr(db, "pubsBak%")
        maxMysql.dropTablesExpr(db, "pubsTest%")
        allTables = maxMysql.listTables(db, "pubs%")
        dbDevTables = [t for t in allTables if t.startswith("pubsDev")]
        notDbDevTables = set(allTables).difference(dbDevTables)
        devTables.extend([db+"."+x for x in dbDevTables])
        prodTables.extend([db+"."+x.replace("Dev","") for x in dbDevTables])
        bakTables.extend([db+"."+x.replace("pubs", "pubsBak") for x in notDbDevTables])
        
    logging.info("Safe Renaming: dev names: %s" % (devTables))
    logging.info("Safe Renaming: prod names: %s" % (prodTables))
    logging.info("Safe Renaming: bak names: %s" % (bakTables))

    maxMysql.renameTables("hg19", prodTables, bakTables, checkExists=True)
    maxMysql.renameTables("hg19", devTables, prodTables)

def runStepSsh(host, publisher, step):
    " run one step of pubMap on a different machine "
    opts = " ".join(sys.argv[3:])
    python = sys.executable
    mainProg = sys.argv[0]
    mainProgPath = join(os.getcwd(), mainProg)

    cmd = "ssh %(host)s %(python)s %(mainProgPath)s %(publisher)s %(step)s %(opts)s" % locals()
    logging.info("Executing command %s" % cmd)
    ret = os.system(cmd)
    if ret!=0:
        logging.info("error during SSH")
        sys.exit(1)

def runStep(publisher, command, d):
    " run one step of the pubMap pipeline with pipeline directories in d "

    stepHost = pubConf.stepHosts.get(command, pubConf.stepHosts["default"])
    if stepHost!='localhost':
        myHost = socket.gethostname()
        if myHost!=stepHost:
            logging.info("hostname is %s, step host name is %s -> running %s via SSH" % \
                (myHost, stepHost, command))
            runStepSsh(stepHost, publisher, command)
            return
        
    if command=="annot":
        # make sure that all batches are finished
        incompBatchId, todoSteps = findUnprocBatch(d.stepProgressFname, d.pipelineSteps)
        if incompBatchId!=None:
            raise Exception("There is a batch in %s that is not at the tables step yet. Cannot continue" 
                % d.stepProgressFname)
        d.batchId = str(int(d.batchId)+1)
        d = defineBatchDirectories(d, d.baseDir, publisher, d.textDir, newBatchId=d.batchId)
        if isdir(d.batchDir):
            raise Exception("%s already exists, is this really a new run?" % d.batchDir)
        else:
            os.makedirs(d.batchDir)
        # find updates to annotate
        d.updateIds = findUnprocUpdateIds(d.stepProgressFname, d.textDir, d.baseDir)
        if d.updateIds==None or len(d.updateIds)==0:
            maxCommon.errAbort("All data files have been processed. Skipping all steps.")

        # get common uppercase words for protein filter
        maxCommon.mustBeEmptyDir(d.dnaAnnotDir, makeDir=True)
        maxCommon.mustBeEmptyDir(d.protAnnotDir, makeDir=True)
        maxCommon.mustBeEmptyDir(d.markerAnnotDir, makeDir=True)
        wordCountBase = "wordCounts.tab"
        wordFile = countUpcaseWords(d.baseDir, wordCountBase, d.textDir, d.updateIds)

        # submit jobs to batch system to run the annotators on the text files
        # use startAnnoId parameters to avoid clashes of annotation IDs
        outDirs = "%s,%s,%s" % (d.markerAnnotDir, d.dnaAnnotDir, d.protAnnotDir)
        options = {"wordFile":wordFile, \
            "startAnnotId.SeqScraper":0, "startAnnotId.ProteinDetect":15000, \
            "startAnnotId.MarkerAnnotate" : 30000 }
        chunkNames = pubAlg.annotate(
            "markerSearch.py:MarkerAnnotate,t2gDnaDetect.py:Annotate,ProteinDetect", 
            d.textDir, options, outDirs, updateIds=d.updateIds, cleanUp=True, runNow=True)

        writeChunkNames(chunkNames, d.chunkListFname)

    elif command=="filter":
        # remove duplicates & short sequence
        chunkNames = list(iterChunkNames(d.chunkListFname))
        submitFilterJobs([d.dnaAnnotDir, d.protAnnotDir], chunkNames, [d.seqDir])
        
    elif command=="blatGenome":
        # convert to fasta and submit blat jobs
        maxCommon.mustBeEmptyDir(d.pslDir, makeDir=True)
        if not options.skipConvert:
            pubToFasta(d.seqDir, d.fastaDir, d.dbList, pubConf.queryFaSplitSize, \
                pubConf.shortSeqCutoff, forceDbs=options.forceDbs)
        submitBlatJobs(d.fastaDir, d.pslDir, blatOptions=pubConf.seqTypeOptions)

    # ==== CDNA COMMANDS, similar to above
    elif command=="blatCdna":
        maxCommon.mustBeEmptyDir([d.cdnaPslDir, d.cdnaFastaDir], makeDir=True)
        if not options.skipConvert:
            maxCommon.mustBeEmptyDir([d.cdnaSeqDir], makeDir=True)
            #logging.info("Searching for sequences that match genome directly")
            #mappedIds = parseAnnotationIds(d.pslSortedDir)
            #writeUnmappedSeqs(mappedIds, d.seqDir, d.cdnaSeqDir)
            #pubToFasta(d.cdnaSeqDir, d.cdnaFastaDir, d.dbList, pubConf.cdnaFaSplitSizes, \
                #pubConf.shortSeqCutoff, forceDbs=options.forceDbs)
            #pubToFasta(d.seqDir, d.fastaDir, d.dbList, pubConf.cdnaFaSplitSizes, \
                #pubConf.shortSeqCutoff, forceDbs=options.forceDbs)
        submitBlatJobs(d.fastaDir, d.cdnaPslDir, cdnaDir=pubConf.cdnaDir)

    elif command=="sortCdna":
        # lift and sort the cdna blat output into one file per organism-cdna 
        submitSortPslJobs("sortDbCdna", d.cdnaPslDir, d.cdnaPslSortedDir, d.dbList)

    # ==== PROTEIN MAPPING COMMANDS, similar to above
    elif command=="blatProt":
        logging.info("Blattting protein sequences on translated cDNA sequences")
        if options.forceDbs:
            cdnaDbs = options.forceDbs
        else:
            cdnaDbs = [basename(dir) for dir in glob.glob(join(pubConf.cdnaDir, "*"))]
        logging.info("These DBs have cDNA data in %s: %s" % (pubConf.cdnaDir, cdnaDbs))
        maxCommon.mustBeEmptyDir(d.protPslDir, makeDir=True)
        if not options.skipConvert:
            splitSizes = pubConf.cdnaFaSplitSizes
            pubToFasta(d.protSeqDir, d.protFastaDir, d.dbList, splitSizes, 0, forceDbs=cdnaDbs)
        submitBlatJobs(d.protFastaDir, d.protPslDir, cdnaDir=pubConf.cdnaDir, \
            blatOptions=pubConf.protBlatOptions, noOocFile=True)

    elif command=="sortProt":
        # sort the cdna blat output into one file per organism-cdna
        logging.info("Sorting protein psls, mapping to genome coord system and prefixing with db")
        cdnaDbs = [basename(dir) for dir in glob.glob(join(pubConf.cdnaDir, "*"))]
        submitSortPslJobs("sortDbProt", d.protPslDir, d.protPslSortedDir, cdnaDbs)

    #elif command=="chainProt":
        #logging.info("Sorting protein matches from all dbs into one file, splitting it, chaining on cluster")
        #mergeSplitChain(d.textDir, d.protPslSortedDir, d.protPslSplitDir, d.protBedDir, pubConf.maxDbMatchCount, dbList, d.updateIds)

    # ==== cDNA/Genome MAPPING COMMANDS ======
    elif command=="sortGenome":
        # sort the sorted cdna, sorted prot and unsorted genome blat output into one file per organism
        submitSortPslJobs("sortDbGenome", d.pslDir, d.pslSortedDir, d.dbList)

    elif command=="chain":
        # join all psl files from each db into one big one for all dbs, filter and re-split
        cdnaPslDirs = glob.glob(join(d.cdnaPslSortedDir, "*"))
        protPslDirs = glob.glob(join(d.protPslSortedDir, "*"))
        addPslDirs = cdnaPslDirs
        addPslDirs.extend(protPslDirs)

        mergeSplitChain(d.textDir, d.pslSortedDir, d.pslSplitDir, d.bedDir, pubConf.maxDbMatchCount, d.dbList, d.updateIds, addDirs=addPslDirs)

    # ==== COMMANDS TO PREP OUTPUT TABLES FOR BROWSER

    elif command=="identifiers":
        # this step creates two files, authors.tab and files.tab in the
        # base output directory
        batchIds = findProcessedBatches(d.baseDir, "identifiers", d.batchId)
        paramDict = {"batchIds" : batchIds, "baseDir" : d.baseDir}

        #pubAlg.mapReduce("unifyAuthors", d.textDir, paramDict, d.displayIdFname, cleanUp=True, runTest=False, skipMap=options.skipConvert, updateIds=d.updateIds)
        pubAlg.mapReduce("getFileDesc", d.textDir, {}, d.fileDescFname, cleanUp=True, runTest=False, skipMap=options.skipConvert, updateIds=d.updateIds)
        logging.info("Results written to %s" % (d.fileDescFname))
        #appendBatchProgress(d.baseDir, d.batchId, d.updateIds, "identifiers")

    elif command=="tables":
        if not options.skipConvert:
            maxCommon.mustBeEmptyDir(d.tableDir, makeDir=True)
        # this step creates tables in batchDir/tables
        logging.info("Reading file descriptions")
        # reformat bed and sequences
        if not options.skipConvert:
            fileDescs  = tabfile.slurpdict(d.fileDescFname, doNotCheckLen=True, encoding="utf8")
            rewriteFilterBedFiles([d.bedDir], d.tableDir, d.dbList)
            rewriteMarkerAnnots(d.markerAnnotDir, "hgFixed", d.tableDir, fileDescs, d.markerArticleFile, d.markerCountFile)
            articleDbs, annotLinks = parseBeds([d.tableDir])
            # read now from tableDir, not bedDir/protBedDir
            writeSeqTables(articleDbs, [d.seqDir, d.protSeqDir], d.tableDir, d.dbList, fileDescs, annotLinks)
        else:
            articleDbs, annotLinks = parseBeds([d.tableDir])

        articleDbs = addHumanForMarkers(articleDbs, d.markerArticleFile)
        # format articles
        writeArticleTables(articleDbs, d.textDir, d.tableDir, d.dbList, d.updateIds)
        #appendBatchProgress(d.baseDir, d.batchId, d.updateIds, "tables")

    # ===== COMMANDS TO LOAD STUFF FROM THE batches/{0,1,2,3...}/tables DIRECTORIES INTO THE BROWSER
    elif command=="load":
        inDirs = []
        for pub in pubConf.loadPublishers:
            inDir = join(pubConf.pubBlatBaseDir, pub)
            assert(isdir(inDir))
            inDirs.append(inDir)
        logging.info("Input is loaded from directories: %s" % inDirs)

        #userTablePrefix = "Dev"
        #if options.finalLoad==True:
            #userTablePrefix = ""
        loadTables(inDirs, d.dbList, MARKERCOUNTSBASE, MARKERDIR, "", options.skipConvert)

    elif command==("switchOver"):
        switchOver()

    # for debugging
    elif command=="_annotMarkers":
        maxCommon.mustBeEmptyDir(d.markerAnnotDir, makeDir=True)
        pubAlg.annotate("markerSearch.py:MarkerAnnotate", d.textDir, {}, \
            d.markerAnnotDir, runNow=(not options.dontRunNow), updateIds=d.updateIds, cleanUp=True)

    # ======== OTHER COMMANDS 
    elif command=="expFasta":
        pubList = publisher.split(",")
        annotDir = pubConf.annotDir
        artMainIds = {}
        for pub in pubList:
            logging.info("Processing %s" % pub)
            textDir = join(pubConf.textBaseDir, pub) 
            logging.info("Reading article identifiers, titles, citation info")
            for article in pubStore.iterArticleDataDir(textDir):
                if article.pmid!="":
                    mainId = article.pmid
                elif article.doi!="":
                    mainId = article.doi
                elif article.externalId!="":
                    mainId = article.externalId

                mainId += " "+article.title+" "+article.journal+" "+article.year
                artMainIds[int(article.articleId)] = mainId
            
            annotDir = pubConf.annotDir
            annotTypes = ["dna", "prot"]
            for annotType in annotTypes:
                maxCommon.mustExistDir(pubConf.faDir, makeDir=True)
                outFname = join(pubConf.faDir, pub+"."+annotType+".fa")
                outFh = codecs.open(outFname, "w", encoding="utf8")
                logging.info("Reformatting %s sequences to fasta %s" % (annotType, outFname))
                tabDir = join(annotDir, annotType, pub)
                for row in maxCommon.iterTsvDir(tabDir):
                    articleId = int(row.annotId[:pubConf.ARTICLEDIGITS])
                    seqId = artMainIds[articleId]
                    outFh.write(">"+row.annotId+"|"+seqId+"\n")
                    outFh.write(row.seq+"\n")

    else:
        maxCommon.errAbort("unknown command: %s" % command)
            
# for recursive calls in cluster operations

if __name__ == "__main__":
    parser = optparse.OptionParser("module is calling itself on cluster machines, not meant to be used from cmdline")
    (options, args) = parser.parse_args()

    command, inName, outName = args

    if command=="filterSeqFile":
        # called internally from "filter"
        filterSeqFile(inName, outName)

    elif command=="sortDbCdna":
        # called internally by submitSortPslJobs (cdna version)
        sortDb(inName, outName, tSeqType="c", pslMap=True)

    elif command=="sortDbProt":
        # called by submitSortPslJobs (prot version)
        sortDb(inName, outName, tSeqType="p", pslMap=True)

    elif command=="sortDbGenome":
        # called by submitSortPslJobs
        sortDb(inName, outName, tSeqType="g")

    elif command=="chainFile":
        # called by submitChainFileJobs
        chainPslToBed(inName, outName, pubConf.dbList, pubConf.maxChainDist, pubConf.getTempDir())

