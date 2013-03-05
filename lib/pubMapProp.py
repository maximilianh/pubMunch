import os, logging, time
from os.path import join, isfile, isdir, basename
import pubGeneric, maxTables, pubStore, pubConf, maxCommon

# name of marker counts file
MARKERCOUNTSBASE = "markerCounts.tab"
MARKERDIRBASE = "markerBeds"

def readList(path):
    " yield lines from path "
    if not isfile(path):
        return []

    identifiers = []
    for line in open(path):
        identifiers.append( line.strip())
    return identifiers


def writeList(path, identifiers):
    " write list of identifiers to file "
    logging.info("Writing %d identifiers to %s" % (len(identifiers), path))
    outFh = open(path, "w")
    for identifier in identifiers:
        outFh.write(basename(identifier)+"\n")

class PipelineConfig:
    """ a class with tons of properties to hold all directories for the pipeline 
        Most of these are relative to a BATCH (=one full run of the pipeline)
    """
    def __init__(self, dataset):
        self.dataset = dataset
        self.textDir = pubConf.resolveTextDir(dataset)
        if self.textDir==None:
            raise Exception("dataset %s can not be resolved to a directory" % dataset)

        self.pubMapBaseDir = pubConf.pubMapBaseDir
        maxCommon.mustExistDir(pubConf.pubMapBaseDir, makeDir=True)

        self._defineBatchDirectories()

    def _defineBatchDirectories(self):
        """ 
        
        search for the highest free batch directory
        add attributes for all input and output directories in this batchDir to object d
        if the batch directory exists
        
        """
        logging.debug("Defining batch directories for %s" % self.pubMapBaseDir)
        # base dir for dataset
        self.baseDir = join(self.pubMapBaseDir, self.dataset)
        global baseDir
        baseDir = self.baseDir

        # define current batch id by searching for:
        # first batch that is not at tables yet

        # bailout if there is no single batch setup yet for this dataset
        if not self._anyBatchSetup():
            self.batchId = None
            return

        self.batchId = 0
        while self.batchIsPastStep("tables", progressDir=\
                join(self.baseDirBatches, str(self.batchId), "progress") ):
            self.batchId += 1
        logging.info("First valid batchId is %d" % (self.batchId))

        self.batchDir = join(self.baseDirBatches, str(self.batchId))
        batchDir = self.batchDir
            
        # --- now define all other directories relative to batchDir
        #self.batchId = int(open(self.currentBatchFname).read())

        # pipeline progress table file
        self.progressDir = join(self.batchDir, "progress")

        # updateIds as part of this batch
        self.updateIdFile = join(self.batchDir, "updateIds.txt")
        self.updateIds = readList(self.updateIdFile)

        # list of textfiles that were processed in batch
        self.chunkListFname = join(batchDir, "annotatedTextChunks.tab")
        self.chunkNames =  readList(self.chunkListFname)

        # directories for text annotations
        # all sequences on all articles, includes tiny seqs&duplicates
        self.dnaAnnotDir    = join(batchDir, "annots", "dna")
        self.protAnnotDir   = join(batchDir, "annots", "prot") # same for proteins
        self.markerAnnotDir = join(batchDir, "annots", "markers") # same for markers

        # tables for genome browser 
        self.tableDir     = join(batchDir, "tables") 

        # non-blat files
        self.fileDescFname      = join(batchDir, "files.tab") # file descriptions for browser tables
        # articleIds associated to any marker
        self.markerArticleFile  = join(batchDir, "markerArticles.tab")
        # number of articles per marker, for base and all updates
        self.markerCountsBase   = MARKERCOUNTSBASE
        self.markerCountFile    = join(batchDir, MARKERCOUNTSBASE)
        # filtered marker beds, annotated with article count
        self.markerDirBase      = MARKERDIRBASE
        self.markerDir          = join(batchDir, MARKERDIRBASE)

        self.textConfigFname = join(batchDir, "textDir.conf") # directory where text files are stored

        # filter step

        filterDir = "filter"
        # unique sequences per article, dups removed
        self.seqDir           = join(batchDir, filterDir, "tab")
        self.protSeqDir       = join(batchDir, filterDir, "protTab")
        # like seqDir and protSeqDir, but in fasta format, one file per target genome
        self.fastaDir         = join(batchDir, filterDir, "fasta")
        self.protFastaDir     = join(batchDir, filterDir, "protFasta")

        # blat ouput directories
        blatDir = "blat"
        self.pslDir           = join(batchDir, blatDir, "genome") # blat output
        self.cdnaPslDir       = join(batchDir, blatDir, "cdna") # blat output
        self.protPslDir       = join(batchDir, blatDir, "prot") # blat output

        # chain output directories
        chainDir = "chain"
        self.pslSortedDir     = join(batchDir, chainDir, "genome", "sortedPsl")
        self.pslSplitDir      = join(batchDir, chainDir, "genome", "splitSortedPsl")
        self.cdnaPslSortedDir = join(batchDir, chainDir, "cdna", "sortedPsl")
        self.protPslSortedDir = join(batchDir, chainDir, "prot", "sortedPsl")
        self.protPslSplitDir  = join(batchDir, chainDir, "prot", "splitSortedPsl")

        # bed step
        self.bedDir           = join(batchDir, "bed") # chained sorted blat output

    def getRunner(self, step):
        " return a runner object for the current dataset and pipelineStep"
        headNode = pubConf.stepHosts.get(step, None)
        logging.debug("Headnode for step %s is %s" % (step, headNode))
        return pubGeneric.makeClusterRunner("pubMap-"+self.dataset+"-"+step, headNode=headNode)

    def writeChunkNames(self, chunkNames):
        writeList(self.chunkListFname, chunkNames)

    def writeUpdateIds(self):
        writeList(self.updateIdFile, self.updateIds)

    #def writeCurrentBatch(self):
        #open(self.currentBatchFname, "w").write(str(self.batchId))

    def createNewBatch(self):
        " increment batch id and update the current batch id file"
        if self.batchId==None:
            self.batchId=0
        else:
            self.batchId = self.batchId+1
        # update the directory object with the new batchDir
        #self.writeCurrentBatch()

        self.batchDir = join(self.baseDirBatches, str(self.batchId))
        os.makedirs(self.batchDir)
        self._defineBatchDirectories()

        if isdir(self.batchDir):
            if not len(os.listdir(self.batchDir))==0:
                raise Exception("%s contains files, is this really a new run?" % self.batchDir)
        else:
            logging.debug("Creating dir %s" % self.batchDir)
            os.makedirs(self.batchDir)

    def _anyBatchSetup(self):
        " return if batches have been setup "
        # no if there is not yet any batch yet at all
        self.baseDirBatches = join(self.baseDir, "batches")
        if not isdir(self.baseDirBatches):
            return False

        # nothing if batches dir does not contain any numbered directories, there is no old
        # batch yet
        subDirs = os.listdir(self.baseDirBatches)
        subDirs = [s for s in subDirs if s.isdigit()]
        if len(subDirs)==0:
            return False

        return True

    def completedSteps(self):
        " return list of steps completed in this batch "

        if not self._anyBatchSetup():
            return []

        if not isdir(self.progressDir):
            return False

        return os.listdir(self.progressDir)

    def batchIsPastStep(self, stepName, progressDir=None):
        """     
        check if the old batch using stepFname is at least past a certain step
        """

        if progressDir==None:
            progressDir = self.progressDir

        if not self._anyBatchSetup():
            return False

        logging.debug("Checking if %s is at %s" % (progressDir, stepName))
        progressFname = join(progressDir, stepName)
        if isfile(progressFname):
            logging.debug("No progress file, not at %s yet" % stepName)
            return False
        else:
            return True
        
    def getAllUpdateIds(self):
        """ 
        go over all subdirs of baseDirBatches, read the updateIds.txt files.
        """
        # parse textDir

        # parse tracking file and get all updateIds
        logging.debug("Parsing subdirs of %s to find all updateIds.txt files" % self.baseDirBatches)
        doneUpdateIds = set()
        for batchDir in os.listdir(self.baseDirBatches):
            if not batchDir.isdigit():
                continue

            updFname = join(self.baseDirBatches, batchDir, "updateIds.txt")
            if isfile(updFname):
                batchUpdateIds = open(updFname).read().split(",")
                doneUpdateIds.update(batchUpdateIds)

        return doneUpdateIds

    def appendBatchProgress(self, step):
        " set flag file to signal batch progress"
        if not isdir(self.progressDir):
            os.makedirs(self.progressDir)
        logging.debug("Flagging step %s as done" % step)
        open(join(self.progressDir, step), "w")
        #if not isfile(self.stepProgressFname):
            #batchFh = open(self.stepProgressFname, "w")
            #headers = "batchId,step,date".split(",")
            #batchFh.write("\t".join(headers)+"\n")
        #else:
            #batchFh = open(self.stepProgressFname, "a")

        #row = [str(self.batchId), step, time.asctime()]
        #batchFh.write("\t".join(row)+"\n")

    def updateUpdateIds(self):
        " update self.updateIds with all new updateIds in baseDir relative to textDir "
        allUpdateIds = pubStore.listAllUpdateIds(self.textDir)
        doneUpdateIds = self.getAllUpdateIds()
        self.updateIds = set(allUpdateIds).difference(doneUpdateIds)
        logging.info("Updates that have not been annotated yet: %s" % self.updateIds)

    def findTableFiles(self, ignoreFilenames):
        """ find all table files across all batches in batchDir, find all files.
            create fileDict as (tableName, fileExt) -> dict of db -> list of files
            return fileDict. 

        >>> findTableFiles("/hive/data/inside/literature/blat/miniEls", ["0"])
        """
        fileDict = {}
        logging.debug("Searching for all table files in %s" % baseDir)
        batchIds = self.findBatchesAtStep("tables")
        for batchId in batchIds:
            tableDir = join(baseDir, "batches", batchId, "tables")
            for tableFname in os.listdir(tableDir):
                tablePath = join(tableDir, tableFname)
                if tablePath in ignoreFilenames:
                    logging.debug("file %s has already been loaded, skipping" % tablePath)
                    continue
                if os.path.getsize(tablePath)==0:
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

    def findFileInAllBatchesAtStep(self, fname, step):
        " return list of file with name X in all batches that have completed a step "
        batchIds = self.findBatchesAtStep(step)
        res = []
        for batchId in batchIds:
            fname = join(self.baseDir, "batches", batchId, fname)
            if isfile(fname):
                logging.debug("Found %s" % fname)
                res.append(fname)
            else:
                logging.warn("Not found: %s" % fname)
        return res

    def findBatchesAtStep(self, step):
        """ return the list of batchIds that have run through 'step'
        """
        #def findProcessedBatches(mainBaseDir, step, currentBatchId=None):
        batchIds = os.listdir(self.baseDirBatches)
        batchIds = [x for x in batchIds if x.isdigit()]

        okBatchIds = []
        for bid in batchIds:
            progressFname = join(self.baseDirBatches, bid, self.progressDir, step)
            logging.info("checking if %s exists" % progressFname)
            if isfile(progressFname):
                okBatchIds.append(bid)
        logging.debug("batchIds in %s with '%s' done: %s" % (self.baseDirBatches, step, okBatchIds))
        return okBatchIds

    def readMarkerCounts(self, counts):
        """ 
        go over all batches and get the total count for all markers in all updates
        uses markerCountFname, a table with <marker>tab<count> created by the 'tables' step
        """
        logging.info("Reading counts from %s" % baseDir)
        # names of marker files
        markerCountNames = self.findFileInAllBatchesAtStep(MARKERCOUNTSBASE, "tables")
        # parse marker count files
        if len(markerCountNames)==0:
            logging.warn("No marker files found with counts")
            return counts

        for markerCountName in markerCountNames:
            counts = addCounts(counts, markerCountName) # e.g. {"rs123231":13, "TP53":5000}
        return counts

def addCounts(countDict, fname):
    " parse line of file with format <id>tab<count>, add counts to dict, return dict "
    logging.debug("Parsing %s" % fname)
    for line in open(fname):
        line = line.strip()
        id, count = line.split("\t")
        count = int(count)
        countDict[id]+=count
    return countDict

