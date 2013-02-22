import os, logging, time
from os.path import join, isfile, isdir, basename
import pubGeneric, maxTables, pubStore, pubConf

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
    def __init__(self, baseDir, dataset, textDir):
        self.dataset = dataset
        self.textDir = textDir
        self.pubMapBaseDir = baseDir
        self._defineBatchDirectories()

    def _defineBatchDirectories(self):
        " add attributes for all input and output directories to object d"
        logging.debug("Defining batch directories for %s" % self.pubMapBaseDir)
        # base dir for dataset
        self.baseDir = join(self.pubMapBaseDir, self.dataset)
        global baseDir
        baseDir = self.baseDir

        # define current batch id by searching for:
        # first batch that is not at tables yet
        # OR: None if there is not batch yet at all
        self.baseDirBatches = join(self.baseDir, "batches")
        if not isdir(self.baseDirBatches):
            self.batchId = None
            return 

        # if batches/ does not contain any numbered directories, there is no old
        # batch yet
        subDirs = os.listdir(self.baseDirBatches)
        subDirs = [s for s in subDirs if s.isdigit()]
        if len(subDirs)==0:
            self.batchId = None
            return 

        self.batchId = 0
        while self.batchIsPastStep("tables", progressFname=\
                join(self.baseDirBatches, str(self.batchId), "progress.tab") ):
            self.batchId += 1
        logging.info("First valid batchId is %d" % (self.batchId))

        self.batchDir = join(self.baseDirBatches, str(self.batchId))
        batchDir = self.batchDir
            
        # --- now define all other directories relative to batchDir
        #self.batchId = int(open(self.currentBatchFname).read())

        # pipeline progress table file
        self.stepProgressFname = join(self.batchDir, "progress.tab")

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

        # genome blat directories
        genomeBlatDir = "blatGenome"
        self.seqDir         = join(batchDir, genomeBlatDir, "seq") # unique sequences per article, dups removed
        self.fastaDir       = join(batchDir, genomeBlatDir, "fasta") # like seq, but in fa format
        self.pslDir         = join(batchDir, genomeBlatDir, "psl") # blat output
        self.pslSortedDir   = join(batchDir, genomeBlatDir, "sortedPsl") # sorted blat output
        self.pslSplitDir    = join(batchDir, genomeBlatDir, "splitSortedPsl") # split blat output, for chaining
        self.bedDir         = join(batchDir, genomeBlatDir, "bed") # chained sorted blat output

        # cdna blat directories
        cdnaBlatDir    = "blatCdna"
        self.cdnaPslDir       = join(batchDir, cdnaBlatDir, "psl") # blat output
        self.cdnaPslSortedDir = join(batchDir, cdnaBlatDir, "cdnaSortedPsl") # sorted blat output

        # protein blat directories
        protBlatDir    = "blatProt"
        self.protSeqDir       = join(batchDir, protBlatDir, "seq") # unique sequences per article, dups removed
        self.protFastaDir     = join(batchDir, protBlatDir, "fasta") # like seq, but in fa format
        self.protPslDir       = join(batchDir, protBlatDir, "psl") # blat output
        self.protPslSortedDir = join(batchDir, protBlatDir, "sortedPsl") # sorted blat output
        self.protPslSplitDir  = join(batchDir, protBlatDir, "splitSortedPsl") # sorted blat output
        self.protBedDir       = join(batchDir, protBlatDir, "protBed") # chained output

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

    def batchIsPastStep(self, stepName, progressFname=None):
        """     
        check if the old batch using stepFname is at least past a certain step
        """

        if progressFname==None:
            progressFname = self.stepProgressFname

        logging.debug("Checking if %s is at %s" % (progressFname, stepName))
        if not isfile(progressFname):
            logging.debug("No progress file, not at %s yet" % stepName)
            return False

        batchSteps = []
        # parse batch info into dict batchId -> set of steps
        tp = maxTables.TableParser(progressFname)
        for row in tp.lines():
            batchSteps.append(row.step)

        batchSteps = set(batchSteps)
        res = (stepName in batchSteps)
        logging.debug("result: %s" % res)
        return res
        
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
        " add a new line to progress tracking file for batch"
        if not isfile(self.stepProgressFname):
            batchFh = open(self.stepProgressFname, "w")
            headers = "batchId,step,date".split(",")
            batchFh.write("\t".join(headers)+"\n")
        else:
            batchFh = open(self.stepProgressFname, "a")

        row = [str(self.batchId), step, time.asctime()]
        batchFh.write("\t".join(row)+"\n")

    def updateUpdateIds(self):
        " update self.updateIds with all new updateIds in baseDir relative to textDir "
        allUpdateIds = pubStore.listAllUpdateIds(self.textDir)
        doneUpdateIds = self.getAllUpdateIds()
        self.updateIds = set(allUpdateIds).difference(doneUpdateIds)
        logging.info("Updates that have not been annotated yet: %s" % self.updateIds)
