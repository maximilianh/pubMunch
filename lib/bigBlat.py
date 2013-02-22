#!/usr/bin/env python

import glob, sys, os, logging, optparse
from os.path import *

# name of script to submit
bigBlatJobSrc = "bigBlatJob.py"

# mark's libraries
GENBANKDIR = "/cluster/data/genbank"
# read the locations of 2bit/nib files and the lft files 
GBCONFFILE     = GENBANKDIR+"/etc/genbank.conf"

# import mark's libraries
sys.path.insert(0, GENBANKDIR+"/lib/py")
import genbank.Config
import genbank.GenomePartition

# convenience functions 

def mustBeEmptyDir(path):
    if not isdir(path):
        logging.error("Directory %s does not exist" % path)
        sys.exit(1)
    if len(os.listdir(path))!=0:
        logging.error("Directory %s is not empty" % path)
        sys.exit(1)

def prefixEmpty(prefix, string, commaAppend=None):
    """ return string with prefix, unless string is None 
      append commaAppend at the with comma-sep, unless is None
    """
    if string==None or string=="":
        result = []
    else:
        result= [string]
    if commaAppend!=None:
        result.append(commaAppend)

    if len(result)==0:
        return ""
    else:
        return prefix+" "+",".join(result)

def resolveToFiles(faList):
    """ resolve a list of input-things, which can be a files or dirs, to a list
    of filenames"""
    faFiles = []
    for faQuery in faList:
        if isdir(faQuery):
            faQueryFiles = glob.glob(join(faQuery, "*.fa"))
        elif isfile(faQuery):
            faQueryFiles = [faQuery]
        else:
            logging.info("%s does not exist" % faQuery)
            raise Exception()
        faFiles.extend(faQueryFiles)
    return faFiles

# class based on mark's gbAlignSetup/GenomeTarget
# defaults for named parameters are copied from /cluster/genbank/genbank/etc/genbank.conf
class GenomeSplitter(genbank.GenomePartition.GenomePartition):
    "object to store info about genome and partition it into windows"

    def __init__(self, db, conf, params):
        liftFile = conf.getDbStrNo(db, "lift")
        unplacedChroms = None
        unSpecs = conf.getDbWordsNone(db, "align.unplacedChroms")
        if unSpecs != None:
            if liftFile == None:
                raise Exception(db + ".align.unplacedChroms requires " + db + ".lift")
            unplacedChroms = genbank.GenomePartition.UnplacedChroms(unSpecs)
        
        self.twoBitFname = conf.getDbStr(db, "clusterGenome")
        assert(self.twoBitFname.endswith(".2bit")) # we don't support old genomes with only nib files
        genbank.GenomePartition.GenomePartition.__init__(self, db, self.twoBitFname,
                                 int(params.get("window", 80000000)),
                                 int(params.get("overlap", 3000000)),
                                 int(params.get("maxGap", 3000000)),
                                 int(params.get("minUnplacedSize", 900)),
                                 liftFile, unplacedChroms)

def getJoblines(targetList, faFiles, outDir, params={}, blatOpt=None, pslFilterOpt=None, splitTarget=True, noOocFile=False):
    """ get individual job command lines for big Blat job.

    faQuery can be a list of fasta filenames, a directory with .fa files or a single fa filename

    targetList can be a list of string with Dbs ("hg19"), 2bit files or fasta-files
    splitTarget works only if Dbs are specified.

    outDir must be an empty directory
    psl results go to outDir/<basename(query)>/<db>/

    configuration to find 2bit files will be read from GENBANKDIR

    params is a dictionary with keys "window", "overlap", "maxGap" and "minUnplacedSize" and integer values
    for each parameter, see /cluster/genbank/genbank/etc/genbank.conf for details. Defaults are:
    window=80000000, overlap=300000, maxGap=300000, minUnplacedSize=900

    blatOpt and pslFilterOpt are comma-sep strings without leading dashes, e.g. minId=20,minCover=0.8
    
    will create a file joblist in the current directory 
       
    splitTarget controls if target is split into reasonable sizes pieces before blatting
    if it is false, one job is submitted per target.
    """ 

    if isinstance(targetList, str): # targetList can be a string
        targetList = targetList.split(",")

    #progDir = dirname(sys.argv[0]) # assume that jobscript is in same dir as we are
    progDir = dirname(__file__) # assume that jobscript is in same dir as we are
    bigBlatPath = join(progDir, bigBlatJobSrc)

    assert(isfile(bigBlatPath)) # job python script for cluster jobs must exist
    assert(isdir(outDir)) # output directory must exist

    faFiles = resolveToFiles(faFiles)

    jobLines = []
    for faFile in faFiles:
        for target in targetList:
            # make output dir for each target
            faBase = splitext(basename(faFile))[0] # strip path and ext
            # append target to outdirname only if several targets specified
            if len(targetList)>1:
                pslDir = join(outDir, faBase, basename(target))
            else:
                pslDir = join(outDir, faBase)
            if not isdir(pslDir):
                os.makedirs(pslDir)

            # run mark's splitter
            conf = None
            if splitTarget:
                conf = genbank.Config.Config(GBCONFFILE) 
                splitter = GenomeSplitter(target, conf, params)
                windows = splitter.windows
                splitSpecs = []
                for win in windows:
                    twoBitSpec = splitter.twoBitFname+":"+win.getSpec()
                    chrom, startPos = win.seq.id, win.start
                    splitSpecs.append( (twoBitSpec, chrom, startPos) )
                oocFile = conf.getDbStrNo(target, "ooc")
            else:
                if isfile(target):
                    twoBitFname = target
                    oocFile = join(dirname(target), "11.ooc")
                else:
                    if conf==None:
                        conf = genbank.Config.Config(GBCONFFILE) 
                    twoBitFname = conf.getDbStr(target, "clusterGenome")
                    oocFile = conf.getDbStrNo(target, "ooc")
                splitSpecs = [ (twoBitFname, "all", 0) ]
            if noOocFile:
                oocFile=None

            #for win in windows:
            for twoBitSpec, chrom, startPos in splitSpecs:
                pslFile = join(pslDir, "%s-%d.psl" % (chrom, startPos))
                # prep blat/pslFilter options
                oocOpt=None
                if oocFile!=None:
                    oocOpt = "ooc="+oocFile
                blatOptString   = prefixEmpty("-b", blatOpt, oocOpt)
                filterOptString = prefixEmpty("-f", pslFilterOpt)

                # assemble command line for joblist
                cmdParts = [bigBlatPath,
                        twoBitSpec,
                        "{check in exists "+faFile+"}",
                        "{check out exists "+pslFile+"}",
                        blatOptString,
                        filterOptString]
                line = " ".join(cmdParts)
                if len(line)>1500:
                    raise Exception("jobList command '%s' is too long for parasol" % line)
                jobLines.append(line)
    return jobLines

def writeJoblist(dbList, faQuery, outDir, params={}, blatOpt="", pslFilterOpt="", append=False, splitTarget=True):
    """ create joblist to blat many fasta files against many genomes
    
    """

    if append:
        jobListFile = open("jobList", "a")
    else:
        jobListFile = open("jobList", "w")

    lineCount=0
    jobLines = getJoblines(dbList, faQuery, outDir, params, blatOpt, pslFilterOpt, splitTarget)
    for line in jobLines:
        jobListFile.write(line+"\n")
        lineCount+=1
    logging.info("written %d lines to jobList file" % lineCount)

# MAIN ENTRY POINT, if script is called from shell
if __name__=="__main__":

    helpMsg="""usage: %prog <dbList> <qFasta1> <qFasta2> <...> <outDir> - write joblist for big blat job of all fasta files against dbs

    qFasta can be a single file or a directory with .fa files
    dbList is a comma-sep list of dbs, like "hg19,mm9"
    outDir has to be empty, will be filled with subdirs <queryBasename>/<db>
    (The <db> of outDir is skipped if only one db is specified)

    example:
    bigBlat.py hg19,mm9 shortSeqs.fa psl --winSize=3000000 --overlap=500000 --blatOpt stepSize=5,minScore=16 ,minMatch=1,oneOff=1,maxIntron=4 --pslOpt minAli=0.7,nearTop=0.01,minNearTopSize=18,ignoreSize,noIntrons
    """

    logging.getLogger().setLevel(logging.DEBUG)
    parser = optparse.OptionParser(helpMsg)
    parser.add_option("-b", "--blatOpt", dest="blatOpt", action="store", help="options to pass to blat with no leading dashes, separated by commas, e.g. minSize=20,fastMap", default="")
    parser.add_option("-f", "--pslFilterOpt", dest="pslFilterOpt", action="store", help="options to pass to pslReps with no leading dashes, separated by commas, e.g. minTop=0.01,minAli=0.8", default="") 
    parser.add_option("", "--winSize", dest="window", action="store", help="genome splitting: max size of per piece", default="80000000") 
    parser.add_option("", "--maxGap", dest="maxGap", action="store", help="genome splitting: maximum size of gap between two pieces", default="3000000") 
    parser.add_option("", "--overlap", dest="overlap", action="store", help="genome splitting: overlap between two pieces", default="3000000") 
    parser.add_option("", "--minUnplacedSize", dest="minUnplacedSize", action="store", help="genome splitting: minimum size of unplaced sequences", default="900") 
    parser.add_option("-a", "--append", dest="append", action="store_true", help="do not create a new joblist file but append to the old one") 
    (options, args) = parser.parse_args()
    if len(args)<3:
        parser.print_help()
    else:
        dbString = args[0]
        qFastas  = args[1:-1]
        outDir   = args[-1]

        #mustBeEmptyDir(outDir)
        params={}
        params["window"]=options.window
        params["overlap"]=options.overlap
        params["maxGap"]=options.maxGap
        params["minUnplacedSize"]=options.minUnplacedSize
        writeJoblist(dbString, qFastas, outDir, params, options.blatOpt, options.pslFilterOpt, options.append)
