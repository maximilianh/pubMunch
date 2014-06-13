#!/usr/bin/env python

import glob, sys, os, logging, optparse
from os.path import *
from itertools import groupby

# name of script to submit
bigBlatJobSrc = "bigBlatJob.py"

# the job script also contains a function we need
import bigBlatJob

# mark's libraries
GENBANKDIR = "/cluster/data/genbank"
# read the locations of 2bit/nib files and the lft files 
GBCONFFILE     = GENBANKDIR+"/etc/genbank.conf"

# import mark's libraries
sys.path.insert(0, GENBANKDIR+"/lib/py")
import genbank.Config
import genbank.GenomePartition

cluster = "ku"

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
    """ resolve a list of input-strings, which can be a files or dirs, to a list
    of filenames"""
    faFiles = []
    for faQuery in faList:
        if isdir(faQuery):
            faQueryFiles = glob.glob(join(faQuery, "*.fa"))
            logging.debug("found %d query .fa files in %s" % (len(faQueryFiles), faQuery))
        elif isfile(faQuery):
            faQueryFiles = [faQuery]
            logging.debug("found query .fa file: %s" % faQuery)
        else:
            logging.info("%s does not exist" % faQuery)
            raise Exception()
        faFiles.extend(faQueryFiles)
    logging.info("Found %d input fasta files" % len(faFiles))
    assert(len(faFiles)!=0)
    return faFiles

def findTwoBitFname(target):
    " return 2bit filename on cluster for target which can be a db or a twobit file "
    if not target.endswith("2bit"):
        conf = genbank.Config.Config(GBCONFFILE)
        twoBitFname = conf.getDbStr(target, "clusterGenome")
    else:
        twoBitFname = target
    return twoBitFname

# class based on mark's gbAlignSetup/GenomeTarget
# defaults for named parameters are copied from /cluster/genbank/genbank/etc/genbank.conf
class GenomeSplitter(genbank.GenomePartition.GenomePartition):
    "object to store info about genome and partition it into windows"

    def __init__(self, db, conf, params):
        #liftFile = conf.getDbStrNo(db, "lift")
        liftFile = conf.getDbStrNone(db, "lift")
        if liftFile!=None and liftFile.strip()=="no":
            liftFile=None
        unplacedChroms = None
        unSpecs = conf.getDbWordsNone(db, "align.unplacedChroms")
        if unSpecs != None:
            if liftFile == None:
                raise Exception(db + ".align.unplacedChroms requires " + db + ".lift")
            unplacedChroms = genbank.GenomePartition.UnplacedChroms(unSpecs)
        
        if not db.endswith("2bit"):
            self.twoBitFname = conf.getDbStr(db, "clusterGenome")
        else:
            self.twoBitFname = db

        assert(self.twoBitFname.endswith(".2bit")) # we don't support old genomes with only nib files

        genbank.GenomePartition.GenomePartition.__init__(self,
                                 db,
                                 self.twoBitFname,
                                 int(params.get("window", 80000000)),
                                 int(params.get("overlap", 3000000)),
                                 int(params.get("maxGap", 3000000)),
                                 int(params.get("minUnplacedSize", 900)),
                                 liftFile, unplacedChroms)

def findJobWithRoom(maxJobSize, pending, need):
    """find a job with room for this window, allowing jobs to overflow
    by the overlap amount. Returns None if none found, or index in
    pending list 
    >>> pending = [[('chr1', 1, 10000000)], [('chr2', 1, 8900000)]]
    >>> findJobWithRoom(10000000, pending, 100000000)
    >>> findJobWithRoom(10000000, pending, 8000)
    1
    >>> findJobWithRoom(9000000, pending, 8000)
    1
    """
    for i, rangeList in enumerate(pending):
        jobSize = sum([end-start for chrom,start,end in rangeList])
        if jobSize + need < maxJobSize:
            return i
    return None

def splitTargetDbSpecs(target, conf, params):
    """ given a db, split the db into overlapping pieces and return 
    a list of specs for blat in the format
    (twoBitFname, list of "job")
    "job" is a list of tuples (chromosome, start, end)

    >>> conf = genbank.Config.Config(GBCONFFILE)
    >>> len(splitTargetDbSpecs("bosTau7", conf, {})[1])
    """
    splitter = GenomeSplitter(target, conf, params)
    windows = splitter.windows

    winSize = params.get("window", 80000000)
    overlap = params.get("overlap", 3000000)
    twoBitFname = splitter.twoBitFname
    #twoBitFname = "/cluster/data/%s/%s.2bit" % (target, target)

    maxJobSize = winSize+overlap

    pendingJobs = []
    pendingJobSizes = []
    fullJobs = []

    winCount = 0
    for win in windows:
        winCount +=1
        chrom, startPos, endPos = win.seq.id, win.start, win.end
        need = (endPos-startPos) - overlap

        i = findJobWithRoom(maxJobSize, pendingJobs, need)
        if i == None:
            # there is no job with room: add a new job
            pendingJobs.append( [ (chrom, startPos, endPos) ] )
            pendingJobSizes.append(0)
        else:
            # there is a job with room: add to job
            job = pendingJobs[i]
            job.append( (chrom, startPos, endPos) )
            pendingJobSizes[i] += (endPos-startPos)
            # check if this job is now full
            #jobSize = sum([end-start for chrom,start,end in job])
            jobSize = pendingJobSizes[i]
            if jobSize >= winSize+overlap:
                fullJobs.append(job)
                # instead of deletion, replace with last element, then delete last element
                # same as: del pendingJobs[i]
                pendingJobs[i] = pendingJobs[-1]
                del pendingJobs[-1]
                pendingJobSizes[i] = pendingJobSizes[-1]
                del pendingJobSizes[-1]

    # now add all pending windows
    logging.debug("win %d" % winCount)
    fullJobs.extend(pendingJobs)

    logging.debug("%d jobs" % len(fullJobs))
    # get sum of all chunks
    allJobsSize = 0
    for job in fullJobs:
        jobSize = sum([end-start for chrom, start, end in job])
        logging.debug("job: %d jobs, size %d" % (len(job), jobSize))
        allJobsSize+= jobSize
    logging.debug("total genome size: %d" % allJobsSize)

    return twoBitFname, fullJobs

def writeBlatSpecs(conf, db, outDir, params={}):
    """ chunks db or 2bit file into pieces as instructed by genbank.conf.
    params can be set (defaults shown):
        "window" = 80000000
        "overlap" = 3000000
        "maxGap" = 3000000
        "minUnplacedSize" = 900

    Outputs chunks into directory in BLAT format, one per line.
    returns filenames of specs.
    """
    twoBitFname, specs = splitTargetDbSpecs(db, conf, params)
    specNames = []
    chroms = set()

    jobId = 0
    allJobsSize = 0
    for jobList in specs:
        outFname = join(outDir, "t%04d.spec" % jobId)
        specNames.append(outFname)
        ofh = open(outFname, "w")
        for win in jobList:
            chrom, start, end = win
            chroms.add(chrom)
            ofh.write("%s:%s:%d-%d" % (abspath(twoBitFname), chrom, start, end))
            ofh.write("\n")
            allJobsSize+= (end-start)
        ofh.close()
        logging.debug("Wrote %s" % outFname)
        jobId += 1
    logging.info("Number of target chunks: %d" % len(specs))
    logging.info("Sum of alignment target job size: %d" % allJobsSize)
    logging.info("Number of seqs: %d" % len(chroms))
    return twoBitFname, specNames

def findChromSizes(twoBitFname):
    """
    return name of chrom.sizes filename based on some guesses
    or create one if really not found
    >>> findChromSizes("/scratch/data/hg19/hg19.2bit")
    '/scratch/data/hg19/chrom.sizes'
    """
    seqDir = dirname(twoBitFname)
    sizeFname = join(seqDir, "chrom.sizes")
    if isfile(sizeFname):
        return sizeFname

    sizeFname = twoBitFname.replace(".2bit", ".sizes")
    if isfile(sizeFname):
        return sizeFname

    dbName = basename(twoBitFname).replace(".2bit", "")
    sizeFname = join("/hive/data/genomes", dbName, "chrom.sizes")
    if isfile(sizeFname):
        return sizeFname

    #raise Exception("Could not find chromosome sizes file for %s" % twoBitFname)
    logging.info("Could not find chromosome sizes file for %s, creating one" % twoBitFname)
    sizeFname = twoBitFname.replace(".2bit", ".sizes")
    cmd = "twoBitInfo %s %s" % (twoBitFname, sizeFname)
    runCommand(cmd)
    return sizeFname

def findOocFname(conf, target, twoBitFname):
    """ try to find name of .ooc file, from either genbank config or other guesses 
    
    >>> conf = genbank.Config.Config(GBCONFFILE)
    >>> findOocFname(conf, "hg19", "/scratch/data/hg19/11.ooc")
    '/scratch/data/hg19/11.ooc'
    >>> findOocFname(conf, "archaea", "/hive/data/inside/pubs/nonUcscGenomes/archaea.2bit")
    '/scratch/data/hg19/11.ooc'
    """
    oocFname = conf.getDbStrNone(target, "ooc")
    if oocFname=="no":
        oocFname = None

    # try various other paths to find ooc file
    if oocFname==None:
        oocFname = join(dirname(twoBitFname), "11.ooc")
        logging.debug("%s not found" % oocFname)
        if not isfile(oocFname):
            oocFname = splitext(twoBitFname)[0]+".11.ooc"
            logging.debug("%s not found" % oocFname)
            if not isfile(oocFname):
                oocFname = splitext(twoBitFname)[0]+".ooc"
                if not isfile(oocFname):
                    logging.debug("%s not found" % oocFname)
                    raise Exception("no ooc statement in gbconf and %s not found" % (oocFname))
    return oocFname

def runCommand(cmd):
    """ run command in shell, exit if not successful.
    using os.system just for mark
    """
    logging.debug("Running command: %s" % cmd)
    ret = os.system(cmd)

    if ret!=0:
            raise Exception("Could not run command (Exitcode %d): %s" % (ret, cmd))

def fasta_iter(fasta_name):
    """
    from http://www.biostars.org/p/710/ 
    given a fasta file. yield tuples of header, sequence
    """
    fh = open(fasta_name)
    # ditch the boolean (x[0]) and just keep the header or sequence since
    # we know they alternate.
    faiter = (x[1] for x in groupby(fh, lambda line: line[0] == ">"))
    for header in faiter:
        # drop the ">"
        header = header.next()[1:].strip()
        # join all sequence lines to one.
        seq = "".join(s.strip() for s in faiter.next())
        yield header, seq

def doChain(target, outDir):
    """
    chain psl files, submit one job per query
    """
    tTwoBitFname = findTwoBitFname(target)
    assert(isfile(tTwoBitFname))

    qTwoBitDir = abspath(join(outDir, "qTwoBit"))
    pslCatDir = abspath(join(outDir, "pslCat", target))
    assert(isdir(pslCatDir))
    assert(isdir(qTwoBitDir))

    chainDir = abspath(join(outDir, "chains", target))
    if not isdir(chainDir):
        os.makedirs(chainDir)

    jlfname = join(outDir, "jobList")
    jlf = open(jlfname, "w")
    for pslName in os.listdir(pslCatDir):
        pslPath = join(pslCatDir, pslName)
        query = pslName.split(".")[0]
        qTwoBitFname = join(qTwoBitDir, query+".2bit")
        chainPath = join(chainDir, query+".chain")
        cmd = "axtChain -psl %(pslPath)s %(tTwoBitFname)s %(qTwoBitFname)s %(chainPath)s -linearGap=loose" \
            % locals()
        jlf.write(cmd+"\n")
    jlf.close()
    logging.info("wrote jobs to %s" % jlfname)

    clusterName = cluster

    outDir = abspath(outDir)
    cmd = "ssh %(clusterName)s 'cd %(outDir)s; para clearSickNodes; para resetCounts; para freeBatch; para make jobList'" % locals()
    runCommand(cmd)

def doSwapNetSubset(target, outDir):
    """
    concats and swaps the input chains, nets the result, subsets the input chains with the net 
    and swaps the result again.
    """
    # cat and swap chains to create query-based chains
    # XX is swapping really needed?, why can't I used the query chains after chainNet?
    logging.info("catting and swapping chains")
    querySizeFn = join(outDir, "queries.sizes")
    chainDir = join(outDir, "chains")
    chainMask = join(chainDir, target, "*.chain")
    qChainFname = join(outDir, "queries.chain")
    cmd = "chainMergeSort %(chainMask)s | chainSwap stdin %(qChainFname)s" % locals()
    runCommand(cmd)

    # net chains
    logging.info("netting chains")
    # remember that query and target is inversed from normal
    # net nomenclature, as we're working with swapped chains here
    targetBase = basename(target).split(".")[0]
    twoBitFname = findTwoBitFname(target)
    tSizeFname = findChromSizes(twoBitFname)

    qSizeFname = join(outDir, "queries.sizes")
    qNetFname = join(outDir, "queries.net")
    tNetFname = join(outDir, targetBase+".net")
    cmd = "chainNet %(qChainFname)s %(qSizeFname)s %(tSizeFname)s %(qNetFname)s %(tNetFname)s" % locals()
    runCommand(cmd)

    # subset chains and swap back so we can load on target
    logging.info("using net to subset chains, swap")
    qOverFname = join(outDir, "%s.queries.over.chain" % targetBase)
    cmd = "netChainSubset %(qNetFname)s %(qChainFname)s stdout" \
        "| chainStitchId stdin stdout " \
        "| chainSwap stdin %(qOverFname)s" % locals()
    runCommand(cmd)
    logging.info("Result is in %s" % qOverFname)

def doPslCat(target, outDir, pslOptions=None, singleOutFname=None):
    """
    concats psl files, creates one per query
    Optionally sort by query and run pslCdnaFilter on it.
    """
    pslDir = abspath(join(outDir, "psl", target))
    pslCatFnames = []

    if singleOutFname:
        if isfile(singleOutFname):
            open(singleOutFname, "w") # truncate to 0 bytes
    else:
        pslCatDir = abspath(join(outDir, "pslCat", target))
        if not isdir(pslCatDir):
            os.makedirs(pslCatDir)

    for queryName in os.listdir(pslDir):
        if singleOutFname:
            pslCatName = singleOutFname
            pipeOp = ">>"
        else:
            pslCatName = join(pslCatDir, queryName)
            pipeOp = ">"
        if not pslCatName.endswith(".psl"):
            pslCatName += ".psl"

        logging.info("Concatting psls for query %s to %s" % (queryName, pslCatName))
        queryPath = join(pslDir, queryName)
        pslCatFnames.append(pslCatName)
        sortCmd = ""
        if pslOptions:
            filtOpt = bigBlatJob.splitAddDashes(pslOptions)
            sortCmd = " | sort -k10,10 | pslCDnaFilter %s stdin stdout " % (filtOpt)

        cmd = "cat %(queryPath)s/* %(sortCmd)s %(pipeOp)s %(pslCatName)s" % locals()
        runCommand(cmd)

def toTwoBit(qFastas, twoBitDir):
    """ accepts a list of filenames and converts all of them to .twoBit in twoBitDir """
    assert(len(qFastas)!=0)
    if not isdir(twoBitDir):
        os.makedirs(twoBitDir)

    existCount = len(glob.glob(join(twoBitDir, "*.2bit")))
    if existCount == len(qFastas):
        logging.warn("Saving time: not converting fa to 2bit, found %d twoBitFiles in %s" %
            (existCount, twoBitDir))
        return

    logging.info("Converting %d query files to twoBits in %s" % (len(qFastas), twoBitDir))
    for qFasta in qFastas:
        qBaseName = basename(qFasta).split('.')[0]
        # convert to twoBit
        twoBitFname = join(twoBitDir, qBaseName+".2bit")
        cmd = "faToTwoBit %(qFasta)s %(twoBitFname)s" % locals()
        runCommand(cmd)

def splitQuery(qFastas, outDir):
    """ split query fas into one fa per input file but split into 5kbp pieces, 
    also write query sizes to sizeDir and a twoBit version of the unsplit seqs to 
    twoBitDir.

    returns a 2-tuple. list of fa filenames and the directory of the sizes-files.
    """
    splitDir = abspath(join(outDir, "queries/"))
    qSizeDir = abspath(join(outDir, "qSizes/"))
    existNames = glob.glob(join(splitDir, "*.fa"))
    existSizes = glob.glob(join(qSizeDir, "*.sizes"))
    if len(existNames)==len(qFastas)==len(existSizes):
        logging.warn("Found fa files in %s, not splitting query" % splitDir)
        return existNames, qSizeDir

    logging.info("Splitting query fastas")
    if not isdir(splitDir):
        os.makedirs(splitDir)
    if not isdir(qSizeDir):
        os.makedirs(qSizeDir)

    splitFnames = []
    sizeFnames = []
    for qFasta in qFastas:
        # open the files for split and size files
        splitName = join(splitDir, basename(qFasta))
        splitFnames.append(splitName)
        splitFh = open(splitName, "w")

        qBaseName = basename(qFasta).split('.')[0]
        sizeName = join(qSizeDir, qBaseName+".sizes")
        sizeFnames.append(sizeName)
        sizeFh = open(sizeName, "w")

        allSizeFh = open(join(outDir, "queries.sizes"), "w")

        logging.info("5kbp-split of query %s to %s" % (qFasta, splitName))
        # now split the fasta seqs
        for seqId, seq in fasta_iter(qFasta):
            seqSize = len(seq)
            sizeFh.write("%s\t%d\n" % (seqId, seqSize))
            allSizeFh.write("%s\t%d\n" % (seqId, seqSize))
            start = 0
            end = 0
            while start+5000 < seqSize:
                end = start+5000
                splitFh.write(">%(seqId)s:%(start)d-%(end)d\n" % locals())
                splitFh.write("%s\n" % seq[start:end])
                start += 5000
            if (end != seqSize):
                splitFh.write(">%(seqId)s:%(end)d-%(seqSize)d\n" % locals())
                splitFh.write("%s\n" % seq[end:])

        splitFh.close()
        sizeFh.close()
        allSizeFh.close()
        
        #cmd = "gzip %s" % splitName
        #runCommand(cmd)
    return splitFnames, qSizeDir

        #cmd = "faSplit gap %(qFasta)s %(splitSize)d %(splitDir)s/query -lift=%(liftFname)s" % locals()
        #runCommand(cmd)
    #else:
        #logging.info("Not splitting query, directory %s already exists" % splitDir)

    #splitFnames = glob.glob(join(splitDir, "*"))
    #return splitFnames

#def prepQuery(qFastas, outDir):
    #""" 
    #convert query fas to 2bit and create .sizes for them 
    #
    #creates directories qTwoBit and qSizes in outdir
    #"""
    #twoBitDir = abspath(join(outDir, "qTwoBit"))
    #sizeDir = abspath(join(outDir, "qSizes"))
#
    #if not isdir(twoBitDir):
        #os.makedirs(twoBitDir)
    #if not isdir(sizeDir):
        #os.makedirs(sizeDir)
#
    #twoBitNames = []
    #sizeNames = []
    #for qFaName in qFastas:
        #qBase = basename(qFaName).split(".")[0]
#
        #twoBitName = join(twoBitDir, qBase+".2bit")
        #cmd = "faToTwoBit %s %s" % (qFaName, twoBitName)
        #runCommand(cmd)
        #twoBitNames.append(twoBitName)
#
        #sizeFname = join(sizeDir, qBase+".sizes")
        #cmd = "faSize -detailed %s > %s" % (qFaName, sizeFname)
        #runCommand(cmd)
        #sizeNames.append(sizeFname)

        #logging.info("Converted %s to %s and %s" % (qFaName, twoBitName, sizeFname))
#
    #return twoBitNames, sizeNames

def chunkTargetWriteSpecs(target, outDir, conf):
    """ create spec files for target, 
    returns twoBitFilename, oocFilename, sizeFname, list of spec filenames 
    """
    # write target specs to outdir/chunks/hg19/
    specDir = abspath(join(outDir, "chunks", basename(target)))
    # make output dirs
    if not isdir(specDir):
        os.makedirs(specDir)

    logging.info("Partitioning target %s to %s" % (target, specDir))
    twoBitFname, specFnames = writeBlatSpecs(conf, target, specDir)
    oocFname = findOocFname(conf, target, twoBitFname)
    sizeFname = findChromSizes(twoBitFname)
    assert(oocFname!="no")
    return twoBitFname, oocFname, sizeFname, specFnames

def getJoblines(targetList, queryFnames, outDir, params={}, \
    splitTarget=False, blatOpt=None, pslFilterOpt=None, \
    qSizeDir=None, noOocFile=False):
    """ get individual job command lines for big Blat job.
    creates a file joblist in outDir.
    This is more or less a rewrite of partitionGenome.pl based on Mark Diekhans code

    - faQuery can be a list of fasta filenames, a directory with .fa files or a single fa filename
    - targetList can be a list of string with Dbs ("hg19"), 2bit files or fasta-files
    - configuration to find 2bit files will be read from GENBANKDIR
    - if splitTargetDir is set, will chunk up target around gaps and write
      blatSpec files to this directory.
    - outDir must be an empty directory
    - psl results go to outDir/psl/<basename(query)>/<db>/
    - splitTarget: blatSpec files go to outDir/blatSpec/<db>/
    - params is a dictionary with keys "window", "overlap", "maxGap" 
      and "minUnplacedSize" and integer values
      for each parameter, see /cluster/genbank/genbank/etc/genbank.conf for details. Defaults are:
      window=80000000, overlap=300000, maxGap=300000, minUnplacedSize=900

    - blatOpt and pslFilterOpt are comma-sep strings without leading dashes, 
      e.g. minId=20,minCover=0.8
    - if sizeDir is set to a directory, will also lift query subranges.
    """ 

    if isinstance(targetList, str): # targetList can be a string
        targetList = targetList.split(",")

    progDir = dirname(__file__) # assume that jobscript is in same dir as we are
    bigBlatPath = join(progDir, bigBlatJobSrc)

    assert(isfile(bigBlatPath)) # job python script for cluster jobs must exist
    assert(isdir(outDir)) # output directory must exist

    queryFnames = resolveToFiles(queryFnames)

    conf = genbank.Config.Config(GBCONFFILE)
    jobLines = []
    for target in targetList:
        if splitTarget:
            twoBitFname, oocFname, tSizeFname, specFnames = \
                chunkTargetWriteSpecs(target, outDir, conf)
        else:
            twoBitFname = target
            oocFname = findOocFname(conf, target, twoBitFname)
            tSizeFname = None
            specFnames = [twoBitFname]

        if noOocFile:
            oocFname = None

        for specFname in specFnames:
            specBase = basename(specFname).split(".")[0]
            # prep blat/pslFilter options
            oocOpt=None
            if oocFname!=None:
                oocOpt = "ooc="+oocFname
            blatOptString   = prefixEmpty("-b", blatOpt, oocOpt)
            filterOptString = prefixEmpty("-f", pslFilterOpt)

            for qIdx, qFaFname in enumerate(queryFnames):
                # write psls to something like outDir/psl/hg19/query1/
                qBase = splitext(basename(qFaFname))[0] # strip path and ext
                pslDir = abspath(join(outDir, "psl", basename(target), qBase))
                if not isdir(pslDir):
                    os.makedirs(pslDir)
                qFaFname = abspath(qFaFname)
                pslFile = abspath(join(pslDir, "%s.psl" % (specBase)))
                cmdParts = [ bigBlatPath,
                        specFname,
                        "{check in exists "+qFaFname+"}",
                        "{check out exists "+pslFile+"}",
                        blatOptString,
                        filterOptString]
                if tSizeFname:
                    cmdParts.extend(["-s", tSizeFname ])

                if qSizeDir:
                    sizeFname = join(qSizeDir, qBase+".sizes")
                    cmdParts.extend(["--querySizes", sizeFname])

                line = " ".join(cmdParts)
                if len(line)>1500:
                    raise Exception("jobList command '%s' is too long for parasol" % line)
                jobLines.append(line)
    return jobLines

def writeJoblist(dbList, faQuery, outDir, params={}, \
    blatOpt="", pslFilterOpt="", append=False, splitTarget=True):
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
    logging.info("Written %d lines to jobList file" % lineCount)

# MAIN ENTRY POINT, if script is called from shell
if __name__=="__main__":
    import doctest
    doctest.testmod()

