#!/usr/bin/env python
# blat one input file against target spec, liftUp and filter

# works with old pythons >2.3 etc on cluster nodes
# based on Hiram's runOne script in /hive/data/genomes/hg19/bed/fosEndPairs/mapEnds/run4M/
# spiced up with some markd-things

import sys, os, logging, tempfile, optparse
from os.path import *
from tempfile import *

# === COMMAND LINE PARSING / HELP ===
# ===== FUNCTIONS =====

def mustRunCommand(cmd):
    logging.debug("Running: %s" % cmd)
    ret = os.system(cmd)
    if ret!=0:
        raise Exception("Could not run command %s" % cmd)

def splitAddDashes(string):
    """ prefix all space-sep string parts with - """
    if string==None:
        return ""
    parts = string.split(",")
    dashedList = ["-"+x for x in parts]
    return " ".join(dashedList)

def readSizes(querySizeFname):
    " parse a name-size file and return as dict "
    sizes = {}
    for line in open(querySizeFname):
        name, size = line.rstrip().split()
        sizes[name] = int(size)
    return sizes
        
from itertools import groupby

#def write5kbSpec(querySpecFile, qFname, querySizes):
    #" write specs of 5kbp chunk of querySizes to querySpecFile and return file name "
    #for queryId, querySize in querySizes.iteritems():
        #start = 0
        #while start+5000 < querySize:
            #end = start+5000
            #querySpecFile.write("%(qFname)s:%(queryId)s:%(start)d-%(end)d\n" % locals())
            #start += 5000
        #if end != querySize:
            #querySpecFile.write("%(qFname)s:%(queryId)s:%(end)d-%(querySize)d\n" % locals())
    #querySpecFile.flush()
    #querySpecFname = querySpecFile.name
    #logging.info("Wrote query specs to %s" % querySpecFname)
    #return querySpecFname

# ===== MAIN ====
if __name__ == "__main__":
    parser = optparse.OptionParser("""usage: %prog [options] <genomeSpec> <queryFile> <outFile> <pslOptions> - run blat on a piece of a twoBit file, lift if necessary, filter with pslCdnaFilter and copy to outFile. 

    genomeSpec format is one of the following:
    - <dbFile>
    - <dbFile>:<chrom>
    - <dbSpecFile>, like the one that can be read by BLAT: one range per line, like chr1:1-1000
      This requires the -s option

    examples:

    bigBlatJob.py  /tmp/test/fasta/ci2.long.fa test.psl -f "minNearTopSize=19 nearTop=0.01 minCover=0.80 ignoreSize"

    bigBlatJob.py /scratch/data/hg19/hg19.2bit:123213213:chr1:227417-267719 /tmp/test/fasta/hg19.fa test.psl -f "minNearTopSize=19 nearTop=0.01 minCover=0.80 ignoreSize" -b "minScore=20"
    """)

    parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages")
    parser.add_option("-f", "--filterOption", dest="filterOptions", action="store", help="add these space-sep options to pslCDnaFilter, do not specify the leading '-'")
    parser.add_option("-b", "--blatOption", dest="blatOptions", action="store", help="add these space-sep options to blat, do not specify the leading '-'")
    parser.add_option("-t", "--tmpDir", dest="tmpDir", action="store", help="local cluster node temp dir, default %s", default="/dev/shm")
    parser.add_option("-s", "--chromSizes", dest="chromSizes", action="store", help="file with sizes for chromosomes, required if using the dbSpecFile target format")
    parser.add_option("-l", "--queryLiftFile", dest="queryLift", action="store", help="file with a query liftUp filename")
    parser.add_option("", "--dbDir", dest="dbDir", action="store", help="local cluster genome dir, default %s", default="/scratch/data/")
    parser.add_option("", "--querySizes", dest="querySizeFname", action="store", help="a file with queryName, querySize, one per line. Triggers query chunking: each job will go over query in 5kbp chunks and create alignments for each chunk, this is required for fastMap mode")

    (options, args) = parser.parse_args()

    if options.debug:
        logger = logging.getLogger('').setLevel(logging.DEBUG)
    else:
        logger = logging.getLogger('').setLevel(logging.INFO)

    if len(args)!=3:
        parser.print_help()
        sys.exit(1)

    tSpec, qFname, outFname = args

    specParts = tSpec.split(":")

    if tSpec.endswith(".spec"):
        # parse the full target spec string
        # /scratch/hg19/hg19.2bit:123213213:chr1:227417-267719
        # or a filename like
        # /hive/data/inside/blatrun/targets/0000.spec
        #twoBitFname, chrom, chromSize, chromRange = specParts
        #db = splitext(basename(twoBitFname))[0]
        #startEnd = chromRange.split("-")
        #tStart = startEnd[0]
        #tEnd = startEnd[1]
        #tChromSize = str(int(tEnd)-int(tStart))
        #tChromSize = chromSize
        #seqFrag = ":".join([chrom, chromRange]) 

        # create a temp liftUp file
        #liftData = [tStart, seqFrag, tChromSize, chrom, tChromSize]
        #liftFilePrefix = "%s-%s-%s" % (db, chrom, tStart)
        #liftFile = NamedTemporaryFile(dir=options.tmpDir, suffix=".lift", prefix=liftFilePrefix)
        #liftFile.write("\t".join(liftData))
        #liftFile.write("\n")
        #liftFile.file.flush()
        #liftFileName = liftFile.name

        # prep 2bit spec with db,chrom,pos
        #twoBitFname = join(options.dbDir, db, db+".2bit")
        blatTargetSpec = tSpec
        chromSizesFname = options.chromSizes
        needTLifting=True

    elif len(specParts)==1:
        # prep 2bit spec from only db
        twoBitFname = specParts[0]
        blatTargetSpec = twoBitFname
        needTLifting = False

    elif len(specParts)==2:
        # prep 2bit spec from db and chrom
        twoBitFname, chrom = specParts
        blatTargetSpec = twoBitFname+":"+chrom
        needTLifting = False

    else:
        raise Exception("illegal format for target spec")
        
    blatOptString = splitAddDashes(options.blatOptions)
    filterOptString = splitAddDashes(options.filterOptions)

    tmpPslFile = NamedTemporaryFile(dir=options.tmpDir, prefix="bigBlatJob", suffix=".psl")
    tmpPslFname = tmpPslFile.name

    if needTLifting:
        blatOutName = "stdout"
    else:
        blatOutName = tmpPslFname

    if options.querySizeFname:
        qSizeFname = options.querySizeFname
        #querySizes = readSizes(options.querySizeFname)
        #querySpecFile = NamedTemporaryFile(dir=options.tmpDir, prefix="bigBlatJob.querySpec", suffix=".spec")
        #querySpecFile = open("/tmp/temp.sizes", "w")
        #qFname = write5kbSpec(querySpecFile, qFname, querySizes)

    blatCmd = "set -o pipefail; blat %(blatTargetSpec)s %(qFname)s %(blatOutName)s -noHead %(blatOptString)s" % locals()

    if needTLifting:
        #blatCmd += "| liftUp -type=.psl %(tmpPslFname)s %(liftFileName)s error stdin" % locals()
        blatCmd += "| pslLiftSubrangeBlat stdin %(tmpPslFname)s -tSizes=%(chromSizesFname)s" % locals()
    mustRunCommand(blatCmd)

    #filterCommand = "pslReps -nohead %(filterOptString)s  %(tmpPslFname)s %(outFname)s /dev/null" % locals()

    if options.querySizeFname:
        filterOutFname = "stdout"
    else:
        filterOutFname = outFname

    filterCommand = "set -o pipefail; pslCDnaFilter %(filterOptString)s  %(tmpPslFname)s %(filterOutFname)s" % locals()
    if options.querySizeFname:
        filterCommand += " | pslLiftSubrangeBlat stdin %(outFname)s -qSizes=%(qSizeFname)s" % locals()

    mustRunCommand(filterCommand)
