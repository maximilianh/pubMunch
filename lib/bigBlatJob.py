#!/usr/bin/env python
# blat one input file against target spec, liftUp and filter

# works with old pythons >2.3 etc on cluster nodes
# based on Hiram's runOne script in /hive/data/genomes/hg19/bed/fosEndPairs/mapEnds/run4M/

import sys, os, logging, tempfile, optparse
from os.path import *
from tempfile import *

# === COMMAND LINE PARSING / HELP ===
parser = optparse.OptionParser("""usage: %prog [options] <genomeSpec> <queryFile> <outFile> <pslOptions> - run blat on a piece of a twoBit file, lift if necessary, filter with pslCdnaFilter and copy to outFile. 

genomeSpec format is one of the following:
- <dbFile>
- <dbFile>:<chrom>
- <dbFile>:<chromSize>:<chrom>:<start>-<end> like /scratch/hg19/hg19.2bit:123213213:chr1:227417-267719

examples:

bigBlatJob.py  /tmp/test/fasta/ci2.long.fa test.psl -f "minNearTopSize=19 nearTop=0.01 minCover=0.80 ignoreSize"

bigBlatJob.py /scratch/data/hg19/hg19.2bit:123213213:chr1:227417-267719 /tmp/test/fasta/hg19.fa test.psl -f "minNearTopSize=19 nearTop=0.01 minCover=0.80 ignoreSize" -b "minScore=20"
""")

parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages")
parser.add_option("-f", "--filterOption", dest="filterOptions", action="store", help="add these space-sep options to pslCDnaFilter, do not specify the leading '-'") 
parser.add_option("-b", "--blatOption", dest="blatOptions", action="store", help="add these space-sep options to blat, do not specify the leading '-'") 
parser.add_option("-t", "--tmpDir", dest="tmpDir", action="store", help="local cluster node temp dir, default %s", default="/scratch/tmp/") 
parser.add_option("", "--dbDir", dest="dbDir", action="store", help="local cluster genome dir, default %s", default="/scratch/data/") 

(options, args) = parser.parse_args()

if options.debug:
    logger = logging.getLogger('').setLevel(logging.DEBUG)

if len(args)!=3:
    parser.print_help()
    sys.exit(1)

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

# ===== MAIN ====
tSpec, qFname, outFname = args

specParts = tSpec.split(":")

if len(specParts)==1:
    # prep 2bit spec from only db
    twoBitFname = specParts[0]
    blatTargetSpec = twoBitFname
    needLifting = False

elif len(specParts)==2:
    # prep 2bit spec from db and chrom
    twoBitFname, chrom = specParts
    blatTargetSpec = twoBitFname+":"+chrom
    needLifting = False

elif len(specParts)==4:
    # parse the full target spec string
    # /scratch/hg19/hg19.2bit:123213213:chr1:227417-267719
    twoBitFname, chrom, chromSize, chromRange = specParts
    db = splitext(basename(twoBitFname))[0]
    startEnd = chromRange.split("-")
    tStart = startEnd[0]
    tEnd = startEnd[1]
    #tChromSize = str(int(tEnd)-int(tStart))
    tChromSize = chromSize
    seqFrag = ":".join([chrom, chromRange]) 

    # create a temp liftUp file
    liftData = [tStart, seqFrag, tChromSize, chrom, tChromSize]
    liftFilePrefix = "%s-%s-%s" % (db, chrom, tStart)
    liftFile = NamedTemporaryFile(dir=options.tmpDir, suffix=".lift", prefix=liftFilePrefix)
    liftFile.write("\t".join(liftData))
    liftFile.write("\n")
    liftFile.file.flush()
    liftFileName = liftFile.name

    # prep 2bit spec with db,chrom,pos
    twoBitFname = join(options.dbDir, db, db+".2bit")
    blatTargetSpec = twoBitFname+":"+chrom+":"+tStart+"-"+tEnd
    needLifting=True

else:
    raise Exception("illegal format for target spec, should look like hg19:chr1:123123123:227417-267719 or hg19:chr1 or hg19")
    
blatOptString = splitAddDashes(options.blatOptions)
filterOptString = splitAddDashes(options.filterOptions)

tmpPslFile = NamedTemporaryFile(dir=options.tmpDir, prefix="bigBlatJob", suffix=".psl")
tmpPslFname = tmpPslFile.name

if needLifting:
    blatOutName = "stdout"
else:
    blatOutName = tmpPslFname

blatCmd = "blat %(blatTargetSpec)s %(qFname)s %(blatOutName)s -noHead %(blatOptString)s" % locals()

if needLifting:
    blatCmd += "| liftUp -type=.psl %(tmpPslFname)s %(liftFileName)s error stdin" % locals()

mustRunCommand(blatCmd)

#filterCommand = "pslReps -nohead %(filterOptString)s  %(tmpPslFname)s %(outFname)s /dev/null" % locals()
filterCommand = "pslCDnaFilter %(filterOptString)s  %(tmpPslFname)s %(outFname)s" % locals()

mustRunCommand(filterCommand)
