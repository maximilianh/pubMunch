#!/usr/bin/env python
import glob, sys, os, logging, optparse
from os.path import *
import bigBlat

# the extension of this script shows that I understand abstractions. Whatever that means.

def pubBigBlat(dbString, qFastas, outDir, options):
    " blat qFastas to outDir "
    if not isdir(outDir):
        logging.info("Creating %s" % outDir)
        os.makedirs(outDir)
    else:
        fnames = os.listdir(outDir)
        if len(fnames)!=0:
            logging.warn("%s is not empty" % outDir)

    #mustBeEmptyDir(outDir)
    params={}
    params["window"]=options.window
    params["overlap"]=options.overlap
    params["maxGap"]=options.maxGap
    params["minUnplacedSize"]=options.minUnplacedSize

    jlName = join(outDir, "jobList")
    mode = "w"
    if options.append:
        mode = "a"
    ofh = open(jlName, mode)
    
    qFastas = bigBlat.resolveToFiles(qFastas)
    twoBitDir = abspath(join(outDir, "qTwoBit"))
    bigBlat.toTwoBit(qFastas, twoBitDir)

    qSizeDir = None
    if options.splitQuery:
        qFastas, qSizeDir = bigBlat.splitQuery(qFastas, outDir)

    logging.info("splitting and chunking genomes: %s" % dbString)
    lineCount = 0
    for line in bigBlat.getJoblines(dbString, qFastas, outDir, params,
        options.chunkTarget, options.blatOpt, options.pslFilterOpt, qSizeDir=qSizeDir):
        ofh.write(line)
        ofh.write("\n")
        lineCount+=1
    ofh.close()
    logging.info("Wrote joblist to %s, %d jobs" % (jlName, lineCount))

def main():
    helpMsg="""usage: %prog <step> <options>  - write JobList for big blat run
    
    step is one of: "aln", "cat", "chain", "net" 

    aln options:
        %prog aln <genomeDbList> <qFasta1> <qFasta2> <...> <outDir>
    cat/chain/net options:
        %prot cat|chain|net <target> <outDir>

    Writes joblist and other required files for big blat job of all query fasta files 
    against a list of genomes

    qFasta can be a single file or a directory with .fa files
    dbList is a comma-sep list of dbs, like "hg19,mm9"
    outDir has to be empty, will be filled with subdirs <queryBasename>/<db>
    (The <db> of outDir is skipped if only one db is specified)

    example:
    mkdir shortAln
    bigBlat aln hg19,mm9 shortSeqs.fa shortAln --winSize=3000000 --overlap=500000 --blatOpt minScore=16 ,minMatch=1,oneOff=1,maxIntron=4 --pslFilterOpt minCover=0.9
    bigBlat cat,chain,net hg19 shortAln
    """

    parser = optparse.OptionParser(helpMsg)

    parser.add_option("-b", "--blatOpt", dest="blatOpt", action="store", \
        help="options to pass to blat with no leading dashes, separated by commas, e.g. minSize=20,fastMap", default="")

    parser.add_option("-d", "--debug", dest="debug", action="store_true", \
        help="output some debug info", default="")

    parser.add_option("-f", "--pslFilterOpt", dest="pslFilterOpt", action="store", \
        help="options to pass to pslCDnaFilter with no leading dashes, separated by commas, e.g. minTop=0.01,minAli=0.8. Can be used in the 'aln' or the 'cat' step", default="")

    parser.add_option("-t", "--chunkTarget", dest="chunkTarget", action="store_true", \
        help="chunk the target into pieces, give spec files to BLAT and lift after blatting")
        
    parser.add_option("", "--winSize", dest="window", action="store", \
        help="target chunking: max size of per piece", type="int", default=80000000)

    parser.add_option("", "--maxGap", dest="maxGap", action="store", \
        help="target chunking: maximum size of gap between two pieces", type="int", default=3000000)

    parser.add_option("", "--overlap", dest="overlap", action="store", \
        help="target chunking: overlap between two pieces", type="int", default=3000000)

    parser.add_option("", "--minUnplacedSize", dest="minUnplacedSize", action="store", \
        help="target chunking: minimum size of unplaced sequences", type="int", default=900)

    parser.add_option("-a", "--append", dest="append", action="store_true", \
        help="do not create a new joblist file but append to the old one")

    parser.add_option("-q", "--splitQuery", dest="splitQuery", action="store_true", \
        help="split queries into 5kb pieces before BLATing and lift after blatting")

    (options, args) = parser.parse_args()

    if options.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    if len(args)<1:
        parser.print_help()
    else:
        command = args[0]

        if command=="aln":
            target = args[1]
            qFastas  = args[2:-1]
            outDir   = args[-1]
            assert(isdir(outDir))
            assert(len(qFastas)!=0)
            pubBigBlat(target, qFastas, outDir, options)
        else:
            target = args[1]
            outDir   = args[2]
            commands = command.split(",")
            if "cat" in commands:
                bigBlat.doPslCat(target, outDir, options.pslFilterOpt)
            if "chain" in commands:
                bigBlat.doChain(target, outDir)
            if "net" in commands:
                bigBlat.doSwapNetSubset(target, outDir)
main()
