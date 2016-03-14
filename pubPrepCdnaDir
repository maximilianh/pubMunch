#!/usr/bin/env python

# first load the standard libraries from python
# we require at least python 2.7
#from sys import *
import sys
if sys.version_info[0]==2 and not sys.version_info[1]>=7:
    print "Sorry, this program requires at least python 2.7"
    print "You can download a more current python version from python.org and compile it"
    print "into your homedir (or anywhere else) with 'configure --prefix ~/python27'; make;"
    print "then run this program again by specifying your own python executable like this: "
    print "   ~/python27/bin/python <%s>" % sys.argv[0]
    print "or add ~/python27/bin to your PATH before /usr/bin"
    exit(1)

# load default python packages
import logging, optparse, os, glob, shutil, gzip
from os.path import *
from collections import defaultdict

# add <scriptDir>/lib/ to package search path
progFile = os.path.abspath(sys.argv[0])
progDir  = os.path.dirname(progFile)
pubToolsLibDir = os.path.join(progDir, "lib")
sys.path.insert(0, pubToolsLibDir)

# now load our own libraries
import pubConf, pubGeneric, pubAlg, maxCommon, bigBlat , bedLoci
from os.path import *

# === COMMAND LINE INTERFACE, OPTIONS AND HELP ===
parser = optparse.OptionParser("""usage: %prog [cdna|loci] <db or "all">: prepare cdna database and loci-partitioning of genomes for a given db or all dbs.

db can be "all".

cdna: get refseq alignments from UCSC database, their sequences and translate
them to peptides. 
      The output directory is required for the cdna alignments of pubMap.
      Required: hgsql, faTrans, getRna, blat, faSplit and UCSC mysql access

loci: convert mrna alignments to chrom,start,end-range, use only longest transcript
      per gene, take exons, assign space around exons to exons and write as bed file
      annotated with gene symbols and entrez ID. 
      The output directory is required for the tables step of pubMap
      Required: a completed cdna step and a copy of the NCBI genes ftp server
""")

parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages") 
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="show more debug messages") 
parser.add_option("", "--db", dest="db", action="store", help="run only on this db, e.g. hg19") 
parser.add_option("", "--onlyMissing", dest="onlyMissing", action="store_true", help="get only the missing databases") 
(options, args) = parser.parse_args()

# ==== FUNCTIONS =====
def prepCdnaDir(dbList, onlyMissing):
    " config targetdir for blatCdna "
    tmpDir    = join(pubConf.getTempDir() , "pubBlatCdna")
    targetDir = pubConf.cdnaDir
    pslTable  = pubConf.cdnaTable

    for db in dbList:
        dbDir = join(targetDir, db)
        if onlyMissing and isdir(dbDir):
            logging.info("dir %s already exists, skipping" % dbDir)
            continue
        maxCommon.mustBeEmptyDir(dbDir, makeDir=True)
        logging.info("db is %s" % db)

        logging.info("getting psl from mysql")
        pslFile    = join(targetDir, db, "cdna.psl")
        cmd = "hgsql %s -NB -e 'select * from %s' | cut -f2- > %s" % (db, pslTable, pslFile)
        hadError = False
        try:
            maxCommon.runCommand(cmd)
        except Exception:
            hadError = True

        if os.path.getsize(pslFile)==0:
            hadError = True

        if hadError:
            logging.warn("Cannot find table %s for db %s, skipping this genome" % (pslTable, db))
            shutil.rmtree(dbDir)
            continue

        # setup temp dir
        dbTmpDir = join(tmpDir, db)
        if isdir(dbTmpDir):
            shutil.rmtree(dbTmpDir)
        maxCommon.makedirs(dbTmpDir)

        logging.info("Writing mrna accession ids to text file")
        accSet = set()
        for psl in maxCommon.iterTsvRows(pslFile, format="psl"):
            accSet.add(psl.qName)

        logging.info("Found %d mRNAs in alignment table" % len(accSet))

        accFile = open(join(dbTmpDir, "mrnaAccessions.lst"), "w")
        for acc in accSet:
            accFile.write(acc+"\n")
        accFile.close()

        logging.info("Getting mrnas with IDs in text file from database")
        faFileUncomp = join(targetDir, db, "cdna.fa")
        cmd = "getRna -cdsUpperAll %s %s %s" % (db, accFile.name, faFileUncomp)
        maxCommon.runCommand(cmd)

        logging.info("translating mrnas to protein")
        faTransFile = join(targetDir, db, "prot.fa")
        cmd = "faTrans -cdsUpper %s %s" % (faFileUncomp, faTransFile)
        maxCommon.runCommand(cmd)

        #logging.info("uncompressing fa file")
        #ftpDir = "/cluster/data/genbank/data/ftp/%s/bigZips/" % db
        #faPath = join(ftpDir, faFile)
        #faFileUncomp = join(dbTmpDir, db+".fa")
        #tmpFaPath = join(dbTmpDir, faFileUncomp)
        #cmd = "gunzip -c %s > %s" % (faPath, faFileUncomp)
        #maxCommon.runCommand(cmd)

        #logging.info("getting sizes of fasta sequences")
        #sizesFile = join(dbTmpDir, "faSizes.tab")
        #cmd = "faSize -detailed %s > %s" % (faFileUncomp, sizesFile)
        #maxCommon.runCommand(cmd)

        logging.info("creating ooc file")
        oocFile = join(targetDir, db, "11.ooc")
        cmd = "blat -makeOoc=%s %s dummy dummy" % (oocFile, faFileUncomp)
        maxCommon.runCommand(cmd)

        logging.info("splitting fasta")
        splitDir = join(dbTmpDir, "split")
        os.makedirs(splitDir)
        splitBase = join(splitDir, basename(faFileUncomp))
        cmd = "faSplit about %s %d %s" % (faFileUncomp, pubConf.cdnaSplitSize, splitBase)
        maxCommon.runCommand(cmd)

        logging.info("converting to 2bit")
        faMask = join(splitDir, "*.fa")
        faFiles = glob.glob(faMask)
        for faFile in faFiles:
            faBase = splitext(basename(faFile))[0]+".2bit"
            twoBitFile = join(targetDir, db, faBase)
            cmd = "faToTwoBit -noMask %s %s" % (faFile, twoBitFile) ## ahh! mrnas are LOWER CASE on UCSC!
            maxCommon.runCommand(cmd)

        #logging.info("filtering psl, removing alignments where tSize doesn't match fasta seq size")
        #dbPslFile = join(targetDir, db, "%s.psl" % db)
        #seqSizes = tabfile.slurpdict(sizesFile, asInt=True)
        #dbPsl = open(dbPslFile, "w")
        #diffCount = 0
        #pslCount = 0
        #for psl in maxCommon.iterTsvRows(tmpPsl, format="psl"):
            #pslCount+=1
            #seqSize = seqSizes.get(psl.qName, None)
            #if seqSize==None:
                #logging.warn("alignment qName=%s not found in fasta file, skipping" % psl.qName)
                #continue
            #if psl.qSize != seqSize:
                #diffCount+=1
                #logging.warn("psl target size difference for %s, skipping" % psl.qName)
                #continue
            #dbPsl.write("\t".join([str(x) for x in psl])+"\n")
        #dbPsl.close()
        #logging.info("Found %d seqs with different target sizes, out of %d" % (diffCount, pslCount))

        # remove tmpDir
        logging.info("deleting tmp dir")
        shutil.rmtree(tmpDir)

def bedToLen(fname):
    " return lengths of genes in bed "
    geneLens = {}
    for l in open(fname):
        fields = l.split("\t")
        chrom, start, end, name = fields[:4]
        length = int(end)-int(start)
        geneLens[name] = length
    logging.info("Got lengths for %d transcripts"  %len(geneLens))
    return geneLens

def parseGeneRefseq(ncbiGenesDir, dbList):
    """
    parse ncbi genes gene2refseq.gz. return as dict refseqId -> (geneId, symbol)
    """
    geneFname = join(ncbiGenesDir, "gene2refseq.gz")

    # for debugging
    hg19Name = join(ncbiGenesDir, "gene2refseq.9606.gz")
    if dbList==["hg19"] and isfile(hg19Name):
        geneFname = hg19Name

    logging.info("Parsing %s, this can take a while" % geneFname)
    ifh = gzip.open(geneFname)
    l1 = ifh.readline()
    assert(l1.startswith("#")) # if error here, format change?

    refseqToGene = {}
    geneToSym = {}
    for line in ifh:
        line = line.rstrip("\n")
        fields = line.split('\t')
        taxId, geneId, status, refseqId = fields[:4]
        refseqId = refseqId.split(".")[0]
        geneId = int(geneId)
        if status=="SUPPRESSED":
            continue
        symbol = fields[15]
        refseqToGene[refseqId]=geneId
        geneToSym[geneId] = symbol
    logging.info("Mapped %d refseqs to %s genes" % (len(refseqToGene), len(geneToSym)))
    return refseqToGene, geneToSym
        
def onlyLongestTranscripts(transLengths, refseqToGene, geneToSym):
    """ given transcript lenghts and transcript to gene / transcript to symbol mappings
    return dict transcriptId -> (geneId, symbol) 
    Transcripts not in this dict are not longest transcripts.
    """
    # index by geneId
    geneToRefseq = defaultdict(set)
    noGene = 0
    for refseqId in transLengths:
        geneId = refseqToGene.get(refseqId, None)
        if geneId==None:
            noGene += 1
            continue
        geneToRefseq[geneId].add(refseqId)
    logging.info("Skipped %d refseqs without a gene ID" % noGene)
    logging.info("Got %d geneIds" % len(geneToRefseq))

    # for each gene, take only longest refseqid
    refseqToGeneSym = {}
    for geneId, refseqIds in geneToRefseq.iteritems():
        #print "gene", geneId
        sym = geneToSym[geneId]
        #print "sym", sym
        refseqs = list(refseqIds)
        refseqAndLens = []
        for rid in refseqs:
            transLen = transLengths[rid]
            refseqAndLens.append( (transLen, rid) )
        refseqAndLens.sort()
        #print "allbyLen", refseqAndLens
        longestTrans = refseqAndLens[-1][1]
        #print "longest", longestTrans
        refseqToGeneSym[longestTrans] = str(geneId)+"|"+sym
    return refseqToGeneSym
    
def bedFilterNamesRewrite(filename, bedNewNames):
    """ Remove all features shorter than minLen bp from 
    filename and return new temp filename """
    tmpFile, tmpFilename = pubGeneric.makeTempFile(prefix="pubPrepCdnaDir_filtered", suffix=".bed")
    if pubGeneric.debugMode:
        tmpFilename = "exons.bed"
        tmpFile = open(tmpFilename, "w")

    logging.debug("Filtering %s to %s" % (filename, tmpFilename))
    for line in open(filename):
        chrom, start, end, name, strand = line.rstrip("\n").split("\t")[:5]
        if name not in bedNewNames:
            continue
        newName = bedNewNames[name]
        line = "\t".join((chrom, start, end, newName, strand))
        tmpFile.write(line)
        tmpFile.write("\n")
    tmpFile.flush()
    return tmpFile, tmpFilename

def writeMidpoints(mids, fname):
    ofh = open(fname, "w")
    for chrom, midTuples in mids.iteritems():
        for mid, name in midTuples:
            row = [chrom, mid, mid+1, name]
            row = [str(x) for x in row]
            ofh.write("\t".join(row))
            ofh.write("\n")
    ofh.close()
    logging.warn("midpoints written to %s" % fname)

def rewriteBedLoci(db, transcriptFname, refseqToGeneSym, lociFname):
    """
    Given a transcript file, create a filtered loci file and annotate with gene ids and symbols
    A "loci" file is a bed file of ranges flanking the midpoints of exons.
    Space between every two midpoints is assigned to both neighbors.
    A loci file allows to assign any feature (e.g. in intron) to only one single transcript.
    """
    # convert transcripts to exons
    exonFh, exonBedFname = pubGeneric.makeTempFile(prefix="pubPreCdnaDir", suffix=".bed")
    cmd = "bedToExons %s stdout | sort -k1,1 -k2,2n > %s" % (transcriptFname, exonBedFname)
    maxCommon.runCommand(cmd)
    exonFh.flush()
    # keep only exons of longest transcripts and replace refseqId with gene|symbol
    geneExonFh, geneExonFname = bedFilterNamesRewrite(exonBedFname, refseqToGeneSym)

    # assign range around exons to closest exon around midpoints
    outf = open(lociFname, "w")
    chromSizesFname = bigBlat.findChromSizes(db)
    outf = open(lociFname, "w")
    mids = bedLoci.parseBedMids(open(geneExonFname))

    # when debugging: output midpoints to file
    if pubGeneric.debugMode:
        writeMidpoints(mids, "temp.bed")

    chromSizes = bedLoci.slurpdict(chromSizesFname)
    bedLoci.outputLoci(mids, chromSizes, outf)
    outf.close()
    logging.info("Loci coords and names written to %s" % lociFname)

def prepLociDir(dbList, onlyMissing):
    " fill the lociDir with loci files, requires entrez genes data "
    genesDir = pubConf.ncbiGenesDir
    lociDir = pubConf.lociDir
    cdnaDir = pubConf.cdnaDir

    refseqToGene, refseqToSym = parseGeneRefseq(pubConf.ncbiGenesDir, dbList)

    for db in dbList:
        # convert psl to bed
        logging.info("creating loci for %s" % db)
        pslFname = join(cdnaDir, db, "cdna.psl")
        if not isfile(pslFname):
            logging.info("Could not find %s, skipping db %s, no refseq alignments" % (pslFname, db))
            continue
        tempFh, transBedFname = pubGeneric.makeTempFile(prefix="pubPreCdnaDir", suffix=".bed")
        cmd = "pslToBed %s %s " % (pslFname, transBedFname)
        maxCommon.runCommand(cmd)

        # rewrite exons in bed: 1) keep only longest transcript per gene 
        transLengths = bedToLen(transBedFname)
        refseqToGeneSym = onlyLongestTranscripts(transLengths, refseqToGene, refseqToSym)
        lociFname = join(lociDir, db+".bed")
        rewriteBedLoci(db, transBedFname, refseqToGeneSym, lociFname)

def main(args, options):
    if len(args)<=1:
        parser.print_help()
        exit(1)

    pubGeneric.setupLogging(progFile, options)
    cmd = args[0]
    db = args[1]

    if db=="all":
        dbList    = pubConf.alignGenomeOrder
    elif "," in db:
        dbList = db.split(",")
    else:
        dbList = [db]

    if cmd=="cdna":
        prepCdnaDir(dbList, options.onlyMissing)
    elif cmd=="loci":
        prepLociDir(dbList, options.onlyMissing)
    else:
        assert(False)

main(args, options)
