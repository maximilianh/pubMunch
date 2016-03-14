#!/usr/bin/env python
# load default python packages
import sys, logging, optparse, os, glob, shutil, gzip, collections
from os.path import *

# add <scriptDir>/lib/ to package search path
progFile = os.path.abspath(sys.argv[0])
progDir  = os.path.dirname(progFile)
pubToolsLibDir = os.path.join(progDir, "lib")
sys.path.insert(0, pubToolsLibDir)

# now load our own libraries
import pubConf, pubGeneric, util, maxCommon
from os.path import *

# === COMMAND LINE INTERFACE, OPTIONS AND HELP ===
parser = optparse.OptionParser("""usage: %prog <markerType>: Download marker info from UCSC database and manualMarkers directory, write to directory specified in pubConf.py

markerType can be gene, snp, genbank, refseq, ensembl or "all"
""")

#parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages") 
#parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="show more debug messages") 
#parser.add_option("-o", "--only", dest="only", action="store", help="only create data for one type, e.g. symbol, band, uniprot, snp") 
pubGeneric.addGeneralOptions(parser, noCluster=True)
(options, args) = parser.parse_args()

# ==== FUNCTIONS =====
def mysqlToFile(fname, db, sql, append=False):
    " write mysql cmd output to file "
    logging.info("Writing %s, mysql query: %s" % (fname, sql))
    redirSym = ">"
    if append==True:
        redirSym = ">>"
    cmd = "mysql %s -NB -e '%s' %s %s"  % (db, sql, redirSym, fname)
    util.execCmdLine(cmd)

def writeNames(fname, outFname):
    " write name fields of bed to gz tab file "
    logging.info("Writing %s" % outFname)
    ofh = gzip.open(outFname, "w")
    #ofh.write("#id\n")
    for line in open(fname):
        fields = line.strip().split("\t")
        name = fields[3]
        chrom = fields[0]
        ofh.write(name)
        ofh.write("\n")

def makeUniqueNames(bedIn, bedOut, stripVersion=False):
    " write from bedIn to bedOut, skip all lines with a duplicate name field or spaces in name or hap chroms"
    logging.info("Filtering %s to %s" % (bedIn, bedOut))
    ofh = open(bedOut, "w")
    oldNames = set()
    inCount = 0
    outCount = 0
    for line in open(bedIn):
        inCount += 1
        fields = line.strip().split("\t")
        name = fields[3]
        if stripVersion:
            name = name.split(".")[0]
            fields[3] = name
            line = "\t".join(fields)+"\n"
        chrom = fields[0]
        if name in ["unknown"] or len(name)<=2:
            continue
        if " " in name:
            continue
        if "hap" in chrom:
            continue
        if name not in oldNames:
            outCount += 1
            ofh.write(line)
        oldNames.add(name)
    logging.info("input lines %d, output lines %d, removed %d lines with duplicate names" % \
        (inCount, outCount, outCount - inCount))
        
def rewriteBands(bedIn, bedOut):
    " prefix names with chrom, for bands, and add the larger scale band, too "
    logging.info("Adding chromosome numbers, filtering %s to %s" % (bedIn, bedOut))
    origBands = {} # dict with [chrom, start, end] tuples
    for line in open(bedIn):
        fields = line.strip().split("\t")
        chrom, start, end, band = fields
        start = int(start)
        end = int(end)
        chromNum = chrom.replace("chr","")
        bandName = chromNum+band
        origBands[bandName] = (chrom, start, end)

    largeBands = {}
    for bandName, coordTuple in origBands.iteritems():
        if not "." in bandName:
            continue
        chrom, start, end = coordTuple
        largeBand = bandName.split(".")[0]
        if not largeBand in largeBands:
            largeBands[largeBand] = (chrom, start, end)
        else:
            oldStart = largeBands[largeBand][1]
            oldEnd = largeBands[largeBand][2]
            largeBands[largeBand] = (chrom, min(oldStart, start, end), max(oldEnd, start, end))

    allBands = origBands.items()
    allBands.extend(largeBands.items())
            
    ofh = open(bedOut, "w")
    for bandName, coordTuple in allBands:
        chrom, start, end = coordTuple
        row = [chrom, str(start), str(end), bandName]
        ofh.write("\t".join(row))
        ofh.write("\n")
    ofh.close()

def rewriteSnp(bedIn, bedOut):
    " massage snp coordinates to force them into normal bed format "
    logging.info("forcing snpbed to bed format, filtering %s to %s" % (bedIn, bedOut))
    ofh = open(bedOut, "w")
    oldNames = set()
    for line in open(bedIn):
        fields = line.strip().split("\t")
        start, end = fields[1:3]
        start, end = int(start), int(end)
        if end<start:
            start, end = end, start
        if end==start:
            end = end+1
        row = [fields[0], str(start), str(end), fields[3]]
        ofh.write("\t".join(row))
        ofh.write("\n")


def addToDict(fname, data):
    " parse tab-sep file with two fields into dict id -> list of values "
    lCount = 0
    oldKeyCount = len(data)
    for line in open(fname):
        key, val = line.strip().split("\t")
        data[key].append(val)
        lCount += 1
    logging.info("Read %d lines, %d new keys" % (lCount, len(data)-oldKeyCount))
    return data

def writeGzDict(data, fname):
    logging.info("Writing %d keys to %s" % (len(data), fname))
    ofh = gzip.open(fname, "w")
    for key, valList in data.iteritems():
        for val in valList:
            assert("|" not in val)
        ofh.write("%s\t%s\n" % (key, "|".join(valList)))

def mysqlAddKeyVal(db, query, dataDict):
    tempFname = join(pubConf.TEMPDIR, "uniprot.tmp.bed")
    mysqlToFile(tempFname, db, query)
    addToDict(tempFname, dataDict)
    os.remove(tempFname)
    return dataDict

#def makeGeneDict(geneDictFname):
#    " use the UniProt database to create a dictionary uniprotId to all other relevant ids "
#    data = collections.defaultdict(list)
#
#    # uniprot alternative acc numbers 
#    mysqlAddKeyVal("uniProt", 'select acc, val from otherAcc', data)
#    # uniprot names
#    mysqlAddKeyVal("uniProt", 'select acc, val from displayId', data)
#    # pdb accessions
#    mysqlAddKeyVal("uniProt", 'select acc, extAcc1 from extDbRef JOIN extDb ON (id=extDb) WHERE val="PDB"', data)
#    # refseq
#    mysqlAddKeyVal("uniProt", 'select acc, extAcc2 from extDbRef JOIN extDb ON (id=extDb) WHERE val="RefSeq";', data)
#    # ensembl gene ids
#    #mysqlAddKeyVal("uniProt", 'select acc, extAcc3 from extDbRef JOIN extDb ON (id=extDb) WHERE val="Ensembl";', data)
#    writeGzDict(data, geneDictFname)

def writePdbFnames(pdbDir, pdbDictFname):
    " find all files in a pdb local copy and write acc numvers to ouput gz files "
    # pdb100d.ent.gz
    logging.info("Reading filenames from %s" % pdbDir)
    accList = []
    for dir, path in pubGeneric.findFiles(pdbDir, ".gz"):
        acc = basename(path).split(".")[0].replace("pdb","")
        accList.append(acc)

    logging.info("Found %d accession numbers, writing to %s" % (len(accList), pdbDictFname))
    ofh = gzip.open(pdbDictFname, "w")
    for acc in accList:
        ofh.write(acc)
        ofh.write("\n")
    ofh.close()
    pubGeneric.indexKvFile(pdbDictFname)

def prepMarkerDir(markerDir, markerType):
    " config targetdir for blatCdna "

    if not isdir(markerDir):
        logging.info("Creating %s" %markerDir)
        os.mkdir(markerDir)

    tempFile = join(pubConf.TEMPDIR, "filter.tmp.bed")

    if markerType=="all" or markerType=="gene":
        # gene symbols
        symbolBedFname = join(markerDir, "hg19.gene.bed")
        mysqlToFile(tempFile, "hg19", 'SELECT chrom, txStart, txEnd, kgXref.geneSymbol from knownGene JOIN kgXref ON kgXref.kgID=knownGene.name')
        makeUniqueNames(tempFile, symbolBedFname)
        #symbolDictFname = join(markerDir, "gene.dict.tab.gz")
        #writeNames(symbolBedFname, symbolDictFname)

    if markerType=="all" or markerType=="band":
        bandBed = join(markerDir, "hg19.band.bed")
        bandDict = join(markerDir, "band.dict.tab.gz")
        mysqlToFile(tempFile, "hg19", "SELECT chrom, chromStart, chromEnd, name from cytoBand")
        rewriteBands(tempFile, bandBed)
        writeNames(bandBed, bandDict)

    if markerType=="all" or markerType=="uniprot":
        upBedFname = join(markerDir, "hg19.uniprot.bed")
        upDictFname = join(markerDir, "uniprotAccs.txt.gz")
        # write all uniprot accessions to the dict file
        upFnames = ["uniprot.tab", "uniprotTrembl.tab"]
        #upFnames = ["uniprot.test.tab"]
        accs = set()
        ofh = gzip.open(upDictFname, "w")
        for upFname in upFnames:
            upFname=join(pubConf.dbRefDir, upFname)
            logging.info("Parsing %s to get a list of all accession IDs" % upFname)
            i = 0
            for row in maxCommon.iterTsvRows(upFname):
                accList = row.accList.split("|")
                #logging.info("Got %d keys" % len(accList))
                for acc in accList:
                    ofh.write("%s\n" % acc)
        ofh.close()
        pubGeneric.indexKvFile(upDictFname)

        #logging.info("Got %s uniprot accessions, writing to %s" % (len(accs), upDictFname))
        #ofh = gzip.open(upDictFname, "w")
        #accs = list(accs)
        #accs.sort() # better compression
        #for acc in accs:
            #ofh.write(acc)
            #ofh.write("\n")
        #ofh.close()
            
        # write a map for uniprot on hg19 
        logging.info("Creating hg19 map")
        mysqlToFile(upBedFname, "hg19", 'SELECT chrom, txStart, txEnd, kgXref.spID from knownGene JOIN kgXref ON kgXref.kgID=knownGene.name where spID<>"" and not spId like "%-%"')
        # rather keep duplicates
        #makeUniqueNames(tempFile, upBedFname)  

    if markerType=="all" or markerType=="pdb":
        # get all pdb accessions
        #pdbDir = join(pubConf.pdbBaseDir, "pdb")
        pdbDir = pubConf.pdbBaseDir
        pdbDictFname = join(markerDir, "pdbAccs.txt.gz")
        writePdbFnames(pdbDir, pdbDictFname)
        pubGeneric.indexKvFile(pdbDictFname)

        # genome map file
        pdbBedFname = join(markerDir, "hg19.pdb.bed")
        mysqlToFile(tempFile, "hg19", 'SELECT chrom, txStart, txEnd, uniProt.extDbRef.extAcc1 from knownGene JOIN kgXref ON kgXref.kgID=knownGene.name JOIN uniProt.extDbRef ON (spID=acc) JOIN uniProt.extDb ON (id=extDb) WHERE extDb.val="PDB" AND kgXref.spID<>"" AND NOT kgXref.spId like "%-%"')
        makeUniqueNames(tempFile, pdbBedFname)
        #writeNames(upBedFname, upDictFname)


    if markerType=="all" or markerType=="snp":
        snpBed = join(markerDir, "hg19.snp.bed")
        mysqlToFile(tempFile, "hg19", "SELECT chrom, chromStart, chromEnd, name from snp137")
        rewriteSnp(tempFile, snpBed)
        # no snp dict - we use regular expressions for these

    if markerType=="genbank" or markerType=="all":
        gbBed = join(markerDir, "hg19.genbank.bed")
        mysqlToFile(tempFile, "hg19", "SELECT tName, tStart, tEnd, qName from gbDnaBigChain")
        mysqlToFile(tempFile, "hg19", "SELECT tName, tStart, tEnd, qName from all_est", append=True)
        mysqlToFile(tempFile, "hg19", "SELECT tName, tStart, tEnd, qName from all_mrna", append=True)
        makeUniqueNames(tempFile, gbBed, stripVersion=True)

    if markerType=="refseq" or markerType=="all":
        refBed = join(markerDir, "hg19.refseq.bed")
        mysqlToFile(refBed, "hg19", "SELECT chrom, txStart, txEnd, name from refGene")

    if markerType=="ensembl" or markerType=="all":
        refBed = join(markerDir, "hg19.ensembl.bed")
        mysqlToFile(refBed, "hg19", "SELECT chrom, txStart, txEnd, name2 from ensGene")

    if isfile(tempFile):
        os.remove(tempFile)

if len(args)==0:
    parser.print_help()
    exit(1)

pubGeneric.setupLogging(progFile, options)
markerDir = pubConf.markerDbDir
markerType = args[0]
prepMarkerDir(markerDir, markerType)
