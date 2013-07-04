from os.path import *

# GENERAL SETTINGS   ================================================
# baseDir for internal data, accessible from cluster (if there is one)
# used for data created during pipeline runs
pubsDataDir = '/hive/data/inside/pubs'

# static data, accessible from cluster, but part of distribution
# these are basic files like gene lists, marker lists, journal lists
# Some of it can be updated with pubPrepXXX commands
staticDataDir = normpath(join(dirname(__file__), "..", "data"))

ucscScriptDir = normpath(join(dirname(__file__), "..", "ucscScripts"))

journalListDir = join(staticDataDir, "extJournalLists")
publisherDir = join(staticDataDir, "publishers")

# a directory on the main server (hgwdev) where program is run, on local disk
# with lots of space available
localHeadDir = '/scratch/max/pubTools'

# DB PARSER SETTINGS ================================================
# directory for files with parsed DBs each DB, e.g. uniprot or pdb
dbRefDir = '/hive/data/inside/pubs/parsedDbs'

# directories with a local copy of PDB and uniprot, ncbi genes, refseq
uniProtBaseDir = '/hive/data/outside/uniProtCurrent'
pdbBaseDir = '/hive/data/outside/pdb'
ncbiGenesDir = '/hive/data/outside/ncbi/genes/'
#ncbiRefseqDir = '/hive/data/outside/ncbi/refseq/release/vertebrate_mammalian'
ncbiRefseqDir = '/hive/data/outside/ncbi/refseq/H_sapiens/mRNA_Prot'

# CONVERTER SETTINGS ================================================
# for pubConvMedline:
# an sqlite db with the content of medline, kept up-to-date
medlineDbPath = join(pubsDataDir, "text", "medline", "medline.db")

# CRAWLER SETTINGS ==================================================
# directory to store publisher <-> ISSN assignment
journalDataDir = join(pubsDataDir, "publishers")

# the useragent to use for http requests
httpUserAgent = 'genomeBot/0.1 (YOUREMAIL, YOURWEB, YOURPHONE)'

# how long to wait for DNS queries, TCP connects, between retries and TCP reads, in seconds
httpTimeout = 20

# how to long wait for the downloading of files, in seconds
httpTransferTimeout = 30

# catalog full publisher name to internal publisher ID
# pubPrepCrawlDir parses journal data and groups journals by publishers.
# In most cases, to download all journal from a publisher, all you have to do
# is to copy the publisher field from <crawlDir>/_journalData/publisher.tab
# here and define a directory name for it
# format: publisher name -> directory name
crawlPubIds = {
# all ISSNs that wiley gave us go into the subdir wiley
"WILEY Wiley" : "wiley",
# we don't have ISSNs for NPG directly, so we use grouped data from NLM
"NLM Nature Publishing Group" : "npg",
"NLM American Association for Cancer Research" : "aacr",
"HIGHWIRE Rockefeller University Press" : "rupress",
"NLM Mary Ann Liebert" : "mal",
"NLM Oxford University Press" : "oup",
"HIGHWIRE American Society for Microbiology" : "asm",
"NLM Future Science" : "futureScience",
"NLM National Academy of Sciences" : "pnas",
"NLM American Association of Immunologists" : "aai",
"HIGHWIRE Cold Spring Harbor Laboratory" : "cshlp",
"HIGHWIRE The American Society for Pharmacology and Experimental Therapeutics" : "aspet",
"HIGHWIRE Federation of American Societies for Experimental Biology" : "faseb",
"HIGHWIRE Society for Leukocyte Biology" : "slb",
"HIGHWIRE The Company of Biologists" : "cob",
"HIGHWIRE Genetics Society of America" : "genetics",
"HIGHWIRE Society for General Microbiology" : "sgm",
"NLM Informa Healthcare" : "informa"
#"Society for Molecular Biology and Evolution" : "smbe"
}

# SFX server for pubmed entries without an outlink
# YOU NEED TO DEFINE THIS IF YOU WANT TO USE SFX
# the UC SFX server is "http://ucelinks.cdlib.org:8888"
# the UR SFX server is "http://sfx.bib-bvb.de/sfx_ubr"
# but please use your own SFX server at your university
# google for it or ask your library for the URL
crawlSfxServer = None

# in some cases, the automatic publisher<->ISSN assignment has errors
# this is either because the journal has switched publishers or
# because it is simply missing in the journal lists that we have.
# For these cases you can add or re-assign a given ISSN to a directory.
# format: ISSN -> (directory, yearStart, yearEnd)
# yearStart and yearEnd can be None if you want all years
# Any ISSN defined here is not automatically assigned to publishers anymore
# only the manual assignments are used for these ISSNs

crawlIssnOverwrite = {
# The journal of hypertension has been transferred to NPG after 2008
"1941-7225" : ("sciencedirect", 1995, 2008),
"1941-7225" : ("npg", 2008, 2099)
}

# a list of file extensions to download when crawling for supplementary files
# better not .mov,.vid or .avi, nor html 
# you can add additional types on a per-publisher basis with "addSuppFileTypes" in the pubCrawler host config
crawlSuppExts = set(['gif', 'svg', 'tiff', 'tif', 'jpg', 'xls', 'doc', 'pdf', 'ppt', 'asc', 'txt', 'csv', 'tsv', 'tab'])

# DOWNLOAD SETTINGS ==================================================

# for Elsevier updates:
# We need the URL to the Consyn Update RSS feed. 
# Login to consyn.elsevier.com, click on batches/RSS feed, paste the URL here
# they look like ttp://consyn.elsevier.com/batch/atom?key=XXXXXXXXXXXXXXXXXXX
consynRssUrl = "YOURURL"

# GENERAL PUBLICATION FILE CONFIG SETTINGS ============================

# used for other defitions in here: basedir for many other dirs
_pubsDir = "/hive/data/inside/pubs"

# which dataset should be loaded by the "load" step in pubMap ?
loadDatasets = ["elsevier", "pmc", "crawler"]

TEMPDIR = "/tmp/pubTools" # local filesystem on cluster nodes
FASTTEMPDIR = TEMPDIR

maxBinFileSize = 20000000 # maximum filesize of any file before conversion to ASCII
maxTxtFileSize = 10000000 # maximum filesize of any file after conversion to ASCII
minTxtFileSize = 500 # minimum filesize of any file after conversion to ASCII
mapReduceTmpDir = _pubsDir + "/mapReduceTemp" # cluster-wide directory to collect results

# parasol batches dir
clusterBatchDir = _pubsDir + "/runs/"
# the base directory for text repository directories
textBaseDir = _pubsDir + "/text/"
# the central base directory for all text annotations
annotDir = _pubsDir + "/annot/"
# central directory for exported fasta file
faDir = _pubsDir + "/fastaExport/"
# all logfiles
logDir = _pubsDir + "/log/"

# head node of cluster
clusterHeadNode = "swarm.cse.ucsc.edu"
# type of cluster, either parasol or sge
clusterType = "parasol"

# base directory for searcher algorithm code, like regex annotation, dna annotation, etc
scriptDir = "/cluster/home/max/projects/pubs/tools/scripts"

# cmdLine to start jython, used to run java annotators
jythonCmd= "/cluster/home/max/software/jre1.7.0/bin/java -jar "+dirname(__file__)+"/jython.jar"

# assignment of pubMap pipeline steps to cluster machines
# pubMap will ssh into a machine to run these steps
stepHosts = {"sortCdna" : "localhost", "sortProt" : "localhost", "sortGenome" : "localhost"}

# email for ncbi eutil requests and error email by pubCrawl
# at ucsc, overriden by local .pubConf
email = "YOUREMAIL"

# how much to wait between two eutils requests
eutilsDelaySecs = 3

# identifiers for articles, files and annotations are all in 64bit space.
# a certain number of digits are used for articles, files and annotations
# 10 digits for articles,
# 3 for files 
# 5 for annotations
# that means that each publisher cannot have more than one billion articles,
# one article not more than 1000 files and one algorithm cannot return more than
# 100.000 annotations per file
ARTICLEDIGITS=10 # number of digits to use for annotation ID 
FILEDIGITS=3 # number of digits to use for annotation ID 
ANNOTDIGITS=5 # number of digits to use for annotation ID 

# articleIds should start at which position in our namespace?
# this is to make sure that articleIds are unique
identifierStart = {
    "pmc"      : 1000000000,
    "elsevier" : 2000000000,
    "medline"  : 3000000000,
    "genbank"  : 4000000000,
    "imgt"     : 4300000000,
    "pdfDir"   : 4400000000,
    "yif"      : 4500000000,
    "crawler"  : 5000000000,
    "free"     : 6000000000  # to indicate end of list, always keep this here
    # the "free" entry is necessary to indicate the range of the 2nd to last entry
}
# commands to convert various filetypes to ascii text 
# $in and $out will be replaced with temp filenames
# pdf2 is only used if output of pdf contains more than 10 unprintable characters
# as pdfbox is quite slow
CONVERTERS = {
    "doc":"catdoc $in > $out",
    "xls":"xls2csv $in > $out",
    "ppt":"catppt $in > $out",
    "htm":"html2text -nobs $in > $out",
    "csv":"COPY",
    "txt":"COPY",
    "asc":"COPY",
    "xml":"XMLTEXT",
    "nxml":"NXMLTEXT",
    "html":"html2text -nobs $in > $out",
    "pdf":"pdftotext -q -nopgbrk -enc UTF-8 -eol unix $in $out",
    "pdf2":"java -Xmx512m -jar /scratch/pdfbox/pdfbox-app-1.6.0.jar ExtractText $in $out -encoding utf8"
}

# sometimes (e.g. if downloaded from the web) we don't have a file extension, but only 
# a mime-type. The following list maps from mime types to file extensions = converters
MIMEMAP = {
    "application/msword":"doc",
    "application/vnd.ms-excel":"xls",
    "application/vnd.ms-powerpoint":"ppt",
    "text/html":"html",
    "text/csv":"csv",
    "text/tab-separated-values":"csv",
    "text/plain":"txt",
    "application/xml":"xml",
    "application/pdf":"pdf"
}

# override the weird highwire delay times for certain URLs with lower values
# you need to check with Highwire on these first
# at ucsc, this is overriden with our local .pubConf 
highwireDelayOverride = {
}

# when splitting text files for cluster jobs,
# how many "chunks" should be created?
# chunkCount = 2000 # XXX  not used 
# when creating chunks, how many files should go into one chunk?
# used by pubConvCrawl
chunkArticleCount = 400

# for conversion jobs: how many cluster jobs should we run in parallel ?
# can be used to limit I/O and be nice to other cluster users
convertMaxJob = 200

# PMID RESOLVER

# filename of pubCompare output file in source dataset directory
idFname = "medline.ids.tab"

# GENBANK CONVERSION

# genbank divisions to run on
genbankDivisions = ["inv","mam","pat","pln","pri","rod","una","vrt"]

# extract sequences for these species from Genbank
genbankTaxons = ["Homo sapiens", "Mus musculus", "Drosophila melanogaster"]

# ignore sequences that are longer than x bp (e.g. BACS)
genbankMaxLen = 40000

# maximum number of accession for a publication reference
# all submission with more sequences than this are skipped
genbankMaxRefCount = 50

# DNA MAPPING / GENOME BLATTING SETTINGS ============================

pubMapBaseDir = "/hive/data/inside/pubs/map/"

# this is the genbank mapping config file by Mark Diekhans' pipeline
# it is required for genome partitioning 
# current one can be downloaded from:
# http://genome-source.cse.ucsc.edu/gitweb/?p=kent.git;a=blob_plain;f=src/hg/makeDb/genbank/etc/genbank.conf;hb=HEAD
GBCONFFILE     = "/cluster/data/genbank/etc/genbank.conf"

# these variables assign genome to keywords in text files
# the orgDetect.py plugin will create annotations on the text files
# for all of these keywords
# pubMap will then blat sequences from a textfile only on genomes
# for which a keyword has been found
# ALL SPECIES that the pipeline needs to blat on need to be defined here
# even if no species recognition for them is performed

speciesNames = {
'hg19' : ['human', 'sapiens', ' homo ', ' Homo ', 'patient', 'cell line','cell culture'],
'mm9' : ['mouse', 'musculus', 'rodent'],
'rn4' : [' rat ', 'novegicus', 'rodent'],
'nonUcsc_archaea' : [],
'danRer7' : ['zebrafish', 'rerio', 'Danio'],
'xenTro2' : [' xenopus', 'Xenopus', 'tropicalis', 'laevis'],
'susScr3' : [' swine ', ' swines ', ' pigs ', ' pig ', ' porcine ', ' scrofa '],
'bosTau7' : [' cattle ', ' cows ', ' cow ', ' bovine ', ' beef ', ' bovis '],
'galGal4' : [' chicken ', ' poultry ', ' chickens ', ' gallus '],
'oryLat2' : ['medaka', 'Medaka', 'latipes', 'Oryzias'],
'dm3' : [' fruitfly ', ' flies ', 'melanogaster', 'Drosophilids', ' fruitflies '],
'ce10' : ['elegans', 'Caenorhabditis', 'nematode', ' worms'],
'ci2' : ['ascidian', 'intestinalis', 'chordates', 'Ciona'],
'sacCer2' : ['cerevisiae', 'Saccharomyces', 'yeast'],
'nonUcsc_arabidopsisTair10' : ['arabidopsis', 'Arabidopsis', 'thaliana', 'thale cress'],
'nonUcsc_ensg17-PlasmodiumFalciparium-ASM276v1' : ['plasmodium', 'Plasmodium', 'falciparium', 'malaria']
}

# During best-genome filtering, sometimes two genomes score equally
# if this is the case, pick the best one, in this order:
# the first one has highest priority

# if a genome is not part of the genbank config system then you need to prefix it with
# "nonUcsc_". Any db name like this will be searched in nonUcscGenomesDir (see below).
# e.g. nonUcsc_archaea will be resolved to /hive/data/inside/pubs/nonUcscGenomes/archaea.2bit
# The blatter also needs a .ooc file with the same name.
alignGenomeOrder = ['hg19', 'mm9', 'rn4', 'nonUcsc_archaea', 'danRer7', 'dm3',
'xenTro2', 'oryLat2', 'susScr3', 'bosTau7', 'galGal4', 'ci2', 'ce10', 'sacCer2', 
'nonUcsc_arabidopsisTair10', 'nonUcsc_ensg17-PlasmodiumFalciparium-ASM276v1']

# these genomes are used if no species name matches are found
defaultGenomes = ["hg19", "mm9", "rn4", "danRer7", "dm3", "ce10", "nonUcsc_archaea"]

# these genomes are always added, no matter which species names are found
# human might not be recognized as a name for cell lines
# bacteria have so many names that we don't recognize them
alwaysUseGenomes = ["hg19", "nonUcsc_archaea"]

# path for genome files that start with 'nonUcsc_'
# this directory has to contain a geneBank.conf file
# in UCSC format to define parameters for these
# assemblies
nonUcscGenomesDir = _pubsDir+"/nonUcscGenomes"


# for some genomes we don't have refseq data
noCdnaDbs = ["sacCer2", "nonUcsc_archaea", "nonUcsc_arabidopsisTair10", "nonUcsc_ensg17-PlasmodiumFalciparium-ASM276v1"]

# some text datasets are just variants of others
# for these, to avoid annotation id overlaps with the main dataset
# we add some offset to their annotation IDs
specDatasetAnnotIdOffset = {"yif" : 12000 }

# each species in alignGenomeOrder has to be in speciesNames
assert(len(set(alignGenomeOrder).intersection(speciesNames))==len(speciesNames))
assert(len(set(defaultGenomes).intersection(speciesNames))==len(defaultGenomes))
assert(len(set(alwaysUseGenomes).intersection(defaultGenomes))==len(alwaysUseGenomes))

# minimum size of dna sequence to be considered for blatting
minSeqLen=17
# minimum size of protein sequence to be considered for blatting
minProtSeqLen=7
# maximum size of dna or protein sequence
maxSeqLen=50000

# maximum size of sequence to be considered "short", sequences can be "short" or "long"
# 50 because: two neighboring primers in a table are recognized as one long primer.
# Blat will split it, but it has to fall into the short category as otherwise
# each individual match won't exceed the 25bp threshold.
shortSeqCutoff = 50

# the sequences from papers will be split into several fa files per organism
# maximum size of fasta file per organism (to keep hippos low, short/long separated)
queryFaSplitSize = {"long" : 8000000, "short" : 400000 }

# how to split the genomes? used for bigBlat.py
genomeSplitParams = { "winSize" : 3000000, "overlap":100000 }

#  same, but for Cdna and protein blatting
cdnaFaSplitSizes = {"long" : 12000000, "short" : 8000000 }
protFaSplitSizes = {"long" : 12000000}

# biggest chunk of split cDna target files
cdnaSplitSize = 6000000
protSplitSize = 6000000

# options for blatting and pslFiltering for each sequence type
# original values were:
# minAli=0.7,nearTop=0.01,minNearTopSize=18,ignoreSize,noIntrons
# short minAli=0.7,nearTop=0.01
seqTypeOptions = {
    "short": ("repeats=lower,stepSize=5,minScore=16,minMatch=1,maxIntron=5", "minQSize=17,minNonRepSize=15,ignoreNs,bestOverlap,minId=0.95,minCover=0.15,globalNearBest=0.01"),
    "long":  ("repeats=lower,minScore=25", "minQSize=18,minNonRepSize=16,ignoreNs,bestOverlap,minId=0.95,minCover=0.15,globalNearBest=0.01")
}

# proteins have only "long" sequences = 6 amino acids
protBlatOptions = {
    "long":  ("t=dnax,q=prot", "minQSize=6,minNonRepSize=6,ignoreNs,bestOverlap,minId=0.95,minCover=0.15,globalNearBest=0.01")
}
# distance for chaining psls from same article into bed
maxChainDist={'default': 50000, 'sacCer2' : 5000}

# maximum number of matches for an article per DB, articles with 
# more matches will be ignored (before chaining)
maxDbMatchCount=1000

# ignore articles with more than a certain number of genomic featuress (after chaining)
maxFeatures = 50

# how many bp does a chain has to cover?
# smaller chains will be considered "noise" and skipped
minChainCoverage=23

# maximum length of chain
# this can sometimes happen if a chain contains many individual
# matches spread evenly over a very long locus
# we eliminate these very long chains, they are likely to be 
# not very informative
maxChainLength=3000000

# when splitting the big psl file, how many chunks should be fused into one?
# this is used to separate the big psl file onto smaller cluster jobs
# e.g. when this is 10 and we have 2000 total chunks, the chaining 
# will run on 200 pieces 
chunkDivider = 10

# which alignment table shall we use for cdna mapping?
# (this is used by pubPrepCdnaDir to download mRNA alignmenst 
# and mRNA fasta files
cdnaTable = 'refSeqAli'

# where to store the cDNA data (alignments and fasta files)
cdnaDir = "/hive/data/inside/pubs/cdnaDb/"

# file with impact factors for bed annotation
impactFname = join(staticDataDir, "map", "impact2011.tab")

# directory with the files t2g.sql, t2gArticle.sql and t2gSequence.sql
# required for loading into data UCSC genome browser
# (for "pubMap <dataset> load")
sqlDir = "/cluster/home/max/projects/pubs/tools/sql/"

# when writing tables for mysql, we cut most columns to a maximum size,
# to account for data errors (elsevier)
maxColLen = 255

# MARKER MAPPING ============================
markerDbDir = "/hive/data/inside/pubs/markerDb/"
humanDb = "hg19"

hgncDir = "/hive/data/outside/hgnc/042613"

# BLASTP
#blastBinDir = "/cluster/bin/blast/x86_64/blast-2.2.20/bin"
blastBinDir = '/hive/data/outside/blast229'

# default Mysql database 
mysqlDb = 'publications'

# UNIPROT PARSING ============================
# only convert records with one of these taxon Ids
uniProtTaxonIds = [9606, 10090, 10116, 7955]


# CLASSIFICATION ==============================
# directory with the SVMlight binaries
svmlBinDir = "/hive/data/inside/pubs/svmlight"

# directory to write html output files, one per DB
classOutHtmlDir = "/cluster/home/max/public_html/mining/classes"
testOutHtmlDir = "/cluster/home/max/public_html/mining/testClasses"

# descriptions for the various classes based on lists of PMIDs from DBs
classDescriptions = {
'gwas' : "Genome-wide association studies",
'redfly' : "Drosophila cis-regulatory assays",
'aptamerBase' : "Aptamers",
'chembl' : "Drug-like small molecules",
'chimerDb' : "Fusion genes",
'clinicalTrials' : "Clinical Trials",
'cosmic' : "Cancer genes and mutations",
'flybase' : "Drosophila Genetics",
'mgi' : "Mouse Genetics",
'omim' : "Human Genetics",
'pdb' : "Protein Structures",
'pharmGKB' : "Genetic variation and drug response",
'reactome' : "Protein interactions",
'wormbase' : "C. elegans genetics",
}

# the final output of the classification step, for pubMap integration
classFname = join(pubsDataDir, "classify", "crawler-elsevier-pmc", "docClasses.tab")

# GENE AND MUTATION RECOGNIZERS ===========================

geneDataDir = join(staticDataDir, "geneData")

# the british national corpus is a list of 30k common words in English
# used for symbol filtering
bncFname = '/hive/data/outside/pubs/wordFrequency/bnc/bnc.txt'

# ACCESS METHODS (convenience) ============================

import sys, logging, os.path, time, random
confName = os.path.expanduser("~/.pubConf")
if os.path.isfile(confName):
    dummy = {}
    newVars = {}
    execfile(confName, dummy, newVars)
    for key, value in newVars.iteritems():
        locals()[key] = value

def getConverters():
    return CONVERTERS

def getFastTempDir():
    " some fast local temp, possibly a ramdisk "
    if not os.path.isdir(FASTTEMPDIR):
        os.makedirs(FASTTEMPDIR)
    return FASTTEMPDIR

def getTempDir():
    " create temp dir, try to resolve race conditions "
    if not os.path.isdir(TEMPDIR):
        time.sleep(random.randint(1,5)) # make sure that we are not all trying to create it at the same time
        if not os.path.isdir(TEMPDIR):
            try:
                os.makedirs(TEMPDIR)
            except OSError:
                logging.info("Ignoring OSError, directory %s seems to exist already" % TEMPDIR)
    return TEMPDIR

def getUcscScriptDir():
    dirname(__file__)
    
def getMaxBinFileSize():
    return MAXBINFILESIZE

def getMaxTxtFileSize():
    return MAXTXTFILESIZE

def resolveTextDir(dataDir, makeDir=False):
    " check if dataDir exists, if not: try if subdir of textDir exists and return "
    inName = dataDir
    if os.path.isfile(dataDir):
        return dataDir
    if not os.path.isdir(dataDir):
        dataDir2 = os.path.join(textBaseDir, dataDir)
        if os.path.isdir(dataDir2):
            dataDir = dataDir2
        else:
            if makeDir:
                logging.info("Creating directory %s" % dataDir2)
                os.makedirs(dataDir2)
            else:
                raise Exception("Neither %s not %s are directories" % (dataDir, dataDir2))
                dataDir = None
    logging.debug("Resolved dataset name %s to dataset directory %s" % (inName, dataDir))
    return dataDir

def resolveTextDirs(dataString):
    " like resolveTextDir but accepts comma-sep strings and yields many "
    dirs = []
    for dataSpec in dataString.split(","):
        dirs.append( resolveTextDir(dataSpec) )
    return dirs
        
def getStaticDataDir():
    """ returns the data dir that is part of the code repo with all static data, e.g. train pmids
    """
    return staticDataDir

#def getDataDir(name):
    #" somewhat more flexible, can be redirected based on program name "
    #if name=="genes":
	#return join(staticDataDir, "mutFinder")
    #else:
        #assert(False)
