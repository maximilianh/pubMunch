from os.path import *

# GENERAL SETTINGS   ================================================
# baseDir for internal data, accessible from cluster (if there is one)
pubsDataDir = '/hive/data/inside/pubs'

# DB PARSER SETTINGS ================================================
# directory for files with pubs references for each DB, e.g. uniprot or pdb
dbRefDir = '/hive/data/inside/pubs/dbRef'

# directory with a local copy of PDB or uniprot
# pubParseDb expects subdirectories called
# "pdb" and "uniprot" in here
dbBaseDir = '/hive/data/outside/'

# CONVERTER SETTINGS ================================================
# for pubConvMedline:
# an sqlite db with the content of medline, kept up-to-date
medlineDbPath = join(pubsDataDir, "text", "medline", "medline.db")

# CRAWLER SETTINGS ==================================================
# the useragent to use for http requests
httpUserAgent = 'genomeBot/0.1 (YOUREMAIL, YOURWEB, YOURPHONE)'

# how long to wait for DNS queries, TCP connects, between retries and TCP reads, in seconds
httpTimeout = 20

# how to long wait for the downloading of files, in seconds
httpTransferTimeout = 30

# the tool pubPrepCrawlDir parses journal data and groups journals by publishers.
# In most cases, to download all journal from a publisher, all you have to do
# is to copy the publisher field from <crawlDir>/_journalData/publisher.tab
# here and define a directory name for it
# format: publisher name -> directory name
crawlPubDirs = {
# all ISSNs that wiley gave us go into the subdir wiley
"WILEY Wiley" : "wiley", 
# we don't have ISSNs for NPG from them, so we use grouped data from NLM
"NLM NPG" : "npg",
# rockefeller university press
"HIGHWIRE The Rockefeller University" : "highwire",
"HIGHWIRE American Society for Microbiology" : "highwire",
"NLM Future Science" : "future",
"NLM National Academy of Sciences" : "pnas"
}

# crawler delay config, values in seconds
# these overwrite the default set with the command line switch to pubCrawl
# special case is highwire, handled in the code:
# (no joke) (all EST): mo-fri: 9-5pm: 120 sec, mo-fri 5pm-9am: 10 sec, sat-sun: 5 sec 
crawlDelays = {
    "www.nature.com"              : 10,
    "onlinelibrary.wiley.com" : 1,
    "dx.doi.org"              : 1,
    "ucelinks.cdlib.org"      : 20,
    "eutils.ncbi.nlm.nih.gov"      : 3
}

# SFX server for pubmed entries without an outlink
crawlSfxServer = "YOURSFXSERVER"

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

# which publishers should be loaded by the "load" step in pubMap ?
loadPublishers = ["elsevier", "pmc"]

TEMPDIR = "/scratch/tmp/pubTools" # local filesystem on cluster nodes
MAXBINFILESIZE = 20000000 # maximum filesize of any file before conversion to ASCII
MAXTXTFILESIZE = 10000000 # maximum filesize of any file after conversion to ASCII
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

# assignment of pubMap pipeline steps to cluster machines
# pubMap will ssh into a machine to run these steps
stepHosts = {'default' : 'swarm.cse.ucsc.edu', 'tables' : 'localhost', 'expFasta' : 'localhost' }

# email for ncbi eutil requests and error email by pubCrawl
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

# which converter should start at which position in our namespace?
identifierStart = {
    "pmc"      : 1000000000,
    "elsevier" : 2000000000,
    "medline"  : 3000000000,
    "genbank"  : 4000000000,
    "imgt"     : 4300000000,
    "pdfDir"   : 5000000000
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
    "pdf2":"java -jar /scratch/pdfbox/pdfbox-app-1.6.0.jar ExtractText $in $out"
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

# when splitting text files for cluster jobs,
# how many "chunks" should be created?
# chunkCount = 2000 # XXX  not used 

# for conversion jobs: how many cluster jobs should we run in parallel ?
# can be used to limit I/O and be nice to other cluster users
convertMaxJob = 200

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

pubBlatBaseDir = "/hive/data/inside/pubs/blat/"

# this is the genbank mapping config file by Mark Diekhans' pipeline
# it is required for genome partitioning 
GBCONFFILE     = "/cluster/data/genbank/etc/genbank.conf"

# these variables assignes genome to keywords in text files
# the orgDetect.py plugin will create annotations on the text files
# for all of these keywords
# pubBlat will then blat sequences from a textfile only on genomes
# for which a keyword has been found

speciesNames = {
'dm3' : ['fruitfly', ' flies ', 'melanogaster', 'Drosophilids'],
'mm9' : ['mouse', 'musculus', 'rodent'],
'hg19' : ['human', 'sapiens', ' homo ', ' Homo ', 'patient', 'cell line','cell culture'],
'danRer6' : ['zebrafish', 'rerio', 'Danio'],
'rn4' : [' rat ', 'novegicus', 'rodent'],
'xenTro2' : [' xenopus', 'Xenopus', 'tropicalis', 'laevis'],
'oryLat2' : ['medaka', 'Medaka', 'latipes', 'Oryzias'],
'ci2' : ['ascidian', 'intestinalis', 'chordates', 'Ciona'],
'ce6' : ['elegans', 'Caenorhabditis', 'nematode', ' worms'],
'sacCer2' : ['cerevisiae', 'Saccharomyces', 'yeast'],
}
#'bac' : ['bacteria', 'bacterial', 'prokaryot'] 

# these genomes are used if no species name matches are found
defaultGenomes = ["hg19", "mm9", "rn4", "dm3"]

# During best-genome filtering, sometimes two genomes score equally
# if this is the case, pick the best one, in this order:
# the first one has highest priority
alignGenomeOrder = ['hg19', 'mm9', 'rn4', 'danRer6', 'dm3', 'xenTro2', 'oryLat2', 'ci2', 'ce6', 'sacCer2']

# each species in alignGenomeOrder has to be in speciesNames
assert(len(set(alignGenomeOrder).intersection(speciesNames))==len(speciesNames))

# minimum size of sequence to be considered for blatting
minSeqLen=17

# maximum size of sequence to be considered "short", sequences can be "short" or "long"
shortSeqCutoff = 35

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
minChainCoverage=21

# maximum length of chain
# this can sometimes happen if a chain contains many individual
# matches spread evenly over a very long locus
# we eliminate these very long chains, they are likely to be 
# not very informative
maxChainLength=3000000

# when splitting the big psl file, how many pieces should be fused into one?
# this is only to separate the big psl file onto smaller cluster jobs
chunkDivider = 10

# which alignment table shall we use for cdna mapping?
# (this is used by pubPrepCdnaDir to download mRNA alignmenst 
# and mRNA fasta files
cdnaTable = 'refSeqAli'

# where to store the cDNA data (alignments and fasta files)
cdnaDir = "/hive/data/inside/pubs/cdnaDb/"

# directory with the files t2g.sql, t2gArticle.sql and t2gSequence.sql
# required for loading into data UCSC genome browser
# (for "pubBlat load")
sqlDir = "/cluster/home/max/projects/pubs/tools/sql/"

# when writing tables for mysql, we cut all columns to a maximum size,
# to account for data errors (elsevier)
maxColLen = 32000

# MARKER MAPPING ============================
markerDbDir = "/hive/data/inside/pubs/markerDb/"
humanDb = "hg19"

# BLASTP
#blastBinDir = "/cluster/bin/blast/x86_64/blast-2.2.20/bin"
blastBinDir = '/hive/data/outside/blast229'

# default Mysql database 
mysqlDb = 'publications'

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

def getTempDir():
    if not os.path.isdir(TEMPDIR):
        time.sleep(random.randint(1,5)) # make sure that we are not all trying to create it at the same time
        if not os.path.isdir(TEMPDIR):
            try:
                os.makedirs(TEMPDIR)
            except OSError:
                logging.info("Ignoring OSError, directory %s seems to exist already" % TEMPDIR)
    return TEMPDIR

def getMaxBinFileSize():
    return MAXBINFILESIZE

def getMaxTxtFileSize():
    return MAXTXTFILESIZE

def resolveTextDir(dataDir):
    " check if dataDir exists, if not: try if subdir of textDir exists and return "
    if os.path.isfile(dataDir):
        return dataDir
    if not os.path.isdir(dataDir):
        dataDir2 = os.path.join(textBaseDir, dataDir)
        if os.path.isdir(dataDir2):
            dataDir = dataDir2
        else:
            logging.error("Neither %s not %s are directories" % (dataDir, dataDir2))
            dataDir = None
    return dataDir
