from os.path import expanduser, join, isdir, isfile, normpath, dirname, abspath
from os import makedirs
import logging
from maxCommon import getAppDir

# first parse the user config file
confName = expanduser("~/.pubConf")
newVars = {}
if isfile(confName):
    dummy = {}
    execfile(confName, dummy, newVars)
    for key, value in newVars.iteritems():
        locals()[key] = value

# GENERAL SETTINGS   ================================================
# baseDir for internal data, accessible from cluster 
# used for data created during pipeline runs
# Preference is given to a locally defined directory
if "pubsDataDir" not in locals():
    pubsDataDir = "."

# static data, accessible from cluster, but part of distribution
# these are basic files like gene lists, marker lists, journal lists
# Some of it can be updated with pubPrepXXX commands
staticDataDir = join(getAppDir(), "data")

# scripts only used at UCSC
ucscScriptDir = normpath(join(dirname(__file__), "..", "ucscScripts"))

# external tools
extToolDir = normpath(join(dirname(__file__), "..", "ext"))

# a directory with files that associate publishers with journals
# one of them is the NLM Catalog, others we got from publishers or created them semi-
# manually
#journalListDir = join(staticDataDir, "journalLists")
#journalListDir = join(staticDataDir, "journalLists")
journalListDir = "/hive/data/inside/pubs/journalLists/"

# base dir for anything journal related in the repo data dir
journalInfoDir = join(staticDataDir, "journals")

# the lists are reformatted into this table. It is created by pubJournals and used by pubPrepCrawl
# it contains the ISSNs and server names for each publisher
# it is required by pubCrawl for the highwire configuration
publisherIssnTable = join(journalInfoDir, "publishers.tab")

# same info, but one line per journal
# not used anymore
journalTable = join(journalInfoDir, "journals.tab")


# directory with various tracking files expected vs retrieved documents
inventoryDir = join(pubsDataDir, "inventory")

# a directory on the main server (hgwdev) on which the scripts is run, on local disk
# with lots of space available. Used to store intermediate results ? -> pubClassify
# localHeadDir = '/scratch/max/pubTools'

# DB PARSER SETTINGS ================================================
# directory for files with parsed DBs each DB, e.g. uniprot or pdb
dbRefDir = '/hive/data/inside/pubs/parsedDbs'

# directories with a local copy of PDB and uniprot, ncbi genes, refseq
uniProtBaseDir = '/hive/data/outside/uniProtCurrent'
pdbBaseDir = '/hive/data/outside/pdb'
ncbiGenesDir = '/hive/data/outside/ncbi/genes/'
#ncbiRefseqDir = '/hive/data/outside/ncbi/refseq/release/vertebrate_mammalian'
ncbiRefseqDir = '/hive/data/outside/ncbi/refseq/H_sapiens/mRNA_Prot'

# VARIANT MAPPING ========================================================
# directory for files that are needed to build the variants at UCSC but not needed 
# to run the programs. Examples are the PSL alignments built at UCSC, before they are
# indexed into sqlite
varBuildDir = '/hive/data/inside/pubs/variants'

# OMIM genemap2 on local disk or via internet
omimUrl = "file:///hive/data/outside/omim/01122013/genemap2.txt"

# CONVERTER SETTINGS ================================================
# for auto mode: base data dir with incoming input files from publishers or databases
extDir = '/hive/data/outside/pubs/'

# for auto mode: base dir of all publisher text files
textDir = join(pubsDataDir, "text")

# gzip compress the text data? Seeking is faster if not gzip compressed.
compress = True

# for pubConvMedline:

# an sqlite db with the content of medline, kept up-to-date
medlineDbPath = join(textDir, "medline", "medline.db")

# CRAWLER SETTINGS ==================================================

# the useragent to use for http requests
httpUserAgent = 'genomeBot/0.1 (YOUREMAIL, YOURWEB, YOURPHONE)'

# how long to wait for DNS queries, TCP connects, between retries and TCP reads, in seconds
httpTimeout = 5

# how to long wait for the downloading of files, in seconds
# after this the download will be canceled and the paper marked as unsuccessful
httpTransferTimeout = 60

# if you need to use a proxy to access journals, set it here
httpProxy = None

# SFX server for pubmed entries without an outlink
# YOU NEED TO DEFINE THIS IF YOU WANT TO USE SFX
# the UC SFX server is "http://ucelinks.cdlib.org:8888"
# the UR SFX server is "http://sfx.bib-bvb.de/sfx_ubr"
# but please use your own SFX server at your university
# google for it or ask your library for the URL
crawlSfxServer = None

# you can set special delay times here. E.g. to make all npg journals
# get crawled slowly at 30secs, set "npg" to 30
crawlDelays = {}

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
# better not .mov,.vid or .avi, nor html (=might get normal webpages)
# you can add additional types on a per-publisher basis with "addSuppFileTypes" in the pubCrawler host config
crawlSuppExts = set(['gif', 'svg', 'tiff', 'tif', 'jpg', 'xls', 'doc', 'pdf', 'ppt', 'asc', 'txt', 'csv', 'tsv', 'tab', 'eps'])

# DOWNLOAD SETTINGS ==================================================

# for Elsevier updates:
# We need the URL to the Consyn Update RSS feed. 
# Login to consyn.elsevier.com, click on batches/RSS feed, paste the URL here
# they look like https://consyn.elsevier.com/batch/atom?key=XXXXXXXXXXXXXXXXXXX
# (at UCSC: this is defined in ~/.pubTools.conf)
consynRssUrl = "YOURURL"

# for springer updates:
# we got a username / password from DDS Support in Heidelberg
# (at UCSC: these are defined in ~/.pubTools.conf)
springerUser = ""
springerPass = ""

# GENERAL PUBLICATION FILE CONFIG SETTINGS ============================

# which dataset should be loaded by the "load" step in pubMap ?
# loadDatasets = ["elsevier", "pmc", "crawler", "springer"]

# bundles are set of datasets and annotations run regularily over them
bundleToText = {
    "pubsTrack" : ["pmc", "springer", "elsevier", "crawler"],
}


TEMPDIR = "/tmp/pubTools" # local filesystem on cluster nodes
FASTTEMPDIR = TEMPDIR

maxBinFileSize = 20000000 # maximum filesize of any file before conversion to ASCII
maxTxtFileSize = 10000000 # maximum filesize of any file after conversion to ASCII
minTxtFileSize = 60 # minimum filesize of any file after conversion to ASCII
# 60 to allow files with just a figure legend to be processed
mapReduceTmpDir = pubsDataDir + "/mapReduceTemp" # cluster-wide directory to collect results

# parasol batches dir
clusterBatchDir = pubsDataDir + "/runs/"
# the base directory for text repository directories
textBaseDir = pubsDataDir + "/text/"
# the central base directory for all text annotations
annotDir = pubsDataDir + "/annot/"
# central directory for exported fasta file
faDir = pubsDataDir + "/fastaExport/"

# all logfiles
logDir = pubsDataDir + "/log/"

# head node of cluster
clusterHeadNode = "ku.sdsc.edu"
# type of cluster, either parasol or sge or localhost
clusterType = "localhost"

# _sourceDir = "/cluster/home/max/projects/pubs/tools"
_sourceDir = getAppDir()

# base directory for searcher algorithm code, like regex annotation, dna annotation, etc
scriptDir = join(_sourceDir,"taggers")

# cmdLine to start jython, used to run java annotators
jythonCmd= "/cluster/home/max/software/jre1.7.0/bin/java -jar "+dirname(__file__)+"/jython.jar"

# assignment of pubMap pipeline steps to cluster machines
# pubMap will ssh into a machine to run these steps
stepHosts = {"sortCdna" : "localhost", "sortProt" : "localhost", "sortGenome" : "localhost"}

# email for ncbi eutil requests and error email by pubCrawl
# at UCSC: overriden by local .pubConf
email = ""

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
    "springer" : 2500000000,
    "medline"  : 3000000000,
    "bing"     : 3500000000,
    "genbank"  : 4000000000,
    "imgt"     : 4300000000,
    "pdfDir"   : 4400000000,
    "yif"      : 4500000000,
    "crawler"  : 5000000000,
    "free"     : 6000000000  # to indicate end of list, always keep this here
    # the "free" entry is necessary to indicate the range of the 2nd to last entry
}

extToolDir = _sourceDir+"/external"

# commands to convert various filetypes to ascii text 
# $in and $out will be replaced with temp filenames
# pdf2 is only used if output of pdf contains more than 10 unprintable characters
# as pdfbox is quite slow
# you can define variables and use them, see extToolDir
CONVERTERS = {
    "doc":"catdoc $in > $out",
    "docx":"%(extToolDir)s/docx2txt-1.2/docx2txt.pl < $in > $out",
    "xls":"xls2csv $in > $out",
    "xlsx":"ssconvert $in $out",
    "ppt":"catppt $in > $out",
    "htm":"html2text $in  --unicode-snob --images-to-alt --ignore-links --ignore-emphasis > $out",
    "html":"html2text $in  --unicode-snob --images-to-alt --ignore-links --ignore-emphasis > $out",
    #"htm":"html2text -style pretty -nobs $in | tr -s ' ' > $out",
    # 'pretty' avoids **** markup for <h3> section names
    #"html":"html2text -style pretty -nobs $in | tr -s ' ' > $out",
    #"htm":"links -dump $in -dump-charset utf8 > $out",
    #"html":"links -dump $in -dump-charset utf8 > $out",
    "csv":"COPY",
    "txt":"COPY",
    "asc":"COPY",
    "xml":"XMLTEXT",
    "nxml":"NXMLTEXT",
    "pdf":"pdftotext -q -nopgbrk -enc UTF-8 -eol unix $in $out",
    "pdf2":"java -Xmx512m -jar %(extToolDir)s/pdfbox-app-1.6.0.jar ExtractText $in $out -encoding utf8"
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

pubMapBaseDir = "./map/"

# directory for exported cdr3 files
cdr3Dir = pubMapBaseDir + "cdr3Export/"

# this is the genbank mapping config file by Mark Diekhans' pipeline
# it is required for genome partitioning 
# current one can be downloaded from:
# http://genome-source.cse.ucsc.edu/gitweb/?p=kent.git;a=blob_plain;f=src/hg/makeDb/genbank/etc/genbank.conf;hb=HEAD
GBCONFFILE     = "/cluster/data/genbank/etc/genbank.conf"

# these variables assign genomes to keywords in text files
# the orgDetect.py plugin will create annotations on the text files
# for all of these keywords
# pubMap will then blat sequences from a textfile only on genomes
# for which a keyword has been found
# ALL SPECIES that the pipeline needs to blat on need to be defined here
# even if no species recognition for them is performed

speciesNames = {
'hg19' : ['human', 'sapiens', ' homo ', ' Homo ', 'patient', 'cell line','cell culture'],
'mm10' : ['mouse', 'musculus', 'rodent'],
'rn4' : [' rat ', 'novegicus', 'rodent'],
#'nonUcsc_archaea' : [],
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
'felCat5' : ["domestic cat", "felis cattus", "felidae", " cats "],
'nonUcsc_arabidopsisTair10' : ['arabidopsis', 'Arabidopsis', 'thaliana', 'thale cress'],
'nonUcsc_Pfalciparum3D7' : ['plasmodium', 'Plasmodium', 'falciparium', 'malaria'],
'nonUcsc_grapevine12x' : [' grapevine ', ' vitis ', 'pinot noir', ' vigne'],
'nonUcsc_bacteria' : ['bacteria ', 'bacterial ', 'microbiology', "coli ", "prokaryotes"],
'nonUcsc_viral' : [' virus ', 'viral '],
'nonUcsc_fungi' : ['Aspergillus'],
'nonUcsc_dnasu' : ['plasmid', 'cloned into', 'cloning vector', 'restriction site'],
'nonUcsc_biobricks' : ['synthetic biology', 'biobricks'],
'nonUcsc_wolb1' : ['wolbachia']
#'nonUcsc_dnasu' : [' plasmid ']
}

# path for genome files that start with 'nonUcsc_'
# this directory has to contain a geneBank.conf file
# in UCSC format to define parameters for these
# assemblies
nonUcscGenomesDir = pubsDataDir+"/nonUcscGenomes"

# During best-genome filtering, sometimes two genomes score equally
# if this is the case, pick the best one, in this order.
# (the first one has highest priority)

# if a genome is not part of the genbank config system then you need to prefix it with
# "nonUcsc_". Any db name like this will be searched in nonUcscGenomesDir (see below).
# e.g. nonUcsc_archaea will be resolved to /hive/data/inside/pubs/nonUcscGenomes/archaea.2bit
# The BLAT-wrapper also needs a .ooc file with the same name.
#alignGenomeOrder = ['hg19', 'mm10', 'rn4', 'nonUcsc_archaea', 'danRer7', 'dm3',
alignGenomeOrder = ['hg19', 'mm10', 'rn4', 'danRer7', 'nonUcsc_wolb1', 'dm3',
'xenTro2', 'oryLat2', 'susScr3', 'bosTau7', 'galGal4', 'felCat5', 'ci2', 'ce10', 'sacCer2',
'nonUcsc_arabidopsisTair10', 'nonUcsc_Pfalciparum3D7', 'nonUcsc_grapevine12x',
'nonUcsc_bacteria', 'nonUcsc_viral', 'nonUcsc_fungi', 'nonUcsc_dnasu', "nonUcsc_biobricks"
]

# these genomes are used if no species name matches are found
#defaultGenomes = ["hg19", "mm9", "rn4", "danRer7", "dm3", "ce10", "nonUcsc_archaea"]
defaultGenomes = ["hg19", "mm10", "rn4", "danRer7", "dm3", "ce10", "nonUcsc_bacteria"]

# these genomes are always added, no matter which species names are found
# human might not be recognized as a name for cell lines
# bacteria have so many names that we don't recognize them
# same for plasmids
#alwaysUseGenomes = ["hg19", "nonUcsc_archaea", "nonUcsc_dnasu"]
#alwaysUseGenomes = ["hg19", "nonUcsc_archaea"]
alwaysUseGenomes = ["hg19"]


# for some genomes we don't have refseq data
#noCdnaDbs = ["sacCer2", "nonUcsc_archaea", "nonUcsc_arabidopsisTair10", "nonUcsc_Pfalciparum3D7"]
noCdnaDbs = ["sacCer2", "nonUcsc_arabidopsisTair10", "nonUcsc_Pfalciparum3D7"]

# some text datasets are just variants of others
# for these, to avoid annotation id overlaps with the main dataset
# we add some offset to their annotation IDs
specDatasetAnnotIdOffset = {"yif" : 12000 }

# each species in alignGenomeOrder has to be in speciesNames
if not len(set(alignGenomeOrder).intersection(speciesNames))==len(speciesNames):
    logging.error("missing in speciesNames: %s" % repr(set(alignGenomeOrder)-set(speciesNames)))
    logging.error("missing in alignGenomeOrder: %s" % repr(set(speciesNames)-set(alignGenomeOrder)))
    raise Exception("illegal species name configuration")

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

# pubMap static data files directory, used below
_pubsDataDir = "/hive/data/inside/pubs/pubMapData"
# where to store the cDNA data (refseq alignments and fasta files)
cdnaDir = _pubsDataDir + "/cdnaDb"

# where to store the loci data (regions around longest transcripts for genes,
# their symbols and entrez IDs) a directory with <db>.bed files that associate
# genomic locations with the closest entrez gene and symbol loci creation also
# requires ncbiGenesDir
lociDir = _pubsDataDir+"/loci"

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

hgncFname = "/hive/data/outside/hgnc/111413/hgnc_complete_set.txt"

# directory with subdirs, one per db, that contains chrom.sizes files
genomeDataDir = "/hive/data/genomes"

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

# directory with lists of accessions
accDataDir = join(staticDataDir, "accessions")

# directory with lots of data about genes
geneDataDir = join(staticDataDir, "genes")

# directory with data required to detect variants
varDataDir = join(staticDataDir, "variants")

# the british national corpus is a list of 30k common words in English
# used for symbol filtering
bncFname = '/hive/data/outside/pubs/wordFrequency/bnc/bnc.txt'

# now overwrite all variables with those defined in local 
# config file ( see start of this file )
for key, value in newVars.iteritems():
    locals()[key] = value

# SOLR =======
solrUrl="http://hgwdev.soe.ucsc.edu:8983/solr"

# ACCESS METHODS (convenience) ============================

debug = False

import sys, logging, time, random

def getConverters():
    return CONVERTERS

def getFastTempDir():
    " some fast local temp, possibly a ramdisk "
    if not isdir(FASTTEMPDIR):
        makedirs(FASTTEMPDIR)
    return FASTTEMPDIR

def getTempDir():
    " create temp dir, try to resolve race conditions "
    if not isdir(TEMPDIR):
        time.sleep(random.randint(1,5)) # make sure that we are not all trying to create it at the same time
        if not isdir(TEMPDIR):
            try:
                makedirs(TEMPDIR)
            except OSError:
                logging.info("Ignoring OSError, directory %s seems to exist already" % TEMPDIR)
    return TEMPDIR

def getUcscScriptDir():
    dirname(__file__)
    
def getMaxBinFileSize():
    return maxBinFileSize

def getMaxTxtFileSize():
    return maxTxtFileSize

def mayResolveTextDir(dataDir):
    """ check if dataDir is a subdirectory of textBaseDir. If not, check if it's a valid path. "
    Return the absolute path or None if neither case is true.
    """
    inName = dataDir
    dataDir2 = join(textBaseDir, dataDir)
    if isdir(dataDir2) and not abspath(dataDir2)==abspath(textBaseDir):
        logging.debug("Resolved dataset name %s to global dataset directory %s" % (inName, dataDir))
        return abspath(dataDir2)

    if isdir(dataDir) and not abspath(dataDir)==abspath(textBaseDir):
        logging.debug("Resolved dataset name %s to directory %s" % (inName, dataDir))
        return abspath(dataDir)

    return None

def resolveTextDir(dataDir, mustFind=True):
    " check if dataDir exists, if not: try subdir with this name of textDir. abort if not found "
    fullPath = mayResolveTextDir(dataDir)
    if mustFind and fullPath == None:
        raise Exception("Could not resolve dataset %s to a directory" % dataDir)
    logging.debug("Resolved dataset name %s to dataset directory %s" % (dataDir, fullPath))
    return fullPath

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

def defaultInOutDirs(datasetName):
    " return the default input and the default output directory for a dataset "
    return join(extDir, datasetName), join(textBaseDir, datasetName)

#def getDataDir(name):
    #" somewhat more flexible, can be redirected based on program name "
    #if name=="genes":
	#return join(staticDataDir, "mutFinder")
    #else:
        #assert(False)

