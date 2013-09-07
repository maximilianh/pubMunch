# this file searches articles for identifiers of genetic markers in text:
# - band names
# - dbsnp identifiers (rs and ss numbers)
# - ensembl gene identifier
# - genbank accession 
# - ucsc genome code + coordinate identifiers
# - PDB accession 
# - uniprot accession 
# - official uppercased gene symbol (HUGO/HGNC)
# - OMIM ID
# - EC ID

# each marker type has its own regular expression

# marker names *can* be restricted by textfiles in 
# e.g. DICTDIR/band.dict.tab.gz
# format 
# <identifier><tab> <syn>|<syn2>|...
# Some identifiers have synonyms that can be resolved using dictionaries.
# Some identifier formats are so general that they need a dictionary to reduce the noise
# (e.g. uniprot)

# The main function returns the fields 'recogId' with the recognized synonym
# and the field 'markerId' with the final resolved identifier 

# can be restricted to search only for certain markers with the parameter
# 'searchType' (comma-sep), e.g. searchType="snp,genbank"

# standard python libraries for regex
import sys, logging, os.path, gzip, glob, doctest, marshal, gdbm, types, operator 
from collections import defaultdict
import fastFind, pubConf, maxbio, pubDnaFind, seqMapLocal, pubGeneric, pubKeyVal
from os.path import *

# try to use re2 if possible
try:
    import re2 as re
except:
    import re

# skip genbank lists like A1234-A1240 with more identifiers than this
MAXGBLISTCOUNT=20

# ignore articles with more than X accessions found
MAXROWS = 500

# initData will read dictionaries and bed files from this directory
DICTDIR= pubConf.markerDbDir
GENEDATADIR=pubConf.geneDataDir

# initData will set this to the names of markers that are searched
# can be genbank, omim, ec, etc
# or seqs for sequences
# or geneNames or symbol for the more complicated identifiers
searchTypes = set()

# global variables hold the nested dictionaries to recognize gene names and symbols
geneNameLex = None
geneSymLex = None
symLeftReqWords = None
symRightReqWords = None

# words that are usually not gene names, rather used for cell lines or pathways or other stuff
stopWords = set(['NHS', 'SDS', 'VIP', 'NSF', 'PDF', 'CD8', 'CD4','JAK','STAT','CD','ROM','CAD','CAM','RH', 'HR','CT','MRI','ZIP','WAF','CIP','APR','OK','II','KO','CD80','H9', 'SMS'])

# Some identifiers are so general that we want to restrict our search
# to documents that contain some keyword
# the reqWordDict hash sets up the lists of keywords in the document
# that are required for certain identifiers
genbankKeywords = ["genbank", "accession", " embl", "ddbj", "insdc", " ena ", "european nucleotide", " acc. ", "ncbi", "gene access"]

# some words are valid identifiers but are actually not used as such
notIdentifiers = set(["1rho", "U46619"])

# keywords are case insensitive
reqWordDict = {
    "genbank" :     genbankKeywords,
    "genbankList" : genbankKeywords,
    #"symbol" : ["gene", "protein", "locus"],
    "pdb" : ["pdb", "rcsb", "protein data bank"],
    "hg18" : ["hg18"],
    "hg19" : ["hg19"],
    "hg17" : ["hg17"],
    "flybase": ["flybase", "drosophila", "melanogaster"],
}

# some data types need filters to reduce the garbage output to a reasonable level
requiresFilter = ["pdb", "uniprot"]
# filters are lazily loaded into this global dict
filterDict = {}

# compiled regexes are kept in a global var
# as list of (name, regexObject) 
markerDictList = None

# band to Entrez mapping
bandToEntrezSyms = None

# mapping pmid to entrez is a global dbm file
pmidToEntrez = None

# separators before or after the regular expressions below
endSep = r'''(?=["'\s:,.()])'''
endSepDash = r'''(?=["'\s:,.()-])'''
startSep = r'''["'\s,.();:=[]'''
startSepDash = r'''["'\s,.();:=[-]'''

# Regular expressions need to define a group named "id"
# see python re engine doc: instead of (bla) -> (?P<id>bla)

# received genbank regex by email from Guy Cochrane, EBI

# == CODE COMMON FOR ANNOTATOR AND MAP TASK 
def compileREs(addOptional=False):
    " compile REs and return as dict type -> regex object "
    genbankRe = re.compile("""[ ;,.()](?P<id>(([A-Z]{1}\d{5})|([A-Z]{2}\d{6})|([A-Z]{4}\d{8,9})|([A-Z]{5}\d{7}))(\.[0-9]{1,})?)%s""" % endSep)
    genbankListRe = re.compile(r'[ ;,.()](?P<id1>(([A-Z]{1}\d{5})|([A-Z]{2}\d{6})|([A-Z]{4}\d{8,9})|([A-Z]{5}\d{7}))(\.[0-9]{1,})?)-(?P<id2>(([A-Z]{1}\d{5})|([A-Z]{2}\d{6})|([A-Z]{4}\d{8,9})|([A-Z]{5}\d{7}))(\.[0-9]{1,})?)%s' % (endSep))
    #snpRsRe = re.compile(r'[ ;,.()]rs[ #-]?(?P<id>[0-9]{4,10})%s' % (endSep))
    snpRsRe = re.compile(r'%s(SNP|dbSNP|rs|Rs|RefSNP|refSNP)( |-| no.| no| No.| ID|ID:| #|#| number)?[ :]?(?P<id>[0-9]{4,19})' % (startSep))
    snpSsRe = re.compile("""[ ;,.()](?P<id>ss[0-9]{4,16})%s""" % (endSep))
    coordRe = re.compile("%s(?P<id>(chr|chrom|chromosome)[ ]*[0-9XY]{1,2}:[0-9,]{4,12}[ ]*-[ ]*[0-9,]{4,12})%s" % (startSep, endSep))
    bandRe = re.compile("""[ ,.()](?P<id>(X|Y|[1-9][0-9]?)(p|q)[0-9]+(\.[0-9]+)?)%s""" % (endSep))
    symbolRe = re.compile("""[ ;,.()-](?P<id>[A-Z][A-Z0-9-]{2,8})%s""" % (endSepDash))

    # http://www.uniprot.org/manual/accession_numbers
    # letter + number + 3 alphas + number,eg A0AAA0
    uniprotRe = re.compile(r'[\s;,.()-](?P<id>[A-NR-ZOPQ][0-9][A-Z0-9][A-Z0-9][A-Z0-9][0-9])%s' % (endSepDash))

    # http://pdbwiki.org/wiki/PDB_code
    pdbRe = re.compile(r'%s(?P<id>[0-9][a-zA-Z][a-zA-Z][a-zA-Z])%s' % (startSepDash, endSepDash)) # number with three letters

    # http://www.ncbi.nlm.nih.gov/RefSeq/key.html#accession
    refseqRe = re.compile(r'%s(?P<id>[XYNAZ][MR]_[0-9]{4,11})%s' % (startSepDash, endSepDash))
    refseqProtRe = re.compile(r'%s(?P<id>[XYNAZ]P_[0-9]{4,11})%s' % (startSepDash, endSepDash))
    ensemblRe = re.compile(r'%s(?P<id>ENS([A-Z]{3})?[GPT][0-9]{9,14})%s' % (startSepDash, endSepDash))

    # OMIM
    omimRe = re.compile(r'O?MIM( )?(#|No|no|number|ID)? ?:? ?[*#$+^]?(?P<id>[0-9]{3,8})')
    entrezRe = re.compile(r'(Locus|LocusLink|Locuslink|LOCUSLINK|LOCUS|Entrez Gene|Entrez|Entrez-Gene|GeneID|LocusID)( )?(#|No|no|number|ID|accession)?( )?(:)?( )?(?P<id>[0-9]{3,8})')
    ecRe = re.compile(r'EC ? ?(?P<id>[0-9][0-9]?\.[0-9][0-9]?\.[0-9][0-9]?\.[0-9][0-9]?)')
    stsRe = re.compile(r'(UniSTS|UNISTS|uniSTS) ?([aA]ccession|[Aa]ccession number|#|ID|[nN]o|[Nn]umber|[Nn]o.)?(:)? ?(?P<id>[0-9]{3,10})')

    reDict = {"genbank": genbankRe,
              "genbankList": genbankListRe,
              "snp": snpRsRe,
              "snpSs": snpSsRe,
              "band": bandRe,
              #"symbol": symbolRe,
              "uniprot": uniprotRe,
              "pdb": pdbRe,
              "refseq" : refseqRe,
              "refseqProt" : refseqProtRe,
              "ensembl" : ensemblRe,
              "hg17" : coordRe,
              "hg18" : coordRe,
              "hg19" : coordRe,
              "omim" : omimRe,
              "ec" : ecRe,
              "entrez" : entrezRe,
              "sts" : stsRe
              }

    if addOptional:
        arrayExprRe = re.compile(r'%s(?P<id>E-[A-Z]{4}-[0-9]+)' % (startSep))
        geoRe = re.compile(r'%s(?P<id>GSE[0-9]{2,8})' % (startSepDash))
        interproRe = re.compile(r'%s(?P<id>IPR[0-9]{5})' % (startSepDash))
        pfamRe = re.compile(r'%s(?P<id>PF[0-9]{5})' % (startSepDash))
        printsRe = re.compile(r'%s(?P<id>PR[0-9]{5})' % (startSepDash))
        pirsfRe = re.compile(r'%s(?P<id>PIRSF[0-9]{6})' % (startSepDash))
        prositeRe = re.compile(r'%s(?P<id>PS[0-9]{5})' % (startSepDash))
        smartRe = re.compile(r'%s(?P<id>SM[0-9]{5})' % (startSepDash))
        supFamRe = re.compile(r'%s(?P<id>SSF[0-9]{5})' % (startSepDash))
        ccdsRe = re.compile(r'%s(?P<id>CCDS[0-9]{1,8})' % (startSepDash))
        affyRe = re.compile(r'%s(?P<id>[0-9]{5,8}(_[sa])?_at)' % (startSepDash))
        keggRe = re.compile(r'%s(?P<id>hsa:[0-9]{5,8})' % (startSepDash))
        hprdRe = re.compile(r'%s(HPRD|hprd)[: ](id [: ])?(?P<id>[0-9]{5,8})' % (startSepDash))
        pharmGkbRe = re.compile(r'%s(?P<id>PA[0-9]{3,6})' % (startSepDash))
        chemblRe = re.compile(r'%s(?P<id>CHEMBL[0-9]{3,6})' % (startSepDash))
        hInvRe = re.compile(r'%s(?P<id>HIX[0-9]{3,7})' % (startSepDash))
        hgncRe = re.compile(r'%s(?P<id>HGNC:[0-9]{2,7})' % (startSepDash))
        ucscRe = re.compile(r'%s(?P<id>uc[0-9]{3}[a-z]{3})' % (startSepDash))
        goRe = re.compile(r'%s(?P<id>GO:[0-9]{6})' % (startSepDash))
        uniGeneRe = re.compile(r'%s(?P<id>Hs\.[0-9]{3,6})' % (startSepDash))
        vegaRe = re.compile(r'%s(?P<id>OTTHUM[TPG][0-9]{11})' % (startSepDash))
        cosmicRe = re.compile(r'%s(?P<id>COSM[0-9]{6})' % (startSepDash))
        mgiRe = re.compile(r'%s(MGI|MGI accession no.|MGI id|MGI accession|MGI acc.)[: ](?P<id>[0-9]{3,8})' % (startSepDash))
        # http://flybase.org/static_pages/docs/nomenclature/nomenclature3.html#2.
        flybaseRe = re.compile("""[ ;,.()-](?P<id>(CG|CR)[0-9]{4,5})""" )
        # http://flybase.org/static_pages/docs/refman/refman-F.html
        flybase2Re = re.compile("""[ ;,.()-](?P<id>FB(ab|al|ba|cl|gn|im|mc|ms|pp|rf|st|ti|tp|tr)[0-9]{7})%s""" )
        wormbaseRe = re.compile(r'%s(?P<id>(WBGene[0-9]{8}|WP:CE[0-9]{5}))' % (startSepDash))
        sgdRe = re.compile(r'%s(?P<id>Y[A-Z]{2}[0-9]{3}[CW](-[AB])?|S[0-9]{9})' % (startSepDash))
        zfinRe = re.compile(r'%s(?P<id>ZDB-GENE-[0-9]{6,8}-[0-9]{2,4})' % (startSepDash))

        # a more or less random selection, not sure if this is really necessary
        global reqWordDict
        reqWordDict.update({
            "arrayExpress": ["arrayexpress"],
            "geo": ["geo"],
            "interpro": ["interpro"],
            "pfam": ["pfam"],
            "pirsf": ["pirsf"],
            "smart": ["smart"],
            "prints": ["prints"],
            "pharmgkb": ["pharmgkb"],
            "flybase": ["flybase"],
            "wormbase": ["wormbase"],
            "sgd": ["sgd"],
        })

        reDict.update({
            "arrayExpress" : arrayExprRe,
            "geo"          : geoRe,
            "interpro"     : interproRe,
            "pfam"         : pfamRe,
            "prints"       : printsRe,
            "pirsf"        : pirsfRe,
            "smart"        : smartRe,
            "supFam"       : supFamRe,
            "affymetrix"   : affyRe,
            "kegg"         : keggRe,
            "hprd"         : hprdRe,
            "pharmGkb"     : pharmGkbRe,
            "chembl"       : chemblRe,
            "hInv"         : hInvRe,
            "hgnc"         : hgncRe,
            "ucsc"         : ucscRe,
            "go"           : goRe,
            "uniGene"      : uniGeneRe,
            "vega"         : vegaRe,
            "cosmic"       : cosmicRe,
            "mgi"          : mgiRe,
            "flybase"      : flybaseRe,
            "flybase2"     : flybase2Re,
            "wormbase"     : wormbaseRe,
            "sgd"          : sgdRe,
            "zfin"         : zfinRe
            })

    return reDict

def readBestWords(fname, count):
    " return the first field of the first count lines in fname as a set "
    logging.info("Reading %d words from %s" % (count, fname))
    vals = []
    i = 0
    for line in open(fname):
        f = line.strip("\n").split("\t")[0]
        vals.append(f)
        i+=1
        if i==count:
            break
    return set(vals)

def initData(markerTypes=None, exclMarkerTypes=None, addOptional=False):
    """ compile regexes and read filter files.
    
    MarkerTypes is the list of markers to prepare, some can be excluded with exclMarkerTypes
    """
    # setup list of marker types as specified
    reDict = compileREs(addOptional)
    if markerTypes==None:
        markerTypes = set(reDict.keys())
        markerTypes.add("geneName")
        markerTypes.add("symbol")
        markerTypes.add("symbolMaybe")
        markerTypes.add("dnaSeq")

    if exclMarkerTypes!=None:
        for m in exclMarkerTypes:
            markerTypes.remove(m)

    global searchTypes
    searchTypes = markerTypes

    global filterDict
    kwDictList = []
    for markerType in markerTypes:
        if markerType=="dnaSeq":
            continue
        # special case for long gene names
        if markerType=="geneName":
            global geneNameLex
            fname = join(GENEDATADIR, "geneNames.marshal.gz")
            logging.info("Loading %s" % fname)
            geneNameLex = fastFind.loadLex(fname)
            continue

        # special case for bands
        if markerType=="band":
            global bandToEntrezSyms
            fname = join(GENEDATADIR, "bandToEntrez.marshal.gz")
            logging.info("Loading %s" % fname)
            bandToEntrezSyms = marshal.loads(gzip.open(fname).read())

        # special case for gene symbols
        if markerType=="symbol" or markerType=="symbolMaybe":
            global geneSymLex
            fname = join(GENEDATADIR, "symbols.marshal.gz")
            logging.info("Loading %s" % fname)
            geneSymLex = fastFind.loadLex(fname)

            global symLeftReqWords, symRightReqWords
            symLeftReqWords = readBestWords(join(GENEDATADIR, "left.tab"), 500)
            symRightReqWords = readBestWords(join(GENEDATADIR, "right.tab"), 500)
            continue

        markerRe = reDict[markerType]
        kwDictList.append((markerType, markerRe))
        if markerType in requiresFilter:
            #filterFname = os.path.join(DICTDIR, markerType+"b.gz")
            filterFname = os.path.join(DICTDIR, markerType+"Accs.txt.gz")
            #filterFname = pubGeneric.getFromCache(filterFname)
            logging.info("Opening %s" % filterFname)
            #filterSet = set(gzip.open(filterFname).read().splitlines())
            filterSet = pubKeyVal.openDb(filterFname)
            filterDict[markerType] = filterSet

    global markerDictList
    markerDictList = kwDictList
    logging.debug("Loaded marker dict for these types: %s" % [x for x,y in markerDictList])

def pmidDbLookup(pmid):
    """ get genes annotated in databases
    >>> pmidDbLookup("21755431")
    [2717]
    """
    # entrez DB lookup
    global pmidToEntrez
    if pmidToEntrez==None:
        fname = join(GENEDATADIR, "pmid2entrez.gdbm")
        logging.info("Opening NCBI genes PMID -> article mapping from %s" % fname)
        pmidToEntrez = gdbm.open(fname, "r")
    pmid = str(pmid)
    data = {}
    genes = []
    if pmid in pmidToEntrez:
        genes = pmidToEntrez[str(pmid)].split(",")
    else:
        logging.debug("no data in NCBI genes")
    logging.debug("Found NCBI genes: %s" % str(genes))
    return [int(x) for x in genes]
    # special case entrez gene annotations
    #if markerType=="entrezDb" and (pmid not in [None, ""]):
        #geneIds = markerToGenes("pmid", pmid)
        #for geneId in geneIds:
            #row = [ 0, 0, "entrezDb", geneId, geneId]
            #rows.append(row)
        #continue

def splitGenbankAcc(acc):
    """ split a string like AY1234 into letter-number tuple, e.g. (AY, 1234)
    >>> splitGenbankAcc("AY1234")
    ('AY', 1234, 4)
    """
    matches = list(re.finditer(r"([A-Z]+)([0-9]+)", acc))
    # re2 has trouble with the .match function
    if len(matches)>1 or len(matches)==0:
        return None
    match = matches[0]
    letters, numbers = match.groups()
    return (letters, int(numbers), len(numbers))

def iterGenbankRows(markerRe, markerType, text):
    """ generate match rows for a list like <id1>-<id2> 
    >>> genbankListRe = compileREs()["genbankList"]
    >>> list(iterGenbankRows(genbankListRe, "genbankList", "    JN011487-JN011488 "))
    [[3, 12, 'genbank', 'JN011487'], [3, 12, 'genbank', 'JN011488']]
    >>> list(iterGenbankRows(genbankListRe, "gbl", " JN011487-AP011488 "))
    []
    """
    markerType = markerType.replace("List", "")
    for match in markerRe.finditer(text):
        word = match.group()
        id1  = match.group("id1")
        id2  = match.group("id2")

        let1, num1, digits1 = splitGenbankAcc(id1)
        let2, num2, digits2 = splitGenbankAcc(id2)
        if let1!=let2 or digits1!=digits2:
            continue
        if (num2-num1) > MAXGBLISTCOUNT:
            continue
        for num in range(num1, num2+1):
            numFmt = "%%0%sd" % digits1
            acc = let1+(numFmt % num)
            start = match.start(0)
            end = match.end(1)
            #yield [ start, end, markerType, word, acc ]
            yield [ start, end, markerType, acc ]

def textContainsAny(text, keywords):
    " brute force string search, for a few keywords this should be not too slow "
    for keyword in keywords:
        if keyword in text:
            return True
    return False

def rangeInSet(start, end, posSet):
    " return true if any position from start-end is in posSet "
    if len(posSet)==0:
        return False
    for i in range(start, end):
        if i in posSet:
            return True
    return False

class ResolvedGene(object):
    def __init__(self, locs, support):
        self.locs = locs
        self.support = support

def resolveSeqs(seqDict, seqCache=None):
    """ 
    input: dict sequence -> list of (start, end)
    returns: dict entrezId -> (seq, list of (start, end))
    >>> resolveSeqs({"GCAAGCTCCCGGGAATTCAGCTC": [(100,200)]})
    {5308: ('GCAAGCTCCCGGGAATTCAGCTC', [(100, 200)])}
    """
    global blatClient
    if blatClient==None:
        blatClient = seqMapLocal.BlatClient(pubConf.genomeDataDir, ["hg19"])
    if len(seqDict)==0:
        return {}

    if seqCache!=None:
        key = marshal.dumps(seqDict)
        logging.debug("Lookup in seq cache")
        if key in seqCache:
            logging.debug("seq mapping result found in seqCache")
            return marshal.loads(seqCache[key])
        else:
            logging.debug("no result in seqCache")
        
    dnaMapper = seqMapLocal.DnaMapper(blatClient)
    dbList = ["hg19"]
    dbSeqToSyms = dnaMapper.mapDnaToGenes(seqDict.keys(), "unknownDoc", dbList)

    # reformat to dict entrez -> list of sequences
    entrezToSeqs = defaultdict(list)
    for db, seqSyms in dbSeqToSyms.iteritems():
        for seq, syms in seqSyms.iteritems():
            for sym in syms:
                if sym not in symToEntrez:
                    logging.debug("Cannot resolve sym %s to entrez" % sym)
                else:
                    eId = symToEntrez[sym]
                    entrezToSeqs[eId].append(seq)

    ret = {}
    for eId, seqs in entrezToSeqs.iteritems():
        ret[eId]= ("/".join(seqs), seqDict[seq])

    if seqCache!=None:
        logging.debug("writing result to seqCache")
        seqCache[key] = marshal.dumps(ret)
    return ret

def resolveNonSymbols(markers):
    """ resolve all non-symbol markers to genes 
        return as geneDict gene -> (markerId, locs) 

    >>> resolveNonSymbols({"entrez":{'2717/5308': [(0, 10)]}})
    {'entrez': {5308: ('2717/5308', [(0, 10)]), 2717: ('2717/5308', [(0, 10)])}}
    """
    #passThrough = []
    geneDict = {}

    for mType, idLocs in markers.iteritems():
        logging.debug("Found matches for %s: %s" % (mType, idLocs))
        if mType in ["symbolMaybe", "symbol", "dnaSeq"]:
            continue

        typeGenes = {}
        # idLocs example: {'11p15.5': [(2310, 2317), (4015, 4022), (10665, 10672)]}
        for markerId, locs in idLocs.iteritems():
            # resolve marker to dict of genes -> symbol
            geneSymDict = markerToGenes(mType, markerId)
            # some markers are not genes, just pass them through
            if geneSymDict==None:
                #logging.debug("Not a gene, passing through marker: %s, %s" % (markerId, locs))
                logging.debug("Not a gene, skipping marker %s, %s, %s" % (mType, markerId, locs))
                #passThrough.append(mType)
                continue
            else:
            # one marker can represent several gene IDs (rare)
                for gene, sym in geneSymDict.iteritems():
                    typeGenes[gene] = (markerId, locs)
        geneDict[mType] = typeGenes

    # also add the passThrough annotations:
    #for mType in passThrough:
        #geneDict[mType] = markers[mType]
    return geneDict

def resolveAmbiguousSymbols(nonSymDict, text, markers):
    """ 
    Resolve ambigous symbols that have more than one meaning by comparing with 
    all non-symbol information.
    e.g. ASM can mean {283120: 'H19', 6609: 'SMPD1'}. If there is any other
    information about ASM, like a gene name or Refseq ID other ID in text, we
    can decide these cases.

    input is a dict markerType -> markerId -> list of (start, end)
    returns a dict markerType -> gene -> (markerId, list of (start, end))

    >>> text = "  mutated ASM (OMIM:607608) "
    >>> d = findMarkersAsDict(text)
    >>> d.items()
    [('omim', {'607608': [(20, 26)]}), ('symbol', {'283120/6609': [(10, 13)]})]
    >>> ns = resolveNonSymbols(d)
    >>> ns
    {'omim': {6609: ('607608', [(20, 26)])}}
    >>> resolveAmbiguousSymbols(ns, text, d).items()
    [('symbol', {6609: ('283120/6609', [(10, 13)])})]
    >>> text = " ASM "
    >>> d2 = findMarkersAsDict(text)
    >>> resolveAmbiguousSymbols({}, text, d2)
    {'symbolMaybe': {283120: ('283120/6609', [(1, 4)]), 6609: ('283120/6609', [(1, 4)])}}
    """
    # create dict gene -> score, where score is number of non-symbol marker types that support it
    geneScoreDict = defaultdict(int)
    for mType, geneLocs in nonSymDict.iteritems():
        for geneId in geneLocs:
            geneScoreDict[geneId] += 1

    geneDict = {}
    # now resolve ambigous symbols by using the score and add to geneDict
    for mType in ["symbolMaybe", "symbol"]:
        if mType not in markers:
            continue
        idLocs = markers[mType]
        typeGenes = {}
        for markerId, locs in idLocs.iteritems():
            geneSymDict = markerToGenes(mType, markerId)
            # no need to do anything if it's a clear, unambiguous symbol
            if len(geneSymDict)==1:
                gene = geneSymDict.keys()[0]
                typeGenes[gene] = (markerId, locs)
                continue
            scores = [(g, geneScoreDict.get(g, 0)) for g in geneSymDict.keys()]
            bestGenes = maxbio.bestIdentifiers(scores)
            # some debugging output
            allSyms = "/".join([str(x) for x in geneSymDict.values()])
            bestSym = geneSymDict[bestGenes[0]]
            exText = text[locs[0][0]:locs[0][1]]
            scoreText = str(scores)
            bestGenesText = "/".join([str(x) for x in bestGenes])
            logging.debug("ambiguous: %(mType)s (syms: %(allSyms)s, e.g. '%(exText)s')." % locals())
            if len(bestGenes)==1:
                logging.debug("resolved to %(bestGenesText)s/%(bestSym)s, scores: %(scoreText)s" % \
                    locals())
                mType = "symbol"
            else:
                logging.debug("not resolved: %(bestGenesText)s/%(bestSym)s, scores: %(scoreText)s" % \
                    locals())
                mType = "symbolMaybe"
            # keep only the best ones 
            for bestGene in bestGenes:
                typeGenes[bestGene] = (markerId, locs)
        geneDict[mType] = typeGenes

    return geneDict
            
def flipUnsureSymbols(text, annotatedGenes):
    """
    look for unsure symbols that might be not gene symbols but are supported
    by some other evidence to be real symbols
     
    input is markerType -> geneId -> (markerId, list of start, end)
    output is the same, with some "symbolMaybe" markerTypes converted to "symbol" markerTypes
    >>> text = "  mutated ASM (OMIM:607608) "
    >>> d = {'omim': {6609: ('607608', [(20, 26)])}, 'symbolMaybe': {6609: ('283120/6609', [(10, 13)])}}
    >>> flipUnsureSymbols(text, d)
    {'omim': {6609: ('607608', [(20, 26)])}, 'symbol': {6609: ('283120/6609', [(10, 13)])}}
    """
    if "symbolMaybe" not in annotatedGenes:
        return annotatedGenes

    sureGenes = defaultdict(list)
    # make a dict of all genes but the unsure symbols
    for markerType, geneMarkers in annotatedGenes.iteritems():
        if markerType=="symbolMaybe":
            continue
        for geneId in geneMarkers:
            sureGenes[geneId].append(markerType)

    # find the unsure symbols with support
    sureSyms = {}
    for geneId, markerLocTuple in annotatedGenes["symbolMaybe"].iteritems():
        #for geneId, markerLocTuple in geneMarkers.iteritems():
        if geneId in sureGenes:
            suppMarker = sureGenes[geneId]
            logging.debug("Treating unsure symbol %s as a real symbol, because of %s support" % \
                (markerLocTuple, suppMarker))
            sureSyms[geneId] = markerLocTuple

    # flip unsure symbols to sure ones
    annotatedGenes.setdefault("symbol", {})
    for geneId, markerLocTuple in sureSyms.iteritems():
        del annotatedGenes["symbolMaybe"][geneId]
        markerId, locs = markerLocTuple
        if geneId not in annotatedGenes["symbol"]:
            annotatedGenes["symbol"][geneId] = (markerId, locs)
        else:
            annotatedGenes["symbol"][geneId][1].extend(locs)

    # remove the whole markerType if no unsure symbols left
    if len(annotatedGenes["symbolMaybe"])==0:
        del annotatedGenes["symbolMaybe"]
    return annotatedGenes

def findGenes(text, pmid=None, seqCache=None):
    """
    return the genes as a dict of entrez ID -> mType -> (markerId, list of (start, end))
    >>> dict(findGenes(" OMIM:609883 NM_000325  ASM "))
    {5308: {'refseq': [('NM_000325', [(13, 22)])]}, 54903: {'omim': [('609883', [(6, 12)])]}}

    Also return a list of positions in text that are part of genes.
    """
    wordCount = len(text.split())

    genesSupport = {}
    genePosList = []
    genes  = findGenesResolveByType(text, pmid=pmid, seqCache=seqCache)
    for mType, geneIdDict in genes.iteritems():
        for geneId, markerLocs in geneIdDict.iteritems():
            idStr, locs = markerLocs
            # only count a gene as found if it's an unambiguous symbol match with count > 3
            # or a not a symbol at all or an ambiguous symbol that occurs many times
            if mType not in ["symbolMaybe", "symbol"] or \
                    mType=="symbol" and (len(locs)>(wordCount/1200)) or \
                    mType=="symbolMaybe" and (len(locs)>10):
                #entrezIds.add(geneId)
                genesSupport.setdefault(geneId, {}).setdefault(mType, []).append((idStr, locs))
            for start, end in locs:
                genePosList.extend(range(start, end))

    return genesSupport, set(genePosList)
    
def findGenesResolveByType(text, pmid=None, seqCache=None):
    """ 
    find markers in text, resolve them to genes and return as dict geneId -> list of (start, end)

    Resolve ambiguous gene symbols and flip unsure symbols to sure symbols if some other
    identifier in the document supports them.

    Return a dict gene -> markerType -> list of start, end)
    """
    markers        = findMarkersAsDict(text, pmid=pmid)
    geneDict       = resolveNonSymbols(markers)
    symDict        = resolveAmbiguousSymbols(geneDict, text, markers)
    geneDict.update(symDict)

    if "dnaSeq" in markers:
        seqDict            = resolveSeqs(markers["dnaSeq"], seqCache)
        geneDict["dnaSeq"] = seqDict

    genes          = flipUnsureSymbols(text, geneDict)

    # now we don't need the bands anymore
    if "band" in genes:
        del genes["band"]
    return genes

def findMarkersAsDict(text, pmid=None):
    """ search text for identifiers and genes, return as 
    dict markerType -> (id, refId, entrezId) -> list of (start, end).
    Use markerToGenes to resolve a marker to entrez geneIds.

    >>> dict(findMarkersAsDict(" OMIM:609883 NM_000325   actgtagatcgtacacc CGAT ATGc hi hi  ASM "))
    {'refseq': {'NM_000325': [(13, 22)]}, 'omim': {'609883': [(6, 12)]}, 'symbolMaybe': {'283120/6609': [(60, 63)]}, 'dnaSeq': {'actgtagatcgtacaccCGATATGc': [(25, 52)]}}
    """
    # find DB identifiers
    res = defaultdict(dict)
    for annot in findIdentifiers(text):
        start, end, markerType, geneId = annot
        res[markerType].setdefault(str(geneId), []).append( (start, end) )

    # find DNA sequences
    dnaPos = set()
    if "dnaSeq" in searchTypes:
        for annot in findSequences(text):
            start, end, seq = annot
            res["dnaSeq"].setdefault(str(seq), []).append( (start, end) )
            dnaPos.update(range(start, end))

    # find gene names and symbols, removing those that overlap a DNA sequence
    if "symbol" in searchTypes or "geneName" in searchTypes:
        for annot in findGeneNames(text):
            start, end, markerType, geneId = annot
            if not rangeInSet(start, end, dnaPos):
                res[markerType].setdefault(str(geneId), []).append( (start, end) )

    # add the entrez Db lookup results
    if "entrezDb" in searchTypes:
        if pmid!=None:
            entrezData = {}
            for gene in pmidDbLookup(pmid):
                entrezData[str(gene)] = {}
            if len(entrezData)!=0:
                res["entrezDb"] = entrezData

    return res

accToUps = None
upToEntrez = None
upToSym = None
entrezToUp = None
symToEntrez = None

# these are the annotations that are already entrez IDs and don't need to be 
# resolved
alreadyEntrez = set(["entrez", "geneName", "symbol", "symbolMaybe", "entrezDb", "dnaSeq"])

blatClient = None

def entrezSymbol(entrezId):
    " resolve entrez Id to gene symbol "
    entrezId = int(entrezId)
    return entrezToSym.get(entrezId, "invalidEntrezId")

def markerToGenes(markerType, markerId):
    """ 
    resolve any accession to a dict of entrez genes, return a dict entrezGeneId -> symbol

    supported accessions: 
    hgnc symbols, omim, ec, uniprot, refseq, genbank, pdb, ensembl, entrez, band

    >>> markerToGenes("band", "8q12.2")
    {55636: 'CHD7', 157807: 'CLVS1'}
    >>> markerToGenes("geneName", "2717")
    {2717: 'GLA'}
    >>> markerToGenes("omim", "300644")
    {2717: 'GLA'}
    >>> markerToGenes("entrez", "2717")
    {2717: 'GLA'}
    >>> markerToGenes("entrez", "57760") # this is an old gene, not located on any chromosome but still in entrez
    {}
    >>> markerToGenes("entrez", "2717/5308")
    {5308: 'PITX2', 2717: 'GLA'}
    >>> markerToGenes("ec", "3.2.1.22")
    {2717: 'GLA'}
    >>> markerToGenes("ec", "2.3.2.13")
    {2162: 'GLA'}
    >>> markerToGenes("uniprot", "P06280")
    {2717: 'GLA'}
    >>> markerToGenes("refprot", "NP_000160.1")
    {2717: 'GLA'}
    >>> markerToGenes("genbank", "X05790")
    {2717: 'GLA'}
    >>> markerToGenes("genbank", "X05790")
    {2717: 'GLA'}
    >>> markerToGenes("pdb", "3HG3")
    {2717: 'GLA'}
    >>> markerToGenes("ensembl", "ENSG00000102393")
    {2717: 'GLA'}
    >>> markerToGenes("refseq", "NM_000169.10")
    {2717: 'GLA'}
    >>> markerToGenes("entrezDb", "2717")
    {2717: 'GLA'}
    """
    global accToUps, upToEntrez, upToSym, entrezToUp, pmidToEntrez, entrezToSym, symToEntrez
    # entrez is already OK, accepts "/" separated lists
    if accToUps==None:
        fname = join(GENEDATADIR, "uniprot.tab.marshal")
        logging.info("Loading %s" % fname)
        data = marshal.load(open(fname))[9606]
        accToUps = data["accToUps"]
        upToEntrez = data["upToEntrez"]
        upToSym = data["upToSym"]
        entrezToUp = data["entrezToUp"]

        fname = join(GENEDATADIR, "entrez.9606.tab.marshal")
        logging.info("Loading %s" % fname)
        data = marshal.load(open(fname))
        entrezToSym = data["entrez2sym"]
        symToEntrez = dict([(y,x) for (x,y) in entrezToSym.iteritems()])

    # don't do a lot if we already have an entrez ID, just map to possible symbols
    if markerType in alreadyEntrez:
        data = {}
        for markerId in markerId.split("/"):
            sym = entrezToSym.get(int(markerId), None)
            if markerType=="entrez" and sym==None:
                logging.debug("entrez %s is probably not mapped to genome")
                continue
            data[int(markerId)] = sym
        return data

    # uniprot needs only one step
    if markerType=="uniprot":
        sym = upToSym.get(markerId, "")
        entrezList = upToEntrez.get(markerId, None)
        if entrezList==None:
            logging.warn("No entrez ID for uniprot acc %s" % markerId)
            return {}
        else:
            return {entrezList[0] : sym}

    # for bands we have a special mapping
    if markerType=="band":
        entrezIdSyms = bandToEntrezSyms.get(markerId, "")
        return entrezIdSyms

    # rewrite marker IDs in some cases
    # - omim needs a prefix
    if markerType=="omim":
        markerId = "omim"+markerId
    # - genbank-like dbs don't need versions
    elif markerType in ["refprot", "refseq", "genbank"]:
        markerId = markerId.split(".")[0]
    # - pdb IDs are always uppercase for us
    elif markerType == "pdb":
        markerId = markerId.upper()

    # normal case for most markers: two-step resolution acc -> uniprot -> entrez gene
    if markerId in accToUps:
        #logging.debug("%s is in accToups" % markerId)
        upIds = accToUps[markerId]
        geneIds = {}
        for upId in upIds:
            for gene in upToEntrez.get(upId, []):
                geneIds[gene] = upToSym.get(upId, "")
        logging.debug("Marker %s -> genes %s"  % (markerId, geneIds))
        return geneIds
    else:
        return None

def findGeneNames(text):
    """
    look for gene names and symbols. Some symbols need flanking trigger words. If these 
    are not present, they are returned as "symbolMaybe"

    >>> initData(addOptional=True)
    >>> list(findGeneNames("thyroid hormone receptor, beta"))
    [(0, 30, 'geneName', '7068')]
    >>> list(findGeneNames("FATE1"))
    [(0, 5, 'symbolMaybe', '89885')]
    >>> list(findGeneNames("FATE1 is overexpressed"))
    [(0, 5, 'symbol', '89885')]
    >>> list(findGeneNames("fate1 is overexpressed"))
    []
    >>> list(findGeneNames("PITX2 overexpression"))
    [(0, 5, 'symbol', '5308')]
    """
    assert(geneSymLex!=None)
    textLower = text.lower()
    for start, end, geneId in fastFind.fastFind(textLower, geneNameLex):
        yield (start, end, 'geneName', geneId)

    flankFindIter = fastFind.fastFindFlankWords(text, geneSymLex, wordDist=2, wordRe=fastFind.SYMRE)
    for start, end, geneId, leftWords, rightWords in flankFindIter:
        # if the symbol is marked as potentially ambiguous, check the flanking words
        if geneId.startswith("?"):
            leftWords = [w.lower() for w in leftWords]
            rightWords = [w.lower() for w in rightWords]
            geneId = geneId.strip("?")
            if len(symLeftReqWords.intersection(leftWords))!=0 or \
                len(symRightReqWords.intersection(rightWords))!=0:
                yield (start, end, 'symbol', geneId)
            else:
                yield (start, end, 'symbolMaybe', geneId)
        # otherwise just pass them though
        else:
            yield (start, end, 'symbol', geneId)
    #rows.extend(list(iterGeneNames(textLower)))
    #continue

def findSequences(text):
    """ find dna in text and return as a list of tuples: (start, end, seq)
    >>> list(findSequences(" actg catgtgtg catgtgc  tgactg crap crap crap tga "))
    [(1, 30, 'actgcatgtgtgcatgtgctgactg')]
    """
    for row in pubDnaFind.nucleotideOccurrences(text):
        if row.seq=="": # can only happen if seq is a restriction site
            continue
        yield row.start, row.end, row.seq

def findIdentifiers(text):
    """  find gene and other marker identifiers, like dbSNP ids
    
        search text for occurences of regular expression + check against dictionary
        yield tuples (start, end, typeOfWord, recognizedId) 
        Does not find gene names or gene symbols, only identifiers as found in the text.

    >>> list(findIdentifiers(" Pfam:IN-FAMILY:PF02311 "))
    [[16, 23, 'pfam', 'PF02311']]
    >>> list(findIdentifiers("  (8q22.1,  "))
    [[3, 9, 'band', '8q22.1']]
    >>> list(findIdentifiers("(NHS,"))
    []
    >>> list(findIdentifiers(" rs 123544 "))
    [[4, 10, 'snp', '123544']]
    >>> list(findIdentifiers("MIM# 609883"))
    [[5, 11, 'omim', '609883']]
    >>> list(findIdentifiers("OMIM: 609883"))
    [[6, 12, 'omim', '609883']]
    >>> list(findIdentifiers(" 1abz protein data bank "))
    [[1, 5, 'pdb', '1abz']]
    >>> list(findIdentifiers(" 1ABZ PDB "))
    [[1, 5, 'pdb', '1abz']]
    >>> list(findIdentifiers(" 3ARC PDB "))
    [[1, 5, 'pdb', '3arc']]
    >>> list(findIdentifiers(" B7ZGX9 P12345 ")) # p12345 is not a uniprot ID
    [[1, 7, 'uniprot', 'B7ZGX9']]
    >>> list(findIdentifiers(" L76943 ena "))
    [[1, 7, 'genbank', 'L76943']]
    >>> list(findIdentifiers(" L76943"))
    []
    >>> list(findIdentifiers(" ENSG001230434 "))
    [[1, 14, 'ensembl', 'ENSG001230434']]
    >>> list(findIdentifiers(" chr1:123,220-123334234 hg19"))
    [[1, 23, 'hg19', 'chr1:123220-123334234']]
    >>> list(findIdentifiers(" LocusLink ID 3945 "))
    [[14, 18, 'entrez', '3945']]
    >>> list(findIdentifiers(" EC 3.2.1.22 "))
    [[4, 12, 'ec', '3.2.1.22']]
    >>> list(findIdentifiers(" [EC 3.2.1.22 "))
    [[5, 13, 'ec', '3.2.1.22']]
    >>> list(findIdentifiers(" UniSTS Accession 12343 "))
    [[18, 23, 'sts', '12343']]
    >>> list(findIdentifiers(" sgd YGL163C "))
    [[5, 12, 'sgd', 'YGL163C']]
    >>> list(findIdentifiers(" sgd S000003131"))
    [[5, 15, 'sgd', 'S000003131']]
    """
    global markerDictList
    global stopWords

    rows = []
    textLower = text.lower()
    for markerType, markerRe in markerDictList:
        logging.debug("Looking for markers of type %s" % markerType)
        # special case list of genbank identifiers like AF0000-AF0010  
        if markerType=="genbankList":
            for row in iterGenbankRows(markerRe, markerType, text):
                rows.append(row)
            continue

        # first check if the text contains the required words for this type
        if markerType in reqWordDict:
            keywords = reqWordDict[markerType]
            if not textContainsAny(textLower, keywords):
                continue

        filterSet = filterDict.get(markerType, None)
        for match in markerRe.finditer(text):
            if len(rows)>1000:
                logging.warn("More than 1000 identifiers in document for %s, stop" % markerType)
                break
            logging.debug("Found %s, for %s/%s" % (match.group(), markerType, markerRe.pattern))
            word = match.group("id")
            if word in stopWords:
                continue

            if markerType=="pdb":
                word = word.lower()

            if markerType in ["hg17", "hg18", "hg19"]:
                word = word.replace(",", "").replace(" ", "")

            if markerType=="band":
                if word not in bandToEntrezSyms:
                    logging.log(5, "%s not in bandToEntrezSyms" % word)
                    continue

            if filterSet!=None:
                if word not in filterSet:
                    logging.debug("%s not in filter" % word)
                    continue

            if word in notIdentifiers:
                logging.log(5, "%s is blacklisted" % word)
                continue
                
            start = match.start("id")
            end = match.end("id")
            row = [ start, end, markerType, word]
            rows.append(row)

    if len(rows)<MAXROWS:
        return rows
    else:
        return []

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    #logging.basicConfig(level=logging.INFO)
    import doctest
    doctest.testmod()
