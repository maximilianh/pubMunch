# search for accession numbers in text
# we don't have a database of all accessions, so only some are verified to be valid identifiers

# validated ones are: uniprot, pdb
# mostly validated: genbank (number of digits and prefix)
# detected but not validated: bands, dbsnp, ensembl, UCSC, omim, ec, and many more
# some accessions need additional keywords in the text to trigger a match, see below

import re, logging
from os.path import join

import pubKeyVal, pubConf

# skip genbank lists like A1234-A1240 with more identifiers than this
MAXGBLISTCOUNT=30

# ignore articles with more than X accessions found
MAXROWS = 500

# read dictionaries and bed files from this directory
DATADIR= pubConf.accDataDir

# some words are valid identifiers but are actually not used as such
notIdentifiers = set(["1rho", "U46619"])

# Some identifiers are so general that we want to restrict our search
# to documents that contain some keyword
# the reqWordDict hash sets up the lists of keywords in the document
# that are required for certain identifiers
# keywords are case insensitive
genbankKeywords = ["genbank", "accession", " embl", "ddbj", "insdc", " ena ", "european nucleotide", " acc. ", "ncbi", "gene access"]

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

# compiled regexes are kept in a global var
# as list of (name, regexObject) 
markerDictList = None

# separators before or after the regular expressions below
endSep = r'''(?=["'\s:,.()])'''
endSepDash = r'''(?=["'\s:,.()-])'''
startSep = r'''["'\s,.();:=]'''
startSepDash = r'''["'\s,.();:=-]'''


def compileREs(addOptional=True):
    " compile REs and return as dict type -> regex object "
    # received genbank regex by email from Guy Cochrane, EBI
    genbankRe = re.compile("""[ ;,.()](?P<id>(([A-Z]{1}\d{5})|([A-Z]{2}\d{6})|([A-Z]{4}\d{8,9})|([A-Z]{5}\d{7}))(\.[0-9]{1,})?)%s""" % endSep)
    # a range of genbank identifiers
    genbankListRe = re.compile(r'[ ;,.()](?P<id1>(([A-Z]{1}\d{5})|([A-Z]{2}\d{6})|([A-Z]{4}\d{8,9})|([A-Z]{5}\d{7}))(\.[0-9]{1,})?)-(?P<id2>(([A-Z]{1}\d{5})|([A-Z]{2}\d{6})|([A-Z]{4}\d{8,9})|([A-Z]{5}\d{7}))(\.[0-9]{1,})?)%s' % (endSep))
    #snpRsRe = re.compile(r'[ ;,.()]rs[ #-]?(?P<id>[0-9]{4,10})%s' % (endSep))
    # dbSNP final rs identifiers
    snpRsRe = re.compile(r'%s(SNP|dbSNP|rs|Rs|RefSNP|refSNP)( |-| no.| no| No.| ID|ID:| #|#| number)?[ :]?(?P<id>[0-9]{4,19})' % (startSep))
    # dbSNP preliminary SS identifiers
    snpSsRe = re.compile("""[ ;,.()](?P<id>ss[0-9]{4,16})%s""" % (endSep))
    # genome coordinates
    coordRe = re.compile("%s(?P<id>(chr|chrom|chromosome)[ ]*[0-9XY]{1,2}:[0-9,]{4,12}[ ]*-[ ]*[0-9,]{4,12})%s" % (startSep, endSep))
    # cytogenetic bands
    bandRe = re.compile("""[ ,.()](?P<id>(X|Y|[1-9][0-9]?)(p|q)[0-9]+(\.[0-9]+)?)%s""" % (endSep))
    # HGNC symbols
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
    # NCBI entrez gene, formerly called LocusLink
    entrezRe = re.compile(r'(Locus|LocusLink|Locuslink|LOCUSLINK|LOCUS|Entrez Gene|Entrez|Entrez-Gene|GeneID|LocusID)( )?(#|No|no|number|ID|accession)?( )?(:)?( )?(?P<id>[0-9]{3,8})')
    # EC Enzyme codes
    ecRe = re.compile(r'EC ? ?(?P<id>[0-9][0-9]?\.[0-9][0-9]?\.[0-9][0-9]?\.[0-9][0-9]?)')
    # UniSTS Marker names
    stsRe = re.compile(r'(UniSTS|UNISTS|uniSTS) ?([aA]ccession|[Aa]ccession number|#|ID|[nN]o|[Nn]umber|[Nn]o.)?(:)? ?(?P<id>[0-9]{3,10})')
    # MEROPS http://merops.sanger.ac.uk/cgi-bin/aaseq?mernum=MER000485
    meropsRe = re.compile(r'(?P<id>MER[0-9]{6})')

    reDict = {"genbank": genbankRe,
              "genbankList": genbankListRe,
              "snp": snpRsRe,
              "snpSs": snpSsRe,
              "cytoBand": bandRe,
              #"symbol": symbolRe,
              "uniprot": uniprotRe,
              "pdb": pdbRe,
              "refseq" : refseqRe,
              "refseqprot" : refseqProtRe,
              "ensembl" : ensemblRe,
              "hg17" : coordRe,
              "hg18" : coordRe,
              "hg19" : coordRe,
              "omim" : omimRe,
              "ec" : ecRe,
              "entrez" : entrezRe,
              "sts" : stsRe,
              "merops" : meropsRe
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
        clinTrialsRe = re.compile(r'%s(?P<id>NCT[0-9]{8})' % startSepDash)

        # a more or less random selection of keywords, not sure if this is really necessary
        global reqWordDict
        reqWordDict.update({
            "arrayexpress": ["arrayexpress"],
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
            "arrayexpress" : arrayExprRe,
            "geo"          : geoRe,
            "interpro"     : interproRe,
            "pfam"         : pfamRe,
            "prints"       : printsRe,
            "pirsf"        : pirsfRe,
            "smart"        : smartRe,
            "supfam"       : supFamRe,
            "affymetrix"   : affyRe,
            "kegg"         : keggRe,
            "hprd"         : hprdRe,
            "pharmgkb"     : pharmGkbRe,
            "chembl"       : chemblRe,
            "hinv"         : hInvRe,
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
            "zfin"         : zfinRe,
            # https://clinicaltrials.gov/show/NCT01204177
            "clintrials"   : clinTrialsRe
            })

    return reDict

# ==== VALIDATING FUNCTIONS ====

pdbAccs = None

def isValidPdb(word):
    " return true if word is a pdb accession "
    global pdbAccs
    if pdbAccs is None:
        pdbFname = join(DATADIR, "pdb.txt")
        pdbAccs = set()
        for line in open(pdbFname).read().splitlines():
            acc = line.split()[0]
            pdbAccs.add(acc)

    if not word in pdbAccs:
        logging.debug("%s is not a PDB accs" % word)
        return False
    return True
        
uniprotDb = None

def isValidUniProt(word):
    " return true if word is a uniprot accession "
    global uniprotDb
    if uniprotDb is None:
        dbFname = join(DATADIR, "uniprot")
        logging.info("opening list of uniprot accessions %s" % dbFname)
        uniprotDb = pubKeyVal.SqliteKvDb(dbFname)
    if not word in uniprotDb:
        logging.debug("%s is not a uniProt accs" % word)
        return False
    return True
        
# dict of prefix -> list of number of digits
# e.g. AY -> [6] means that AY is always followed by six digits in genbank accessions
genbankSchema = None

def isValidGenbank(word):
    " return true if word is a genbank accession. Checks only prefix + number of digits. "
    global genbankSchema
    if genbankSchema is None:
        txtFname = join(DATADIR, "genbankFormat.txt")
        logging.info("Loading %s" % txtFname)
        genbankSchema = parseKeyValList(txtFname, isInt=True)

    prefix, digits, _ = splitGenbankAcc(word)
    digitLen = len(digits)
    if prefix not in genbankSchema or digitLen not in genbankSchema[prefix]:
        logging.debug("%s does not look like a genbank accs" % word)
        return False

    return True

# validators are functions that check if the accession looks valid
validatorFuncs = {
    'pdb' : isValidPdb,
    'uniprot' : isValidUniProt,
    'genbank' : isValidGenbank
}

# ==== END OF CONFIGURATION 

def parseKeyValList(fname, isInt=False):
    " parse a text file in the format key<tab>val1|val2|... and return as dict of key -> list of values "
    data = dict()
    for line in open(fname):
        key, valStr = line.rstrip("\n").split("\t")[:2]
        vals = valStr.split("|")
        if isInt:
            vals = [int(x) for x in vals]
        data[key] = vals
    return data

def splitGenbankAcc(acc):
    """ split a string like AY1234 into letter-number tuple, e.g. (AY, '1234')
    >>> splitGenbankAcc("AY1234")
    ('AY', '1234', 4)
    """
    matches = list(re.finditer(r"([A-Z]+)([0-9]+)", acc))
    # re2 has trouble with the .match function
    if len(matches)>1 or len(matches)==0:
        return None
    match = matches[0]
    letters, numbers = match.groups()
    return (letters, numbers, len(numbers))

def textContainsAny(text, keywords):
    " brute force string search, for a few keywords this should be not too slow "
    for keyword in keywords:
        if keyword in text:
            return True
    return False

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
        num1 = int(num1)
        num2 = int(num2)
        if let1!=let2 or digits1!=digits2:
            continue
        if (num2-num1) > MAXGBLISTCOUNT:
            print "too big"
            logging.debug("genbank list range too big")
            continue
        for num in range(num1, num2+1):
            numFmt = "%%0%sd" % digits1
            acc = let1+(numFmt % num)
            start = match.start(0)
            end = match.end(1)
            #yield [ start, end, markerType, word, acc ]
            yield [ start, end, markerType, acc ]

class AccsFinder():
    """ main accession finder 
    >>> a = AccsFinder()
    >>> list(a.findAccessions(" genbank CW123456.1 "))
    [[9, 19, 'genbank', 'CW123456.1']]
    >>> list(a.findAccessions(" genbank CW123456-CW123459 "))
    [[8, 17, 'genbank', 'CW123456'], [8, 17, 'genbank', 'CW123457'], [8, 17, 'genbank', 'CW123458'], [8, 17, 'genbank', 'CW123459']]
    >>> list(a.findAccessions(" Pfam:IN-FAMILY:PF02311 "))
    [[16, 23, 'pfam', 'PF02311']]
    >>> list(a.findAccessions("  (8q22.1,  "))
    [[3, 9, 'cytoBand', '8q22.1']]
    >>> list(a.findAccessions(" rs 123544 "))
    [[4, 10, 'snp', 'rs123544']]
    >>> list(a.findAccessions(" Pfam:IN-FAMILY:PF02311 "))
    [[16, 23, 'pfam', 'PF02311']]
    >>> list(a.findAccessions("  (8q22.1,  "))
    [[3, 9, 'cytoBand', '8q22.1']]
    >>> list(a.findAccessions("(NHS,"))
    []
    >>> list(a.findAccessions(" rs 123544 "))
    [[4, 10, 'snp', 'rs123544']]
    >>> list(a.findAccessions("MIM# 609883"))
    [[5, 11, 'omim', '609883']]
    >>> list(a.findAccessions("OMIM: 609883"))
    [[6, 12, 'omim', '609883']]
    >>> list(a.findAccessions(" 1abz protein data bank "))
    [[1, 5, 'pdb', '1abz']]
    >>> list(a.findAccessions(" 1ABZ PDB "))
    [[1, 5, 'pdb', '1abz']]
    >>> list(a.findAccessions(" 3ARC PDB "))
    [[1, 5, 'pdb', '3arc']]
    >>> list(a.findAccessions(" B7ZGX9 A11111 ")) # A11111 is not a valid uniprot ID
    [[1, 7, 'uniprot', 'B7ZGX9']]
    >>> list(a.findAccessions(" L76943 ena "))
    [[1, 7, 'genbank', 'L76943']]
    >>> list(a.findAccessions(" L76943"))
    []
    >>> list(a.findAccessions(" ENSG001230434 "))
    [[1, 14, 'ensembl', 'ENSG001230434']]
    >>> list(a.findAccessions(" chr1:123,220-123334234 hg19"))
    [[1, 23, 'hg19', 'chr1:123220-123334234']]
    >>> list(a.findAccessions(" LocusLink ID 3945 "))
    [[14, 18, 'entrez', '3945']]
    >>> list(a.findAccessions(" EC 3.2.1.22 "))
    [[4, 12, 'ec', '3.2.1.22']]
    >>> list(a.findAccessions(" [EC 3.2.1.22 "))
    [[5, 13, 'ec', '3.2.1.22']]
    >>> list(a.findAccessions(" UniSTS Accession 12343 "))
    [[18, 23, 'sts', '12343']]
    >>> list(a.findAccessions(" sgd YGL163C "))
    [[5, 12, 'sgd', 'YGL163C']]
    >>> list(a.findAccessions(" sgd S000003131"))
    [[5, 15, 'sgd', 'S000003131']]
    """
    def __init__(self, onlyDbs=None, removeDbs=None):
        """ compile regexes.
            you can remove DBs with removeDbs or restrict to onlyDbs 
        """
        self.reDict = compileREs(addOptional=True)
        if onlyDbs is not None:
            newReDict = {}
            for db in onlyDbs:
                newReDict[db] = self.reDict[db]
            self.reDict = newReDict

        if removeDbs is not None:
            for db in removeDbs:
                del self.reDict[db]

    def findAccessions(self, text):
        """  find accession numbers in text.
             Yield tuples (start, end, accessionType, accession)
             Does not find gene names or gene symbols.
        """
        rows = []
        textLower = text.lower()
        for accType, accRe in self.reDict.iteritems():
            logging.log(5, "Looking for markers of type %s" % accType)
            # special case list of genbank identifiers like AF0000-AF0010  
            if accType=="genbankList":
                for row in iterGenbankRows(accRe, accType, text):
                    rows.append(row)
                continue

            # first check if the text contains the required words for this type
            if accType in reqWordDict:
                keywords = reqWordDict[accType]
                if not textContainsAny(textLower, keywords):
                    continue

            for match in accRe.finditer(text):
                if len(rows)>1000:
                    logging.warn("More than 1000 identifiers in document for %s, stop" % accType)
                    break
                logging.debug("Found %s, for %s/%s" % (match.group(), accType, accRe.pattern))
                word = match.group("id")

                # clean the word of spurious characters
                if accType=="pdb":
                    word = word.lower()
                if accType in ["hg17", "hg18", "hg19"]:
                    word = word.replace(",", "").replace(" ", "")

                if word in notIdentifiers:
                    logging.log(5, "%s is blacklisted" % word)
                    continue
                    
                # stage2 filtering functions
                filterFunc = validatorFuncs.get(accType, None)
                if filterFunc!=None:
                    if not filterFunc(word):
                        logging.debug("%s not accepted by filter" % word)
                        continue

                start = match.start("id")
                end = match.end("id")
                if accType=="snp":
                    word = "rs"+word
                row = [ start, end, accType, word]
                rows.append(row)

        if len(rows)<MAXROWS:
            return rows
        else:
            logging.debug("accsFinder: too many rows, not returning anything")
            return []

if __name__ == "__main__":
    #logging.basicConfig(level=logging.DEBUG)
    # just output current list of accession types
    print ", ".join(sorted(compileREs(addOptional=True).keys()))
    logging.basicConfig(level=logging.INFO)
    import doctest
    doctest.testmod()
