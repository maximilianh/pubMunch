# find descriptions of variants in text
import logging, gdbm, marshal, zlib, copy, struct, random
from collections import defaultdict, namedtuple
from os.path import join

# I highly recommend installing re2, it's way faster than re here
# we fallback to re just in case
try:
    import re2 as re
except ImportError:
    import re

from pubSeqTables import threeToOneLower, threeToOne, oneToThree, aaToDna, dnaToAa
from pycbio.hgdata.Psl import Psl
import pslMapBed, pubAlg, maxbio, pubConf, maxCommon

# ===== DATA TYPES ========
Mention = namedtuple("Mention", "patName,start,end")

""" A mapped variant is a type-range-sequence combination from a text, 
    can be located on none, one or multiple types of sequences
    All positions are 0-based
"""
VariantFields = [
    "mutType", # sub, del or ins
    "seqType", # prot, dna or dbSnp 
    "seqId",  # protein or nucleotide accession
    "geneId", # entrez gene id, if found nearby in text
    "start",  # original position in text
    "end",    # end position in text
    "origSeq", # wild type seq, used for sub and del, also used to store rsId for dbSnp variants
    "mutSeq",  # mutated seq, used for sub and ins
    ]

# A completely resolved mutation
mutFields = \
    (
    "chrom",       # chromosome
    "start",       # on chrom
    "end",         # on chrom
    "varId",       # a unique id
    "inDb",        # list of db names where was found
    "patType",     # the type of the patterns (sub, del, ins)
    "hgvsProt",    # hgvs on protein, can be multiple, separated with |
    "hgvsCoding",  # hgvs on cdna, can be multiple, separated with |
    "hgvsRna",     # hgvs on refseq, separated by "|"
    "comment",     # comment on how mapping was done
    "rsIds",       # possible rsIds, separated by "|", obtained by mapping from hgvsRna
    "protId",      # the protein ID that was used for the first mapping
    "texts",        # mutation match in text
    #"mutSupport",  # prot, dna, protDna
    #"mutCount",    # how often was this mutation mentioned?
    "rsIdsMentioned", # mentioned dbSnp IDs that support any of the hgvsRna mutations
    "dbSnpStarts" ,   # mentioned dbSnp IDs in text, start positions
    "dbSnpEnds",      # mentioned dbSNP Ids in text, end positions

    "geneSymbol",  # symbol of gene
    "geneType",    # why was this gene selected (entrez, symNearby, symInTitle, symInAbstract)
    "entrezId",    # entrez ID of gene
    "geneStarts",  # start positions of gene mentions in document
    "geneEnds",    # end positions of gene mentions in document

    "seqType",     # the seqType of the patterns, dna or protein
    "mutPatNames",    # the names of the patterns that matched, separated by |
    "mutStarts",   # start positions of mutation pattern matches in document
    "mutEnds",     # end positions of mutation pattern matches in document
    "mutSnippets",  # the phrases around the mutation mentions, separated by "|"
    "geneSnippets",  # the phrases around the gene mentions, separated by "|"
    "dbSnpSnippets" # mentioned dbSNP Ids in text, snippets
    )

# fields of the output file
MutRec = namedtuple("mutation_desc", mutFields)


# ======= GLOBALS ===============
# this can be used to shuffle all protein sequences before matching
# to get a random background estimate
doShuffle = False

geneData = None

# these look like mutations but are definitely not mutations
blackList = [
    ("D", 11, "S"), # satellites
    ("D", 12, "S"),
    ("D", 13, "S"),
    ("D", 14, "S"),
    ("D", 15, "S"),
    ("D", 16, "S"),
    ("A", 84, "M"), # cell lines...
    ("A", 84, "P"), # all of these copied...
    ("A", 94, "P"), # from http://bioinf.umbc.edu/EMU/ftp/Cell_line_list_short.txt
    ("A", 94, "P"),
    ("C", 127, "I"),
    ("C", 86, "M"),
    ("C", 86, "P"),
    ("L", 283, "R"),
    ("H", 96, "V"),
    ("L", 5178, "Y"),
    ("L", 89, "M"),
    ("L", 89, "P"),
    ("L", 929, "S"),
    ("T", 89, "G"),
    ("T", 47, "D"),
    ("T", 84, "M"),
    ("T", 98, "G"),
    ("S", 288, "C"), # yeast strain names
    ("T", 229, "C"),

    # these are from cellosaurus:
    # "pubPrepGeneDir cells" to re-generate this list
    ('F', 442, 'A'), ('A', 101, 'D'), ('A', 2, 'H'), ('A', 375, 'M'), ('A', 375, 'P'), ('A', 529, 'L'), ('A', 6, 'L'), ('B', 10, 'R'), ('B', 10, 'S'), ('B', 1203, 'L'), ('C', 2, 'M'), ('C', 2, 'W'), ('B', 16, 'V'), ('B', 35, 'M'), ('B', 3, 'D'), ('B', 46, 'M'), ('C', 33, 'A'), ('C', 4, 'I'), ('C', 127, 'I'), ('C', 463, 'A'), ('C', 611, 'B'), ('C', 831, 'L'), ('D', 18, 'T'), ('D', 1, 'B'), ('D', 2, 'N'), ('D', 422, 'T'), ('D', 8, 'G'), ('F', 36, 'E'), ('F', 36, 'P'), ('F', 11, 'G'), ('F', 1, 'B'), ('F', 4, 'N'), ('G', 14, 'D'), ('G', 1, 'B'), ('G', 1, 'E'), ('H', 2, 'M'), ('H', 2, 'P'), ('H', 48, 'N'), ('H', 4, 'M'), ('H', 4, 'S'), ('H', 69, 'V'), ('C', 3, 'A'), ('C', 1, 'R'), ('H', 766, 'T'), ('I', 51, 'T'), ('K', 562, 'R'), ('L', 5178, 'Y'), ('L', 2, 'C'), ('L', 929, 'S'), ('M', 59, 'K'), ('M', 10, 'K'), ('M', 10, 'T'), ('M', 14, 'K'), ('M', 22, 'K'), ('M', 24, 'K'), ('M', 25, 'K'), ('M', 28, 'K'), ('M', 33, 'K'), ('M', 38, 'K'), ('M', 9, 'A'), ('M', 9, 'K'), ('H', 1755, 'A'), ('H', 295, 'A'), ('H', 295, 'R'), ('H', 322, 'M'), ('H', 460, 'M'), ('H', 510, 'A'), ('H', 676, 'B'), ('P', 3, 'D'), ('R', 201, 'C'), ('R', 2, 'C'), ('S', 16, 'Y'), ('S', 594, 'S'), ('N', 303, 'L'), ('N', 1003, 'L'), ('N', 2307, 'L'), ('N', 1108, 'L'), ('T', 47, 'D'), ('T', 27, 'A'), ('T', 88, 'M'), ('T', 98, 'G'), ('H', 5, 'D'), ('C', 1, 'A'), ('C', 1, 'D'), ('C', 2, 'D'), ('C', 2, 'G'), ('C', 2, 'H'), ('C', 2, 'N'), ('V', 79, 'B'), ('V', 9, 'P'), ('V', 10, 'M'), ('V', 9, 'M'), ('X', 16, 'C')
]

# ===== FUNCTIONS TO INIT THE GLOBALS =================

def loadDb():
    global geneData
    geneData = SeqData(pubConf.geneDataDir, 9606)
    global regexes
    regexes = parseRegex(pubConf.geneDataDir)
    logging.info("Blacklist has %d entries" % len(blackList))

# ===== CLASSES =================
class SeqData(object):
    """ functions to get sequences and map between identifiers for entrez genes,
    uniprot, refseq, etc """

    def __init__(self, mutDataDir, taxId):
        " open db files, compile patterns, parse input as far as possible "
        if mutDataDir==None:
            return
        self.mutDataDir = mutDataDir
        # load uniprot data into mem
        logging.info("Reading variant data: uniprot, entrez, ")
        fname = join(mutDataDir, "uniprot.tab.marshal")
        logging.debug("Reading uniprot data from %s" % fname)
        data = marshal.load(open(fname))
        self.entrez2Up = data[taxId]["entrezToUp"]
        self.upToSym = data[taxId]["upToSym"]
        self.upToIsos = data[taxId]["upToIsos"]
        self.upToGbProts = data[taxId]["upToGbProts"]

        # load mapping entrez -> refseq/refprot/symbol
        fname = join(mutDataDir, "entrez.%s.tab.marshal" % taxId)
        logging.debug("Reading entrez data from %s" % fname)
        entrezRefseq = marshal.load(open(fname))
        self.entrez2refseqs = entrezRefseq["entrez2refseqs"]
        self.entrez2refprots = entrezRefseq["entrez2refprots"]
        self.entrez2sym = entrezRefseq["entrez2sym"]

        # pmid -> entrez genes
        #fname = join(mutDataDir, "pmid2entrez.gdbm")
        #logging.info("opening %s" % fname)
        #pmid2entrez = gdbm.open(fname, "r")
        #self.pmid2entrez = pmid2entrez
        
        # refseq sequences
        fname = join(mutDataDir, "seqs.dbm")
        logging.info("opening %s" % fname)
        seqs = gdbm.open(fname, "r")
        self.seqs = seqs
        
        # refprot to refseqId
        # refseq to CDS Start
        fname = join(mutDataDir, "refseqInfo.tab")
        logging.debug("Reading %s" % fname)
        self.refProtToRefSeq = {}
        self.refSeqCds = {}
        for row in maxCommon.iterTsvRows(fname):
            self.refProtToRefSeq[row.refProt] = row.refSeq
            self.refSeqCds[row.refSeq] = int(row.cdsStart)-1

        self.seqCache = {}

        # -- these four parts could be fused into one
        # refseq to genome
        liftFname = join(mutDataDir, "refGene.%s.psl.dbm" % taxId)
        logging.debug("Opening %s" % liftFname)
        self.refGenePsls = gdbm.open(liftFname, "r")
        self.refGenePslCache = {}
        # refseq to old refseq
        liftFname = join(mutDataDir, "oldRefseqToRefseq.%s.prot.psl.dbm" % taxId)
        # oldRefseqToRefseq.9606.prot.psl.dbm
        logging.debug("Opening %s" % liftFname)
        self.oldRefseqPsls = gdbm.open(liftFname, "r")
        self.oldRefseqPslCache = {}
        # uniprot to refseq
        liftFname = join(mutDataDir, "upToRefseq.%s.psl.dbm" % taxId)
        logging.debug("Opening %s" % liftFname)
        self.uniprotPsls = gdbm.open(liftFname, "r")
        self.uniprotPslCache = {}
        # genbank to refseq
        liftFname = join(mutDataDir, "genbankToRefseq.%s.psl.dbm" % taxId)
        logging.debug("Opening %s" % liftFname)
        self.genbankPsls = gdbm.open(liftFname, "r")
        self.genbankPslCache = {}

        logging.info("Reading of data finished")

        # dbsnp dbm file handles
        self.snpDbmCache = {}
        self.rsIdDbmCache = {}

    def getSeq(self, seqId):
        " get seq from db , cache results "
        logging.log(5, "Looking up sequence for id %s" % seqId)
        seqId = str(seqId) # gdbm doesn't like unicode
        if seqId not in self.seqCache:
            if seqId not in self.seqs:
                return None
            comprSeq = self.seqs[seqId]
            seq = zlib.decompress(comprSeq)
            self.seqCache[seqId] = seq
        else:
            seq = self.seqCache[seqId]
        return seq

    def lookupDbSnp(self, chrom, start, end):
        " return the rs-Id of a position or None if not found "
        if chrom in self.snpDbmCache:
            dbm = self.snpDbmCache[chrom]
        else:
            fname = join(self.mutDataDir, "snp137."+chrom+".dbm")
            logging.debug("Opening %s" % fname)
            dbm = gdbm.open(fname, "r")
            self.snpDbmCache[chrom] = dbm

        # crazy bit-packing of coordinates, probably too much effort here
        # keeps dbsnp file small
        packCoord = struct.pack("<ll", int(start), int(end))
        if packCoord in dbm:
            packRsId = dbm[packCoord]
            rsId = struct.unpack("<l", packRsId)[0]
            rsId = "rs"+str(rsId)
        else:
            rsId = None
        return rsId

    def rsIdToGenome(self, rsId):
        " given the rs-Id, return chrom, start, end of it"
        lastDigit = rsId[-1]
        # lazily open dbms
        if lastDigit in self.rsIdDbmCache:
            dbm = self.rsIdDbmCache[lastDigit]
        else:
            fname = join(self.mutDataDir, "snp137.coords."+lastDigit+".dbm")
            logging.debug("Opening %s" % fname)
            dbm = gdbm.open(fname, "r")
            self.rsIdDbmCache[lastDigit] = dbm

        rsIdInt = int(rsId)
        if not -2147483648 <= rsIdInt <= 2147483647:
            logging.warn("clearly invalid rsId %s, out of int bounds" % rsId)
            return None, None, None

        rsIdPack = struct.pack("<l", rsIdInt)
        if not rsIdPack in dbm:
            logging.debug("rsId %s not found in db" % rsId)
            return None, None, None
        else:
            packCoord = dbm[rsIdPack]
            chrom, start, end = maxbio.unpackChromCoord(packCoord)
            logging.debug("rsId %s maps to %s, %d, %d" % (rsId, chrom, start, end))
            return chrom, start, end

    def entrezToUniProtIds(self, entrezGene):
        " return all uniprot isoform IDs for an entrez gene "
        upIds = self.entrez2Up.get(entrezGene, [])
        allIsos = []
        for upId in upIds:
            isoIds = self.upToIsos[upId]
            allIsos.extend(isoIds)
        logging.debug("entrez gene %s has uniprot IDs %s" % (entrezGene, allIsos))
        return allIsos

    def entrezToGenbankProtIds(self, entrezGene):
        " return all genbank protein IDs for an entrez gene "
        upIds = self.entrez2Up.get(entrezGene, [])
        allGbIds = []
        for upId in upIds:
            gbIds = self.upToGbProts.get(upId, None)
            if gbIds==None:
                logging.debug("entrezGene %s -> uniprot %s, but uniprot has not genbank IDs here" %
                    (entrezGene, upId))
                continue
            allGbIds.extend(gbIds)
        logging.debug("entrez gene %s has genbank IDs %s" % (entrezGene, allGbIds))
        return allGbIds

    def entrezToOtherDb(self, entrezGene, db):
        " return accessions (list) in otherDb for entrezGene "
        if db=="refseq":
            protIds = self.entrezToRefseqProts(entrezGene)
        elif db=="oldRefseq":
            protIds = self.entrezToOldRefseqProts(entrezGene)
        elif db=="uniprot":
            protIds = self.entrezToUniProtIds(entrezGene)
        elif db=="genbank":
            protIds = self.entrezToGenbankProtIds(entrezGene)
        else:
            assert(False)
        return protIds

    def entrezToSym(self, entrezGene):
        entrezGene = int(entrezGene)
        if entrezGene in self.entrez2sym:
            geneSym = self.entrez2sym[entrezGene]
            logging.debug("Entrez gene %s = symbol %s" % (entrezGene, geneSym))
            return geneSym
        else:
            return None

    def entrezToRefseqProts(self, entrezGene):
        " map entrez gene to refseq prots like NP_xxx "
        if entrezGene not in self.entrez2refprots:
            logging.debug("gene %d is not valid or not in selected species" % entrezGene)
            return []
        protIds = self.entrez2refprots[entrezGene]
        logging.debug("Entrez gene %s is mapped to proteins %s" % \
            (entrezGene, ",".join(protIds)))
        return protIds

    def entrezToOldRefseqProts(self, entrezGene):
        " map entrez gene to old refseq prots "
        newProtIds = self.entrezToRefseqProts(entrezGene)
        protIds = newToOldRefseqs(newProtIds)
        if len(protIds)>0:
            logging.debug("Also trying old refseq protein IDs %s" % protIds)
        return protIds

    def getCdsStart(self, refseqId):
        cdsStart = self.refSeqCds[refseqId]
        return cdsStart

    #def getEntrezGenes(self, pmid):
        #if not pmid in self.pmid2entrez:
            #return None
        #dbRes = self.pmid2entrez[pmid]
        #entrezGenes = dbRes.split(",")
        #entrezGenes = [int(x) for x in entrezGenes]
        #return entrezGenes

    def getRefSeqId(self, refProtId):
        " resolve refprot -> refseq using refseq data "
        refseqId = self.refProtToRefSeq.get(refProtId, None)
        return refseqId

    def getProteinPsls(self, db, protId):
        if db=="uniprot":
            return self.getUniprotPsls(protId)
        elif db=="oldRefseq":
            return self.getOldRefseqPsls(protId)
        elif db=="genbank":
            return self.getGenbankPsls(protId)
        else:
            assert(False)

    # ---- these three could be folded into a single method
    def getOldRefseqPsls(self, protId):
        psls = getPsls(protId, self.oldRefseqPslCache, self.oldRefseqPsls)
        return psls
    def getUniprotPsls(self, uniprotId):
        psls = getPsls(uniprotId, self.uniprotPslCache, self.uniprotPsls)
        return psls
    def getGenbankPsls(self, protId):
        " strip version of id and return psl "
        psls = getPsls(protId, self.genbankPslCache, self.genbankPsls, stripVersion=True)
        return psls
    #  ---------------------
    def getRefseqPsls(self, refseqId):
        """ return psl objects for regseq Id
            as ucsc refseq track doesn't support version numbers, we're stripping those on input
        """
        psls = getPsls(refseqId, self.refGenePslCache, self.refGenePsls, stripVersion=True)
        return psls

    # end of class seqData

class MappedVariant(object):
    """ A mapped variant is a type-range-sequence combination from a text,
        located on one or multiple sequences
        It has a name, a unified textual description.
    """
    __slots__=VariantFields

    def __init__(self, mutType, seqType, start, end, origSeq, mutSeq, seqId=None):
        self.mutType = mutType # sub, del or ins or dbSnp
        self.seqType = seqType # cds, rna or prot
        self.seqId = seqId
        self.geneId = ""
        self.start = int(start)
        self.end = int(end)
        self.origSeq = origSeq
        self.mutSeq = mutSeq

    def getName(self):
       " return HGVS text for this variant "
       #logging.debug("Creating HGVS type %s for vars %s" % (hgvsType, variants))
       if self.seqId==None:
           name = "p.%s%d%s" % (self.origSeq, self.start, self.mutSeq)
       elif self.mutType=="dbSnp":
           name = self.origSeq
       else:
           name = makeHgvsStr(self.seqType, self.seqId, self.origSeq, self.start, self.mutSeq)
       return name

    def asRow(self):
        row =[]
        for i in self.__slots__:
            row.append(unicode(getattr(self, i)))
        return row
        
    def copyNoLocs(self):
        #" return copy of myself without any seqIdLocs "
        #newObj = MappedVariant(self.mutType, self.seqType, self.start, self.end, self.origSeq, self.mutSeq)
        #return newObj
        pass

    def __repr__(self):
        return ",".join(self.asRow())
    
class SeqVariantData(object):
    """ the full information about variant located on a sequence, with mentions from the text that support it
        This is the final output of this module, including all information about mapped variants and their genes.
    
    """
    __slots__ = mutFields

    def __init__(self, varId="", protVars=[], codVars=[], rnaVars=[],  \
            comment="", beds=[], entrezGene="", geneSym="", rsIds=[],  \
            dbSnpMentionsByRsId={}, mentions=[], text="", seqType="prot", patType="sub"):
        self.varId   = varId
        self.inDb    = ""
        self.patType = patType
        self.seqType = seqType
        self.chrom      = ""
        self.start      = ""
        self.end        = ""
        self.geneSymbol = geneSym
        self.entrezId   = entrezGene
        self.hgvsProt   = "|".join([v.getName() for v in protVars])
        self.hgvsCoding = "|".join([v.getName() for v in codVars])
        self.hgvsRna    = "|".join([v.getName() for v in rnaVars])
        self.comment    = comment
        self.rsIds      = "|".join(rsIds)
        self.protId     = ""
        self.geneType   = "entrez"
        self.geneStarts = ""
        self.geneEnds   = ""
        self.geneSnippets = ""

        # for each rsId mentioned, concat their starts/ends/snips
        # for each mention there is one rsId in rsIdsMentioned
        starts = []
        ends = []
        snippets = []
        mentionedRsIds = []
        for rsId, mentions in dbSnpMentionsByRsId.iteritems():
            rsStarts, rsEnds, rsPatNames, rsSnips, rsTexts = mentionsFields(mentions, text)
            starts.extend(rsStarts)
            ends.extend(rsEnds)
            snippets.extend(rsSnips)
            # for each mention there is one rsId
            for m in mentions:
                mentionedRsIds.append(rsId)
        self.dbSnpStarts = ",".join(starts)
        self.dbSnpEnds   = ",".join(ends)
        self.dbSnpSnippets = "|".join(snippets)
        self.rsIdsMentioned = "|".join(mentionedRsIds)

        mutStarts, mutEnds, patNames, snippets, texts = mentionsFields(mentions, text)
        self.mutStarts = ",".join(mutStarts)
        self.mutEnds   = ",".join(mutEnds)
        self.mutPatNames= "|".join(patNames)
        self.mutSnippets = "|".join(snippets)
        self.texts = "|".join(set(texts))

    def asRow(self, rawStr=False):
        row =[]
        for i in self.__slots__:
            s = getattr(self, i)
            if rawStr:
                s = str(s)
            else:
                s = unicode(s)
            row.append(s)
        return row
        
    def __repr__(self):
        return ",".join(self.asRow())
        
# ===== FUNCTIONS =================
# helper methods for SeqData
def getPsls(qId, cache, dbm, stripVersion=False):
    """ load psls from compressed dbm, create Psl objects, use a cache 
    reverse complement is on negative strand
    """
    qId = str(qId)
    if stripVersion:
        qId = str(qId).split(".")[0]
    logging.debug("Getting mapping psl for %s" % qId)
    if qId in cache:
        psls = cache[qId]
    else:
        if not qId in dbm:
            logging.error("Could not find PSL for %s" % qId)
            return []
        pslLines = zlib.decompress(dbm[qId])
        psls = []
        for line in pslLines.split("\n"):
            psl = Psl(line.split("\t"))
            psls.append(psl)
        cache[qId] = psls
    logging.debug("Got mapping psl %s" % str(psls[0]))
    
    corrPsls = []
    for p in psls:
        if p.strand=="-":
            p2 = p.reverseComplement()
        else:
            p2 = p
        corrPsls.append(p2)

    return corrPsls

def makeMention(match, patName):
    start = match.start()
    end = match.end()
    mention = Mention(patName, start, end)
    return mention

def parseRegex(mutDataDir):
    """ parse and compile regexes to list (seqType, mutType, patName, pat) """
    # read regexes, translate placeholders to long form and compile
    replDict = {
    "sep"         : r"""(?:^|[\s\(\[\'"/,\-])""",
    "fromPos"     : r'(?P<fromPos>[1-9][0-9]+)',
    "toPos"       : r'(?P<toPos>[1-9][0-9]+)',
    "pos"         : r'(?P<pos>[1-9][0-9]+)',
    "origAaShort" : r'(?P<origAaShort>[CISQMNPKDTFAGHLRWVEYX])',
    "mutAaShort"  : r'(?P<mutAaShort>[CISQMNPKDTFAGHLRWVEYX*])',
    "skipAa"  : r'(CYS|ILE|SER|GLN|MET|ASN|PRO|LYS|ASP|THR|PHE|ALA|GLY|HIS|LEU|ARG|TRP|VAL|GLU|TYR|TER|GLUTAMINE|GLUTAMIC ACID|LEUCINE|VALINE|ISOLEUCINE|LYSINE|ALANINE|GLYCINE|ASPARTATE|METHIONINE|THREONINE|HISTIDINE|ASPARTIC ACID|ARGININE|ASPARAGINE|TRYPTOPHAN|PROLINE|PHENYLALANINE|CYSTEINE|SERINE|GLUTAMATE|TYROSINE|STOP|X)',
    "origAaLong"  : r'(?P<origAaLong>(CYS|ILE|SER|GLN|MET|ASN|PRO|LYS|ASP|THR|PHE|ALA|GLY|HIS|LEU|ARG|TRP|VAL|GLU|TYR|TER|GLUTAMINE|GLUTAMIC ACID|LEUCINE|VALINE|ISOLEUCINE|LYSINE|ALANINE|GLYCINE|ASPARTATE|METHIONINE|THREONINE|HISTIDINE|ASPARTIC ACID|ARGININE|ASPARAGINE|TRYPTOPHAN|PROLINE|PHENYLALANINE|CYSTEINE|SERINE|GLUTAMATE|TYROSINE|STOP|X))',
    "mutAaLong"  : r'(?P<mutAaLong>(CYS|ILE|SER|GLN|MET|ASN|PRO|LYS|ASP|THR|PHE|ALA|GLY|HIS|LEU|ARG|TRP|VAL|GLU|TYR|TER|GLUTAMINE|GLUTAMIC ACID|LEUCINE|VALINE|ISOLEUCINE|LYSINE|ALANINE|GLYCINE|ASPARTATE|METHIONINE|THREONINE|HISTIDINE|ASPARTIC ACID|ARGININE|ASPARAGINE|TRYPTOPHAN|PROLINE|PHENYLALANINE|CYSTEINE|SERINE|GLUTAMATE|TYROSINE|STOP|X))',
    "dna"         : r'(?P<dna>[actgACTG])',
    "origDna"     : r'(?P<origDna>[actgACTG])',
    "mutDna"      : r'(?P<mutDna>[actgACTG])',
    "fs"          : r'(?P<fs>(fs\*?[0-9]*)|fs\*|fs|)?',
    }
    regexTab = join(mutDataDir, "regex.txt")
    logging.info("Parsing regexes from %s" % regexTab)
    regexList = []
    counts = defaultdict(int)
    for row in maxCommon.iterTsvRows(regexTab, commentPrefix="#"):
        logging.log(5, "Translating %s" % row.pat)
        patName = row.patName
        if patName=="":
            patName = row.pat
        patFull = row.pat.format(**replDict)
        logging.log(5, "full pattern is %s" % patFull)
        flags = 0
        if "Long}" in row.pat:
            flags = re.IGNORECASE
            logging.log(5, "ignoring case for this pattern")
        patComp = re.compile(patFull, flags=flags)
        regexList.append((row.seqType, row.mutType, patName, patComp))
        counts[(row.seqType, row.mutType)] += 1

    for regexType, count in counts.iteritems():
            logging.info("regexType %s, found %d regexes" % (str(regexType), count))
    return regexList

def parseMatchRsId(match, patName):
    """ given a regular expression match object, 
    return special mutation object for rsIds
    that includes the chromosome coordinates """
    groups = match.groupdict()
    mutType = "dbSnp"
    rsId = groups["rsId"]
    chrom, start, end = geneData.rsIdToGenome(rsId)
    if chrom==None:
        return None
    var = MappedVariant("dbSnp", "dbSnp", start, end, chrom, "rs"+rsId)
    return var

def isBlacklisted(let1, pos, let2):
    " check if a string like T47D is blacklisted, like cell names or common chemical symbols "
    if (let1, pos, let2) in blackList:
        logging.debug("Variant %s,%d,%s is blacklisted" % (let1, pos, let2))
        return True
    if let1=="H" and pos<80 and let2 in "ACDE":
        logging.debug("Variant %s,%d,%s looks like chemical symbol" % (let1, pos, let2))
        return True
    if let1=="C" and pos<80 and let2 in "H":
        logging.debug("Variant %s,%d,%s looks like chemical symbol" % (let1, pos, let2))
        return True
    return False

def parseMatchSub(match, patName):
    " given a regular expression match object, return mutation and mention objects "
    groups = match.groupdict()
    # grab long and short versions of amino acid
    if "origAaShort" in groups:
        origSeq = groups["origAaShort"]
        seqType = "prot"
    if "origAaLong" in groups:
        origSeq = threeToOneLower[groups["origAaLong"].lower()]
        seqType = "prot"

    if "mutAaShort" in groups:
        mutSeq = groups["mutAaShort"]
        seqType = "prot"
    if "mutAaLong" in groups:
        mutSeq = threeToOneLower[groups["mutAaLong"].lower()]
        seqType = "prot"

    if "origDna" in groups:
        origSeq = groups["origDna"]
        seqType = "dna"
    if "mutDna" in groups:
        mutSeq = groups["mutDna"]
        seqType = "dna"


    mutSeq = mutSeq.upper()
    origSeq = origSeq.upper()

    if "fromPos" in groups:
        pos = int(groups["fromPos"])
        protStart = pos-1

    if "toPos" in groups:
        protEnd = int(groups["toPos"])-1
    else:
        pos = int(groups["pos"])
        if isBlacklisted(origSeq, pos, mutSeq):
            return None
        protStart = pos-1
        protEnd = pos

    var = MappedVariant("sub", seqType, protStart, protEnd, origSeq, mutSeq)
    return var

def isOverlapping(match, exclPos):
    posSet = set(range(match.start(), match.end()))
    if len(posSet.intersection(exclPos))!=0:
        logging.debug("regex overlaps an excluded position (gene?)")
        return True
    return False

def findVariantMentions(text, exclPos=set()):
    """ put mutation mentions from document together into dicts indexed by normal form 
        return dict of "prot"|"dna"|"dbSnp" -> list of (MappedVariant, list of Mention)
        uses global variable "regexes", see loadDb()
    """
    exclPos = set(exclPos)
    varMentions = defaultdict(list)
    varDescObj = {}
    for seqType, mutType, patName, pat in regexes:
        for match in pat.finditer(text):
            logging.debug("Match: Pattern %s, text %s" % (patName, match.groups()))
            if isOverlapping(match, exclPos):
                continue
            if mutType=="sub":
                variant = parseMatchSub(match, patName)
            elif mutType=="dbSnp":
                variant = parseMatchRsId(match, patName)
            else:
                continue
            if variant==None:
                continue

            mention = makeMention(match, patName)
            varDescObj[variant.getName()] = variant
            varMentions[variant.getName()].append(mention)
            debugSnip = pubAlg.getSnippet(text, mention.start, mention.end, maxContext=60)
            logging.debug("Found Variant: %s, snippet %s" % (str(variant), debugSnip))

    # convert to dict of "prot"|"dna"|"dbSnp" -> list (variant, mentions)
    variants = defaultdict(list)
    for varName, mentions in varMentions.iteritems():
        variant = varDescObj[varName]
        variants[variant.seqType].append((variant, mentions))
    return variants
    
def makeHgvsStr(seqType, seqId, origSeq, pos, mutSeq):
    if seqType=="prot":
        desc = "%s:p.%s%d%s" % (seqId, oneToThree[origSeq], pos+1, oneToThree[mutSeq])
    elif seqType=="cds":
        desc = "%s:c.%d%s>%s" % (seqId, pos+1, origSeq, mutSeq)
    elif seqType=="rna":
        desc = "%s:r.%d%s>%s" % (seqId, pos+1, origSeq, mutSeq)
    return desc
  
def firstDiffNucl(str1, str2):
    """Return pos and letters where strings differ. Returns None if more than one diff char"""
    assert(len(str1)==len(str2))
    if str1==str2:
        return None
    diffs = 0
    i = 0
    for ch1, ch2 in zip(str1, str2):
        if ch1 != ch2:
            diffs += 1
            diffCh1 = ch1
            diffCh2 = ch2
            diffPos = i
        i+=1
    if diffs>1 or diffs==0:
        return None
    else:
        return (diffPos, diffCh1, diffCh2)

def possibleDnaChanges(origAa, mutAa, origDna):
    """ figure out which nucleotides were possibly mutated by an amino acid change 
    will only look for single-bp mutations
    returns list of: position of nucleic acid, original and new basepair
    >>> possibleDnaChanges("V", "V", "GTA")
    [(2, 'A', 'T'), (2, 'A', 'C'), (2, 'A', 'G')]
    >>> possibleDnaChanges("V", "I", "GTA")
    [(0, 'G', 'A')]
    >>> possibleDnaChanges("G", "G", "GGC")
    [(2, 'C', 'T'), (2, 'C', 'G'), (2, 'C', 'A')]
    """
    origDna = origDna.upper()
    ret = set()
    mutDnas = backTrans(mutAa)
    logging.debug("Looking for possible DNA change. Aa change %s -> %s, original dna %s" % (origAa, mutAa, origDna))
    for mutDna in mutDnas:
        diffTuple = firstDiffNucl(origDna, mutDna)
        if diffTuple!=None:
            ret.add( diffTuple )
            logging.debug("found possible mutated DNA: %s" % (mutDna))
    if len(ret)==0:
        logging.debug("No possible DNA change found.")
        
    return list(ret)

def newToOldRefseqs(accs):
    """ given a list of new accessions return all previous versions
    >>> newToOldRefseqs(["NM_000325.5"])
    ['NM_000325.1', 'NM_000325.2', 'NM_000325.3', 'NM_000325.4']
    """ 
    oldAccs = []
    for newAcc in accs:
        prefix,suffix = newAcc.split(".")
        version = int(suffix)-1 
        if version!=0:
            oldVersions = range(0, version)
            oldVersions = [ov+1 for ov in oldVersions]
            for oldVersion in range(0, version):
                oldVersion = oldVersion+1 
                oldAcc = prefix+"."+str(oldVersion)
                oldAccs.append(oldAcc)
    return oldAccs

def backTrans(aa):
    """ back translate protein to all nucleotide strings 
    Returns the back-translated nucleotide sequences for a protein and codon 
    table combination.
    copied from http://www.biostars.org/p/3129/
    >>> protein = 'FVC'
    >>> len(backTrans(protein))
    16
    >>> backTrans('CD')
    ['TGTGAT', 'TGCGAT', 'TGTGAC', 'TGCGAC']
    """
    # create initial sequences == list of codons for the first amino acid
    sequences = [codon for codon in aaToDna[aa[0]]]
    for amino_acid in aa[1:]:
        # add each codon to each existing sequence replacing sequences
        # leaves (num_codons * num_sequences) for next amino acid 
        to_extend = sequences
        sequences = []
        for codon in aaToDna[amino_acid]:
            for sequence in to_extend:
                sequence += codon
                sequences.append(sequence)
    return sequences

def translate(dna):
    " return the aa translation of a dna seq "
    aaSeq = []
    for i in range(0, len(dna), 3):
        codon = dna[i:i+3].upper()
        aa = dnaToAa[codon]
        aaSeq.append(aa)
    return "".join(aaSeq)

def dnaAtCodingPos(refseqId, start, end, expectAa):
    """ 
    get nucleotide at CODING position in refseqId, check against expected aa
    also return positions on cdna
    """
    logging.debug("Paranoia check: making sure that codons from %d-%d in %s correspond to %s" % 
        (start, end, refseqId, expectAa))
    cdsStart    = geneData.getCdsStart(str(refseqId))
    nuclStart   = cdsStart + (3*start)
    nuclEnd     = nuclStart + 3*(end-start)
    cdnaSeq     = geneData.getSeq(refseqId)
    if cdnaSeq==None:
        logging.warn("Could not find seq %s (update diff between UCSC/NCBI maps?)" % refseqId)
        return None, None, None
    nuclSeq     = cdnaSeq[nuclStart:nuclEnd]
    foundAa     = translate(nuclSeq)
    logging.debug("CDS start is %d, nucl pos is %d, codon is %s" % (cdsStart, nuclStart, nuclSeq))
    if not doShuffle:
        assert(foundAa==expectAa)
    return nuclSeq, nuclStart, nuclEnd


def mapToCodingAndRna(protVars):
    """ given ref protein positions and refseq proteinIds, try to figure out the nucleotide 
    changes on the refseq cdna sequence and add these to the variant object
    """
    codVars = []
    rnaVars  = []
    for protVar in protVars:
        transId     = geneData.getRefSeqId(protVar.seqId)
        if transId==None:
            logging.error("could not resolve refprot to refseq. This is due to a difference between"
                    "UniProt and Refseq updates. Skipping this protein.")
            continue
        pos         = protVar.start
        origDnaSeq, cdnaStart, cdnaEnd  = dnaAtCodingPos(transId, pos, \
            pos+len(protVar.origSeq), protVar.origSeq)
        if origDnaSeq==None:
            return None, None
        possChanges = possibleDnaChanges(protVar.origSeq, protVar.mutSeq, origDnaSeq)
        for relPos, oldNucl, newNucl in possChanges:
            cdStart = 3 * protVar.start + relPos
            cdEnd   = cdStart+len(origDnaSeq)
            codVar = MappedVariant(protVar.mutType, "cds", cdStart, cdEnd, oldNucl, newNucl, transId)
            codVars.append(codVar)

            cdnaNuclStart = cdnaStart + relPos
            cdnaNuclEnd   = cdnaNuclStart + len(newNucl)
            rnaVar = MappedVariant(protVar.mutType, "rna", cdnaNuclStart, cdnaNuclEnd, \
                oldNucl, newNucl, transId)
            rnaVars.append(rnaVar)
            #hgvsStr = makeHgvsStr("c", transId, oldNucl, nuclPos, newNucl)
            #ret.append(hgvsStr)
    #return "|".join(ret)
    return codVars, rnaVars

def bedToRsIds(beds):
    " return a comma-sep string of rsIds given bed (12-tuple) features "
    ret = []
    for bed in beds:
        chromCoord = bed[0], bed[1], bed[2]
        snpId = geneData.lookupDbSnp(*chromCoord)
        if snpId==None:
            logging.debug("Chromosome location %s does not map to any dbSNP" % str(chromCoord))
            ret.append("na")
        else:
            logging.debug("Chromosome location %s corresponds to dbSNP %s " % (chromCoord, snpId))
            ret.append(snpId)
    return ret

def mentionsFields(mentions, text):
    " convert the mention objects to something that fits into a tab-sep file "
    mutStarts = []
    mutEnds = []
    snippets = []
    patNames = []
    texts = []
    for m in mentions:
        mutStarts.append(str(m.start))
        mutEnds.append(str(m.end))
        snippets.append(pubAlg.getSnippet(text, m.start, m.end).replace("|"," "))
        patNames.append(m.patName)
        texts.append(text[m.start:m.end].strip("() -;,."))
    return mutStarts, mutEnds, patNames, snippets, texts

def unmappedRsVarsToFakeVariants(varMentionsList, mappedRsIds, text):
    muts = []
    for dbSnpId, mentions in varMentionsList:
        if not dbSnpId in mappedRsIds:
            muts.append(SeqVariantData(seqType="dbSnp", mentions=mentions, text=text, patType="dbSnp"))
    return muts

def ungroundedMutToFakeSeqVariant(variant, mentions, text):
    """ convert mutations that could not be grounded to "fake" variants
        that are not located on any sequence but are easy to write to a file
    """
    #muts = []
    #for mut, mentions in ungroundedMuts:
    #muts.append(SeqVariantData(seqType=mut.seqType, mentions=mentions, text=text))
    var = SeqVariantData(seqType=variant.seqType, mentions=mentions, text=text)
    return var

def isSeqCorrect(protId, variant):
    " check if wild type sequence in protein corresponds to mutation positions "
    protStart = variant.start # uniprot is 1-based, we are 0-based
    protEnd   = variant.end
    seq = geneData.getSeq(protId)
    if seq==None:
        # uniprot sometimes uses other species as support
        logging.debug("sequence %s is not human or not available" % protId)
        return False

    if not protEnd<=len(seq):
        logging.debug("sequence %s is too short" % protId)
        return False
    
    if doShuffle:
        s = list(seq)
        random.shuffle(s)
        seq = "".join(s)

    aaSeq = seq[protStart:protEnd]
    if aaSeq==variant.origSeq:
        logging.debug("Seq match: Found %s at pos %d-%d in seq %s" % \
            (aaSeq, protStart, protEnd, protId))
        return True
    else:
        logging.debug("No seq match: Need %s, but found %s at pos %d-%d in seq %s" % \
            (variant.origSeq, aaSeq, protStart, protEnd, protId))
        return False

def hasSeqAtPos(protIds, variant):
    " check a list of protein IDs return those with a wild-type sequence at a position "
    if protIds==None:
        return []
    # try all protein seqs
    foundProtIds = []
    for protId in protIds:
        if isSeqCorrect(protId, variant):
            foundProtIds.append(protId)
    return foundProtIds

def tryProteinDbs(variant, entrezGene):
    " try various protein databases if they have matches for the wildtype aa at the right position "
    logging.debug("Trying entrez gene %s" % entrezGene)
    #for db in ["refseq", "oldRefseq", "uniprot"]:
    #for db in ["refseq", "uniprot", "genbank"]:
    #for db in ["refseq", "oldRefseq", "uniprot", "genbank"]:
    #for db in ["uniprot"]:
    for db in ["refseq"]:
        protIds = geneData.entrezToOtherDb(entrezGene, db)
        if len(protIds)==0:
            continue
        foundProtIds = hasSeqAtPos(protIds, variant)
        if len(foundProtIds)!=0:
            return db, foundProtIds
    return None, []

def rewriteToRefProt(variant, protIds):
    " create new MappedVariants, one for each protein Id "
    varList = []
    for protId in protIds:
        varNew = copy.copy(variant)
        varNew.seqId = protId
        varList.append(varNew)
    return varList

def mapToRefProt(db, variant, protIds):
    " map protein position from some DB to refseq proteins"
    logging.debug("Mapping original variant to refprot: %s" % str(variant))
    mappedVars = []
    newVar = None
    for protId in protIds:
        psls = geneData.getProteinPsls(db, protId)
        for psl in psls:
            newVar = pslMapVariant(variant, psl)
            if newVar==None:
                logging.warn("Cannot map a variant to refprot")
                continue
            # some variants are in uniprot but cannot be mapped to refseq at all
            if not hasSeqAtPos([newVar.seqId], newVar):
                logging.warn("variant %s is unique to db %s" % (newVar, db))
                continue
            mappedVars.append(newVar)
    logging.debug("Mapped to refprot variants: %s" % str(newVar))
    return mappedVars
        
def pslMapVariant(variant, psl):
    " map variant through psl on target, given query position, and create a new variant "
    maker = pslMapBed.PslMapBedMaker()
    maker.mapQuery(psl, variant.start, variant.end)
    bed = maker.getBed()
    if bed==None:
        return None

    varNew = copy.deepcopy(variant)
    varNew.seqId = bed[0]
    varNew.start = int(bed[1])
    varNew.end = int(bed[2])
    return varNew
    
def mapToGenome(rnaVars, protVars, bedName):
    " map to genome from refseq, remove duplicate results, return as 12-tuple (=BED) "
    maker = pslMapBed.PslMapBedMaker()
    beds = []
    for rnaVar in rnaVars:
        logging.debug("Mapping rnaVar %s:%d-%d to genome" % (rnaVar.seqId, rnaVar.start, rnaVar.end))
        maker.clear()
        # get psl
        pslList = geneData.getRefseqPsls(rnaVar.seqId)
        if len(pslList)>1:
            logging.warn("refSeq %s maps to multiple places, using only first one" % rnaVar.seqId)
        if len(pslList)==0:
            logging.warn("No mapping for %s, skipping variant" % rnaVar.seqId)
            continue
        mapPsl = pslList[0]

        # map rna var through psl
        start = rnaVar.start
        end = rnaVar.end
        maker.mapQuery(mapPsl, start, end)
        bed = maker.getBed(name=bedName)
        if bed==None:
            logging.debug("found mapping psl but nothing was mapped")
            continue
        bed.append("%s:%d" % (rnaVar.seqId, start))

        protVarDescs = ["%s:%d" % (p.seqId, p.start) for p in protVars]
        bed.append(",".join(protVarDescs))

        logging.debug("Got bed: %s" % str(bed))
        beds.append(bed)
    return beds
    
def getSnpMentions(mappedRsIds, varList):
    """ find all variants + their mentions with any of the mapped rsIds 
        returns dict rsId -> list of mentions
    """
    if len(varList)==0:
        return {}

    mappedRsIds = set(mappedRsIds)
    if "na" in mappedRsIds:
        mappedRsIds.remove("na")
    if len(mappedRsIds)==0:
        return {}

    res = defaultdict(list)
    for var, mentions in varList:
        rsId = var.origSeq
        if rsId in mappedRsIds:
            res[rsId].extend(mentions)
    return res

def groundVariant(docId, text, variant, mentions, snpMentions, genes):
    """ ground mutations onto genes and return results """
    groundedMuts = []
    ungroundedMuts = []
    mappedRsIds = []

    allBeds = []
    logging.debug("Grounding mutation %s onto genes %s" % (variant, genes))
    groundSuccess=False
    # try all entrez genes in article
    for entrezGene in genes:
        geneSym = geneData.entrezToSym(entrezGene)
        if not geneSym:
            logging.warn("No symbol for entrez gene %s. Skipping gene." % str(entrezGene))
            continue
        db, protIds = tryProteinDbs(variant, entrezGene)
        if len(protIds)!=0:
            # we found a sequence hit
            if db=="refseq":
                protVars = rewriteToRefProt(variant, protIds)
                comment  = ""
            # if needed, map to refseq from uniprot or genbank
            else:
                protVars = mapToRefProt(db, variant, protIds)
                comment  = "mapped via %s, IDs: %s" % (db, ",".join(protIds))
                if protVars==None:
                    logging.warn("found seqs, but no PSLs for %s" % protIds)
                    continue

            # map variants to coding and rna sequence coordinates
            varId              = str(docId)
            codVars, rnaVars   = mapToCodingAndRna(protVars)
            if codVars==None:
                continue
            beds               = mapToGenome(rnaVars, protVars, varId)
            # add all relevant dbSnp IDs from the document to this variant 
            varRsIds           = bedToRsIds(beds)
            #mentionedDbSnpVars = getSnpMentions(varRsIds, mutations["dbSnp"])
            mentionedDbSnpVars = getSnpMentions(varRsIds, snpMentions)
            mappedRsIds.extend(mentionedDbSnpVars.keys())

            groundedVar = SeqVariantData(varId, protVars, codVars, rnaVars, comment, beds, \
                entrezGene, geneSym, varRsIds, mentionedDbSnpVars, mentions, text)
            #isInDb = dbAnnots.addCheckVariant(groundedVar)
            groundedMuts.append(groundedVar)
            allBeds.extend(beds)
            groundSuccess=True

    #if not groundSuccess:
        #ungroundedMuts.append((variant,  mentions))

    #if not groundSuccess:
        #ungroundVar = ungroundedMutToFakeSeqVariant(variant, mentions, text)
    ungroundVar = None
    if not groundSuccess:
        ungroundVar = SeqVariantData(seqType=variant.seqType, mentions=mentions, text=text)

    unmappedRsVars = unmappedRsVarsToFakeVariants(snpMentions, mappedRsIds, text)
    #ungroundVarData.extend(unmappedRsVars)
    return groundedMuts, ungroundVar, allBeds

