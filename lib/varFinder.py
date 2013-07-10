# find descriptions of variants in text

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
geneData = None

class SeqData(object):
    """ functions to get sequences and map between identifiers for entrez genes,
    uniprot, refseq, etc """

    def __init__(self, mutDataDir, taxId):
        " open db files, compile patterns, parse input as far as possible "
        if mutDataDir==None:
            return
        self.mutDataDir = mutDataDir
        # load uniprot data into mem
        logging.info("Reading data")
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
        fname = join(mutDataDir, "pmid2entrez.gdbm")
        logging.info("opening %s" % fname)
        pmid2entrez = gdbm.open(fname, "r")
        self.pmid2entrez = pmid2entrez
        
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
            #if not "," in row.cdsStart:
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
        logging.verbose("Looking up sequence for id %s" % seqId)
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

        rsIdPack = struct.pack("<l", int(rsId))
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

    def getEntrezGenes(self, pmid):
        if not pmid in self.pmid2entrez:
            return None
        dbRes = self.pmid2entrez[pmid]
        entrezGenes = dbRes.split(",")
        entrezGenes = [int(x) for x in entrezGenes]
        return entrezGenes

    def getRefSeqId(self, refProtId):
        return self.refProtToRefSeq[refProtId]

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
            as Gb doesn't support version numbers, we're stripping those on input
        """
        psls = getPsls(refseqId, self.refGenePslCache, self.refGenePsls, stripVersion=True)
        return psls

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
            row.append(str(getattr(self, i)))
        return row
        
    def copyNoLocs(self):
        #" return copy of myself without any seqIdLocs "
        #newObj = MappedVariant(self.mutType, self.seqType, self.start, self.end, self.origSeq, self.mutSeq)
        #return newObj
        pass

    def __repr__(self):
        return ",".join(self.asRow())
    
