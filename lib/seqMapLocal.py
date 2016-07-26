import shutil, logging, doctest
import maxCommon, maxTables, pubGeneric, pubConf, pubMap
from os.path import join
from collections import defaultdict

def splitSeqs(seqs, cutoff):
    " split sequences by length into short and long "
    shortSeqs = []
    longSeqs = []
    for seq in seqs:
        if len(seq) <= cutoff:
            shortSeqs.append(seq)
        else:
            longSeqs.append(seq)
    return shortSeqs, longSeqs

def rewriteCatPsls(fnames, db, outFname):
    ofh = open(outFname, "w")
    for fname in fnames:
        for line in open(fname):
            fields = line.split("\t")
            # field 14 must be: db, chrom, seqType (="g")
            fields[13] = ",".join([db, fields[13], "g"])
            ofh.write("\t".join(fields))
    ofh.close()
    logging.debug("Wrote %s" % outFname)
    #outFnames[db].append(outFname)

class BlatClient(object):
    def __init__(self, seqDir, dbList=["hg19"]):
        self.blatServers = {}
        self.dbList = dbList
        self.seqDir = seqDir
        self._getBlatServersFromHgCentral()

    def _getBlatServersFromHgCentral(self):
        config = maxCommon.parseConfig("~/.hg.conf")
        hgCentralDb = config["central.db"]
        conn = maxTables.hgSqlConnect(hgCentralDb, config=config)
        # | db      | host   | port  | isTrans | canPcr |
        # +---------+--------+-------+---------+--------+
        # | petMar2 | blat4b | 17839 |       0 |      1 | 
        dbList = ["'%s'" % db for db in self.dbList]
        dbStr  = ",".join(dbList)
        sql    = "SELECT db, host, port FROM blatServers WHERE db IN (%s) AND canPcr=1" % dbStr
        rows = maxTables.sqlGetRows(conn, sql)

        self.blatServers = {}
        for row in rows:
            host = row["host"]+".cse.ucsc.edu"
            self.blatServers[row["db"]] = (host, row["port"])
        logging.debug("Using blat servers: %s " % self.blatServers)

    def _writeSeqsToFile(self, seqIter):
        ofh, tmpName = pubGeneric.makeTempFile(prefix="findGenes", suffix=".fa")
        for seqId, seq in seqIter:
                ofh.write(">%s\n%s\n" % (seqId, seq))
        ofh.flush()
        return ofh, tmpName
        
    def blatSeqs(self, dbList, seqs, outDir):
        """ given a list of (seqId, seq) tuples, 
        writes seqs to temp fa files, runs blat, writes psl files, two per db, to outDir 
        returns dict db -> list of pslFname
        """
        shortSeqs, longSeqs = splitSeqs(seqs, pubConf.shortSeqCutoff)
        shortFh, shortFaName = self._writeSeqsToFile(shortSeqs)
        longFh, longFaName = self._writeSeqsToFile(longSeqs)
        logging.debug("got %d short seqs, %d long seqs" % (len(shortSeqs), len(longSeqs)))

        outFnames = {}
        longPslFh, shortPslFh = None, None # prevents auto-delete
        for db in dbList:
            pslFnames = []
            if len(shortSeqs)>0:
                shortPslFh, shortPslFname = self.blatFasta(db, shortFaName, \
                    ["-minScore=19", "-maxIntron=3"])
                pslFnames.append(shortPslFname)
            if len(longSeqs)>0:
                longPslFh, longPslFname = self.blatFasta(db, longFaName, ["-minScore=19"])
                pslFnames.append(longPslFname)

            outPslFname = join(outDir, db+".psl")
            rewriteCatPsls(pslFnames, db, outPslFname)
            outFnames[db] = outPslFname
        #pslFnames = defaultdict(list)
        #for db, fnames in shortSeqPsls:
            #pslFnames[db].extend(fnames)
        #for db, fnames in longSeqPsls:
            #pslFnames[db].extend(fnames)
        return outFnames

    def blatFasta(self, db, faFname, params=[]):
        """ blat fasta files against a db, create temporary write psl files
        returns a (file, filename) of temp file
        """
        seqDir = join(self.seqDir, db)
        outFnames  = defaultdict(list)
        logging.debug("Blatting %s against %s" % (faFname, seqDir))
        server, port = self.blatServers[db]
        tmpFh, tmpFname = pubGeneric.makeTempFile("blatOut.")
        cmd1 = ["gfClient", server, str(port), seqDir, faFname, "stdout", "-nohead"]
        cmd1.extend(params)
        cmd2 = ["sort", "-k10,10 "]
        cmd3 = ["pslCDnaFilter", "stdin", tmpFname,\
                "-globalNearBest=0", "-filterWeirdOverlapped", "-ignoreIntrons"]
        cmds = []
        cmds.append(" ".join(cmd1))
        cmds.append(" ".join(cmd2))
        cmds.append(" ".join(cmd3))
        cmd = "|".join(cmds)
        maxCommon.runCommand(cmd)
        return tmpFh, tmpFname

class DnaMapper():
    def __init__(self, blatClient=None):
        if blatClient==None:
            self.blatClient = BlatClient(pubConf.genomeDataDir, ["hg19"])
        else:
            self.blatClient = blatClient

    def mapDnaToBed(self, seqs, docId, dbList, outDir):
        """ seqs is a list of (seqId, seq)
        """
        pslDir = pubGeneric.makeTempDir(prefix="geneFinderPsls")
        # create tuples (seqId, seq)
        dbPslFnames = self.blatClient.blatSeqs(dbList, seqs, pslDir)
        for db, fname in dbPslFnames.items():
            oneBed = join(outDir, "chained.%s.bed" % db)
            pslFname = join(pslDir, db+".psl")
            dbBedNames = pubMap.chainPslToBed(pslFname, oneBed, pipeSep=True, onlyFields=12)

        if not pubConf.debug:
            shutil.rmtree(pslDir)

        return dbBedNames

    def mapDnaToGenes(self, seqs, docId, dbList):
        """
        returns a dict seq -> set of gene symbols

        >>> d = DnaMapper()

        # simple case
        >>> seqs = ["GCAAGCTCCCGGGAATTCAGCTC"]
        >>> d.mapDnaToGenes(seqs, "1234", ["hg19"])
        {'hg19': {'GCAAGCTCCCGGGAATTCAGCTC': set(['PITX2'])}}

        # harder case
        >>> seqs = ["ACTGGGAGAAGGGTGGTCAG", "TGTGTCCCTGAGCCAGTGAC"]
        >>> d.mapDnaToGenes(seqs, "1234", ["hg19"])
        {'hg19': {'ACTGGGAGAAGGGTGGTCAG': set(['CLN6']), 'TGTGTCCCTGAGCCAGTGAC': set(['CLN6'])}}
        """
        seqs = [(docId+"|"+str(i), seq) for i, seq in enumerate(seqs)]
        seqIdToSeq = dict(seqs)
        bedDir = pubGeneric.makeTempDir(prefix="geneFinderBeds")
        dbBedNames = self.mapDnaToBed(seqs, docId, dbList, bedDir)
        dbAnnotGenes = {}
        for db, bedName in dbBedNames.items():
            annotToGenes = pubMap.findLoci(bedName, dbList)
            seqIdToGenes = {}
            for annotId, genes in annotToGenes.items():
                seqId, seqRange = annotId.split(":")
                logging.debug("Found match for %s (%s) for genes %s" % (seqId, seqRange, genes))
                seq = seqIdToSeq[seqId]
                seqIdToGenes.setdefault(seq, set()).update(genes)
            dbAnnotGenes[db] = seqIdToGenes

        if not pubConf.debug:
            shutil.rmtree(bedDir)

        return dbAnnotGenes

if __name__=="__main__":
    logging.basicConfig(level=logging.DEBUG)
    #logging.basicConfig(level=5)
    import doctest
    doctest.testmod()
