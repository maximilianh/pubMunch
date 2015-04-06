# coding: utf-8
#headers = ["start", "end", "mutation"]
headers = ["start", "end", "gene", "wtRes", "pos", "mutRes", "rsId", "text"]

import pubConf
import marshal, zlib, logging

# add the path of the jar file
from os.path import *
import sys
myDir = dirname(__file__)
jarName = join(myDir, "java", "seth.jar")
mutName = join(myDir, "java", "seth.mutations.txt")
propName = join(myDir, "java", "seth.property.xml")
sys.path.append(jarName)

# import the java classes
from java.util import Properties
from java.io import FileInputStream, File
from de.hu.berlin.wbi.objects import DatabaseConnection, dbSNP, UniprotFeature
from seth import SETH

print "init'ing seth"
# get some mutations from text
seth = SETH(mutName)

# setup db connection
print "setup mysql connection"
property = Properties()
property.loadFromXML(FileInputStream(File(propName)))
db = DatabaseConnection(property)
db.connect()
dbSNP.init(db, "PSM", "hgvs")
UniprotFeature.init(db, "uniprot")

def test():
    gene = 1312
    potentialSNPs = dbSNP.getSNP(gene);
    features = UniprotFeature.getFeatures(gene);
    mutations = seth.findMutations("p.A123T and Val158Met")
    for mut in mutations:
        start, end = mut.getStart(), mut.getEnd()
        wtRes, mutRes = mut.getWtResidue(), mut.getMutResidue()
        pos, text = mut.getPosition(), mut.getText()
        mut.normalizeSNP(potentialSNPs, features, False)
        normalized = mut.getNormalized()
        for snp in normalized:
            rsId = snp.getRsID()
            print ",".join([start, end, wtRes, mutRes, pos, "rs"+str(rsId), text])

# open a dbm with a pmid -> genes mapping
print "opening dbm connections"
mutDataDir = pubConf.getDataDir('mutations')
entrezFname = join(mutDataDir, "pmid2entrez.marshal.gz")
print "opening %s" % entrezFname
pmidToGenes = marshal.loads(zlib.decompress(open(entrezFname).read()))
print "seth ok"
# get some mutations from text

def annotateFile(art, file):
    # example
    pmid = art.pmid
    if art.pmid=="" or not int(pmid) in pmidToGenes:
        logging.debug("No pmid for article %s" % art.articleId)
        raise StopIteration
    genes = pmidToGenes[int(art.pmid)].split(",")
    genes = [int(x) for x in genes]
    mutations = seth.findMutations(file.content)

    for gene in genes:
        potentialSNPs = dbSNP.getSNP(int(gene))
        features = UniprotFeature.getFeatures(gene);

        for mut in mutations:
            print "mutation found: ", str(mut)
            start = mut.getStart()
            end = mut.getEnd()
            mut.normalizeSNP(potentialSNPs, features, False)
            normalized = mut.getNormalized()
            for snp in normalized:
                wtRes = mut.getWtResidue()
                mutRes = mut.getMutResidue()
                pos = mut.getPosition()
                text = mut.getText()
                rsId = snp.getRsID()
                yield [start, end, str(gene), wtRes, pos, mutRes, "rs"+str(rsId), text]
