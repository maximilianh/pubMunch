#!/usr/bin/env python2.7
import sys
sys.path.append("../lib/")
import maxCommon, pubConf
from os.path import join
from os import system
from collections import defaultdict

urls = {"var": "http://web.expasy.org/cgi-bin/variant_pages/get-sprot-variant.pl?",
        "uniProt" : "http://www.uniprot.org/uniprot/",
        "pubmed" : "http://www.ncbi.nlm.nih.gov/pubmed/"
        }

def htmlLink(urlType, accs):
    strList = []
    for acc in accs:
        strList.append('<a href="%s%s">%s</a>' % (urls[urlType], acc, acc))
    return ", ".join(strList)

def run(cmd):
    ret = system(cmd)
    if ret!=0:
        print "Could not run %s" % cmd
        sys.exit(1)

if __name__ == '__main__':
    mutData = defaultdict(list)

    taxonId = "9606"
    db = "hg19"

    cmd = "rm -rf work; mkdir work"
    run(cmd)

    # create chrom sizes
    uniprotFa = join(pubConf.dbRefDir, "uniprot.%s.fa" % taxonId)
    cmd = "faSize %s -detailed | gawk '{$2=$2*3; print}'> work/chromSizes" % uniprotFa
    run(cmd)
    # get uniprot IDs for this species
    speciesAccs = set([line.split()[0] for line in open("work/chromSizes")])

    # read data, write bed to file
    ofh = open("work/temp.bed", "w")
    uniProtMutFname = join(pubConf.dbRefDir, "uniprot.mut.tab")
    for mut in maxCommon.iterTsvRows(uniProtMutFname):
        if mut.acc not in speciesAccs:
            continue
        mutName = mut.acc+":"+mut.origAa+mut.position+mut.mutAa
        mutPos = 3*(int(mut.position)-1)
        if mutName not in mutData:
            ofh.write("\t".join([mut.acc, str(mutPos), str(mutPos+3), mutName])+"\n")
        mutData[mutName].append(mut)
    ofh.close()
    
    # lift
    cmd = "bedToPsl work/chromSizes work/temp.bed stdout | pslMap stdin uniProtVsGenome.psl stdout | pslToBed stdin stdout | sort -k1,1 -k2,2n > work/temp.lifted.bed" 
    run(cmd)

    # read lifted bed and add other data again
    ofh = open("uniprotMutations.%s.bed" % db, "w")
    count = 0
    for line in open("work/temp.lifted.bed"):
        bed = line.strip().split()
        muts = mutData[bed[3]]
        varIds = set([mut.varId for mut in muts])
        pmids = set([mut.pmid for mut in muts])
        diseases = list(set([mut.disease for mut in muts]))
        acc = muts[0].acc

        # create shorter disease name
        firstDis = diseases[0].split("|")[0].replace("-", " ").replace(" type", "")
        disWords = firstDis.split()
        if disWords[2]=="of":
            disWords = disWords[:4]
        else:
            disWords = disWords[:3]
        shortDisName = " ".join(disWords)

        bed[3] = shortDisName
        bed.append(",".join(diseases))
        bed.append("position %s, amino acid %s changed to %s" % \
            (mut.position, mut.origAa, mut.mutAa))
        bed.append(htmlLink('var', varIds))
        bed.append(htmlLink('uniProt', [acc]))
        bed.append(htmlLink('pubmed', pmids))
        bed[5] = "."
        bedLine = "\t".join(bed)+"\n"
        ofh.write(bedLine)
        count += 1

    print "%d features written to %s" % (count, ofh.name)
    ofh.close()

    #print "%d sequences not mapped to genome" % len(notMapped)
    cmd = "bedToBigBed -as=bed12UniProtMut.as uniprotMutations.%s.bed /scratch/data/%s/chrom.sizes uniprotMutations.%s.bb -type=bed12+ -tab" % (db, db, db)
    run(cmd)

    cmd = "rm -rf work"
    run(cmd)
