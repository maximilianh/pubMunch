#!/usr/bin/env python2.7
from __future__ import print_function
import sys
sys.path.append("../lib/")
import maxCommon, pubConf
from os.path import join

urls = {"var": "http://web.expasy.org/cgi-bin/variant_pages/get-sprot-variant.pl?",
        "uniProt" : "http://www.uniprot.org/uniprot/",
        "pubmed" : "http://www.ncbi.nlm.nih.gov/pubmed/"
        }

def htmlLink(urlType, acc):
    return '<a href="%s%s">%s</a>' % (urls[urlType], acc, acc)

if __name__ == '__main__':

    psls = indexPsls("uniProtVsGenome.psl")
    ofh = open("uniprotMutations.bed", "w")
    ofh2 = open("temp.bed", "w")
    uniProtMutFname = join(pubConf.dbRefDir, "uniprot.mut.tab")
    count = 0
    notMapped = []
    for mut in maxCommon.iterTsvRows(uniProtMutFname):
        mapper = PslMapBedMaker()
        if mut.acc not in psls:
            notMapped.append(mut.acc)
            continue
        mutPos = 3*(int(mut.position)-1)
        ofh2.write("\t".join([mut.acc, str(mutPos), str(mutPos+3), mut.acc+":"+mut.origAa+mut.position+mut.mutAa])+"\n")

        mapPsls = psls[mut.acc]
        for psl in mapPsls:
            bed = mapper.mapQuery(psl, mutPos, mutPos+3)
            if bed==None:
                print("Could not map: ", mut)
                continue
            bed[3] = " ".join((mut.disease.split("|")[0]).replace("-", " ").replace(" type", "").split()[:3])
            bed.append(mut.disease)
            bed.append("position %s: %s->%s" % \
                (mut.position, mut.origAa, mut.mutAa))
            bed.append(htmlLink('uniProt', mut.acc))
            bed.append(htmlLink('var', mut.varId))
            bed.append(htmlLink('pubmed', mut.pmid))
            bedLine = "\t".join(bed)+"\n"
            ofh.write(bedLine)
            count +=1
            mapper.clear()
            #print psl.tName
    print("%d features written to %s" % (count, ofh.name))
    print("%d sequences not mapped to genome" % len(notMapped))
