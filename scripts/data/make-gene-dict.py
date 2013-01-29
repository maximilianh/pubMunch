import sys, re
sys.path.append("../../lib")
import maxCommon, pubConf, fastFind, logging
from os.path import join

# create dictionary for fastFind from uniprot accessions and names
# creates two entries for each record:
# - one with all accession numbers (refseq, embl, etc), prefixed with "*"
# - one with all other names (hugo, etc)

def appendAll(idList, inList, prefixList=[""]):
    " all all non-empty parts to list, optionally prefixed by prefixes "
    for part in inList:
        for prefix in prefixList:
            if part!="":
                idList.add(prefix+part)
    return idList

notGenes = set(["ok", "ko", "ii", "c0", "ct", "ms", "mri", "zip", "waf", "cip", "oct", "apr", "sep", "nov", "dec", "jan", "feb", "top", "fop", "flash", 'nhs', 'sds', 'vip', 'nsf', 'pdf', 'cd','rom','cad','cam','rh', 'hr','ct','h9', 'sms', "for", "age", "anova", "med", "soc", "tris", "eng", "proc", "appl", "acta", "dis", "engl", "exp", "rec", "nuc", "nsf", "comp", "prot", "ctrl", "dtd", "cit", "gov"])

# can be real genes, but probably aren't
problematicGenes = set([
"bam", # is a rare gene, but mostly restriction enzyme
"copd", # in almost all cases this is a disease, not a gene
"sch", #found only one match in scholar
"jun", # also a gene, but can also be a month
"ca2", # calcium
"neb", # a company
"mim", # =OMIM
"prism", # 
"CAS", # chemical abstracts
"mg2" # magnesium
])

aaCodes = set(["Ala", "Arg", "Asn", "Asp", "Cys", "Gln", "Glu", "Gly", "His", "Ile", "Leu", "Lys", "Met", "Phe", "Pro", "Ser", "Thr", "Trp", "Tyr", "Val", "Asx", "Glx"])

# common words, but they are really genes
areGenes = set(["p53", "atf", "bcr"])

nucl = set(['a', 'c', 'g', 't'])

ignoredWords = []

def debug(msg):
    debug = False
    if debug==True:
        print msg
    
def prepNames(names):
    " filter out names that contain only numbers "
    goodNames = []
    for n in names:
        debug("name: %s" % n)
        if len(re.findall("[0-9.,]", n))==len(n):
            debug("ignore %s" % n)
            continue
        goodNames.append(n)
    return goodNames

def prepSymbols(stringList, bncWords):
    """ - convert uppercase (PITX2) to mixed case (Pitx2), retain both versions 
        - remove all words that are in BNC or in notGenes
        - except those from areGenes
    """
    goodSyms = []
    for s in stringList:
        debug("symbol "+s)
        # ignore short symbols
        if len(s)<3:
            debug("ignored: too short")
            continue

        lowS = s.lower()
        # ignore symbols that are common english words
        if (lowS in bncWords or lowS in notGenes) and lowS not in areGenes:
            ignoredWords.append(s)
            debug("ignored: too common")
            continue

        # ignore symbols that contain only ACTG letters (e.g. CAT)
        if len(set(list(lowS)).difference(nucl))==0:
            ignoredWords.append(s)
            debug("ignored: looks like DNA")
            continue

        # ignore symbols that look like amino acids
        if s in aaCodes:
            ignoredWords.append(s)
            debug("ignored: looks like amino acid")
            continue

        # add dashed and spaced versions of symbols
        # eg for PITX2 add Pitx-2 and PITX-2
        m = re.match("^([A-Za-z]+)([0-9]+[A-Za-z]?)$", s)
        if m != None:
            name, num = m.groups()
            if len(name)==1 and len(num)<3:
                print name, num
                ignoredWords.append(s)
                debug("ignored: too few letters")
                continue
            mixName = name[0].upper()+"".join(name[1:]).lower()
            if mixName not in aaCodes:
                goodSyms.append(mixName+num)

            dashName = name+"-"+num
            dashMixName = mixName+"-"+num
            goodSyms.extend([dashName, dashMixName])

            if name.lower() not in bncWords and len(name)>1:
                spcName = name+" "+num
                spcMixName = mixName+" "+num
                goodSyms.extend([spcName, spcMixName])
            
        mixedS = s[0].upper() + "".join(s[1:]).lower()
        goodSyms.append(s)
        goodSyms.append(mixedS)

    debug("Accepted symbols and variants: %s" % ",".join(goodSyms))
    return goodSyms
    
def parseBnc():
    " parse bnc wordlist "
    s = set()
    for line in open("bnc.txt"):
        if line.startswith("#"):
            continue
        word = line.strip("\n")
        s.add(word)
        #s.add(word[:3])
        #s.add(word[:4])
    return s


uniprotFname = join(pubConf.dbRefDir, "uniprot.tab")
print "Reading %s" % uniprotFname

dictFh = open("uniProt.dict.tab", "w")

print ("parsing BNC")
bncWords = parseBnc()

print ("constructing dictionary")
for row in maxCommon.iterTsvRows(uniprotFname):
    if row.taxonId!="9606":
        continue

    accs = set()
    accs = appendAll(accs, row.accList.split("|"))
    accs = appendAll(accs, [x.split(".")[0] for x in row.refSeq.split("|")]) # remove version number
    accs = appendAll(accs, row.ensemblProt.split("|"))
    accs = appendAll(accs, row.ensemblGene.split("|"))
    accs = appendAll(accs, row.embl.split("|"))
    accs = appendAll(accs, row.pdb.split("|"))
    accs = appendAll(accs, row.uniGene.split("|"))
    accs = appendAll(accs, row.omim.split("|"), prefixList=["omim ", "OMIM ", "MIM "])
    accs = list(set(accs))
    for delChar in ["*", ",", ".", "/", "(", ")"]: 
        accs = [acc.replace(delChar," ").replace("  ", " ") for acc in accs]
    dictFh.write("\t".join( ("*"+row.acc, "|".join(accs)) )+"\n")

    names = set()
    names = appendAll(names,prepNames(row.protFullNames.split("|")))
    names = appendAll(names,prepNames(row.protShortNames.split("|")))
    names = appendAll(names,prepNames(row.protAltNames.split("|")))

    names = appendAll(names,prepSymbols(row.hugo.split("|"), bncWords))
    names = appendAll(names,prepSymbols(row.geneName.split("|"), bncWords))
    names = appendAll(names,prepSymbols(row.geneSynonyms.split("|"), bncWords))

    #names = appendAll(names,row.isoNames.split("|"))
    names = appendAll(names,row.geneOrdLocus.split("|"))
    names = appendAll(names,row.geneOrf.split("|"))
    # certain characters cannot be part of a word, replace them with a space
    for delChar in ["*", ",", ".", "/", "(", ")"]:
        names = [name.replace(delChar," ").replace("  ", " ") for name in names if len(name)>2]
    names = list(set(names))
    dictFh.write("\t".join( (row.acc, "|".join(names)) )+"\n")

print "Wrote to %s" % (dictFh.name)
#fastFind.compileDict(dictFh.name, toLower=True)
print "Compiling dict to gzipped marshal file"
fastFind.compileDict(dictFh.name)
ignoredWords = list(set(ignoredWords))
ignoredWords.sort()
print "Ignored these symbols:", ",".join(ignoredWords)
