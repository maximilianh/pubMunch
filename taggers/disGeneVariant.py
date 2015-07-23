import pubNlp, geneFinder, unidecode
import string, re, logging

# a special pubtools library
import varFinder

headers = ["start", "end", "section", "drugs", "diseases", "genes", "variants", "sentence"]

aaList =  '[CISQMNPKDTFAGHLRWVEYX]'
mutRe = re.compile("(^|[ .,;(-])([CISQMNPKDTFAGHLRWVEYX])([0-9]+)([CISQMNPKDTFAGHLRWVEYX])([ ;,.)-]|$)")

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

# easier as strings here
blackListStr = set(["%s%d%s" % (aa1,pos,aa2) for aa1, pos,aa2 in blackList])

def rangeIntersection(start1, end1, start2, end2):
    """ return amount that two ranges intersect, <0 if no intersection 
    >>> rangeIntersection(1,10, 9, 20)
    1
    """
    s = max(start1,start2);
    e = min(end1,end2);
    return e-s;

def rangeAnyOverlap(start, end, coords):
    """ returns true if start,end overlaps any item in coords. Coords is a list of
    (start, end, ...) tuples.
    >>> rangeAnyOverlap(1, 10, [(20,30), (9, 13)])
    True
    >>> rangeAnyOverlap(1, 10, [(20,30)])
    False
    """
    for el in coords:
        start1, end1 = el[:2]
        if rangeIntersection(start, end, start1, end1) > 0:
            return True
    return False

def rangeRemoveOverlaps(list1, list2):
    """ given tuples that have (start, end) as their (0,1) elements, 
    remove from list1 all that overlap any in list2 and return a new filtered 
    list1. Does not assume sorted lists.  Careful: Stupid brute-force. quadratic runtime.
    >>> rangeRemoveOverlaps( [(1,10), (10,20)], [])
    [(1, 10), (10, 20)]
    >>> rangeRemoveOverlaps( [(1,10), (10,20)], [(15,16)])
    [(1, 10)]
    """
    if len(list2)==0:
        return list1

    newList2 = []
    for el1 in list1:
        start1, end1 = el1[:2]
        if not rangeAnyOverlap(start1, end1, list2):
            newList2.append(el1)
    return newList2
            
def rangeTexts(text, rangeList, useSym=False):
    """ given a list of (start, end, ...) tuples, return a list of text substrings
    Optionally resolves entrez IDs to symbols
    >>> rangeTexts("Hallo World!", [(0,5), (6,13)])
    ['Hallo', 'World!']
    """
    textList = []
    for el in rangeList:
        start, end = el[:2]
        if useSym:
            entrezId = el[-1]
            sym = geneFinder.entrezToDispSymbol(entrezId)
            snip = sym
        else:
            snip = text[start:end]
        textList.append(snip)
    return textList
            
# translation table to remove some spec characters for desc string
descTbl = string.maketrans('-|:', '   ')

def rangeDescs(text, rangeList, useSym=False):
    """
    given a list of (start, end, identifier) tuples, return a string "start-end:text=identifier|..."
    >>> rangeDescs("Hallo World!", [(0,5, "word1"), (6,13, "word2")])
    '0-5:Hallo=word1|6-13:World!=word2'
    """
    descs = []
    snips = rangeTexts(text, rangeList, useSym=useSym)
    for snip, rangeTuple in zip(snips, rangeList):
        start, end, ident = rangeTuple[:3]
        snip = unidecode.unidecode(snip)
        ident = unidecode.unidecode(ident)
        descs.append("%s-%s:%s=%s" % (str(start), str(end), snip, ident))
    return "|".join(descs)

def startup(paramDict):
    geneFinder.initData(exclMarkerTypes=["dnaSeq", "band"])
    #varFinder.loadDb(loadSequences=False)
    varFinder.loadDb()
    
def findDisGeneVariant(text):
    """
    >>> geneFinder.initData(exclMarkerTypes=["dnaSeq", "band"])
    >>> varFinder.loadDb(loadSequences=False)
    >>> list(findDisGeneVariant("Diabetes is caused by a PITX2 mutation, V234T and influenced by Herceptin."))
    [(0, 74, 'probablyAbstract', '64-73:Herceptin=Trastuzumab', '0-8:Diabetes=Diabetes Mellitus', '24-29:PITX2=symbol', 'V233T', 'Diabetes is caused by a PITX2 mutation, V234T and influenced by Herceptin.')]
    >>> #list(findDisGeneVariant("We undertook a quantitative review of the literature to estimate the effectiveness of desferrioxamine and deferiprone in decreasing hepatic iron concentrations (HIC) in thalassemia major."))
    >>> list(findDisGeneVariant("his mutation, we cotransfected C3H10T cells with expression vectors encoding SMO-WT or SMO-D473H "))
    """
    docGenes = list(geneFinder.findGeneNames(text))
    docEntrezIds = set([r[-1] for r in docGenes])

    for section, start, end, sentence in pubNlp.sectionSentences(text):
        conds = list(pubNlp.findDiseases(sentence))
        drugs = list(pubNlp.findDrugs(sentence))
        genes = list(geneFinder.findGeneNames(sentence))
        #print conds, drugs, genes, section, sentence
        # remove drugs and conds that are also genes
        drugs = rangeRemoveOverlaps(drugs, genes)
        conds = rangeRemoveOverlaps(conds, genes)

        #geneSnips = rangeTexts(sentence, genes, useSym=True)
        #condSnips = rangeTexts(sentence, conds)
        #drugSnips = rangeTexts(sentence, drugs)
#
        mutDescs = []
        mutDict = varFinder.findVariantDescriptions(sentence)
        if "prot" in mutDict:
            for varDesc, mentions in mutDict["prot"]:
                if varDesc.mutType!="sub":
                    continue
                logging.debug("grounding variant: %s %s"% (varDesc, mentions))
                groundedMuts, ungroundVar, beds = \
                    varFinder.groundVariant(None, sentence, varDesc, mentions, [], docEntrezIds)

                for mutInfo in groundedMuts:
                    shortDesc = varDesc.origSeq+str(varDesc.start+1)+varDesc.mutSeq # 0-based!!
                    mutDescs.append(shortDesc+"=%s:%s"%(mutInfo.geneSymbol,mutInfo.hgvsProt))
            
        #mutMatches =  list(mutRe.finditer(sentence))
        #mutDescs = [(m.group(1),m.group(2), m.group(3)) for m in mutMatches]
        #mutDescSet = set(mutDescs)
        #blackListMuts = mutDescSet.intersection(blackListStr)
        #if len(mutMatches)==0:
            #logging.debug("No mutation found, skipping")
            #continue
        #if len(blackListMuts)!=0:
            #logging.debug("At least one blacklisted mutation found, skipping")
            #continue
        #if len(drugs)==0:
            #logging.debug("No drugs found, skipping")
            #continue
        #if len(genes)==0:
            #logging.debug("No genes found, skipping")
            #continue
    
        mutDesc = "|".join(mutDescs)
        drugDesc = rangeDescs(sentence, drugs)
        condDesc = rangeDescs(sentence, conds)
        geneDesc = rangeDescs(sentence, genes, useSym=True)

        ret = (start, end, section, drugDesc, condDesc, geneDesc, mutDesc, sentence)
        yield ret

def annotateFile(article, file):
    if file.fileType == "supp":
        return
    text = file.content
    for row in findDisGeneVariant(text):
        yield row
        
if __name__ == "__main__":
    import doctest
    doctest.testmod()
