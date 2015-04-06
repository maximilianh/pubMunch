import geneFinder, varFinder
import pubNlp

onlyMain = True
preferPdf = True

#headers = list(varFinder.mutFields)
#headers.insert(0, "section")
headers = ["section", "genes", "diseases", "drugs", "variants", "sentence"]

def startup(paramDict):
    varFinder.loadDb()
    geneFinder.initData(exclMarkerTypes=["dnaSeq"])

def findGenes(pmid, text):
    """ return dict of entrezGene id -> mType -> (markerId, list of start, end)
    """
    genes, genePosSet = geneFinder.findGenes(text, pmid)
    return genes, genePosSet

def annotateFile(artData, fileData):
    pmid = artData.pmid
    text = fileData.content
    for r in findVarDisGeneDrug(pmid, text):
        yield r

def findVarDisGeneDrug(pmid, text):
    """
    >>> startup({})
    >>> list(findVarDisGeneDrug(0, "The R71G BRCA1 is a breast cancer founder mutation not treatable with Herceptin"))
    """
    textLow = text.lower()
    # very basic filter, remove documents without some basic keywords
    if " variant " not in textLow and " mutation" not in textLow and " substitution" not in textLow and \
            " mutant " not in textLow:
        return

    for section, sentStart, sentEnd, sentText in pubNlp.sectionSentences(text):
        genes = list(geneFinder.findGeneNames(sentText))
        if len(genes)==0:
            continue
        #print "genes", genes, sentText

        conds = list(pubNlp.findDiseases(sentText))
        drugs = list(pubNlp.findDrugs(sentText))
        # remove diseases and drugs that are also genes
        drugs = pubNlp.rangeRemoveOverlaps(drugs, genes)
        conds = pubNlp.rangeRemoveOverlaps(conds, genes)
        # check if we still have a disease and drug left
        if len(conds)==0 or len(drugs)==0:
            continue
        print "drugs", drugs
        print "diseases", conds

        geneSnips = pubNlp.rangeTexts(sentText, genes)
        condSnips = pubNlp.rangeTexts(sentText, conds)
        drugSnips = pubNlp.rangeTexts(sentText, drugs)

        genePosSet = pubNlp.rangeToPosSet(genes)
        variants  = varFinder.findVariantDescriptions(sentText, exclPos=genePosSet)

        # the last field of the genes rows is the entrez ID
        entrezIds = [r[-1] for r in genes]

        # we need a protein variant, not DNA
        if "prot" not in variants:
            continue

        for variant, mentions in variants["prot"]:
            print "grounding variant", variant, mentions
            groundedMuts, ungroundVar, beds = \
                varFinder.groundVariant(pmid, sentText, variant, mentions, [], entrezIds)

            for mutInfo in groundedMuts:
                coords = [(m.start, m.end) for m in mentions]
                varSnips = pubNlp.rangeTexts(sentText, coords)
                row = [section, "|".join(geneSnips), "|".join(condSnips), "|".join(drugSnips), "|".join(varSnips), sentText]
                yield row
                #row.extend(mutInfo.asRow())
            #if ungroundVar!=None:
                #yield ungroundVar.asRow()

if __name__=="__main__":
    import doctest
    doctest.testmod()
