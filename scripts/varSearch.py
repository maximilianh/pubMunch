import geneFinder, varFinder

onlyMain = True
preferPdf = True

headers = list(varFinder.mutFields)

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
    textLow = text.lower()
    # very basic filter
    if " variant " not in textLow and " mutation" not in textLow and " substitution" not in textLow and \
            " mutant " not in textLow:
        return
    genes, genePosSet     = findGenes(pmid, text)
    variants  = varFinder.findVariantMentions(text, exclPos=genePosSet)

    for variant, mentions in variants["prot"]:
        groundedMuts, ungroundVar, beds = \
            varFinder.groundVariant(pmid, text, variant, mentions, variants["dbSnp"], genes)
        for m in groundedMuts:
            yield m.asRow()
        if ungroundVar!=None:
            yield ungroundVar.asRow()

