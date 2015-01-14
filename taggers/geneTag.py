# search for gene names as defined by uniprot
import re, os, logging
import geneFinder

onlyMain= True
preferPdf = True

# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
headers = ["start", "end", "geneId", "geneSym", "score", "rank", "support"]

# this method is called ONCE on each cluster node, when the article chunk
# is opened, it fills the hugoDict variable
def startup(paramDict):
    """ parse HUGO file into dict """
    geneFinder.initData(exclMarkerTypes=["dnaSeq"])

def annotateFile(article, file):
    text = file.content
    geneScores, geneSupport = geneFinder.rankGenes(text, pmid=article.pmid)
    rank = 1
    rows = []
    for geneId, geneScore in geneScores:
        supportStrs = []
        start, end = None,None
        firstStart = None
        snippets = []
        sym = geneFinder.entrezToSym.get(geneId, "<NOSYM?>")
        uniprotIds = ",".join(geneFinder.entrezToUp.get(geneId, ["<NOUNIPROT>"]))

        for markerType, recogId, startEndList in geneSupport[geneId]:
            supportStr=markerType+"/"+recogId
            for start, end in startEndList:
                row = [start, end, geneId, sym, geneScore, rank, uniprotIds, \
                    supportStr]
            rows.append(row)
            rank += 1
    if len(rows)>200:
        logging.warn("too many genes found in document, skipping all")
        return None
    return rows
        
if __name__=="__main__":
    import doctest
    doctest.testmod()
