import tabfile
from collections import defaultdict
# example file for pubtools map/reduce framework
# illustrates how to map/reduce article meta data

# reports file sizes and file counts

preferPdf = True
sectioning = False
#onlyMeta = True

headers = ["object", "size"]

# this method is called ONCE on each cluster node
def startup(paramDict, resultDict):
    resultDict.setdefault("articleCount", set())

# this method is called for each article
# resultDict is the output dictionary: write all your results to this dictionary
def map(article, file, text, resultDict):
    " "
    keys = ["totalSize", "journalBytes:"+article.journal, "journalFiles:"+article.journal, 
            "fileCount", "mimeTypeFiles:"+file.mimeType, "mimeTypeBytes:"+file.mimeType,
            "articleTypeFiles:"+article.articleType, "abstractBytes", "totalSizeMain", "totalSizeSupp"
            ]

    for key in keys:
        resultDict.setdefault(key, 0)

    resultDict.setdefault("articleCount_"+article.year, set())
    resultDict.setdefault("pmidCount_"+article.publisher+"_"+article.year, set())
    resultDict.setdefault("pmidCount", set())
    resultDict.setdefault("articleCountWithAbstract_"+article.year, set())
    resultDict.setdefault("maxMainSize", {})

    resultDict["totalSize"] += len(text)
    if file.fileType=="supp":
        resultDict["totalSizeSupp"] += len(text)
    elif file.fileType=="main":
        resultDict["totalSizeMain"] += len(text)
        maxMainDict = resultDict["maxMainSize"]
        oldSize = maxMainDict.get(article.articleId, 0)
        maxMainDict[article.articleId] = max(oldSize, len(text))

    resultDict["fileCount"] += 1
    resultDict["articleCount"].add(int(article.articleId))
    if article.pmid!="":
        pmid = int(article.pmid)
        resultDict["pmidCount"].add(pmid)
        resultDict["pmidCount_"+article.publisher+"_"+article.year].add(pmid)

    resultDict["articleCount_"+article.year].add(int(article.articleId))
    if len(article.abstract)>30:
        resultDict["articleCountWithAbstract_"+article.year].add(int(article.articleId))
    resultDict["journalBytes:"+article.journal] += len(text)
    resultDict["journalFiles:"+article.journal] += 1
    resultDict["mimeTypeFiles:"+file.mimeType] += 1
    resultDict["mimeTypeBytes:"+file.mimeType] += len(text)
    resultDict["articleTypeFiles:"+article.articleType] += 1
    resultDict["abstractBytes"] += len(article.abstract)+len(article.title)+len(article.authors)

# this is called after all jobs are finished, on the main machine
# it is called once for each key
def reduce(key, valList):
    if key.startswith("articleCount") or key.startswith("pmidCount"):
        yield key, len(valList)
    elif key=="maxMainSize" and len(valList)!=0:
        yield key, max(valList)
    else:
        yield key, sum(valList)
