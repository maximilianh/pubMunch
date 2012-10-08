import tabfile
# example file for pubtools map/reduce framework
# illustrates how to map/reduce article meta data

# this scripts reads the file doi.tab and outputs all article IDs with a DOI
# from this file

# this variable indicates that we only want to read article meta data
# NOT the fulltext files (will speed up the jobs, they will not touch 
# the ".files" table
#skipFiles = True

headers = ["object", "size"]

# this method is called ONCE on each cluster node
def startup(paramDict, resultDict):
    #doiSet = set(tabfile.slurplist("doi.tab"))
    #for doi in doiSet:
        #resultDict[doi]=set()
    pass

# this method is called for each article
# resultDict is the output dictionary: write all your results to this dictionary
def map(article, file, text, resultDict):
    " check if doi is in doiSet, add to result dictionary"
    keys = ["totalSize", "journalBytes:"+article.journal, "journalFiles:"+article.journal, 
            "fileCount", "mimeTypeFiles:"+file.mimeType, "mimeTypeBytes:"+file.mimeType,
            "articleTypeFiles:"+article.articleType, "abstractBytes"]
    for key in keys:
        resultDict.setdefault(key, 0)

    resultDict.setdefault("articleCount", set())
    resultDict.setdefault("articleCount_"+article.year, set())
    resultDict.setdefault("articleCountWithAbstract_"+article.year, set())

    resultDict["totalSize"] += len(text)
    resultDict["fileCount"] += 1
    resultDict["articleCount"].add(int(article.articleId))
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
    if key.startswith("articleCount"):
        yield key, len(valList)
    else:
        yield key, sum(valList)
