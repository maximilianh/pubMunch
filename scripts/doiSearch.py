import tabfile
from os.path import *
# example file for pubtools map/reduce framework
# illustrates how to map/reduce article meta data

# this scripts reads the file doi.tab and outputs all article IDs with a DOI
# from this file

# global variable, holds the  list of DOIs that we are interested in
doiSet = set()

# this variable indicates that we only want to read article meta data
# NOT the fulltext files (will speed up the jobs, they will not touch 
# the ".files" table
skipFiles = True

headers = ["doi", "pmcId", "desc", "text"]

# this method is called ONCE on each cluster node
def startup(paramDict, resultDict):
    global doiSet
    doiFname = join(dirname(__file__), "data/doi.tab")
    doiSet = set(tabfile.slurplist(doiFname))
    for doi in doiSet:
        resultDict[doi]=set()

# this method is called for each article
# resultDict is the output dictionary: write all your results to this dictionary
def map(article, file, text, resultDict):
    " check if doi is in doiSet, add to result dictionary"
    doi = article.doi
    global doiSet
    if doi in doiSet:
        resultDict[doi].add((doi, article.pmcId, file.desc, file.content))

# this is called after all jobs are finished, on the main machine
# it is called once for each key
def reduce(key, valList):
    for doi, pmcId, desc, text in valList:
        yield doi, pmcId, desc, text
