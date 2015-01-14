# find disease ontology terms in abstracts
import re, gzip, os
from os.path import *

import fastFind, pubAlg

# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
headers = ["start", "end", "disease", "snippet"]

# the path to humanDiseases.tab.gz, a dict with disease -> synonyms
dataFname = join(dirname(__file__), "data", "diseases.tab.gz")
dictFname = join(dirname(__file__), "data", "diseases.marshal.gz")

# global variable, holds the mapping name => disease name
disTerms = set()

lex = None

onlyMain = True 

# this method is called ONCE on each cluster node, when the article chunk
# is opened, it fills the hugoDict variable
def startup(paramDict):
    """ parse mesh file into set """
    global disTerms
    for line in gzip.open(dataFname):
        name= line.strip("\n")
        disTerms.add(name)
    global lex
    lex = fastFind.parseLex(dictFname)

def searchDiseases(text):
    text = text.lower()
    annots = list(fastFind.fastFind(text, lex))
    newAnnots = []
    for annot in annots:
        start, end, id = annot
        snippet = pubAlg.getSnippet(text, start, end)
        dis = text[start:end]
        newAnnots.append( (start, end, dis, snippet) )
    return newAnnots

# this method is called for each FILE. one article can have many files
# (html, pdf, suppl files, etc). article data is passed in the object 
# article, file data is in "file". For a list of all attributes in these 
# objects please see the file ../lib/pubStore.py, search for "DATA FIELDS"
def annotateFile(article, file):
    " go over words of text and check if they are in dict "
    # first try to find diseases in mesh terms
    meshTerms = article.keywords.split("/")
    meshTerms = set([m.strip("*") for m in meshTerms])
    annots = []
    for term in meshTerms:
        if term in disTerms:
            foundTerms = True
            annots.append( (0,0,term,term) )

    if len(annots)!=0:
        return annots

    # if not successful try title
    annots = searchDiseases(article.title)
    if len(annots)!=0:
        return annots

    annots = searchDiseases(article.abstract)
    if len(annots)!=0:
        return annots

    annots = searchDiseases(file.content)
    if len(annots)!=0:
        return [annots[0]]

    return None
