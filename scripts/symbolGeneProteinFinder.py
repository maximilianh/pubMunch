# count how often a symbol is flanked by gene or protein
import os
from os.path import *
import pubConf

import fastFind, pubAlg, re

# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
# headers = ["symbol", "word", "leftWord", "rightWord"]
headers = ["word", "allCount", "geneCount", "geneRatio"]

# the path to symbols.marshal.gz, a fastFind dict with symbol -> official symbol
symFname = join(pubConf.geneDataDir, "symbols.marshal.gz")

lex = None

onlyMain = True 
#onlyBestMain = True

wordRe = re.compile("^[A-Za-z0-9 -]+$")

blackList = None

# this method is called ONCE on each cluster node, when the article chunk
# is opened, it fills the lex variable
def startup(paramDict, result):
    """ parse sym file into lex """
    global lex, blackList
    lex = fastFind.loadLex(symFname)
    #result["geneCount"] = {}
    #result["allCount"] = {}
    blackList = set(open(pubConf.bncFname).read().splitlines()[:1000])

# this method is called for each FILE. one article can have many files
# (html, pdf, suppl files, etc). article data is passed in the object 
# article, file data is in "file". For a list of all attributes in these 
# objects please see the file ../lib/pubStore.py, search for "DATA FIELDS"
def map(article, file, text, result):
    " go over words of text and check if they are in dict "
    text = file.content
    annots = list(fastFind.fastFindFlankWords(text, lex, wordDist=1))
    newAnnots = []
    for annot in annots:
        start, end, id, leftWords, rightWords = annot
        # get and clean word
        word = text[start:end]
        if not wordRe.match(word): # remove if garbage between words like ( or # etc
            continue
        word = " ".join(word.split()) # remove multi whitespace
        # remove word if it contains more than one dash
        letters = list(word)
        if letters.count("-")>1:
            continue
        word = word.replace(" -", "-")
        word = word.replace("- ", "-")
        if word in blackList:
            continue

        result.setdefault( word, [0,0] ) # need to return it as a double-list
        result[ word ][0] += 1

        # check and inc count if flanking word is gene or protein
        allFlanks = list(leftWords)
        allFlanks.extend(rightWords)
        allFlanks = [w.lower() for w in allFlanks]
        if "protein" in allFlanks or "gene" in allFlanks or "locus" in allFlanks:
            result[ word ][1] += 1

def reduce(word, valList):
    allSum = 0
    geneSum = 0
    for allCount, geneCount in valList:
        allSum += allCount
        geneSum += geneCount
    if allSum==0:
        geneRatio = "undef"
    else:
        geneRatio = "%0.3f" % (float(geneSum) / allSum)
    yield (word, allSum, geneSum, geneRatio)
