# get flanking words around a list of keywords
# list of keywords is specified with the parameter wordFname (full path)
import os, operator, logging
from os.path import *
import pubConf
from collections import defaultdict, Counter

import fastFind, pubAlg, re, pubConf

# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
# headers = ["symbol", "word", "leftWord", "rightWord"]
headers = ["word", "leftCounts", "rightCounts"]

lex = None

onlyMain = True 
#onlyBestMain = True

wordRe = re.compile("^[A-Za-z0-9 -]+$")

blackList = None

# this gets called only once per batch, on startup
def batchStartup(paramDict):
    logging.info("Compiling wordlist to fastFind-file")
    fastFind.compileDict(paramDict["wordFname"], wordRe=fastFind.DASHWORDRE)

# this method is called ONCE on each cluster node, when the article chunk
# is opened, it fills the lex variable
def startup(paramDict, result):
    """ parse sym file into lex """
    global lex
    wordFname = paramDict["wordFname"]
    lex = fastFind.loadLex(join(dirname(wordFname), wordFname.split(".")[0]+".marshal.gz"))
    global blackList
    blackList = set(open(pubConf.bncFname).read().splitlines()[:10000])

def addToResults(word, flankWords, side, result):
    if len(flankWords)==0:
        return
    for flankWord in flankWords:
        flankWord = flankWord.lower()
        result[word][side].setdefault(flankWord, 0)
        result[word][side][flankWord]+= 1

# this method is called for each FILE
def map(article, file, text, result):
    " go over words of text and check if they are in dict "
    text = file.content
    annots = list(fastFind.fastFindFlankWords(text, lex, wordDist=2))
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

        result.setdefault( word, {"l" : {}, "r" : {} } ) # need to return it as a double-list
        addToResults(word, leftWords, "l", result)
        addToResults(word, rightWords, "r", result)

leftCommon = Counter()
rightCommon = Counter()

def getBestToString(countDict):
    counts = countDict.items()
    counts.sort(key=operator.itemgetter(1), reverse=True)
    bestCounts = counts[:30]
    bestStrings = ["%s=%s" % (word, count) for word, count in bestCounts]
    countsString = "|".join(bestStrings)
    return countsString

def reduce(word, dictList):
    word = word.lower()
    sumCounts = {"l":defaultdict(int), "r" :defaultdict(int)}
    for countDict in dictList:
        if "l" in countDict:
            for w, count in countDict["l"].iteritems():
                w = w.lower()
                sumCounts["l"][w] += count
        if "r" in countDict:
            for w, count in countDict["r"].iteritems():
                w = w.lower()
                sumCounts["l"][w] += count
                sumCounts["r"][w] += count

    for w, count in sumCounts["l"].iteritems():
        leftCommon[w]+=1
    for w, count in sumCounts["r"].iteritems():
        rightCommon[w]+=1
        
    leftDesc = getBestToString(sumCounts["l"])
    rightDesc = getBestToString(sumCounts["r"])
    yield (word, leftDesc, rightDesc)

def writeWords(counter, blackList, outFname, maxCount=10000):
    i = 0
    ofh = open(outFname, "w")
    for word, count in counter.most_common():
        if word.lower() not in blackList and len(word)>3:
            ofh.write(word+"\t"+str(count)+"\n")
            i+=1
        if i==maxCount:
            break
    ofh.close()

def cleanup():
    blackList = set(open(pubConf.bncFname).read().splitlines()[:10000])
    writeWords(leftCommon, blackList, "left.tab")
    writeWords(rightCommon, blackList, "right.tab")
    logging.info("Wrote most common left and right flanking words to left.tab and right.tab")
