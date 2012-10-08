# library to create a term-document matrix for R from documents

import re, os, logging, gzip, sys
from os.path import *

# load our own libraries
import pubConf, pubGeneric, pubStore, maxCommon, maxRun, pubAlg
#import nltk.stem.porter
import nltk.stem.snowball

# ===== GLOBALS ======
def chunkMatrix(inChunk, outName):
    print inChunk, outName

# ===== FUNCTIONS ====
def runMatrixJobs(datasets, wordListFname, skipMap):
    outFname = "./matrix.tab"
    batchDir = join(pubConf.clusterBatchDir, "pubExpMatrix-matrix")
    clusterRun = maxRun.Runner(batchDir=batchDir, headNode="swarm.cse.ucsc.edu", clusterType="parasol")
    pubAlg.mapReduce(__file__+":MatrixMaker", datasets, {"wordFile": wordListFname}, outFname, skipMap=skipMap, \
        deleteDir=False, runTest=True, runner=clusterRun)

def exportMatrix(outFname, datasets, skipMap):
    wordListFname = os.getcwd()+"/wordList.txt.gz"
    if not isfile(wordListFname):
        logging.info("%s not found" % wordListFname)
        buildWordList(datasets, skipMap)
        logging.info("raw word list created, use your own command now to reduce it to something smaller")
        logging.info("e.g. cat wordFreq.tab | gawk '($2<50000) && ($2>100)' | cut -f1 | lstOp remove stdin /hive/data/outside/pubs/wordFrequency/google-ngrams/fiction/top100k.tab  | lstOp remove stdin /hive/data/outside/pubs/wordFrequency/bnc/bnc.txt > wordList.txt")

    else:
        logging.info("Word file found, creating matrix")
        runMatrixJobs(datasets, wordListFname, skipMap)

def buildWordList(datasets, skipMap):
    #outFname = join(outDir, "matrix.tab")
    outFname = "./wordFreq.tab"
    batchDir = join(pubConf.clusterBatchDir, "pubExpMatrix-wordCount")
    clusterRun = maxRun.Runner(batchDir=batchDir, headNode="swarm.cse.ucsc.edu", clusterType="parasol")
    pubAlg.mapReduce(__file__+":WordCounter", datasets, {}, outFname, skipMap=skipMap, \
        deleteDir=False, runTest=False, cleanUp=False, runner=clusterRun)

def runChunkMatrix(outDir, datasets):
    batchDir = join(pubConf.clusterBatchDir, "pubExpMatrix")
    maxCommon.mustExistDir(batchDir, makeDir=True)
    cluster = maxRun.Runner(batchDir=batchDir)
    for chunkName in pubStore.iterChunks(datasets):
        outFname = join(outDir, basename(chunkName)+".tab.gz")
        params = ["{check in exists %s}" % chunkName, "{check out exists %s}" % outFname]
        cluster.submitPythonFunc(__file__, "chunkMatrix", params)
    cluster.finish()

def isAscii(word):
    try:
        word.decode('ascii')
    except UnicodeDecodeError:
        return False
    except UnicodeEncodeError:
        return False
    else:
        return True
    
MINWORDLEN = 5
MINCOUNT = 10 
#stemmer = nltk.stem.porter.PorterStemmer()
stemmer = nltk.stem.snowball.EnglishStemmer()
nonLetterRe = re.compile(r'[^A-Za-z-]') # non-alphanumeric/non-dash characters

def findMethods(text):
    " find mat & method section in text "
    sections = pubGeneric.sectionRanges(text)
    if sections==None:
        return None
    if "methods" not in sections:
        return None
    methodStart, methodEnd  = sections["methods"]
    text = text[methodStart:methodEnd]
    return text

def iterWords(text):
    " yield lowercase, normal-ASCII words in string, run stemmer "

    words = text.replace("\n", " ").replace("\a", " ").split()
    for word in words:
        word = word.lower()
        word = word.rstrip(",.:")
        word = word.strip("()[]{}-'\"")
        if not isAscii(word):
            continue
        if len(word) < MINWORDLEN or len(word) > 30:
            continue 
        stemWord = stemmer.stem(word)
        if nonLetterRe.search(stemWord)!=None:
            continue
        if len(stemWord) < MINWORDLEN:
            continue 
        #print word, stemWord
        yield stemWord

class WordCounter:
    """ 
    map-reduce algorithm to count number of articles where word occurs
    """
    def __init__(self):
        self.wordCounts = {}
        self.headers = ["word", "count"]

    def map(self, article, file, text, results):
        " called once for each file. create dict word -> set of articleIds "
        #print repr(file)
        logging.info(" ".join([article.articleId, article.externalId, file.fileType, file.mimeType]))
        if file.fileType!="main":
            return

        metText = findMethods(text)
        if metText==None:
            return

        wordCount = 0
        for word in iterWords(metText):
            results.setdefault(word, set())
            results[word].add(int(article.articleId))
            wordCount += 1
        if wordCount!=0:
            logging.info("Processed %d words" % wordCount)

    def end(self, results):
        " called once per cluster node after job has ended. convert dict to word -> count "
        newResults = {}
        for key, valList in results.iteritems():
            newResults[key] = len(valList)
        return newResults

    def reduce(self, word, articleCounts):
        " called once per cluster run after all nodes are finished. take sum of all counts "
        articleCount = sum(articleCounts)
        if len(word) < MINWORDLEN:
            yield None
        if articleCount < MINCOUNT:
            yield None
        else:
            yield [word, str(articleCount)]

class MatrixMaker:
    """ 
    map-reduce algorithm to output term-document-matrix given a wordlist
    """
    def __init__(self):
        self.wordCounts = {}
        self.headers = ["pmid"]

    def startup(self, paramDict, results):
        """ called when job starts on node, parse dictioary of keyterms """
        terms = gzip.open(paramDict["wordFile"]).readlines()
        terms = [w.strip() for w in terms]
        self.termList = terms # keep both: list for order, set for speed
        self.termSet = set(terms)

        self.termToPos = {}
        for pos, term in enumerate(self.termList):
            self.termToPos[term] = pos

    def reduceStartup(self, paramDict):
        " called before reducer starts "
        self.headers.extend(self.termList)

    def map(self, article, file, text, results):
        " called once for each input file. create dict pmid -> set of words "
        logging.info(" ".join([article.articleId, article.externalId, file.fileType, file.mimeType]))
        if article.pmid=="":
            logging.info("no PMID")
            return

        results.setdefault(article.articleId, set())
        pmid = article.articleId
        for term in iterWords(text):
            if term in self.termSet:
                results[pmid].add(term)

    #def end(self, results):
        #" called once per cluster node after job has ended. convert dict to pmid -> list of termVec "
        #newResults = {}
        #for pmid, termSet in results.iteritems():
            #termVec = len(self.termList)*[0]
            #for term in termSet:
                #pos = self.termToPos[term]
                #termVec[pos]=1
            #newResults.setdefault(pmid, []).append(termVec)
        #return newResults

    def reduce(self, pmid, terms):
        " called once per cluster run after all nodes are finished. output vectors "
        #if len(termVecs)>1:
            #logging.warn("duplicate PMID %s" % str(pmid))
        #termVec = termVecs[0]
        row = [pmid, "PMID"+str(pmid)]
        row.extend(terms)
        yield row
