# library to create a term-document matrix for R from documents

import re, os, logging, gzip, sys, codecs
from os.path import *

# load our own libraries
import pubConf, pubGeneric, pubStore, maxCommon, maxRun, pubAlg, tabfile
#import nltk.stem.porter
import nltk.stem.snowball

# ===== GLOBALS ======
def chunkMatrix(inChunk, outName):
    print inChunk, outName

# ===== FUNCTIONS ====
def runMatrixJobs(outFname, datasets, wordListFname, pmidListFname, skipMap):
    for dataset in datasets:
        batchDir = join(pubConf.clusterBatchDir, "pubExpMatrix-matrix")
        clusterRun = maxRun.Runner(batchDir=batchDir, headNode="swarm.cse.ucsc.edu", clusterType="parasol")
        pmidOutFname = splitext(outFname)[0]+".svmlight.pmids"
        paramDict = {"wordFile": wordListFname, "pmidFile" : pmidListFname, "pmidOutFile" : pmidOutFname}
        pubAlg.mapReduce(__file__+":MatrixMaker", dataset, paramDict, \
            outFname, skipMap=skipMap, deleteDir=False, runTest=True, runner=clusterRun)

def expData(inDirs, pmidListFname, outBase):
    logging.info("Reading %s" % pmidListFname)
    pmids = set([int(x) for x in tabfile.slurplist(pmidListFname)])
    logging.info("Read %d pmids" % len(pmids))
    posFname = outBase+".pos.tab"
    negFname = outBase+".neg.tab"
    posFh = codecs.open(posFname, "w", encoding="utf8")
    negFh = codecs.open(negFname, "w", encoding="utf8")
    
    for dataDir in inDirs:
        logging.debug(dataDir)
        for article, fileList in pubStore.iterArticleDirList(dataDir):
            if article.pmid=="":
                continue
            if int(article.pmid) in pmids:
                ofh = posFh
                txtClass = "pos"
            else:
                ofh = negFh
                txtClass = "neg"

            for fileData in fileList:
                if fileData.fileType=="main":
                    text = fileData.content
                    text = text.replace("\a", " ")
                    ofh.write("%s\t%s\t%s\n" % (article.pmid, txtClass, text))

def exportMatrix(inDirs, pmidListFname, outBase, wordListFname, skipMap):
    if wordListFname:
        wordListFname = join(os.getcwd(),wordListFname)
        if not isfile(wordListFname):
            logging.info("%s not found" % wordListFname)
            buildWordList(inDirs, skipMap)
            logging.info("raw word list created, use your own command now to reduce it to something smaller")
            logging.info("e.g. cat wordFreq.tab | gawk '($2<50000) && ($2>100)' | cut -f1 | lstOp remove stdin /hive/data/outside/pubs/wordFrequency/google-ngrams/fiction/top100k.tab  | lstOp remove stdin /hive/data/outside/pubs/wordFrequency/bnc/bnc.txt > wordList.txt")

    pmidListFname = join(os.getcwd(), pmidListFname)
    logging.info("creating matrix")
    runMatrixJobs(outBase, inDirs, wordListFname, pmidListFname, skipMap)
    #expData(inDirs, pmidListFname, outBase)

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

class AnnotMaker:
    """ 
    annotation algorithm to reformat text
    """
    def __init__(self):
        self.headers = ["class", "text"]

    def annotateFile(self, articleData, fileData):
        if articleData.pmid!="":
            yield articleData.pmid, fileData.content

class MatrixMaker:
    """ 
    map-reduce algorithm to output term-document-matrix given a wordlist and an optional list of PMIDs
    """
    def __init__(self):
        self.wordCounts = {}
        self.headers = ["pmid", "class"]

    def startup(self, paramDict, results):
        """ called when job starts on node, read terms """
        if "wordFile" in paramDict:
            terms = gzip.open(paramDict["wordFile"]).readlines()
            terms = [w.strip() for w in terms]
            self.termList = terms # keep both: list for order, set for speed
            self.termSet = set(terms)
            self.termToId = {}
            for termId, term in enumerate(self.termList):
                self.termToId[term] = termId # svmlight doesn't like 0
        else:
            self.termSet = None
            self.termList = None
            self.termToId = None

        if "pmidFile" in paramDict:
            pmidFname = paramDict["pmidFile"]
            assert(pmidFname.endswith(".gz"))
            pmids = gzip.open(pmidFname).readlines()
            pmids = [int(i.strip()) for i in pmids]
            self.pmidSet = set(pmids)
        else:
            self.pmidSet = None

    def map(self, article, file, text, results):
        " called once for each input file. create dict pmid -> set of words "

        if article.pmid=="":
            logging.info("no PMID")
            return

        results.setdefault(article.pmid, set())
        pmid = int(article.pmid)
        #if self.pmidSet!=None and pmid not in self.pmidSet:
            #continue
            
        results.setdefault(pmid, set())
        for term in iterWords(text):
            if (self.termSet==None) or term in self.termSet:
                results[pmid].add(term)

        logging.info(" ".join([article.articleId, article.externalId, file.fileType, file.mimeType]))
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

    def reduceStartup(self, resultDict, paramDict):
        " called before reducer starts "
        #self.headers.extend(self.termList)
        self.headers = ["#noHeader"]
        print paramDict
        self.pmidOfh = open(paramDict["pmidOutFile"], "w")

    def _termsToSvmLight(self, pmid, isPos, termSet):
        " return as a string in svmlight format "
        if isPos:
            target = "+1"
        else:
            target = "-1"

        termIds = []
        termSet = set(termSet)
        for term in termSet:
            termId = self.termToId[term]+1
            termIds.append(termId)
        termIds.sort()
        ftStrings = [str(termId)+":1" for termId in termIds]
        line = "%s %s" % (target, " ".join(ftStrings))
        return line

    def reduce(self, pmid, termSet):
        " called once per cluster run after all nodes are finished. output vectors "
        #if len(termVecs)>1:
            #logging.warn("duplicate PMID %s" % str(pmid))
        #termVec = termVecs[0]
        isPos = int(pmid) in self.pmidSet

        # make sure that we don't output more negatives than positives
        #if (classStr=="neg" and self.negCount < len(self.pmidSet)) or (classStr=="pos"):
        #row = [pmid, classStr]
        #row.extend(terms)
        #yield row
        if len(termSet)!=0:
            lineStr = self._termsToSvmLight(pmid, isPos, termSet)
            self.pmidOfh.write(str(pmid)+"\n")
            yield [lineStr]
