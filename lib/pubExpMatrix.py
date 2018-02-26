from __future__ import print_function
# library to create a term-document matrix for R from documents

import re, os, logging, gzip, sys, codecs, array
from os.path import *

# load our own libraries
import pubConf, pubGeneric, pubStore, maxCommon, maxRun, pubAlg, tabfile
#import nltk.stem.porter
#import nltk.stem.snowball


# ===== GLOBALS ======
def chunkMatrix(inChunk, outName):
    print(inChunk, outName)

# ===== FUNCTIONS ====
def parsePmids(fname):
    " return an array of ints parsed from text file "
    if fname==None:
        return []

    logging.info("Parsing %s" % fname)
    pmids = []
    for line in open(fname):
        pmid = int(line.strip())
        pmids.append(pmid)
    #pmidArr = array.array("L", pmids)
    return pmids

def parseTerms(termFname):
    " return a list of words one per line, from a text file"
    terms = gzip.open(termFname).readlines()
    terms = [w.strip() for w in terms]
    termList = terms # keep both: list for order, set for speed
    termSet = set(terms)
    termToId = {}
    assert(len(termList)==len(termSet))
    return termList

def runMatrixJobs(outFname, datasets, wordListFname, posPmidFname, negPmidFname, \
        skipMap, outFormat, onlyTest, docIdFname, posPmids=None, negPmids=None, runner=None):
    """ run jobs to convert the articles to a bag-of-words matrix """

    assert (outFormat in ["svml", "arff", "pmidsvml"])

    if isinstance(datasets, basestring):
        datasets = [datasets]

    if runner==None:
        runner = pubGeneric.makeClusterRunner(__file__)

    logging.debug("pos and neg pmid fnames are: %s, %s" % (posPmidFname, negPmidFname))
    if posPmidFname!=None:
        posPmids = parsePmids(posPmidFname)
    if negPmidFname!=None:
        negPmids = parsePmids(negPmidFname)

    termList = parseTerms(wordListFname)

    paramDict = {"termList" : termList, "posPmids"  : posPmids, \
                 "negPmids" : negPmids, "outFormat" : outFormat }
    paramDict["docIdOutFname"] = docIdFname

    pubAlg.mapReduce(__file__+":MatrixMaker", datasets, paramDict, \
        outFname, skipMap=skipMap, runTest=True, runner=runner, onlyTest=onlyTest)

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

def buildWordList(runner, datasets, skipMap, outFname):
    pubAlg.mapReduce(__file__+":WordCounter", datasets, {}, outFname, skipMap=skipMap, \
        runTest=False, cleanUp=True, runner=runner)

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
#stemmer = nltk.stem.snowball.EnglishStemmer()
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
    map-reduce algorithm to create a list of words and count number of articles where word occurs
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

    def startup(self, paramDict, results):
        """ called when job starts on node """
        self.termList = paramDict["termList"] # for iteration
        self.termSet = set(self.termList) # for fast access
        self.termToId = {}
        for termId, term in enumerate(self.termList):
            self.termToId[term] = termId

        if paramDict["posPmids"]!=None:
            self.posPmids = set(paramDict["posPmids"])
            self.negPmids = set(paramDict["negPmids"])
        else:
            self.posPmids = []
            self.negPmids = []

        self.outFormat = paramDict["outFormat"]
        self.docIdCount = 0

    def map(self, article, file, text, results):
        " called once for each input file. create dict pmid -> list of sorted termIds "
        if file.fileType!="main":
            logging.info("not main")
            return
            
        # if any pmids were provided, ignore non-PMID articles
        if (self.posPmids!=None and self.negPmids!=None) or \
                (len(self.posPmids)!=0 and len(self.negPmids)!=0) :
            if article.pmid=="":
                logging.info("no PMID in article")
                return

        docId = article.articleId+"/"+article.externalId+"/"+article.pmid
        #docId = int(article.pmid)
        pmid = int(article.pmid)
        if self.posPmids!=None and pmid not in self.negPmids and pmid not in self.posPmids:
            logging.debug("neither in pos nor in neg set")
            return
            
        termRow = []
        for term in iterWords(text):
            if term in self.termSet:
                termId = self.termToId[term]
                termRow.append(termId)

        results[docId] = termRow
        logging.info(" ".join([article.articleId, article.externalId, file.fileType, file.mimeType]))

    def reduceStartup(self, resultDict, paramDict, outFh):
        " called before reducer starts "
        if self.outFormat in ["svml", "pmidsvml"]:
            self.headers = ["#"]
        else:
            self.headers = ["% "]
            outFh.write("@RELATION pubExpMatrix\n")
            for term in self.termList:
                outFh.write("@ATTRIBUTE %s NUMERIC\n" % term)
            outFh.write("@ATTRIBUTE class {pos,neg}\n")
            outFh.write("@DATA\n")

        if self.outFormat == "pmidsvml":
            self.docIdOfh = None
        else:
            self.docIdOfh = open(paramDict["docIdOutFname"], "w")
        
        self.docIdCount = 0

    def _termsToArff(self, pmid, isPos, termIdSet):
        " return a string in arff format "
        termRow = []
        for i in range(0, len(self.termList)):
            if i in termIdSet:
                val = 1
            else:
                val = 0
            termRow.append(str(val))

        # add class (=last field in arff)
        if isPos:
            target = "pos"
        else:
            target = "neg"
        termRow.append(target)

        line = ",".join(termRow)
        return line

    def _termsToSvmLight(self, docId, isPos, termIdSet, docIdAsClass):
        " return a string in svmlight format "
        if docIdAsClass:
            target = docId
        elif isPos==None:
            target = "0"
        elif isPos==True:
            target = "+1"
        else:
            target = "-1"

        termIds = list(termIdSet)
        termIds.sort()
        ftStrings = [str(termId+1)+":1" for termId in termIds]
        line = "%s %s" % (target, " ".join(ftStrings))
        return line

    def reduce(self, docId, termIdList):
        " output vectors and document identifiers "
        docIdAsClass = False
        if self.outFormat=="pmidsvml":
            docIdAsClass = True

        if (self.posPmids==None and self.negPmids==None) or \
            (len(self.posPmids)==0 and len(self.negPmids)==0):
            isPos = None
        else:
            pmid = int(docId.split("/")[2])
            isPos = pmid in self.posPmids

        termIdSet = set(termIdList)
        if len(termIdSet)!=0:
            if self.outFormat.endswith("svml"):
                lineStr = self._termsToSvmLight(docId, isPos, termIdSet, docIdAsClass)
            elif self.outFormat=="arff":
                lineStr = self._termsToArff(docId, isPos, termIdSet)
            else:
                assert(False)

            if self.docIdOfh!=None:
                #self.docIdOfh.write(str(self.pmidCount)+"\t"+str(pmid)+"\n")
                self.docIdOfh.write(str(docId)+"\n")
            self.docIdCount+=1
            yield [lineStr]

    def reduceEnd(self, results):
        if self.docIdOfh!=None:
            self.docIdOfh.close()
