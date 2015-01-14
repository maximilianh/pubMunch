# annotator that returns lines with word=count tuples
import unidecode, marshal, logging, operator, subprocess
from os.path import join, basename, dirname

try:
    import re2 as re
except:
    import re

from collections import Counter
import doctest

# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
#headers = ["pmid", "wordCounts"]

# keep mapping word -> list of pmids
wordsToPmids = {}

# this plugin produces marshal output files
outTypes = ["marshal"]

# we want to run on fulltext files
runOn = "files"
# we only want main files
onlyMain = True
# only give us one single main article file
preferXml = True

sentSplitter = re.compile(r'[.!?;][ ]')
wordSplitter = re.compile(r'[;:",. !?=\[\]()\t\n\r\f\v]')

addTwoGrams = False
addMeta = False
pmids = None
outFhs = None

# run before outTypes is read or any files are opened
# can be used to change outTypes depending on paramDict
def setup(paramDict):
    global addTwoGrams
    global addTwoGrams
    global pmids
    addTwoGrams = bool(paramDict["addTwoGrams"])
    addMeta = bool(paramDict["addMeta"])
    pmids = set(paramDict["pmids"])

# run after the files have been opened
# can be used to write something to the out files, e.g. headers
def startup(outFiles):
    global outFhs
    outFhs = outFiles
    #outFiles["tab"].write("\t".join(headers)+"\n")
    
class D:
    pass

def isAscii(word):
    try:
        word.decode('utf8').encode('ascii')
    except UnicodeDecodeError:
        return False
    except UnicodeEncodeError:
        return False
    else:
        return True
    
# from nltk.corpus
stopWords = set(["i","me","my","myself","we","our","ours","ourselves","you","your","yours","yourself","yourselves","he","him","his","himself","she","her","hers","herself","it","its","itself","they","them","their","theirs","themselves","what","which","who","whom","this","that","these","those","am","is","are","was","were","be","been","being","have","has","had","having","do","does","did","doing","a","an","the","and","but","if","or","because","as","until","while","of","at","by","for","with","about","against","between","into","through","during","before","after","above","below","to","from","up","down","in","out","on","off","over","under","again","further","then","once","here","there","when","where","why","how","all","any","both","each","few","more","most","other","some","such","no","nor","not","only","own","same","so","than","too","very","s","t","can","will","just","don","should","now"])

def goodWord(word):
    return len(word)>2 and \
        isAscii(word) and \
        not word[0].isdigit() and \
        len(set(word))>2 and \
        (word not in stopWords) and \
        "_" not in word and \
        "<" not in word and \
        ">" not in word

def addWordsToCounts(counts, text, prefix):
    if len(text)<5:
        return

    words = wordSplitter.split(text)
    words = [w.lower() for w in words if w!='']
    lastWord = None
    for word in words:
        if not goodWord(word):
            lastWord = None
            continue
        counts[prefix+word] += 1

        if lastWord!=None and addTwoGrams:
            twoGram = lastWord+"_"+word
            counts[prefix+twoGram] += 1
        lastWord = word

# this method is called for each FILE. one article can have many files
# (html, pdf, suppl files, etc). article data is passed in the object 
# article, file data is in "file". For a list of all attributes in these 
# objects please see the file ../lib/pubStore.py, search for "DATA FIELDS"

def getLastAuthor(string):
    " get last author family name and remove all special chars from it"
    string = string.split(";")[-1].split(",")[0].strip()
    string = unidecode.unidecode(string)
    return string

def countWords(article, fileList, acceptNonPubmed=False):
    " return a counter object with counts of all words in article/fileList "
    if not acceptNonPubmed:
        if article.year=='' or int(article.year)<1990:
            return
        if article.pmid == "":
            return
    if len(fileList)==0:
        return
    text = fileList[0].content
    text = text.replace("\a", " ") # get rid of linebreaks
    text = text.encode('utf8') # for re2 speed
    phrases = sentSplitter.split(text)
    counts = Counter()
    for phrase in phrases:
        addWordsToCounts(counts, phrase, "")
    addWordsToCounts(counts, article.title.encode("utf8"), "T:")
    addWordsToCounts(counts, article.abstract.encode("utf8"), "A:")

    addMetaField(counts, article.printIssn, "J:")
    addMetaField(counts, article.year, "Y:")
    lastAuthor = getLastAuthor(article.authors)
    addMetaField(counts, lastAuthor, "L:")
    return counts

def addMetaField(counts, string, prefix):
    if string!="":
        counts[unidecode.unidecode(prefix+string)] = 1

def annotate(article, fileList):
    """
    >>> d = D()
    >>> d.content = " a=5% ++++ --- the hello world. Springer-Verlage  ! \a \t [hello, ]world, (here) we go! random garbage"
    >>> d.printIssn = "1234-1234"
    >>> annotateFile(d, d)
    [['world=2,hello_world=2,hello=2,garbage=1,random_garbage=1,random=1,springer-verlage=1,ISSN:1234-1234=1']]

    go over words of text and return string with their counts
    """
    if article.pmid=="":
        return
    pmid = int(article.pmid)
    if pmid not in pmids:
        return

    counts = countWords(article, fileList)
    if counts==None:
        return

    for word in counts:
        wordsToPmids.setdefault(word, []).append(pmid)

    #countStrings = ["%s=%d" % (w, c) for w,c in counts.most_common()]
    #row = (article.pmid, ",".join(countStrings))
    #outFhs["tab"].write("\t".join(row))
    #outFhs["tab"].write("\n")

def cleanup():
    marshal.dump(wordsToPmids, outFhs["marshal"])

class WordPmids():
    def startup(self, paramDict, results):
        addTwoGrams = paramDict["addTwoGrams"]
        addMeta = paramDict["addMeta"]
        self.pmids = set(paramDict["pmids"])

    def map(self, article, fileData, text, results):
        if article.pmid!="" and int(article.pmid) in self.pmids:
            counts = countWords(article, [fileData])
            if counts==None:
                return
            for word in counts:
                results.setdefault(word, []).append(int(article.pmid))

    def combineStartup(self, data, paramDict):
        self.outFname = paramDict["outFname"]

    def combine(self, data, partDict):
        for word, pmidList in partDict.iteritems():
            self.data.setdefault(word, []).extend(pmidList)

    def combineCleanup(self, data):
        delWords = []
        for word, pmidList in self.data.iteritems():
            if len(pmidList)<5:
                delWords.append(word)

        for word in delWords:
            del self.data[word]

        return data

class SvmlWriter():
    """ fiven a dict with db -> words convert textfiles to svml format. This
       is an annotation writer, because we create one file per db.
       Also creates a .docIds file to keep track of document IDs.
   """
    def __init__(self):
        " to make pylint happy "
        self.bestWords = {}
        self.bestWordsSet = {}
        self.wordsToIdx = {}
        self.bestMain=True
        self.onlyMain=True

    def setup(self, paramDict):
        dbList = paramDict['dbWords'].keys()
        self.outTypes = []
        self.outTypes.append("docIds")
        for db in dbList:
            self.outTypes.append(db+".svml")
            self.outTypes.append(db+".classes")
        global addTwoGrams
        global addMeta
        addTwoGrams = paramDict["addTwoGrams"]
        addMeta = paramDict["addMeta"]
        bestWordDict = paramDict["dbWords"]
        # create lookups dicts:
        # 1) dict db -> word -> index 
        # 2) dict db -> list of words (right order, so no need to sort)
        for db, bestWords in bestWordDict.iteritems():
            self.bestWords[db] = bestWords
            self.bestWordsSet[db] = set(bestWords)
            dbWordsToIdx = {}
            for wordIdx, word in enumerate(bestWords):
                dbWordsToIdx[word] = wordIdx
            self.wordsToIdx[db] = dbWordsToIdx
        self.modelDir = paramDict["modelDir"]
        self.svmlBinDir = paramDict["svmlBinDir"]

    def startup(self, outFiles):
        self.outFiles = outFiles
        # write headers
        docHeaders = ["articleId", "extId", "pmid"]
        for key, fh in self.outFiles.iteritems():
            if key.endswith("docIds"):
                fh.write("\t".join(docHeaders)+"\n")

    def annotate(self, article, fileList):
        # make sure that all preconditions are fulfilled
        # count words
        counts = countWords(article, fileList, acceptNonPubmed=True)
        if counts==None:
            logging.warn("No files for article %s/%s" % (article.articleId, article.externalId))
            return

        self.outFiles["docIds"].write("%s\t%s\t%s\n" % \
            (article.articleId, article.externalId, article.pmid))
        for db, dbBestWords in self.bestWords.iteritems():
            # get relevant words in document
            goodWords = self.bestWordsSet[db].intersection(counts)

            svmlFeats = [(0,1)]
            wordToIndex = self.wordsToIdx[db]
            for word in goodWords:
                #svmlFeats.append((wordToIndex[word]+1, counts[word]))
                svmlFeats.append((wordToIndex[word]+1, 1))
            if len(goodWords)==0:
                svmlFeats = [(0,0)]
            svmlFeats.sort(key=operator.itemgetter(0))

            ftStrings = [str(wi+1)+":"+str(wc) for wi, wc in svmlFeats]
            self.outFiles[db+".svml"].write("0 %s\n" % (" ".join(ftStrings)))

    def cleanup(self):
        svmPath = join(self.svmlBinDir, "svm_classify")
        modelDir = self.modelDir

        # run svml on all output svml files
        for ftype, svmlFile in self.outFiles.iteritems():
            if not ftype.endswith(".svml"):
                continue
            svmlFile.close()
            db = ftype.split(".")[0]
            modelFname = join(self.modelDir, db+".model")
            outFname = self.outFiles[db+".classes"].name
            cmd = [svmPath, svmlFile.name, modelFname, outFname]
            subprocess.check_call(cmd)

if __name__=="__main__":
    import doctest
    doctest.testmod()

