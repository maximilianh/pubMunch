# example file for pubtools

# this file can search articles for a list of keywords contained in a separate
# file, one keyword per line, with an optional identifier tab-sep'ed from it.
# (=annotator)
# It can also count the number of keywords. (=a map/reduce task)

# illustrates how to mix annotator and mapper in one file so they can
# share code

# standard python libraries for regex
import re, sys, logging

# == CODE COMMON FOR ANNOTATOR AND MAP TASK 
def searchText(text, kwDict):
    """ search string for occurences of kwDict words and yield 
    tuple (start, end, word) """
    #upCaseWord = re.compile("\w[A-Z0-9]+\w")
    #for match in upCaseWord.finditer(text):
        #word = match.group()
        #if word in kwDict:
            #result = [ match.start(), match.end(), word ]
            #yield result
    text = text.upper()
    for word, wordId in kwDict.iteritems():
        word = word.upper()
        startPos = text.find(word)
        if startPos == -1:
            continue
        endPos = startPos + len(word)
        result = [ startPos, endPos, word, wordId ]
        yield result

def parseKwFile(kwFilename):
    " parse key<tab>value file and return as dict, value is optional "
    kwDict = {}
    for line in open(kwFilename):
        fields = line.strip("\n").split("\t")
        if len (fields) == 1:
            keyword = fields[0]
            id = None
            hasId = False
        else:
            keyword, id = fields[:2]
            hasId = True
        kwDict.setdefault(keyword, set()).add(id)
    return kwDict, hasId

# === ANNTOATOR ====
class Annotate:
    def __init__(self):
        # this variable has to be defined, otherwise the jobs will not run.
        # The framework will use this for the headers in table output file
        # I never write tables without headers, if I can avoid it
        self.headers = ["start", "end", "keyword"]

        # flag that indicates if keyword file contains ids
        self.hasId = False

        # holds the keywords, or the mapping KEYWORD => id, if 
        # ids were found
        self.kwDict = {}

        # let's ignore files with more than 1000 matches
        self.MAXCOUNT = 1000

    # this method is called ONCE on each cluster node, when the article chunk
    # is opened, it fills the hugoDict variable
    def startup(self, paramDict):
        """ parse dictioary of keywords """
        if not "keywords" in paramDict:
            print("You need to specify a parameter named 'keywords' with a file")
            print("containing the keywords you are planning to search")
            sys.exit(1)
        kwFilename = paramDict["keywords"]
        self.kwDict, self.hasId = parseKwFile(kwFilename)
        if self.hasId:
            self.headers.append("id")

    # this method is called for each FILE. one article can have many files
    # (html, pdf, suppl files, etc). article data is passed in the object 
    # article, file data is in "file". For a list of all attributes in these 
    # objects please see the file ../lib/pubStore.py, search for "DATA FIELDS"
    def annotateFile(self, article, file):
        " go over words of text and check if they are in dict "
        resultRows = []
        text = file.content
        count = 0
        for result in searchText(text, self.kwDict):
            if self.hasId:
                word = result[-1]
                ids = self.kwDict[word]
                idString = ",".join()
                result.append (idString)
            if count>self.MAXCOUNT: # we skip files with more than X matches
                continue
            count+=1
            resultRows.append(result)
        # the framework expects the FIRST TWO FIELDS to be the start and end position
        # of the match on the file. It will add the file and article identifers.
        # The results returned here should correspond to "self.headers" defined above
        return resultRows
            
# === MAP/REDUCE TASK ====
class FilterKeywords:
    def __init__(self):
       self.headers = ["keyword", "articleId"] 

    def startup(self, paramDict, resultDict):
        logging.debug("paramDict: %s" % paramDict)
        if "onlyAbstract" in paramDict and paramDict["onlyAbstract"]==True:
            logging.debug("Searching only abstracts")
            self.skipFiles = True

        self.maxCount = paramDict.get("maxCount", 0)
        kwFilename = paramDict.get("keywordFile", None)
        if kwFilename!=None:
            self.kwDict, hasId = parseKwFile(kwFilename)
        else:
            self.kwDict = paramDict.get("keywordDict")
        logging.debug("keywords are: %s" % str(self.kwDict))

    def map(self, articleInfo, fileInfo, text, resultDict):
        if text==None:
            text = articleInfo.title + articleInfo.abstract
        matches = list(searchText(text, self.kwDict))
        for start, end, word, wordId in matches:
            logging.debug("found: %s" % word)
            resultDict.setdefault(wordId, []).append(articleInfo.articleId)

    def end(self, data):
        logging.debug("data: %s" % data)

    # this is called after all jobs are finished, on the main machine
    # it is called once for each key
    def reduce(self, word, fileIds):
        if self.maxCount==0 or (self.maxCount!=0 and len(fileIds) < self.maxCount):
            #return word, len(set(fileIds))
            for fileId in fileIds:
                yield word, fileId
