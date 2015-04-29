# example file for pubtools
# general regex searcher

# options:
# "re" : regular expression string

# make sure you have the keyword itself within ()
# example regex: [ ,;.-](NCT[0-9]{8})[ ,;.-]
# note the separators will get stripped off, thanks to the ( ) grouping

# standard python libraries for regex
import re, sys, logging

# == CODE COMMON FOR ANNOTATOR AND MAP TASK 
def findRe(text, reObj):
    for match in reObj.finditer(text):
        logging.debug("MATCH"+match.group(0))
        word = match.group()
        result = [ match.start(), match.end(), word ]
        yield result

# === ANNTOATOR ====
class Annotate:
    def __init__(self):
        # this variable has to be defined, otherwise the jobs will not run.
        # The framework will use this for the headers in table output file
        # I never write tables without headers, if I can avoid it
        self.headers = ["start", "end", "reMatch"]

        # holds the regex
        self.reObj = {}

    # this method is called ONCE on each cluster node, when the article chunk
    # is opened, it fills the hugoDict variable
    def startup(self, paramDict):
        """ parse dictioary of keywords """
        reStr = paramDict["re"]
        self.reObj = re.compile(reStr)

    # this method is called for each FILE. one article can have many files
    # (html, pdf, suppl files, etc). article data is in the object 
    # article, file data is in "file". For a list of all attributes in these 
    # objects please see the file ../lib/pubStore.py, search for "DATA FIELDS"
    def annotateFile(self, article, file):
        " go over words of text and check if they are in dict "
        text = file.content
        annots = list(findRe(text, self.reObj))
        for result in annots:
            yield result
            
# === MAP/REDUCE TASK ====
class Map:
    def __init__(self):
       self.headers = ["externalId", "hasMatch"] 

    def startup(self, paramDict, resultDict):
        reStr = paramDict["re"]
        self.reObj = re.compile(reStr)
        logging.debug("Got regex: %s" % reStr)

    def map(self, article, file, text, resultDict):
        matches = findRe(text, self.reObj)
        if resultDict.get(article.externalId, None)=='1':
            return
        resultDict[article.externalId] = str(0)
        for result in matches:
            resultDict[article.externalId] = str(1)
            break

    # this is called after all jobs are finished, on the main machine
    # it is called once for each key
    def reduce(self, articleId, resultList):
        yield articleId, resultList[0]
