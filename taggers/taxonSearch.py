# search file for pubtools
# uses fast text search to find taxon names

# standard python libraries for regex
import re, sys, logging, gzip
from os.path import *
# our fast finder
import fastFind

# == CODE COMMON FOR ANNOTATOR AND MAP TASK 

# === ANNTOTATOR ====
class Annotate:
    def __init__(self):
        # this variable has to be defined, otherwise the jobs will not run.
        self.headers = ["start", "end", "type", "id"]

        # holds the keywords, or the mapping KEYWORD => id, if 
        # ids were found
        self.lex = {}

        # ignore files with more than 20 matches
        self.MAXCOUNT = 20

    def startup(self, paramDict):
        """ parse dictioary of keywords """
        dictFname = join(dirname(__file__), "data/speciesDict.marshal.gz")
        logging.info("Reading %s" % dictFname)
        self.lex = fastFind.loadLex(dictFname)
        #print "DICT", self.lex.keys()[:10]

    def annotateFile(self, article, file):
        " go over words of text and check if they are in dict "
        resultRows = []
        if len(article.abstract)==0:
            logging.info("No abstract for %s" % article.externalId)
            return None
        #print "annotate", article, file
        text = file.content
        #print "text", text
        annots = list(fastFind.fastFind(text, self.lex, toLower=False))
        if len(annots)>self.MAXCOUNT:
            logging.info("more than %d annotations, skipping %s" % (self.MAXCOUNT, article.externalId))
            return None
        return annots
            
# === MAP/REDUCE TASK ====
class FilterKeywords:
    def __init__(self):
       self.headers = ["keyword"] 

    def startup(self, paramDict, resultDict):
        self.maxCount = paramDict["maxCount"]
        kwFilename = paramDict["keywords"]
        #self.kwDict, hasId = parseKwFile(kwFilename)

    def map(self, article, file, text, resultDict):
        matches = list(searchText(text, self.kwDict))
        for start, end, word in matches:
            resultDict.setdefault(word, set()).add(file.fileId)

    # this is called after all jobs are finished, on the main machine
    # it is called once for each key
    def reduce(self, word, fileIds):
        if len(fileIds) < self.maxCount:
            return word, len(fileIds)
