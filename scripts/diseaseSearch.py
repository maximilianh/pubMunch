# search file for pubtools
# uses fast text search to find disease names

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
        self.headers = ["start", "end", "id"]
        self.onlyMain = True
        
        # holds the keywords, or the mapping KEYWORD => id, if 
        # ids were found
        self.lex = {}

        # ignore files with more than X matches
        self.MAXCOUNT = 2000

    def startup(self, paramDict):
        """ parse dictioary of keywords """
        if "dict" not in paramDict:
            dictFname = "/hive/data/inside/pubs/geneDisease/diseaseDictionary/malacards/dictionary.marshal.gz"
        else:
            dictFname = paramDict["dict"]
        logging.info("Reading %s" % dictFname)
        self.lex = fastFind.loadLex(dictFname)
        #print "DICT", self.lex.keys()[:10]

    def annotateFile(self, article, file):
        " go over words of text and check if they are in dict "
        resultRows = []
        #print "annotate", article, file
        text = file.content
        #print "text", text
        annots = list(fastFind.fastFind(text, self.lex, toLower=True))
        if len(annots)>self.MAXCOUNT:
            logging.info("more than %d annotations, skipping %s" % (self.MAXCOUNT, article.externalId))
            return None
        return annots
            
