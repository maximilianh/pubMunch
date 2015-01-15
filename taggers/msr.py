# two taggers that have to be run consecutively:
# - splitSent to find sentences with two proteins and write them to a directory in chunks
#   of around 15kb
# - another one to run these sentences through the MSRNLP pipeline

import pubGeneric, pubNlp, pubAlg
import logging

chunkSize = 16000 # maximum size of output chunks

class splitSent:
    """
    a tagger that chunks input text into sentences with at least two
    genes/proteins each.  
    """
    def __init__(self):
        self.headers = ["section", "sentence", "start", "end"]
        self.preferXml = True # only run on one main text file and prefer XML files
        #self.preferPdf = True # only run on one main text file and prefer PDF files

    def startup(self, paramDict):
        " called once upon startup on each cluster node "
        pubNlp.initCommonWords()

    def annotateFile(self, article, file):
        text = file.content
        for res in pubNlp.sectionSentences(text, file.fileType):
            yield res
