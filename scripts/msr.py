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

    def startup(self, paramDict):
        " called once upon startup on each cluster node "
        pass

    def annotateFile(self, article, file):
        text = file.content
        for secStart, secEnd, section in pubNlp.sectionSplitter(text, file.fileType):
            if section=="refs":
                logging.info("Skipping ref section %d-%d" % (secStart, secEnd))
                continue

            secText = text[secStart:secEnd]
            for sentStart, sentEnd, sentence in pubNlp.sentSplitter(secText):
                if sentEnd-sentStart < 30:
                    logging.debug("Sentence too short: %s" % sentence)
                    continue
                yield [section, secStart+sentStart, secStart+sentEnd, sentence]
