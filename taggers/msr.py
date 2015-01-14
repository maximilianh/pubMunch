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
        pubNlp.initCommonWords()

    def annotateFile(self, article, file):
        text = file.content
        for secStart, secEnd, section in pubNlp.sectionSplitter(text, file.fileType):
            if section=="refs":
                logging.info("Skipping ref section %d-%d" % (secStart, secEnd))
                continue

            print secStart, secEnd, section
            secText = text[secStart:secEnd]
            for sentStart, sentEnd, sentence in pubNlp.sentSplitter(secText):
                if sentEnd-sentStart < 30:
                    logging.debug("Sentence too short: %s" % sentence)
                    continue

                sentWords = pubNlp.wordSet(sentence)
                if len(sentWords)<5:
                    logging.debug("Sentence skipped, too few words: %s" % sentence)
                    continue

                commSentWords = sentWords.intersection(pubNlp.commonWords)
                if len(commSentWords)==0:
                    logging.debug("Sentence skipped, no common English word: %s" % sentence)
                    continue
                    
                nlCount = sentence.count("\n")
                if nlCount > 10:
                    logging.debug("Sentence spread over too many lines: %s" % sentence)
                    continue

                spcCount = sentence.count(" ")
                if spcCount < 4:
                    logging.debug("Sentence has too few spaces: %s" % sentence)
                    continue
                sentence = sentence.replace("\n", " ")
                yield [section, nlCount, secStart+sentStart, secStart+sentEnd, sentence]
