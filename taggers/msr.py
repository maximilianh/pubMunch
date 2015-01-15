# two taggers that have to be run consecutively:
# - splitSent to find sentences with two proteins and write them to a directory in chunks
#   of around 15kb
# - another one to run these sentences through the MSRNLP pipeline

import pubGeneric, pubNlp, pubAlg
import logging
import geneFinder

chunkSize = 16000 # maximum size of output chunks

class splitSent:
    """
    a tagger that chunks input text into sentences with at least two
    genes/proteins each.  
    """
    def __init__(self):
        self.headers = ["section", "sentence", "start", "end", "genes"]
        self.preferXml = True # only run on one main text file and prefer XML files
        #self.preferPdf = True # only run on one main text file and prefer PDF files

    def startup(self, paramDict):
        " called once upon startup on each cluster node "
        pubNlp.initCommonWords()
        geneFinder.initData(exclMarkerTypes=["dnaSeq"])

    def annotateFile(self, article, file):
        text = file.content
        for row in pubNlp.sectionSentences(text, file.fileType):
            text = row[-1]
            genes = list(geneFinder.findGeneNames(text))
            if len(genes) < 2:
                continue
            geneDescs = ["%d-%d/%s/%s/%s" % (start,end,text[start:end],name,gid) for start,end,name,gid in genes]
            geneDesc = "|".join(geneDescs)
            row = list(row)
            row.append(geneDesc)
            yield row
