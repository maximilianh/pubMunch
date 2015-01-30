# a tagger that uses sentences with two proteins and runs them through the MSR
# protein interaction pipeline

import pubGeneric, pubNlp, pubAlg, pubConf
import maxCommon
import logging, subprocess
import geneFinder
from os.path import join
from collections import namedtuple
from datetime import datetime

pubConf.debug=True

# path of the MSR binary
msrDir = join(pubConf.extToolDir, "msr")

# path of the java binary
#javaDir = "/usr/bin"
#javaDir = "/hive/users/max/software/jre1.7.0/bin"

textHeaders =  ["pmid", "section", "start", "end", "text", "geneDesc"]

msrFields = ["chunkSentId","sentId", "eventType","triggerTokenId","themeProteinType","themeProteinHgncIds","themeProtein","themeTokenId","themeTokenStart","themeTokenEnd","themeEventPath","causeProteinType","causeProteinHgncIds","causeProtein","causeTokenId","causeTokenStart","causeTokenEnd","causeEventPath","sentence"]

MsrRec = namedtuple("msrRec", msrFields)

def parseMsrOut(tempFnameOut):
    logging.info("Parsing MSR output")
    isEvent = None
    inData = open(tempFnameOut).read()
    logging.debug("full msr file "+str(inData))
    for line in inData.splitlines():
        logging.debug(line)
        if line.startswith("</event"):
            isEvent = False
            continue
        elif line.startswith("<event"):
            isEvent = True
            continue
        elif not isEvent:
            continue

        fields = line.rstrip("\n").split("\t")
        fields[0] = fields[0].replace('pmid="','').strip('"')
        rec = MsrRec(*fields)
        logging.debug("msr rec"+str(rec))
        yield rec

def writeMsrIn(sentRows):
    """
    """
    ofh, tempFnameIn = pubGeneric.makeTempFile("msrNlpIn", ".txt")
    #tempFnameIn = "crash5.xml"
    #ofh = open(tempFnameIn, "w")
    # write sentences to temp file
    logging.info("Writing input sentences to %s" % tempFnameIn)
    for i, row in enumerate(sentRows):
        text = row[-2]
        ofh.write('<txt pmid="%d">\n' % i)
        ofh.write("%s\n" % text)
        ofh.write('</txt>\n\n')
    ofh.flush()
    logging.debug("in file: %s" % open(tempFnameIn).read())
    return ofh, tempFnameIn

class MsrRunner:
    def __init__(self):
        self.headers = textHeaders
        self.headers.extend(msrFields)
        self.rows = []

    def processRow(self, row):
        if row.section!="supplement":
            self.rows.append(row) 

    def allResults(self):
        """ given a list of rows with sentences as their -1 field, run these through
        the MSR pipeline 
        """
        tstart = datetime.now()
        inFh, tempFnameIn = writeMsrIn(self.rows)
        logging.info("Running MSR pipeline on %d sentences" % len(self.rows))
        #logging.info("Running MSR pipeline on %s " % sentences)
        ofh2, tempFnameOut = pubGeneric.makeTempFile("msrNlpOut", ".txt")

        cmd = "%s/runMsr.sh %s %s" % (msrDir, tempFnameIn, tempFnameOut)
        maxCommon.runCommand(cmd)

        joinedRows = []
        logging.info("Parsing MSR output")
        for msrRow in parseMsrOut(tempFnameOut):
            textRow = list(self.rows[int(msrRow.chunkSentId)])
            textRow.extend(msrRow)
            joinedRows.append(textRow)
        inFh.close()
        ofh2.close()
        logging.debug("results " + repr(joinedRows))

        tend = datetime.now()
        secs = (tend-tstart).seconds
        logging.info("msr runtime: %d" % secs)
        return joinedRows

class splitSent:
    """
    a tagger that chunks input text into sentences with at least two
    genes/proteins each. Output files are limited to 200 sentences each.
    """
    def __init__(self):
        #self.headers = ["section", "sentence", "start", "end", "genes"]
        retFields = textHeaders
        #retFields.extend(msrFields)

        self.headers = retFields
        self.preferXml = True # only run on one main text file and prefer XML files
        #self.preferPdf = True # only run on one main text file and prefer PDF files
        self.sentData = []

    def startup(self, paramDict):
        " called once upon startup on each cluster node "
        geneFinder.initData(exclMarkerTypes=["dnaSeq"])
        self.rowCount = 0

    def annotateFile(self, article, file):
        text = file.content
        pmid = article.pmid
        if file.fileType=="supp":
            return
        for row in pubNlp.sectionSentences(text, file.fileType):
            section, sentStart, sentEnd, text = row
            tokens = text.split()
            if len(tokens)<6:
                logging.debug("Sentence too short: %d tokens" % len(tokens))
                continue
            if len(tokens)>40:
                logging.debug("Sentence too long: %d tokens" % len(tokens))
                continue
            if len(text)<20:
                logging.debug("Sentence too short: %d characters" % len(text))
                continue
            if len(text)>1000:
                logging.debug("Sentence too long: %d characters" % len(text))
                continue
            if text.count('"') > 20 or text.count(",")>20:
                logging.debug("Too many strange characters")
                continue

            genes = list(geneFinder.findGeneNames(text))
            if len(genes) < 2:
                continue
            if len(genes) > 20:
                logging.debug("Too many genes, %d" % len(genes))
                continue
            geneDescs = ["%d-%d/%s/%s/%s" % (start,end,text[start:end],name,gid) \
                for start,end,name,gid in genes]
            geneDesc = "|".join(geneDescs)

            row = [pmid, section, start, end, text, geneDesc]
            yield row
            self.rowCount += 1
            if self.rowCount % 200 == 0:
                yield [] # tell caller to start a new output file
            
            #self.sentData.append(row)
            #logging.info("inData "+str(row))

    #def dontRunThis(self):
        #" run at the end of the cluster job "
        #rows = runMsr(testData)
        #logging.info(rows)

        #if len(self.sentData)==0:
            #return

        #sentData = self.sentData[:100]
        #rows = runMsr(sentData)
        # debugging code: run each sentence in a separate instance - ARGH!
        #for sentData in self.sentData:
            #s = [sentData]
            #rows = runMsr(s)
        #logging.debug(rows)
        #return rows
        
#parseMsrOut("../ext/msr/test3.out")
