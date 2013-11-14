# annotator to look for certain protein interaction keywords
import logging, maxCommon, re
from os.path import join, dirname
from collections import defaultdict


# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
headers = ["start", "end", "geneId", "keyword", "snippet"]

onlyMain = True
bestMain = True

# require that genes appear at least x times in doc
MINCOUNT = 2

geneIds = {}
# CAREFUL! RE2 is 1000 times slower for simple regexes like this than RE
# maybe due to unicode conversions?
wordRe = re.compile("[.a-zA-Z0-9-]{3,15}")

def startup(paramDict):
    global geneIds
    fname = join(dirname(__file__), "data", "wormFinder", "wormIds.tab.gz")
    geneCount = 0
    for row in maxCommon.iterTsvRows(fname):
        if row.locus!="":
            geneIds[row.locus] = row.geneId
        if row.seqId!="":
            geneIds[row.seqId] = row.geneId
        geneCount +=1
        #if row.geneId!="":
            #geneIds[row.geneId] = row.geneId
    logging.info("Loaded %d words mapped to %d genes" % (len(geneIds), geneCount))
    
def annotateFile(article, file):
    " go over words of text and check if they are in dict "
    geneToRows = defaultdict(list)
    text = file.content
    rowCount = 0
    #if file.fileType=="supp":
        #return
    if "elegans" not in text and "nematode" not in text:
        return
    for match in wordRe.finditer(text):
        start, end, word = match.start(), match.end(), match.group(0)
        #logging.debug("word %s" % word)
        if word in geneIds:
            geneId = geneIds[word]
            row =[start, end, geneId, word] 
            geneToRows[geneId].append(row)
            rowCount +=1
    if rowCount>150:
        logging.info("Too many IDs, skipping doc %s" % article.articleId)
    else:
        rows = []
        for geneId, geneRows in geneToRows.iteritems():
            if len(geneRows)>=MINCOUNT:
                rows.extend(geneRows)
        return rows



                

