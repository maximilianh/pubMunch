# annotator to look for a few drug names
import logging, maxCommon, re
from os.path import join, dirname
from collections import defaultdict


# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
headers = ["start", "end", "drugName", "snippet"]

onlyMain = True
preferPdf = True

drugNames = {}

def startup(paramDict):
    global geneIds
    #fname = join(dirname(__file__), "data", "wormFinder", "wormIds.tab.gz")
    fname = "/cluster/home/max/projects/pubs/analysis/drugs/sittlerDrugs.txt"
    for line in open(fname):
        drugName = line.strip()
        syns = drugName.split("/")
        for syn in syns:
            synRe = re.compile(syn)
            drugNames[syn] = (synRe, drugName)

            syn= syn.replace(" ", "-")
            synRe = re.compile(syn)
            drugNames[syn] = (synRe, drugName)

            syn= syn.replace(" ", "")
            synRe = re.compile(syn)
            drugNames[syn] = (synRe, drugName)
    logging.info("Loaded %d drug names" % (len(drugNames)))
    
#wordRe = re.compile("[.a-zA-Z0-9-]{3,15}")

def annotateFile(article, file):
    " go over words of text and check if they are in dict "
    rows = []
    text = file.content
    rowCount = 0
    #if file.fileType=="supp":
        #return
    if "drug" not in text and "compound" not in text and "inhibitor" not in text and "agent" not in text:
        return
    #for match in wordRe.finditer(text):
    for syn, reAndDrugName in drugNames.iteritems():
        #logging.debug("word %s" % word)
        synRe, drugName = reAndDrugName
        if syn in text:
            for match in synRe.finditer(text):
                start, end, word = match.start(), match.end(), match.group(0)
                row =[start, end, drugName]
                rows.append(row)
                rowCount +=1
    if rowCount>150:
        logging.info("Too many IDs, skipping doc %s" % article.articleId)
    else:
        return rows



                

