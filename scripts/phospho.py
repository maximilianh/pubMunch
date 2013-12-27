# a searcher for phosphorylation sites

# we need the regular expressions module to split text into words
# (unicode-awareness) and gzip
import re, gzip, logging
import nltk.tokenize
from os.path import *

import geneFinder

# FRAMEWORK
# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
headers = ["start", "end", "pmid", "protein", "aminoAcid", "position", "phosphoWords"]

# a word that starts with phospho-
phosphoWordRe = re.compile(" ([Pp]hospho[a-z]+) ")
# a serine and a position
siteRes = [re.compile(" (?P<aa>[SYT])(?P<pos>[0-9]+) "), \
re.compile(" (?P<aa>[sS]erines?|Ser|[tT]hreonines?|Threo|Thr|[tT]yrosines?|Tyr)([ -]|at residue)*(?P<pos>[0-9]+)")]

# this method is called ONCE on each cluster node, when the article chunk
# is opened, it fills the hugoDict variable
def startup(paramDict):
    """ 
    """
    global seqCache
    # don't use seqs for gene finding
    geneFinder.initData(exclMarkerTypes=["dnaSeq"])
    #seqCacheFname = join(dirname(inFname), "seqCache.gdbm")
    #logging.debug("Opening seqCache %s" % seqCacheFname)
    #seqCache = gdbm.open(seqCacheFname, "w")

def findMatches(reDict, text):
    for reType, reObj in reDict.iteritems():
        for match in reObj.finditer(text):
            yield reType, match

def matchesAny(string, reList):
    for reObj in reList:
        if reObj.match(string):
            return True
    return False

# this method is called for each FILE. one article can have many files
# (html, pdf, suppl files, etc). Article data is passed in the object 
# article, file data is in "file". For a list of all attributes in these 
# objects please see the file ../lib/pubStore.py, search for "DATA FIELDS"
def annotateFile(article, file):
    " go over words of text and check if they are in dict "
    resultRows = []
    text = file.content

    tokenizer = nltk.tokenize.RegexpTokenizer(r'[.:?!] ', gaps=True)
    #tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
    resultRows = []

    # search for genes in text
    genes, geneSupp = geneFinder.rankGenes(text, pmid=article.pmid)
    if len(genes)==0:
        logging.warn("No gene found")
        return

    # resolve to uniprot IDs
    uniprotIds = []
    topGenes = [genes[0][0]]
    for entrezId in topGenes:
        upIds = geneFinder.entrezToUp.get(entrezId, None)
        if upIds==None:
            logging.warn("cannot map %s to uniprot" % str(entrezId))
            continue
        uniprotIds.extend(upIds)

    if len(uniprotIds)==0:
        uniprotIds = ["UNKNOWN"]
    #if len(uniprotIds)!=1:
        #logging.warn("more than one uniprot ID found, skipping text")
        #return

    # now find sites in text
    for sentStart, sentEnd in tokenizer.span_tokenize(text):
        phrase = text[sentStart:sentEnd]
	phosWords = []
        for match in phosphoWordRe.finditer(phrase):
		phosWords.append(match.group(1))

        for siteRe in siteRes:
            for match in siteRe.finditer(phrase):
                aa = match.group("aa")
                sitePos = match.group("pos")
                row =[sentStart, sentEnd, article.pmid, ",".join(uniprotIds), aa, sitePos, ",".join(phosWords)]
                resultRows.append(row)

    #if len(resultRows)<200:
    return resultRows
