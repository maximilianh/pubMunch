# a searcher for phosphorylation sites

# we need the regular expressions module to split text into words
# (unicode-awareness) and gzip
import re, gzip
import nltk.tokenize
from os.path import *

# FRAMEWORK
# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
headers = ["start", "end", "phosphoWord", "mutation"]

# a word that starts with phospho-
phosphoWord = re.compile(" ([Pp]hospho[a-z]+) ")
# a mutation of a peptide
mutRe = re.compile(" ([SYT][0-9]+) ")

# this method is called ONCE on each cluster node, when the article chunk
# is opened, it fills the hugoDict variable
def startup(paramDict):
    """ 
    dict format is:
    """
    pass

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

    #if " phospho" in text:
        #print "phos in text", article.pmid
    for sentStart, sentEnd in tokenizer.span_tokenize(text):
        phrase = text[sentStart:sentEnd]
        #print "phase", phrase
        #if " phospho" in phrase:
            #print "phos in phrase", phrase
	phosWords = []
	mutWords = []
        for match in phosphoWord.finditer(phrase):
		phosWords.append(match.group(1))
                #print "phosword", match.group(1)
        for match in mutRe.finditer(phrase):
		mutWords.append(match.group(1))
                #print "mutWord", match.group(1)
	if len(phosWords)!=0 and len(mutWords)!=0:
            resultRows.append( [sentStart, sentEnd, ",".join(phosWords), ",".join(mutWords)] )

    if len(resultRows)<200:
        return resultRows
