# exmaple file for pubtools
# illustrates how to search for text, going over words and output

# we need the regular expressions module to split text into words
# (unicode-awareness) and gzip
import gzip, os
from os.path import dirname
from re2 import compile

# global variable
symPat = "\w|[,.;]([A-Z][A-Za-z0-9-]{3,10})"
symRe = compile(symPat)

noDotPat = "([^.]+)"
pats = {"def" : [
    "%s is responsible for %s",
    "%s is defined as",
    "%s is a %s",
    "%s is the %s",
    "%s is an %s"
    "%s was identified as %s"
    "%s encodes %s"
    "%s is necessary for %s"
    "%s serves as %s"
    "shown that %s %s"
    "%s is the gene %s"
    "%s is essential for %s"
    "%s plays a (crucial|important) role in %s"
    "%s functions as %s"
    "%s is known to cause %s"
    "%s is required for %s"
    "shown that %s is %s"
    "%s was shown to be %s"
    "identified %s as %s"
    "%s promotes %s"
    "%s has an important role in %s"
    ],

    "expr" : ["%s (mRNA )?is expressed %s", "%s expression is observed %s", "%s expression was observed %s", "expression of %s is %s", "overexpression of %s occurs in %s", "%s is commonly overexpressed %s", "%s expression covers %s", "%s is first expressed %s", "%s is widely expressed %s"],
    "ubiq" : ["%s is ubiquinated %s"],
    "met" :  ["%s is methylated %s"],
    "acet" : ["%s is acetylated by %s"],
    "phospho" :  ["%s is phosphorylated  %s"],
    "reg" : ["%s regulates %s", "%s controls %s", "%s modulates %s"],
    "isReg" : ["%s is controlled by %s", "%s inhibits %s"],
    "marker" : ["%s is a marker for %s", "%s as a marker for %s"],
    "transmem" : ["%s transmembrane %s", "%s has a transmembrane %s"],
    "secr" : ["%s is secreted %s", "%s is released %s", "%s can also be secreted %s", "extracellular %s %s", "extracellular role of %s %s", "extracellular release of %s %s"]
    }

# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
headers = ["start", "end", "HgncId", "symbol"]

# the path to hugo.tab, list of hugo symbols
dataFname = os.path.join(dirname(__file__), "data", "hugo.tab.gz")

# global variable, holds the mapping KEYWORD => off symbol
hugoDict = {}

# this method is called ONCE on each cluster node, when the article chunk
# is opened, it fills the hugoDict variable
def startup(paramDict):
    """ parse HUGO file into dict """
    for line in gzip.open(dataFname):
        hugoId, symbol, synString = line.strip("\n").split("\t")
        hugoDict.setdefault(symbol, set()).add(hugoId)
        #synonyms = synString.split(", ")
        hugoDict[symbol] = symbol
        #for syn in synonyms:
            #if syn=='':
                #continue
            #if syn in hugoDict:
                #print "ignoring", syn
                #del hugoDict[syn] # remove non-unique symbols
            #hugoDict[syn].add(symbol)
    reDict = gene

def annotateFile(article, file):
    " go over words of text and check if they are in dict "
    count = 0
    resultRows = []
    text = file.content
    phrases = text.split(". ")
    for match in symRe.finditer(text):
        word = match.group()
        word = word.replace("-","")
        word = word.upper()
        if word in hugoDict:
            count+=1
            if count>150: # we skip files with too many genes
                continue
            result = [ match.start(), match.end(), ",".join(hugoDict[word]), word ]
            resultRows.append(result)
    return resultRows
        
