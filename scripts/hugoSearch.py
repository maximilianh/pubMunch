# exmaple file for pubtools
# illustrates how to search for text, going over words and output

# we need the regular expressions module to split text into words
# (unicode-awareness) and gzip
import re, gzip, os

# global variable
upCaseWord = re.compile("\w[A-Z0-9]+\w")

# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
headers = ["start", "end", "HgncId", "symbol"]

# the path to hugo.tab, list of hugo symbols
dataFname = os.path.join(dirname(__file__), "data", "hugo.tab.gz")

# global variable, holds the mapping KEYWORD => hugo-Id
hugoDict = {}

# this method is called ONCE on each cluster node, when the article chunk
# is opened, it fills the hugoDict variable
def startup(paramDict):
    """ parse HUGO file into dict """
    for line in gzip.open(dataFname):
        hugoId, symbol, synString = line.strip("\n").split("\t")
        hugoDict.setdefault(symbol, set()).add(hugoId)
        synonyms = synString.split(", ")
        for syn in synonyms:
            hugoDict.setdefault(symbol, set()).add(hugoId)

# this method is called for each FILE. one article can have many files
# (html, pdf, suppl files, etc). article data is passed in the object 
# article, file data is in "file". For a list of all attributes in these 
# objects please see the file ../lib/pubStore.py, search for "DATA FIELDS"
def annotateFile(article, file):
    " go over words of text and check if they are in dict "
    count = 0
    resultRows = []
    text = file.content
    for match in upCaseWord.finditer(text):
        word = match.group()
        if word in hugoDict:
            # the framework expects the FIRST TWO FIELDS to be the start and end position
            # of the match on the file. It will add the file and article identifers.
            # The results returned here should correspond to "headers" above
            count+=1
            if count>1000: # we skip files with more than 1000 genes 
                continue
            result = [ match.start(), match.end(), ",".join(hugoDict[word]), word ]
            resultRows.append(result)
    return resultRows
        
