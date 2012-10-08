# exmaple file for pubtools
# illustrates how to search for text, going over words and output

# we need the regular expressions module to split text into words
# (unicode-awareness!)
import re, logging

# global variable
bandRe = re.compile(" (x|y|[1-9][0-9]?)(p|q)[0-9]+(\.[0-9]+)? ")

# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
headers = ["start", "end", "band"]

# global variable, holds the mapping KEYWORD => hugo-Id
hugoDict = {}

# this method is called ONCE on each cluster node, when the article chunk
# is opened, it fills the hugoDict variable
def startup(paramDict):
    pass

# this method is called for each FILE. one article can have many files
# (html, pdf, suppl files, etc). article data is passed in the object 
# article, file data is in "file". For a list of all attributes in these 
# objects please see the file ../lib/pubStore.py, search for "DATA FIELDS"
def annotateFile(article, file):
    " go over words of text and check if they are in dict "
    text = file.content
    count = 0
    for match in bandRe.finditer(text):
        word = match.group()
        count += 1
        if count > 1000: # we skip files with more than 1000 genes 
            logging.info("too many matches per file")
            continue
        result = [ match.start(), match.end(), word ]
        yield result
