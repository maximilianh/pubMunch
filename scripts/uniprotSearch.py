# very, very simple search for uniprot accessions
# has many false positives, look into markerSearch.py for a better version

# we need the regular expressions module to split text into words
import re, logging

# global variable
upAccRe = re.compile("[\s.,;()-][A-NR-ZOPQ][0-9][A-Z0-9][A-Z0-9][A-Z0-9][0-9][\s.,;()-]")

MAXCOUNT = 100

# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
headers = ["start", "end", "accession"]

# this method is called for each FILE. one article can have many files
# (html, pdf, suppl files, etc). article data is passed in the object 
# article, file data is in "file". For a list of all attributes in these 
# objects please see the file ../lib/pubStore.py,  "DATA FIELDS"
def annotateFile(article, file):
    " go over words of text and check if they are in dict "
    text = file.content
    count = 0
    for match in upAccRe.finditer(text):
        word = match.group()
        count += 1
        if count > MAXCOUNT: # we skip files with more than 100 accession Ids 
            logging.info("too many matches per file")
            continue
        result = [ match.start(), match.end(), word ]
        yield result
