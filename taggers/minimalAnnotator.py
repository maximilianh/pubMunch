# exmaple file for pubtools
# minimal annotator

# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
headers = ["start", "end", "text"]

# this indicates that we don't want to run on PDF files if there is an XML
# file for the main text
# relevant for PMC and crawler articles at the moment
preferXml = True

# this method is called for each FILE. one article can have many files
# (html, pdf, suppl files, etc). article data is passed in the object 
# article, file data is in "file". For a list of all attributes in these 
# objects please see the file ../lib/pubStore.py, search for "DATA FIELDS"
def annotateFile(article, file):
    " go over words of text and check if they are in dict "
    text = file.content
    count = 0
    result = [ 0, 1, file ]
    yield result
