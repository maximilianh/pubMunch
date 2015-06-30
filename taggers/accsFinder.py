from pubFindAccessions import AccsFinder

# find accession numbers in text

headers = ["start", "end", "accType", "acc"]

accFind = None

# this method is called once on each cluster node
def startup(paramDict):
    global accFind
    accFind = AccsFinder()

# this method is called for each FILE. one article can have many files
# (html, pdf, suppl files, etc). article data is passed in the object 
# article, file data is in "file". For a list of all attributes in these 
# objects please see the file ../lib/pubStore.py, "DATA FIELDS"
def annotateFile(article, file):
    " go over words of text and check if they are in dict "
    text = file.content
    count = 0
    for match in accFind.findAccessions(text):
        yield match
