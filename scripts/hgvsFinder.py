# exmaple file for pubtools
# illustrates how to search for text, going over words and output

# we need the regular expressions module to split text into words
# (unicode-awareness!)
import re, logging

# global variable
refseq = "(?P<gene>[XYNAZ][PRM]_[0-9]{4,11}([.][0-9]{1,2}))"
hgnc = "(?P<gene>[A-Zorf]{1,6})"
uniprot = r'(?P<gene>[A-NR-ZOPQ][0-9][A-Z0-9][A-Z0-9][A-Z0-9][0-9])'
pdbRe = r'(?P<gene>[0-9][a-zA-Z][a-zA-Z][a-zA-Z])' 
ensembl = r'(?P<gene>ENS([A-Z]{3})?[GPT][0-9]{9,14})'
gene = "(%s|%s|%s|%s|%s)" % (refseq, hgnc, uniprot, pdbRe, ensembl)

nuclSub = "([cr][.][0-9]{1,6}[actguACTGU]>[actguACTGU])"
protSub = "(p[.][A-Za-z]{1,3}[0-9]{1,5}[A-Za-z]{1,3})"
delDupInsInv = "([crp][.][a-zA-Z]{1,3}[0-9]{1,6}(del|dup|ins|inv)[a-zA-Z]{1,3})"
ddivr = re.compile(delDupInsInv)
sub = "(%s|%s)" % (nuclSub, protSub)

#pr = re.compile(protSub)
#mutr = re.compile(mut)
#nr = re.compile(nuclSub)
#gr = re.compile(gene)
#hgr = re.compile(hgnc)
#rsr = re.compile(refseq)
#print subMutStr

subMutStr = "[,.; -]%(refseq)s[ :]*%(sub)s[,.; -]" % locals()
subMutRe = re.compile(subMutStr)

# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
headers = ["start", "end", "mut"]

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
    for match in subMutRe.finditer(text):
        word = match.group()
        count += 1
        if count > 1000: # we skip files with more than 1000 genes 
            logging.info("too many matches per file")
            continue
        result = [ match.start(), match.end(), word ]
        yield result
