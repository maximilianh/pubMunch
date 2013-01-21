# search for gene names as defined by uniprot
import re, gzip, os, logging
from fastFind import fastFind, loadLex

# this variable has to be defined, otherwise the jobs will not run.
# The framework will use this for the headers in table output file
headers = ["start", "end", "uniProtAcc", "idType", "word"]

# the path to the dictionary of gene names
dataFname = os.path.join(os.path.dirname(__file__), "data", "uniProt.marshal.gz")

# dictionary
lex = None

# this method is called ONCE on each cluster node, when the article chunk
# is opened, it fills the hugoDict variable
def startup(paramDict):
    """ parse HUGO file into dict """
    global lex
    lex = loadLex(dataFname)

def getAcronyms(text):
    """
    find acryonym defitinions in text that look like "minor allele frequency (MAF)"
    >>> getAcronyms("With a minor allele frequency (MAF) of 0.4, our sample size had")
    {'MAF': 'minor allele frequency'}
    """
    words = text.split()
    brackWordPos = [pos for pos, w in enumerate(words) if w.startswith("(") and w.endswith(")")]
    res = {}
    for brackPos in brackWordPos:
        brackWord = words[brackPos]
        acro = brackWord.strip("()")
        backStep = len(acro)
        if brackPos > backStep:
            prevWords = words[brackPos-backStep:brackPos]
            predAcro = "".join([w[0] for w in prevWords]).upper()
            if predAcro==acro:
                res[predAcro] = " ".join(prevWords)
    return res

def getAuthorAcronyms(auStr):
    """ 
    return dict with acronyn -> name 
    >>> getAuthorAcronyms("Kittrell, Frances S; Malur, Sabine")
    {'Sabine': 'Malur, Sabine', 'Kittrell': 'Kittrell, Frances S', 'FSK': 'Kittrell, Frances S', 'Frances': 'Kittrell, Frances S', 'SM': 'Malur, Sabine', 'Malur': 'Malur, Sabine'}
    """
    if len(auStr)==0:
        return {}

    res = {}
    for autName in auStr.split(";"):
        autName = autName.strip()
        if not "," in autName:
            continue
        auParts = autName.split(",")
        if len(auParts)!=2:
            continue
        fam, first = auParts
        res[fam] = autName
        firstParts = first.split()
        for fp in firstParts:
            if len(fp)>2:
                res[fp] = autName
        if len(fam)==0:
            continue
        acro = "".join([f[0] for f in firstParts]) + fam[0]
        res[acro] = autName
    return res
                
def annotateFile(article, file):
    " go over words of text and check if they are in dict "
    text = file.content
    rows = fastFind(text, lex, wordRegex=r"[\w'_-]+")
    autAcros = getAuthorAcronyms(article.authors)
    acros = getAcronyms(text)

    #print rows
    if len(rows)>200:
        logging.info("%d gene matches in file (>200), skipping article %s/file %s, fileType %s" % (len(rows), article.externalId, file.fileId, file.fileType))
        return []

    newRows = []
    for start, end, id in rows:
        word = text[start:end]
        if word in autAcros:
            logging.debug("Ignoring %s: looks like author acronym for author %s" % (word, autAcros[word]))
            continue

        if word in acros:
            logging.debug("Ignoring %s: looks like an acronym defined in the text for %s" % (word, acros[word]))
            continue

        type = "name"
        if id.startswith("*"):
            id = id.lstrip("*")
            type = "acc"
        newRows.append((start, end, id, type, word))
    return newRows

if __name__=="__main__":
    import doctest
    doctest.testmod()
