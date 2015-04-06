# a tagger for the GNAT webservice
import urllib, urllib2, logging

headers = ["gnatStart", "gnatEnd", "taxIds", "entrezIds", "gnatWord"]

def gnatOnText(text):
    url = 'http://bergman.smith.man.ac.uk:8081/'
    species = "9606,10090"
    #data = urllib.urlencode({'text' : text, 'species' : species, 'task':"gnorm"})
    data = urllib.urlencode({'text' : text, 'species' : species, 'task':"gner"})
    resp = urllib2.urlopen(url=url, data=data).read()
    # example output:
    # ['UnknownId', 'UserQuery', 'gocode', '-', '60047', '37', '53', 'heart contraction', '-']
    # ['UnknownId', 'UserQuery', 'gene', '10090;10090;9606;9606', '22059;22060;352997;7157', '0', '2', 'p53', '1.0']
    if len(resp.strip())==0:
        logging.warn("empty response from GNAT")

    for line in resp.splitlines():
        #print line
        gnatRow = line.split("\t")
        docId, docType, entType, taxonIds, geneIds, startPos, endPos, snippet, score = gnatRow
        if entType!="gene":
            continue
        
        row = (startPos, endPos, taxonIds, geneIds, snippet)
        yield row

def annotateFile(article, file):
    """ interface for pubRun to get annotation lines for text 
    >>> list(gnatOnText("p53 gene on chromosome 21 related to heart contraction"))
    [('0', '2', '10090;10090;9606;9606', '22059;22060;352997;7157')]
    """
    text = file.content
    text = text.encode("latin1", errors="replace")
    logging.info("Running %s through GNAT webservice" % article.externalId)
    lcount = 0
    for row in gnatOnText(text):
        #print row
        yield row
        lcount +=1
    logging.info("Got %d rows" % lcount)


# ----- 
if __name__ == "__main__":
    import doctest
    doctest.testmod()

