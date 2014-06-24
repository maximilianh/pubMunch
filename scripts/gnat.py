# a tagger for the GNAT webservice
import urllib, urllib2

headers = ["start", "end", "taxIds", "entrezIds"]

def gnatOnText(text):
    url = 'http://bergman.smith.man.ac.uk:8081/'
    species = "9606,10090"
    data = urllib.urlencode({'text' : text, 'species' : species, 'task':"gnorm"})
    content = urllib2.urlopen(url=url, data=data).read()
    # example output:
    # ['UnknownId', 'UserQuery', 'gocode', '-', '60047', '37', '53', 'heart contraction', '-']
    # ['UnknownId', 'UserQuery', 'gene', '10090;10090;9606;9606', '22059;22060;352997;7157', '0', '2', 'p53', '1.0']
    for line in content.splitlines():
        gnatRow = line.split("\t")
        docId, docType, entType, taxonIds, geneIds, startPos, endPos, snippet, score = gnatRow
        if entType!="gene":
            continue
        
        row = (startPos, endPos, taxonIds, geneIds)
        yield row

def annotateFile(article, file):
    """ interface for pubRun to get annotation lines for text 
    >>> list(gnatOnText("p53 gene on chromosome 21 related to heart contraction"))
    [('0', '2', '10090;10090;9606;9606', '22059;22060;352997;7157')]
    """
    text = file.content


# ----- 
if __name__ == "__main__":
    import doctest
    doctest.testmod()

