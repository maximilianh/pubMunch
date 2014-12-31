# a few convenience functions to query the new crossref API

import urllib, urllib2, json, doctest, logging
import maxCommon

#def dois(issn):
#    queryStr = ", ".join(queryFields)
#    queryData = {"q" : queryStr, "pages" : "1", "rows" : 1}
#    urlParams = urllib.urlencode(queryData)
#    url = "http://search.crossref.org/dois?" + urlParams
#    # send request
#    jsonStr = urllib2.urlopen(url).read()
#    xrdata = json.loads(jsonStr)
#    # parse result
#    if len(xrdata)==0:
#        logging.debug("Empty cross reply")
#        return None

def lookupDoi(metaInfoDict, repeatCount=2, delaySecs=5):
    """ take author, vol, journal etc from metaInfoDict, query crossref 'links' and return DOI if found 

    >>> lookupDoi({"authors":"M. Henrion, D. J. Mortlock, D. J. Hand, and A. Gandy", "title":"A Bayesian approach to star-galaxy classification", "journal":"Monthly Notices of the Royal Astronomical Society", "vol":"414", "issue":"4", "page":"2286", "year":"2011", "printIssn" : ""})
    u'10.1111/j.1365-2966.2010.18055.x'
    """

    # construct url
    mid = metaInfoDict
    logging.debug("Looking up DOI for article %s, %s with crossref links api" % (mid["authors"], mid["title"]))
    freeFormCitFields = [mid["authors"], '"%s"' % mid["title"], mid["journal"],mid["year"], "vol. "+mid["vol"], "no. "+ mid["issue"], "pp. "+mid["page"],  mid["printIssn"]]
    freeFormCitStr = ", ".join(freeFormCitFields)
    queryData = {"q" : freeFormCitStr}
    url = "http://search.crossref.org/links?" 
    jsonParam = json.dumps([freeFormCitStr])
    logging.debug("JSON string %s" % jsonParam)
    queryParam = {"q" : jsonParam}

    # send request
    httpResp = maxCommon.retryHttpRequest(url, jsonParam, delaySecs=delaySecs, repeatCount=repeatCount)
    if httpResp==None:
        logging.debug("HTTPError while sending crossref request")
        return None

    jsonStr = httpResp.read()
    xrdata = json.loads(jsonStr)

    # parse result
    if len(xrdata)==0:
        logging.debug("Empty cross reply")
        return None

    if not xrdata["query_ok"]:
        logging.debug("Query error from crossref")
        return None
    elif "results" not in xrdata or len(xrdata["results"])<1:
        logging.debug("no results in crossref reply")
        return None

    firstRes = xrdata["results"][0]
    if not firstRes["match"]:
        logging.debug("no match in crossref resply")
        return None
        
    logging.debug("Best match from Crossref: %s" % firstRes)
    doi = firstRes["doi"]
    doi = doi.replace("http://dx.doi.org/","") # crossref now always adds the url, strip it
    logging.debug("Got DOI: %s" % doi)
    return doi

if __name__=="__main__":
    import doctest
    doctest.testmod()
