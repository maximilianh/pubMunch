# a few convenience functions to query the new crossref API

import urllib2, json, doctest, logging
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
    logging.debug("crossref.org query %s" % freeFormCitStr)
    url = "https://api.crossref.org/works"

    geturl =  url + "?query=" + urllib2.quote(freeFormCitStr.encode('utf-8'))

    # send request
    httpResp = maxCommon.retryHttpRequest(geturl, None, delaySecs=delaySecs, repeatCount=repeatCount)
    if httpResp==None:
        logging.debug("HTTPError while sending crossref request")
        return None
    jsonStr = ""
    try:
        jsonStr = httpResp.read()
        httpResp.close()
    except:
        logging.debug("sslError while reading httpResp")
        return None
    xrdata = json.loads(jsonStr)

    # parse result
    if len(xrdata)==0:
        logging.debug("Empty cross reply")
        return None

    try:
        items = xrdata["message"]["items"]
    except KeyError:
        logging.debug("Unexpected JSON content from crossref")
        return None
    if len(items) == 0:
        logging.debug("no results in crossref reply")
        return None

    firstRes = items[0]

    logging.debug("Best match from Crossref: %s" % firstRes)
    doi = firstRes["DOI"]
    logging.debug("Got DOI: %s" % doi)
    return doi

if __name__=="__main__":
    import doctest
    doctest.testmod()
