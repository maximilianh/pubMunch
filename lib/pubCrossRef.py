# a few convenience functions to query the new crossref API

import urllib, urllib2, json

#def dois(issn):
#    queryStr = ", ".join(queryFields)
#    queryData = {"q" : queryStr, "pages" : "1", "rows" : 1}
#    urlParams = urllib.urlencode(queryData)
#    url = "http://search.labs.crossref.org/dois?" + urlParams
#    # send request
#    jsonStr = urllib2.urlopen(url).read()
#    xrdata = json.loads(jsonStr)
#    # parse result
#    if len(xrdata)==0:
#        logging.debug("Empty cross reply")
#        return None

def links(metaInfoDict):
    " take author, vol, journal etc from metaInfoDict, query crossref and return DOI if found "

    # construct url
    mid = metaInfoDict
    freeFormCit = [mid["authors"], '"%s"' % mid["title"], mid["journal"],mid["year"], "vol. "+mid["vol"], "no. "+ mid["issue"], "pp. "+mid["page"],  mid["printIssn"]]
    queryStr = ", ".join(queryFields)
    queryData = {"q" : queryStr, "pages" : "1", "rows" : 1}
    #urlParams = urllib.urlencode(queryData)
    #url = "http://search.labs.crossref.org/dois?" 
    url = "http://search.labs.crossref.org/links?" 
    jsonParam = json.dumps([freeFormCit])
    queryParam = {"q" : jsonParam}

    # send request
    jsonStr = urllib2.urlopen(url, jsonParam).read()
    xrdata = json.loads(jsonStr)

    # parse result
    if len(xrdata)==0:
        logging.debug("Empty cross reply")
        return None

    if not xrdata["query_ok"]:
        logging.debug("Query error from crossref")
        return None
    elif "results" not in xrdata or len(xrdata["results"])<1:
        logging.debug("missing results in crossref reply")
        return None

    firstRes = xrdata["results"][0]
    if not firstRes["match"]:
        logging.debug("no match in crossref resply")
        return None
        
    logging.debug("Best match from Crossref: %s" % firstRes)
    doi = firstRes["doi"]
    logging.debug("Got DOI: %s" % doi)
    return doi
