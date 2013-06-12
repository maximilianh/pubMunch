#!/usr/bin/env python

import logging, sys, optparse
import urllib
import urllib2
import xml.etree.ElementTree as et
import time

# download sequences from eutils, use like this:
# downloadFromEutils(accs, outFile, oneByOne=options.oneByOne, retType=options.format)
# all copied from old script retrEutils

BASEURL = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils"
MAXRATE = 3 # number of requests per second
DELAY   = 1/MAXRATE

def chunkedDownloadFromEutils(db, accs, outFh, retType="fasta", chunkSize=5000):
    " download in chunks of 5000 accs from eutils "
    for chunkStart in range(0, len(accs), chunkSize):
        logging.info("Chunkstart is %d" % chunkStart)
        downloadFromEutils(db, accs[chunkStart:chunkStart+chunkSize], outFh, retType=retType)

def eSearch(db, query, retMax):
    " run an esearch and return list of GIs "
    url = BASEURL+'/esearch.fcgi';
    params = {"db":db, "term":query, "retMax" : retMax}
    logging.debug("Running esearch with %s" % params)
    data = urllib.urlencode(params)
    req  = urllib2.Request(url, data)
    try:
        xmlStr = urllib2.urlopen(req).read()
    except urllib2.HTTPError:
        logging.error("Error when searching")
        raise
    logging.debug("XML reply: %s" % xmlStr)
    # parse xml
    root = et.fromstring(xmlStr)

    # parse from XML list of GIs
    try:
        gis = []
        count = int(root.find("Count").text)
        idEls = root.findall("IdList/Id")
        for idEl in idEls:
            gis.append(idEl.text)
    except AttributeError:
        logging.info("Error, XML is %s" % xmlStr)
        raise
    logging.debug("Got GIs: %s" % gis)
    wait(DELAY)
    return gis

def accsToGis_oneByOne(db, accs):
    """ if accs are old/supressed or with version number at the end, need to use this, a lot slower 
    
    """
    logging.info("Getting GIs for %d accessions, rate %d req/sec, one dot = 50 requests" % (len(accs), MAXRATE))
    gis = []
    count = 0
    for acc in accs:
        oneGi = eSearch(db, acc, 3)
        assert(len(oneGi)==1)
        gis.extend(oneGi)
        if count%50==0:
            sys.stdout.write(".")
            sys.stdout.flush()
        count += 1
    return gis

def accsToGis(db, accs):
    """ search for accessions and post to eutils history server 
    return webenv, key, count
    """
    # query and put into eutils history
    logging.info("Running eutils search for %d accessions" % len(accs))
    accFull = [acc+"[accn]" for acc in accs]
    query = " OR ".join(accFull)
    query += ""
    gis = eSearch(db, query, len(accs))
    return gis

lastCallSec = 0

def wait(delaySec):
    " make sure that delaySec seconds have passed between two calls, sleep if necessary "
    global lastCallSec
    delaySec = float(delaySec)
    nowSec = time.time()
    sinceLastCallSec = nowSec - lastCallSec
    if sinceLastCallSec > 0.01 and sinceLastCallSec < delaySec :
        waitSec = delaySec - sinceLastCallSec
        logging.debug("Waiting for %f seconds" % waitSec)
        time.sleep(waitSec)
    lastCallSec = time.time()

def eFetch(db, gis, outFh, retType="fasta", retMax=500):
    " download GIs "
    url = BASEURL + "/efetch.fcgi"
    logging.debug("URL: %s" % url)
    count = len(gis)
    retMode = "text"
    if retType=="xml":
        retMode = "xml"
        retType = "gb"
    for retStart in range(0, count, retMax):
        partGis = gis[retStart:retStart+retMax]
        logging.info("Retrieving %d records, start %d..." % (len(partGis), retStart))
        params = {"db":db, "id":",".join(partGis), "rettype":retType, "retmode" : retMode}
        data = urllib.urlencode(params)

        # issue http request
        logging.debug("HTTP post Data: %s" % data)
        req  = urllib2.Request(url, data)
        resp = urllib2.urlopen(req).read()
        outFh.write(resp)
        wait(DELAY)
    
def downloadFromEutils(db, accs, outFh, retType="fasta", retMax=1000, oneByOne=False):
    """ combine an esearch with efetch to download from eutils
    If version numbers are included in the accession list, you need to set oneByOne=True
    """
    if oneByOne:
        gis = accsToGis_oneByOne(db, accs)
    else:
        gis = accsToGis(db, accs)
    logging.info("Got %d GIs" % len(gis))
    eFetch(db, gis, outFh, retType, retMax)

