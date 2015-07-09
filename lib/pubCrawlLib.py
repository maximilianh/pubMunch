# library to crawl pdf and supplemental files from publisher websites using pubmed

import logging, os, shutil, tempfile, codecs, re, \
    urllib2, re, zipfile, collections, urlparse, time, atexit, socket, signal, \
    sqlite3, doctest, urllib, hashlib, string
from os.path import *
from collections import defaultdict, OrderedDict
from distutils.spawn import find_executable

# load our own libraries
import pubConf, pubGeneric, pubStore, pubCrossRef, pubPubmed
import maxTables, html, maxCommon

import chardet # guessing encoding, ported from firefox
import unidecode # library for converting to ASCII, ported from perl

# try to load the http requests module, but it's not necessary
try:
    import requests
    requestsLoaded = True
except:
    requestsLoaded = False

# the old version of BeautifulSoup is very slow, but was the only parser that
# did not choke on invalid HTML a single time. Lxml was by far not as tolerant.
from BeautifulSoup import BeautifulSoup, SoupStrainer, BeautifulStoneSoup # parsing of non-wellformed html

# sometimes using etree, as it's faster
import xml.etree.ElementTree as etree

# ===== GLOBALS ======

# for each ISSN
issnYearErrorCounts = defaultdict(int)

# options for wget 
# (python's http implementation is extremely buggy and tends to hang for minutes)
WGETOPTIONS = " --no-check-certificate --tries=3 --random-wait --waitretry=%d --connect-timeout=%d --dns-timeout=%d --read-timeout=%d --ignore-length " % (pubConf.httpTimeout, pubConf.httpTimeout, pubConf.httpTimeout, pubConf.httpTimeout)

# fixed options for curl
CURLOPTIONS = ' --insecure --ignore-content-length --silent --show-error --location --header "Connection: close"'

# global variable, http userAgent for all requests
userAgent = None

# name of document crawling status file
PMIDSTATNAME = "docStatus.tab"

# name of issn status file
ISSNSTATNAME = "issnStatus.tab"

# maximum of suppl files
SUPPFILEMAX = 25

# max number of consecutive errors
# will abort if exceeded
MAXCONSECERR = 50

# maximum number of errors per issn and year
# after this number of errors per ISSN, an ISSN will be ignored
MAXISSNERRORCOUNT = 30

# number of seconds to wait after an error * multipled by number of errors that happened in a row
ERRWAIT = 5

# wait for key after each doc?
DO_PAUSE = False

# do not output files to disk, instead only write their document ID
# and the SHA1 of the contents
TEST_OUTPUT = False

# full Path to the program to download files
# None = not known yet
DOWNLOADER = None

# Always download meta information via eutils
SKIPLOCALMEDLINE = False

# GLOBALS 

# global crawler delay config, values in seconds
# key is either a domain name or a crawler name
crawlDelays = {
    "onlinelibrary.wiley.com" : 1,
    "dx.doi.org"              : 1,
    "ucelinks.cdlib.org"      : 20,
    "eutils.ncbi.nlm.nih.gov"      : 3,
    "www.ncbi.nlm.nih.gov"      : 10, # fulltext crawled from PMC
    "lww" : 10,
    "npg" : 10,
    "nejm" : 10,
    "elsevier" : 10,
    "wiley" : 10,
    "springer" : 10,
    "silverchair" : 10
}

# the config file can contain site-specific delays, e.g. for testing
crawlDelays.update(pubConf.crawlDelays)

# default delay secs if nothing else is found
defaultDelay = 20
# can be set from outside to force all delays to one fixed number of secs
globalForceDelay = None

# filenames of lockfiles
lockFnames = []

# http page cache, to avoid duplicate downloads
webCache = {}

addHeaders = [ # additional headers for fulltext download metaData
"mainHtmlUrl", # the main fulltext HTML URL
"mainPdfUrl", # the main fulltext PDF URL
"suppUrls", # a comma-sep list of supplemental file URLs
"mainHtmlFile", # the main text file on local disk, relative to the metaData file
"mainPdfFile", # the main text pdf file on local disk, relative to the metaData file
"suppFiles", # comma-sep list of supplemental files on local disk
"landingUrl" # can be different from mainHtml
]

# this is a copy from pubStore.py
# to avoid ANY change to this, copy/pasted here
# these fields should NEVER change while a crawl is running otherwise it will mess up the
# tab file field count in the crawl output files (the crawler always appends)

articleFields=[
"articleId",  # internal number that identifies this article in the pubtools system
"externalId", # original string id of the article, e.g. PMC12343 or doi:123213213/dfsdf or PMID123123
"source",  # the origin of the article, something like "elsevier" or "pubmed" or "medline"
"origFile", # the original file where the article came from, e.g. the zipfile or genbank file
"journal",      # journal or book title
"printIssn",    # ISSN of the print edition of the article
"eIssn",        # optional: ISSN of the electronic edition of the journal/book of the article
"journalUniqueId", # medline only: NLM unique ID for journal
"year",         # first year of publication (electronic or print or advanced access)
"articleType", # research-article, review or other
"articleSection",  # elsevier: the section of the book/journal, e.g. "methods", "chapter 5" or "Comments" 
"authors",  # list of author names, usually separated by semicolon
"authorEmails",  # email addresses of authors
"authorAffiliations",  # authors' affiliations
"keywords", # medline: mesh terms or similar, separated by / (medline is using , internally)
"title",    # title of article
"abstract", # abstract if available
"vol",      # volume
"issue",    # issue
"page",            # first page of article, can be ix, x, or S4
"pmid",            # PubmedID if available
"pmcId",           # Pubmed Central ID
"doi",             # DOI, without leading doi:
"fulltextUrl",     # URL to (pdf) fulltext of article
"time"     # date of download
]

metaHeaders = articleFields
metaHeaders.extend(addHeaders)

# ===== EXCEPTIONS ======

class pubGetError(Exception):
    def __init__(self, longMsg, logMsg, detailMsg=None):
        self.longMsg = longMsg
        self.logMsg = logMsg
        self.detailMsg = detailMsg
    def __str__(self):
        return unidecode.unidecode(self.longMsg+"/"+self.logMsg+"/"+self.detailMsg)

# ===== FUNCTIONS =======

def resolveDoiWithSfx(sfxServer, doi):
    " return the fulltext url for doi using the SFX system "
    logging.debug("Resolving doi %s with SFX" % doi)
    xmlQuery = '%s/SFX_API/sfx_local?XML=<?xml version="1.0" ?><open-url><object_description><global_identifier_zone><id>doi:%s</id></global_identifier_zone><object_metadata_zone><__service_type>getFullTxt</__service_type></object_metadata_zone></object_description></open-url>' % (sfxServer, str(doi))
    return resolveWithSfx(sfxServer, xmlQuery)

def resolvePmidWithSfx(sfxServer, pmid):
    " return the fulltext url for pmid using the SFX system "
    logging.debug("Resolving pmid %s with SFX" % pmid)
    xmlQuery = '%s/SFX_API/sfx_local?XML=<?xml version="1.0" ?><open-url><object_description><global_identifier_zone><id>pmid:%s</id></global_identifier_zone><object_metadata_zone><__service_type>getFullTxt</__service_type></object_metadata_zone></object_description></open-url>' % (sfxServer, str(pmid))
    return resolveWithSfx(sfxServer, xmlQuery)

def resolveWithSfx(sfxServer, xmlQuery):
    sfxResult = httpGetDelay(xmlQuery, forceDelaySecs=0)
    xmlResult = sfxResult["data"]
    soup = BeautifulStoneSoup(xmlResult, convertEntities=BeautifulSoup.HTML_ENTITIES, smartQuotesTo=None)
    urlEls = soup.findAll("url")
    if len(urlEls)==0 or urlEls==None:
        return None
    if urlEls[0]==None:
        return None
    urls = [x.string for x in urlEls]
    logging.debug("SFX returned (using only first of these): %s" % urls)
    if urls[0]==None:
        return None
    url = urls[0].encode("utf8")
    return url

def getLandingUrlSearchEngine(articleData):
    """ given article meta data, try to find landing URL via a search engine:
    - medlina's DOI
    - a Crossref search with medline data
    - Pubmed Outlink
    - an SFX search
    #>>> findLandingUrl({"pmid":"12515824", "doi":"10.1083/jcb.200210084", "printIssn" : "1234", "page":"8"})
    'http://jcb.rupress.org/content/160/1/53'
    """
    #logging.log(5, "Looking for landing page")

    landingUrl = None
    # try medline's DOI
    # note that can sometimes differ e.g. 12515824 directs to a different page via DOI
    # than via Pubmed outlink, so sometimes we need to rewrite the doi urls
    if articleData["doi"]!="":
        landingUrl = resolveDoi(articleData["doi"])
        if landingUrl!=None:
            return landingUrl

    # try crossref's search API to find the DOI 
    if articleData["doi"]=="":
        xrDoi = pubCrossRef.lookupDoi(articleData)
        if xrDoi != None:
            articleData["doi"] = xrDoi.replace("http://dx.doi.org/","")
            landingUrl = resolveDoi(xrDoi)
            if landingUrl!=None:
                return landingUrl

    # try pubmed's outlink
    if articleData["pmid"]!="":
        outlinks = pubPubmed.getOutlinks(articleData["pmid"])
        if outlinks==None:
            logging.info("pubmed error, waiting for 120 secs")
            time.sleep(120)
            raise pubGetError("pubmed outlinks http error", "PubmedOutlinkHttpError")

        if len(outlinks)!=0:
            landingUrl =  outlinks.values()[0]
            logging.debug("landing page based on first outlink of Pubmed, URL %s" % landingUrl)
            return landingUrl

    # try SFX
    if pubConf.crawlSfxServer==None:
        logging.warn("You have not defined an SFX server in pubConf.py, cannot search for fulltext")
    else:
        landingUrl = resolvePmidWithSfx(pubConf.crawlSfxServer, articleData["pmid"])

    if landingUrl==None:
        raise pubGetError("No fulltext for this article", "noOutlinkOrDoi") 

    return landingUrl

def parseWgetLog(logFile, origUrl):
    " parse a wget logfile and return final URL (after redirects) and mimetype as tuple"
    #   Content-Type: text/html; charset=utf-8 
    lines = logFile.readlines()
    logging.log(5, "Wget logfile: %s" % " / ".join(lines))
    mimeType, url, charset = None, None, "utf8"
    lastUrl = None
    for l in lines:
        l = l.strip()
        if l.lower().startswith("content-type:"):
            logging.log(5, "wget mime type line: %s" % l)
            mimeParts = l.strip("\n").split()
            if len(mimeParts)>1:
                mimeType = mimeParts[1].strip(";")
            if len(mimeParts)>2:
                charset = mimeParts[2].split("=")[1]

        elif l.lower().startswith("location:"):
            logging.log(5, "wget location line: %s" % l)
            url = l.split(": ")[1].split(" ")[0]
            scheme, netloc = urlparse.urlsplit(url)[0:2]
            if netloc=="":
                #assert(lastUrl!=None)
                if lastUrl==None:
                    lastUrl = origUrl
                logging.log(5, "joined parts are %s, %s" % (lastUrl, url))
                url = urlparse.urljoin(lastUrl, url)
                logging.log(5, "URL did not contain server, using previous server, corrected URL is %s" % url)
            else:
                lastUrl = url

    logging.log(5, "parsed transfer log files:  URL=%s, mimetype=%s, charset=%s" % (url, mimeType, charset))
    return mimeType, url, charset


def getDelaySecs(host, forceDelaySecs):
    """ return the number of seconds to pause. Either globally forced from command line,
    or set by-host or some global default if everything fails
    returns number of secs
    """
    if globalForceDelay!=None:
        logging.log(5, "delay time is set globally to %d seconds" % globalForceDelay)
        return globalForceDelay
    if forceDelaySecs!=None:
        logging.log(5, "delay time is set for this download to %d seconds" % forceDelaySecs)
        return forceDelaySecs

    logging.debug("Looking up delay time for host %s" % host)

    if host in crawlDelays:
        delaySecs = crawlDelays.get(host, defaultDelay)
        logging.info("Delay time for host %s set to %d seconds" % (host, delaySecs))
        return delaySecs

    logging.debug("Delay time for host %s not known" % (host))
    return defaultDelay

def findDownloader():
    " find either curl or wget on this system and return the path "
    #appDir = maxCommon.getAppDir()
    #winPath = join(appDir, "curl.exe")
    #if isfile(winPath):
        #return winPath
    if os.name=="nt" or os.name=="posix":
        if requestsLoaded:
            return ""
        else:
            logging.warn("The requests module is not loaded")
         
    binPath = None
    downloaders = ["curl", "curl.exe", "wget", "wget.exe"]
    for binName in downloaders:
        binPath = find_executable(binName)
        if binPath!=None:
            logging.log(5,"cwd is %s" % os.getcwd())
            logging.log(5, "%s found at %s" % (binName, binPath))
            return binPath
        
    raise Exception("cannot find wget nor curl")

def httpGetDelay(url, forceDelaySecs=None):
    """ download with curl or wget and make sure that delaySecs (global var)
    secs have passed between two calls special cases for highwire hosts and
    some hosts configured in config file.

    returns dict with these keys: url, mimeType, charset, data
    Follows redirects, "url" is really the final URL.
    """
    global webCache
    if url in webCache:
        logging.log(5, "Using cached http results")
        return webCache[url]

    logging.info("Downloading %s" % url)
    host = urlparse.urlsplit(url)[1]
    delaySecs = getDelaySecs(host, forceDelaySecs)
    wait(delaySecs, host)

    global DOWNLOADER
    if DOWNLOADER is None:
        DOWNLOADER = findDownloader()

    url = url.replace("'", "")

    # construct user agent
    global userAgent
    if userAgent==None:
        userAgent = pubConf.httpUserAgent
    userAgent = userAgent.replace("'", "")

    if "wget" in DOWNLOADER:
        page = runWget(url, userAgent)
    elif "curl" in DOWNLOADER:
        page = runCurl(url, userAgent)
    elif requestsLoaded:
        page = downloadBuiltIn(url, userAgent)
    else:
        raise Exception("illegal value of DOWNLOADER")
        

    return page

curlCookieFile = None

def downloadBuiltIn(url, userAgent):
    """
    download a url with the requests module, return a dict with the keys
    url, mimeType, charset and data
    """
    headers = {"user-agent" : userAgent}
    r = requests.get(url, headers=headers)
    page = {}
    page["url"] = r.url
    page["data"] = r.content
    page["mimeType"] = r.headers["content-type"].split(";")[0]
    page["encoding"] = r.encoding

    webCache[r.url] = page
    webCache[url] = page

    return page


def runCurl(url, userAgent):
    """ download url with wget. Return dict with keys, url, mimeType, charset, data """
    if pubConf.httpProxy!=None:
        logging.log(5, "Using proxy %s" % pubConf.httpProxy)
        env = {"http_proxy" : pubConf.httpProxy}
    else:
        env = {}

    global curlCookieFile
    if curlCookieFile==None:
        curlCookieFile = tempfile.NamedTemporaryFile(dir = pubConf.getTempDir(), \
                prefix="pub_curlScraper_cookies", suffix=".txt")

    logging.debug("Downloading %s with curl" % url)

    tmpFile = tempfile.NamedTemporaryFile(dir = pubConf.getTempDir(), \
        prefix="pub_curlScraper", suffix=".data")

    timeout = pubConf.httpTransferTimeout
    url = url.replace("'","")

    os.system("free -g")
    cmd = [DOWNLOADER, url, "-A", userAgent, "--cookie-jar",
        curlCookieFile.name, "-o", tmpFile.name, "--max-time", str(timeout),
        "-w", '%{url_effective} %{content_type}', CURLOPTIONS] 
    #cmd = [DOWNLOADER, "--help"]

    stdout, stderr, ret = pubGeneric.runCommandTimeout(cmd, timeout=timeout, env=env, shell=False)
    if ret!=0:
        raise pubGetError("non-null return code from curl: stdout: "+stdout+", stderr:"+stderr, "curlRetNotNull", " ".join(cmd))

    data = tmpFile.read()
    logging.log(5, "Download OK, size %d bytes" % len(data))
    if len(data)==0:
        raise pubGetError("empty http reply from %s" % url, "emptyHttp",url)
    tmpFile.close()

    logging.debug("status from curl: %s" % stdout)
    # http://www.ncbi.nlm.nih.gov/pmc/articles/PMC3183000/ text/html; charset=UTF-8
    outParts = string.split(stdout, " ", 1)
    finalUrl, mimeType = outParts[:2]
    mimeType = mimeType.strip(";")
    if len(outParts)==3:
        charset = outParts[2].replace("charset=","")
    else:
        charset = "ascii"

    page = {}
    page["url"] = finalUrl
    page["mimeType"] = mimeType
    page["encoding"] = charset
    page["data"] = data

    webCache[finalUrl] = page
    webCache[url] = page

    return page


def runWget(url, userAgent):
    """ download url with wget and return dict with keys url, mimeType, charset, data
    global variable userAgent is used if possible
    """
    logging.debug("Downloading %s with wget" % url)
    # construct & run wget command
    tmpFile = tempfile.NamedTemporaryFile(dir = pubConf.getTempDir(), \
        prefix="pub_wgetCrawler", suffix=".data")
    cmd = "wget '%s' -O %s --server-response " % (url, tmpFile.name)
    cmd += WGETOPTIONS
    cmd += "--user-agent='%s'" % userAgent

    logFile = tempfile.NamedTemporaryFile(dir = pubConf.getTempDir(), \
        prefix="pubGetPmid-Wget-", suffix=".log")
    cmd += " -o %s " % logFile.name
    logging.log(5, "command: %s" % cmd)
    #print cmd

    env = None
    if pubConf.httpProxy!=None:
        logging.log(5, "Using proxy %s" % pubConf.httpProxy)
        env= {"http_proxy" : pubConf.httpProxy}
    timeout = pubConf.httpTransferTimeout

    stdout, stderr, ret = pubGeneric.runCommandTimeout(cmd, timeout=timeout, env=env)
    if ret!=0:
        raise pubGetError("non-null return code from wget", "wgetRetNonNull", url.decode("utf8"))

    # parse wget log
    mimeType, redirectUrl, charset = parseWgetLog(logFile, url)
    if mimeType==None:
        raise pubGetError("No mimetype found in http reply", "noMimeType", url)

    if redirectUrl!=None:
        finalUrl = redirectUrl
    else:
        finalUrl = url
    
    data = tmpFile.read()
    logging.log(5, "Download OK, size %d bytes" % len(data))
    if len(data)==0:
        raise pubGetError("empty http reply from %s" % url, "emptyHttp",url)

    if mimeType in ["text/plain", "application/xml", "text/csv"]:
        logging.log(5, "Trying to guess encoding of text file with type %s" % mimeType)
        data = recodeToUtf8(data)

    ret = {}
    ret["url"] = finalUrl
    ret["mimeType"] = mimeType
    ret["encoding"] = charset
    ret["data"] = data

    webCache[finalUrl] = ret
    webCache[url] = ret

    return ret

#def soupToText(soup): # not needed?
    #' convert a tag to a string of the text within it '
    #text = soup.getText()
    #return text 

    # soup has children: need to get them and concat their texts
    #texts = [text]
    #allTags = soup.findAll(True)
    #for t in allTags:
        #if t.string!=None:
            #texts.append(t.getText())
    #return " ".join(texts)

def anyMatch(regexList, queryStr):
    for regex in regexList:
        if regex.match(queryStr):
            logging.debug("url %s ignored due to regex %s" % (queryStr, regex.pattern))
            return True
    return False

def htmlParsePage(page):
    " parse the html page with beautifulsoup 3 "
    if "parsedHtml" not in page:
        logging.debug("Parsing HTML")
        html = page["data"]
        html = html.replace(' xmlns="http://www.w3.org/1999/xhtml"', '')
        page["parsedHtml"] = BeautifulSoup(html)

def htmlExtractPart(page, tag, attrs):
    """
    return a part of an html page as a string given a tag and the required attribute values
    If not found, return the full html text string. Parsing result is cached in the page.
    """
    try:
        htmlParsePage(page)
    except UnicodeEncodeError:
        logging.warn("could not parse html")
        return page["data"]

    bs = page["parsedHtml"]
    el = bs.find(tag, attrs=attrs)
    if el!=None:
        return str(el)
    else:
        logging.debug("Could not strip html")
        return page["data"]

def htmlFindLinkUrls(page, attrs={}):
    """ parses the whole page and finds links with certain attributes, returns the href URLs 
    This is really slow.
    """
    htmlParsePage(page)
    bs = page["parsedHtml"]
    elList = bs.findAll("a", attrs=attrs)
    urls = []
    for el in elList:
        if not el.has_key("href"):
            continue
        url = el["href"]
        url = urlparse.urljoin(page["url"], url)
        urls.append(url)
    return urls


def parseHtmlLinks(page, canBeOffsite=False, landingPage_ignoreUrlREs=[]):
    """ 
    find all A-like links and meta-tag-info from a html string and add 
    to page dictionary as keys "links", "metas" and "iframes"
    """

    # use cached results if page has already been parsed before
    if "links" in page:
        #logging.debug("Using cached parsing results")
        return page

    logging.debug("Parsing HTML links")
    htmlString = page["data"]
    baseUrl = page["url"]
    urlParts = urlparse.urlsplit(baseUrl)
    basePath = urlParts[2]
    baseLoc = urlParts[1]

    logging.log(5, "Parsing %s with bs3" % page["url"])
    linkStrainer = SoupStrainer(['a', 'meta', 'iframe']) # to speed up parsing
    try:
        fulltextLinks = BeautifulSoup(htmlString, smartQuotesTo=None, \
            convertEntities=BeautifulSoup.ALL_ENTITIES, parseOnlyThese=linkStrainer)
    except ValueError, e:
        raise pubGetError("Exception during bs html parse", "htmlParseException", e.message)
    logging.log(5, "bs parsing finished")

    linkDict = OrderedDict()
    metaDict = OrderedDict()
    iframeDict = OrderedDict()

    for l in fulltextLinks:
        logging.log(5, "got link %s" % l)
        if l.name=="iframe":
            src = l.get("src")
            if src==None or "pdf" not in src:
                continue
            id = l.get("id", "pdfDocument")
            iframeDict[id] = src

        elif l.name=="a":
            text = l.getText() # used to be: soupToText(l)
            text = text.encode("utf8")
            url = l.get("href")
            if url==None:
                logging.log(5, "url is None")
                continue
            try:
                linkLoc = urlparse.urlsplit(url)[1]
                linkPath = urlparse.urlsplit(url)[2]
            except ValueError:
                raise pubGetError("Value error on url split %s" % url, "urlSplitError", url)
            # skip links that point to a different server
            if canBeOffsite==False and linkLoc!="" and linkLoc!=baseLoc:
                logging.log(5, "skipping link %s, is offsite" % url)
                continue

            # remove #xxxx fragment identifiers from link URL
            fullUrl = urlparse.urljoin(baseUrl, url)
            parts = list(urlparse.urlsplit(fullUrl)[:4])
            if parts[0]=="javascript":
                logging.log(5, "skipping link %s, is javascript" % url)
                continue
            parts.append("")
            fullUrlNoFrag = urlparse.urlunsplit(parts)
            #logging.debug("Checking link against %s" % landingPage_ignoreUrlREs)
            if anyMatch(landingPage_ignoreUrlREs, fullUrlNoFrag):
                logging.log(5, "skipping link %s, because of ignore REs" % url)
                continue
            linkDict[fullUrlNoFrag] = text
            logging.log(5, "Added link %s for text %s" % (repr(fullUrlNoFrag), repr(text)))

        elif l.name=="meta":
            # parse meta tags
            name = l.get("name")
            if name!=None:
                #(name.startswith("prism") or \
                #name.startswith("citation") or \
                #name.startswith("DC")):
                content = l.get("content")
                metaDict[name] = content

    logging.log(5, "Meta tags: %s" % metaDict)
    logging.log(5, "Links: %s" % linkDict)
    logging.log(5, "iframes: %s" % iframeDict)

    page["links"] = linkDict
    page["metas"] = metaDict
    page["iframes"] = iframeDict
    logging.log(5, "HTML parsing finished")
    return page

def recodeToUtf8(data):
    " use chardet to find out codepage and recode to utf8"
    # first try utf8
    try:
        data = data.decode("utf8").encode("utf8")
        return data
    # then use chardet
    except UnicodeDecodeError:
        encoding = chardet.detect(data)['encoding']
        logging.log(5, "encoding should be %s" % encoding)
        if encoding==None:
            encoding = "latin1"

        try:
            data = data.decode(encoding).encode("utf8")
        except UnicodeDecodeError:
            logging.warn("Error when decoding as %s" % encoding)
            data = data
        except LookupError:
            logging.warn("Unknown encoding when decoding as %s" % encoding)
            data = data
        return data

def parseDocIdStatus(outDir):
    " parse status file and return a set with pmids that should not be crawled "
    donePmids = set()
    statusFname = join(outDir, PMIDSTATNAME)
    logging.info("Parsing %s" % statusFname)
    if isfile(statusFname):
        for l in open(statusFname):
            docId = l.strip().split("#")[0].split("\t")[0]
            if docId=="":
                continue
            donePmids.add(docId)
        logging.info("Found %d PMIDs that have some status" % len(donePmids))

    return donePmids

def parseIssnStatus(outDir):
    " parse outDir/issnStatus.tab and return a set with (issn, year) hat should be ignored "
    statusFname = join(outDir, ISSNSTATNAME)
    if not isfile(statusFname):
        statusFname = join(outDir, "pmidStatus.tab")
    logging.info("Parsing %s" % statusFname)
    ignoreIssns = set()
    if isfile(statusFname):
        for row in maxCommon.iterTsvRows(statusFname):
            ignoreIssns.add((row.issn, row.year))
    return ignoreIssns

lastCallSec = {}

def wait(delaySec, host="default"):
    " make sure that delaySec seconds have passed between two calls"
    global lastCallSec
    delaySec = float(delaySec)
    nowSec = time.time()
    sinceLastCallSec = nowSec - lastCallSec.get(host, 0)
    #print "host", host, "now", nowSec, "lastCallSecs", lastCallSec
    #print "sinceLastCallSec", sinceLastCallSec
    #logging.debug("sinceLastCall %f" % float(sinceLastCallSec))
    if sinceLastCallSec > 0.1 and sinceLastCallSec < delaySec :
        waitSec = delaySec - sinceLastCallSec
        logging.info("Waiting for %f seconds before downloading from host %s" % (waitSec, host))
        time.sleep(waitSec)

    lastCallSec[host] = time.time()

def iterateNewPmids(pmids, ignorePmids):
    """ yield all pmids that are not in ignorePmids """
    ignorePmidCount = 0

    ignorePmids = set([int(p) for p in ignorePmids])
    pmids = set([int(p) for p in pmids])
    todoPmids = pmids - ignorePmids
    #todoPmids = list(todoPmids)
    #random.shuffle(todoPmids) # to distribute error messages

    logging.debug("Skipped %d PMIDs" % (len(pmids)-len(todoPmids)))
    for pmidPos, pmid in enumerate(todoPmids):
        logging.debug("%d more PMIDs to go" % (len(todoPmids)-pmidPos))
        yield str(pmid)

    #if ignorePmidCount!=0:
        #logging.debug("Skipped %d PMIDs" % ignorePmidCount)

def readLocalMedline(pmid):
    " returns a dict with info we have locally about PMID, None if not found "
    logging.debug("Trying PMID lookup with local medline copy")
    medlineDb = pubStore.getArtDbPath("medline")
    if not isfile(medlineDb):
        logging.debug("%s does not exist, no local medline lookups" % medlineDb)
        return None

    con, cur = maxTables.openSqlite(medlineDb)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    rows = None
    tryCount = 60

    while rows==None and tryCount>0:
        try:
            rows = list(cur.execute("SELECT * from articles where pmid=?", (pmid,)))
        except sqlite3.OperationalError:
            logging.info("Database is locked, waiting for 60 secs")
            time.sleep(60)
            tryCount -= 1

    if rows==None:
        raise Exception("Medline database was locked for more than 60 minutes")
        
    if len(rows)==0:
        logging.info("No info in local medline for PMID %s" % pmid)
        return None

    # the last entry should be the newest one
    lastRow = rows[-1]

    # convert sqlite results to dict
    result = {}
    for key, val in zip(lastRow.keys(), lastRow):
        result[key] = unicode(val)

    result["source"] = ""
    result["origFile"] = ""
    return result
        
def downloadPubmedMeta(pmid):
    """ wrapper around pubPubmed that converts exceptions"""
    try:
        wait(3, "eutils.ncbi.nlm.nih.gov")
        ret = pubPubmed.getOnePmid(pmid)
    except urllib2.HTTPError, e:
        raise pubGetError("HTTP error %s on Pubmed" % str(e.code), "pubmedHttpError" , str(e.code))
    except pubPubmed.PubmedError, e:
        raise pubGetError(e.longMsg, e.logMsg)
        
    if ret==None:
        raise pubGetError("empty result when requesting metadata from NCBI Eutils for PMID %s" % str(pmid), \
            "pubmedEmpty")
    for h in addHeaders:
        ret[h] = ""
    return ret

def writeMeta(outDir, metaData, fulltextData):
    " append one metadata dict as a tab-sep row to outDir/articleMeta.tab and articls.db "
    filename = join(outDir, "articleMeta.tab")
    #if testMode!=None:
        #filenames = join(outDir, "testMeta.tab")
    logging.debug("Appending metadata to %s" % filename)

    # overwrite fields with identifers and URLs
    minId = pubConf.identifierStart["crawler"]
    metaData["articleId"] = str(minId+int(metaData["pmid"]))
    if "main.html" in metaData:
        metaData["fulltextUrl"] = metaData["main.html"]
    else:
        metaData["fulltextUrl"] = metaData["landingUrl"]

    # save all URLs to metadata object, nice for debugging
    #metaData["mainHtmlUrl"] = fulltextData.get("main.html",{}).get("url", "")
    #metaData["mainPdfUrl"] = fulltextData.get("main.pdf",{}).get("url", "")
            
    # write to tab file
    if not isfile(filename):
        codecs.open(filename, "w", encoding="utf8").write(u"\t".join(metaHeaders)+"\n")
    maxCommon.appendTsvDict(filename, metaData, metaHeaders)

    # write to sqlite db
    row = []
    for h in metaHeaders:
        row.append(metaData.get(h, ""))

    dbFname = join(outDir, "articles.db")
    con, cur = maxTables.openSqliteCreateTable (dbFname, "articles", metaHeaders, \
        idxFields=["pmid","pmcId", "doi"], \
        intFields=["pmid", "articleId", "pmcId"], primKey="pmid", retries=100)

    # keep retrying if sqlite db is locked
    writeOk = False
    tryCount = 100
    logging.log(5, "%s" % row)
    while not writeOk and tryCount > 0:
        try:
            try:
                maxTables.insertSqliteRow(cur, con, "articles", metaHeaders, row)
            except sqlite3.IntegrityError:
                logging.warn("Already present in meta info db")
            writeOk = True
        except sqlite3.OperationalError:
            logging.info("sqlite db is locked, waiting for 60 secs")
            time.sleep(60)
            tryCount -= 1
    if not writeOk:
        raise Exception("Could not write to sqlite db")

def detFileExt(page):
    " determine file extension based on either mimetype or url file extension "
    fileExt = pubConf.MIMEMAP.get(page["mimeType"], None)
    linkUrl = page["url"]
    if fileExt!=None:
        logging.debug("Determined filetype as %s based on mime type" % fileExt)
    else:
        urlPath = urlparse.urlparse(linkUrl)[2]
        fileExt = os.path.splitext(urlPath)[1]
        logging.debug("Using extension %s based on URL %s" % (fileExt, linkUrl))
    fileExt = fileExt.strip(".")
    return fileExt

def urlHasExt(linkUrl, linkText, searchFileExts, pattern):
    " return True if url ends with one of a list of extensions "
    if searchFileExts==None:
        return True
    urlPath = urlparse.urlparse(linkUrl)[2]
    urlExt = os.path.splitext(urlPath)[1].strip(".")
    for searchFileExt in searchFileExts:
        if urlExt==searchFileExt:
            logging.debug("Found acceptable extension AND matching link for pattern %s: text %s, url %s" % \
                (pattern, repr(linkText), linkUrl))
            return True
    return False

def containsAnyWord(text, ignWords):
    for word in ignWords:
        if word in text:
            logging.debug("blacklist word %s found" % word)
            return True
    return False

def findMatchingLinks(links, searchTextRes, searchUrlRes, searchFileExts, ignTextWords):
    """ given a dict linktext -> url, yield the URLs that match:
    (one of the searchTexts in their text or one of the searchUrlRes) AND 
    one of the file extensions"""

    assert(searchTextRes!=None or searchUrlRes!=None)
    if (len(searchTextRes)==0 and len(searchUrlRes)==0):
        raise Exception("config error: didn't get any search text or url regular expressions")

    doneUrls = set()

    for linkText, linkUrl in links.iteritems():
        if containsAnyWord(linkText, ignTextWords):
            logging.debug("Ignoring link text %s, url %s" % (repr(linkText), repr(linkUrl)))
            continue

        for searchRe in searchTextRes:
            if searchRe.match(linkText):
                if urlHasExt(linkUrl, linkText, searchFileExts, searchRe.pattern):
                        yield linkUrl
                        continue

        for searchRe in searchUrlRes:
            logging.log(5, "Checking url %s against regex %s" % (linkUrl, searchRe.pattern))
            if searchRe.match(linkUrl):
                if linkUrl in doneUrls:
                    continue
                if urlHasExt(linkUrl, linkText, searchFileExts, searchRe.pattern):
                    logging.log(5, "Match found")
                    doneUrls.add(linkUrl)
                    yield linkUrl

def getSuppData(fulltextData, suppListPage, crawlConfig, suppExts):
    " given a page with links to supp files, add supplemental files to fulltextData dict "
    if "landingPage_hasSuppList" in crawlConfig:
        configPrefix = "landingPage"
    elif "fulltextPage_hasSuppList" in crawlConfig:
        configPrefix = "fulltextPage"
    else:
        configPrefix = "suppListPage"

    # this function can accept the regexes from either fulltextPage_suppFile_textREs or
    # suppListPage_suppFile_textREs or landingPage_suppFile_textREs
    textReTag = "%s_suppFile_textREs" % configPrefix
    urlReTag = "%s_suppFile_urlREs" % configPrefix
    logging.debug("Looking for %s and %s" % (textReTag, urlReTag))
    suppTextREs = crawlConfig.get(textReTag, [])
    suppUrlREs = crawlConfig.get(urlReTag, [])
    if len(suppTextREs)==0 and len(suppUrlREs)==0:
        return fulltextData

    ignSuppTextWords = crawlConfig.get("ignoreSuppFileLinkWords", [])

    suppFilesAreOffsite = crawlConfig.get("suppFilesAreOffsite", False)
    landingPage_ignoreUrlREs = crawlConfig.get("landingPage_ignoreUrlREs", [])
    suppListPage = parseHtmlLinks(suppListPage, suppFilesAreOffsite, \
        landingPage_ignoreUrlREs=landingPage_ignoreUrlREs)
    suppLinks = suppListPage["links"]
    htmlMetas = suppListPage["metas"]
    suppUrls  = list(findMatchingLinks(suppLinks, suppTextREs, suppUrlREs, suppExts, ignSuppTextWords))

    if len(suppUrls)==0:
        logging.debug("No links to supplementary files found")
    else:
        logging.debug("Found %d links to supplementary files" % len(suppUrls))
        suppIdx = 1
        for url in suppUrls:
            suppFile = httpGetDelay(url)
            for ignoreTag in crawlConfig.get("ignoreSuppFileContentText", []):
                if ignoreTag in suppFile["data"]:
                    logging.debug("Ignoring this supp file, found word %s" % ignoreTag)
                    continue
            fileExt = detFileExt(suppFile)
            fulltextData["S"+str(suppIdx)+"."+fileExt] = suppFile
            suppIdx += 1
            if suppIdx > SUPPFILEMAX:
                raise pubGetError("max suppl count reached", "tooManySupplFiles", str(len(suppUrls)))
    return fulltextData

def replaceUrl(landingUrl, landingUrl_pdfUrl_replace):
    " try to find link to PDF/suppInfo based on just the landing URL alone "
    replaceCount = 0
    newUrl = landingUrl
    for word, replacement in landingUrl_pdfUrl_replace.iteritems():
        if word in newUrl:
            replaceCount+=1
            newUrl = newUrl.replace(word, replacement)
        elif word=="$":
            newUrl = newUrl+replacement
    if replaceCount==0:
        logging.debug("Could not replace words in URL")
        return None

    logging.debug("Replacing words in URL %s yields new URL %s" % (landingUrl, newUrl))
    try:
        newPage = httpGetDelay(newUrl)
        newUrl = newPage["url"]
    except pubGetError:
        logging.debug("replaced URL is not valid / triggers wget error")
        newUrl = None
    return newUrl

def blacklistIssnYear(outDir, issnYear, journal):
    " append a line to issnStatus.tab file in outDir "
    issn, year = issnYear
    fname = join(outDir, ISSNSTATNAME)
    if isfile(fname):
        outFh = open(fname, "a")
        colCount = len(open(fname).next().split("\t"))
    else:
        outFh = open(fname, "w")
        outFh.write("issn\tyear\tjournal\n")
        colCount = 3

    if colCount == 3:
        outFh.write("%s\t%s\t%s\n" % (issn, year, journal))
    else:
        outFh.write("%s\t%s\n" % (issn, year))
    outFh.close()

def writeDocIdStatus(outDir, pmid, msg, detail=""):
    " append a line to doc status file in outDir "
    fname = join(outDir, PMIDSTATNAME)
    if isfile(fname):
        outFh = codecs.open(fname, "a", encoding="utf8")
    else:
        outFh = codecs.open(fname, "w", encoding="utf8")

    outFh.write("%s\t%s\t%s\n" % (str(pmid), msg, repr(detail)))

def removeLocks():
    " remove all lock files "
    global lockFnames
    for lockFname in lockFnames:
        if isfile(lockFname):
            logging.debug("Removing lockfile %s" % lockFname)
            os.remove(lockFname)
    lockFnames = []

def containsLockFile(outDir):
    lockFname = join(outDir, "_pubCrawl.lock")
    return isfile(lockFname)

def checkCreateLock(outDir):
    " creates lockfile, squeaks if exists, register exit handler to delete "
    global lockFnames
    lockFname = join(outDir, "_pubCrawl.lock")
    if isfile(lockFname):
        raise Exception("File %s exists - it seems that a crawl is running now. \
        If you're sure that this is not the case, remove the lockfile and retry again" % lockFname)
    logging.debug("Creating lockfile %s" % lockFname)
    open(lockFname, "w")
    lockFnames.append(lockFname)
    atexit.register(removeLocks) # register handler that is executed on program exit

def parseDocIds(outDir):
    " parse docIds.txt in outDir and return as list, ignore duplicates "
    docIdFname1 = join(outDir, "docIds.txt")
    if not isfile(docIdFname1):
        docIdFname2 = join(outDir, "pmids.txt")
        if not isfile(docIdFname2):
            logging.info("%s does not exist, skipping dir %s" % (docIdFname1, outDir))
            return None

    pmidFname = join(outDir, "docIds.txt")
    logging.debug("Parsing %s" % pmidFname)
    if not isfile(pmidFname):
        raise Exception("file %s not found. You need to create this manually or "
            " run pubPrepCrawl pmids to create this file." % pmidFname)
    logging.debug("Parsing document IDs / PMIDs %s" % pmidFname)
    pmids = []
    seen = set()
    # read IDs, remove duplicates but keep the order
    for line in open(pmidFname):
        if line.startswith("#"):
            continue
        pmid = line.strip().split("#")[0].strip()
        if pmid=="":
            continue
        if pmid in seen:
            continue
        pmids.append((pmid, outDir))
        seen.add(pmid)
    logging.debug("Found %d documentIds/PMIDS" % len(pmids))
    return pmids

def findLinkMatchingReList(links, searchLinkRes, searchUrls=False):
    """ given a list of (text, url) and search strings, return first url where text or url matches 
    one of the search strings. if searchUrls is True, searches URLs of links, otherwise their text.
    """
    for searchLinkRe in searchLinkRes:
        for linkName, linkUrl in links.iteritems():
            logging.log(5, "checking link %s, linkUrl %s for %s, (searchUrls:%s)" %
            (repr(linkName), repr(linkUrl), searchLinkRe.pattern, searchUrls))
            if (not searchUrls and searchLinkRe.match(linkName)) or \
                    (searchUrls and searchLinkRe.match(linkUrl)):
                suppListUrl = linkUrl
                logging.debug("Found link: %s, %s" % (repr(linkName), repr(linkUrl)))
                return suppListUrl

def findSuppListUrl(landingPage, fulltextPage, crawlConfig):
    " given the landing page, find the link to the list of supp files "
    fulltextPageHasSupp = crawlConfig.get("fulltextPage_hasSuppList", False)
    if fulltextPageHasSupp:
        logging.debug("Supp. file list is on fulltext page")
        if fulltextPage==None:
            logging.debug("No fulltext page -> no suppl files")
            return None
        else:
            return fulltextPage["url"]

    landingPageHasSupp = crawlConfig.get("landingPage_hasSuppList", False)
    if landingPageHasSupp:
        logging.debug("Config says suppl files can be located on landing page")
        return landingPage["url"]

    ignoreUrls = crawlConfig.get("landingPage_ignoreUrlREs", [])
    landingPage = parseHtmlLinks(landingPage, landingPage_ignoreUrlREs=ignoreUrls)

    links = landingPage["links"]
    htmlMetas = landingPage["metas"]

    suppListUrl = None
    # first try if we can derive suppListUrl from main URL
    if "landingUrl_suppListUrl_replace" in crawlConfig:
        landUrl = landingPage["url"]
        suppListUrlRepl = replaceUrl(landUrl, crawlConfig["landingUrl_suppListUrl_replace"])
        if suppListUrlRepl!=None:
            suppListUrl = suppListUrlRepl

    # then try to find URLs in links
    if suppListUrl==None and "landingPage_suppFileList_urlREs" in crawlConfig:
        searchLinkRes = crawlConfig.get("landingPage_suppFileList_urlREs", [])
        logging.debug("Searching for links to suppl list on page %s using URLs" % landingPage["url"])
        suppListUrl = findLinkMatchingReList(links, searchLinkRes, searchUrls=True)

    # then try text description in links
    if suppListUrl==None and "landingPage_suppListTextREs" in crawlConfig:
        # if link url replacement not configured, try to search for links
        searchLinkRes = crawlConfig.get("landingPage_suppListTextREs", [])
        logging.debug("Searching for links to suppl list on page %s using link text" % landingPage["url"])
        suppListUrl = findLinkMatchingReList(links, searchLinkRes)

    if suppListUrl!=None:
        logging.debug("Found link to supplemental list page: %s" % suppListUrl)
    else:
        logging.debug("No link to list of supplemental files found")
    return suppListUrl

def ignoreCtrlc(signum, frame):
    logging.info('Signal handler called with signal %s' % str (signum))

def printPaperData(paperData):
    " output summary info of paper data obtained, for testing "
    if paperData==None:
        logging.info("No data received from crawler")
        return

    printFileHash(paperData)
    #for suffix, pageDict in paperData.iteritems():
        #logging.info("Got file: Suffix %s, url %s, mime %s, content %s" % \
            #(suffix, pageDict["url"], pageDict["mimeType"], repr(pageDict["data"][:10])))

def storeFilesNoZip(pmid, metaData, fulltextData, outDir):
    """ write files from dict (keys like main.html or main.pdf or s1.pdf, value is binary data) 
    to directory <outDir>/files
    """
    warnMsgs = []
    fileDir = join(outDir, "files")
    if not isdir(fileDir):
        os.makedirs(fileDir)

    suppFnames = []
    suppUrls = []
    pdfFound = False
    for suffix, pageDict in fulltextData.iteritems():
        if suffix in ["status", "crawlerName"]:
            continue
        if suffix=="landingPage":
            metaData["landingUrl"] = pageDict["url"]
            continue

        filename = pmid+"."+suffix

        warnMinSize = 5000
        if len(pageDict["data"]) < warnMinSize:
            warnMsgs.append("%s is smaller than %d bytes" % (suffix, warnMinSize))

        if suffix=="main.html":
            if "<html" in pageDict["data"]:
                warnMsgs.append("main.html contains html tag")
            metaData["mainHtmlFile"] = filename
            metaData["mainHtmlUrl"] = pageDict["url"]

        elif suffix=="main.pdf":
            pdfFound = True

            # check mime type
            if pageDict["mimeType"]!="application/pdf":
                raise pubGetError("invalidPdf", "invalid mimetype of PDF. dir %s, docId %s, title %s" % \
                    (outDir, pmid, metaData["title"]), pageDict["url"])
            # check for PDF header
            if not "PDF-" in pageDict["data"][:15]:
                raise pubGetError("invalidPdf", "main PDF is not a PDF. dir %s, docId %s, title %s" % \
                    (outDir, pmid, metaData["title"]), pageDict["url"])

            metaData["mainPdfFile"] = filename
            metaData["mainPdfUrl"] = pageDict["url"]

        elif suffix.startswith("S"):
            suppFnames.append(filename)
            suppUrls.append(pageDict["url"])
            
        fileData = pageDict["data"]
        
        filePath = join(fileDir, filename)
        logging.debug("Writing file %s" % filePath)
        fh = open(filePath, "wb")
        fh.write(fileData)
        fh.close()

    if not pdfFound:
        warnMsgs.append("No PDF file")

    # "," in urls? this happened 2 times in 1 million files
    suppFnames = [s.replace(",", "") for s in suppFnames]
    suppFnames = [s.replace("\t", "") for s in suppFnames]
    suppUrls = [s.replace(",", "") for s in suppUrls]
    suppUrls = [s.replace("\t", "") for s in suppUrls]

    metaData["suppFiles"] = ",".join(suppFnames)
    metaData["suppUrls"] = ",".join(suppUrls)

    return metaData, warnMsgs

def printFileHash(fulltextData):
    " output a table with file extension and SHA1 of all files "
    crawlerName = fulltextData["crawlerName"]
    for ext, page in fulltextData.iteritems():
        if ext in ["crawlerName", "status"]:
            continue
        sha1 = hashlib.sha1(page["data"]).hexdigest() # pylint: disable=E1101
        row = [crawlerName, ext, page["url"], str(len(page["data"])), sha1]
        print "\t".join(row)

def writePaperData(docId, pubmedMeta, fulltextData, outDir):
    " write all paper data to status and fulltext output files in outDir "
    if TEST_OUTPUT:
        printFileHash(fulltextData)
        return

    pubmedMeta, warnMsgs = storeFilesNoZip(docId, pubmedMeta, fulltextData, outDir)

    oldHandler = signal.signal(signal.SIGINT, ignoreCtrlc) # deact ctrl-c during write

    writeMeta(outDir, pubmedMeta, fulltextData)
    addStatus = ""
    if "status" in fulltextData:
        addStatus = fulltextData["status"]
    crawlerName = fulltextData["crawlerName"]

    docIdStatus = "OK\t%s\t%s %s, %d files\t%s" % \
        (crawlerName, pubmedMeta["journal"], pubmedMeta["year"], len(fulltextData), addStatus)
    writeDocIdStatus(outDir, docId, docIdStatus, ";".join(warnMsgs))

    signal.signal(signal.SIGINT, oldHandler) # react ctrl c handler

def parseIdStatus(fname):
    " parse crawling status file, return as dict status -> count "
    res = {}
    if not os.path.isfile(fname):
        logging.info("%s does not exist" % fname)
        res["OK"] = []
        return res

    for line in open(fname):
        pmid, status = line.strip().split()[:2]
        status = status.split("\\")[0]
        status = status.strip('"')
        status = status.strip("'")
        res.setdefault(status, [])
        pmid = int(pmid)
        res[status].append(pmid)
    return res

# temporarily pulling this in from pubCrawlConf

crawlPubIds = {
# got a journal list from Wolter Kluwer by email
"LWW lww" : "lww",
# all ISSNs that wiley gave us go into the subdir wiley
"WILEY Wiley" : "wiley",
# we don't have ISSNs for NPG directly, so we use grouped data from NLM
"NLM Nature Publishing Group" : "npg",
"NLM American College of Chest Physicians" : "chest",
"NLM American Association for Cancer Research" : "aacr",
"NLM Mary Ann Liebert" : "mal",
"NLM Oxford University Press" : "oup",
"NLM Future Science" : "futureScience",
"NLM National Academy of Sciences" : "pnas",
"NLM American Association of Immunologists" : "aai",
"NLM Karger" : "karger",
# we got a special list of Highwire ISSNs from their website
# it needed some manual processing
# see the README.txt file in the journalList directory
"HIGHWIRE Rockefeller University Press" : "rupress",
"HIGHWIRE American Society for Microbiology" : "asm",
"HIGHWIRE Cold Spring Harbor Laboratory" : "cshlp",
"HIGHWIRE The American Society for Pharmacology and Experimental Therapeutics" : "aspet",
"HIGHWIRE American Society for Biochemistry and Molecular Biology" : "asbmb",
"HIGHWIRE Federation of American Societies for Experimental Biology" : "faseb",
"HIGHWIRE Society for Leukocyte Biology" : "slb",
"HIGHWIRE The Company of Biologists" : "cob",
"HIGHWIRE Genetics Society of America" : "genetics",
"HIGHWIRE Society for General Microbiology" : "sgm",
"NLM Informa Healthcare" : "informa"
#"Society for Molecular Biology and Evolution" : "smbe"
}

def writeReport(baseDir, htmlFname):
    " parse pmids.txt and pmidStatus.tab and write a html report to htmlFname "
    h = html.htmlWriter(htmlFname)
    h.head("Genocoding crawler status", stylesheet="bootstrap/css/bootstrap.css")
    h.startBody("Crawler status as of %s" % time.asctime())

    publDesc = {}
    for key, value in crawlPubIds.iteritems():
        publDesc[value] = key

    totalPmidCount = 0
    totalOkCount = 0
    totalDownCount = 0


    for name in os.listdir(baseDir):
        dirName = join(baseDir, name)
        if not isdir(dirName) or name.startswith("_"):
            continue
        logging.info("processing dir %s" % name)
        pmidFile = join(dirName, "pmids.txt")
        if not isfile(pmidFile):
            continue
        pmidCount = len(open(pmidFile).readlines())
        issnCount = len(open(join(dirName, "issns.tab")).readlines())
        statusPmids = parseIdStatus(join(dirName, "pmidStatus.tab"))
        publisher = basename(dirName)
        isActive = isfile(join(dirName, "_pubCrawl.lock"))
        totalPmidCount += pmidCount
        totalOkCount  += len(statusPmids.get("OK", []))
        totalDownCount  += len(statusPmids.values())

        if publisher in publDesc:
            h.h4("Publisher: %s (%s)" % (publDesc[publisher], publisher))
        else:
            h.h4("Publisher: %s (%s)" % (publisher, publisher))
        h.startUl()
        h.li("Crawler is currently running: %s" % isActive)
        h.li("Number of journals: %d" % issnCount)
        h.li("Total PMIDs scheduled: %d" % pmidCount)
        h.li("Crawler progress rate: %0.2f %%" % (100*len(statusPmids.get("OK", ""))/float(pmidCount)))
        h.startUl()
        for status, pmidList in statusPmids.iteritems():
            exampleLinks = [html.pubmedLink(pmid) for pmid in pmidList[:10]]
            #if status=="OK":
                #exampleLinkStr = ""
            #else:
            exampleLinkStr = "&nbsp;&nbsp; (examples: %s, ...)" % ",".join(exampleLinks)
            h.li("Status %s: %d %s" % (status, len(pmidList), exampleLinkStr))
        h.endUl()
        h.endUl()

    h.h4("Total PMIDs scheduled to download: %d" % totalPmidCount)
    h.h4("Overall PMID download tried: %d" % totalDownCount)
    h.h4("Overall PMIDs downloaded successfully: %d" % totalOkCount)
    h.endHtml()

def getIssn(artMeta):
    " get the eIssn or the pIssn, prefer eIssn "
    issn = artMeta["eIssn"]
    if issn=="":
        issn = artMeta["printIssn"]
    return issn

def getIssnYear(artMeta):
    " return a tuple (issn, year). fallback to journal name if we have no ISSN. "
    if artMeta==None:
        return None
    issn = getIssn(artMeta)
    if issn=="":
        issn=artMeta["journal"]
    if issn=="":
        return "noJournal", artMeta["year"]

    issnYear = (issn, artMeta["year"])
    return issnYear

def checkIssnErrorCounts(pubmedMeta, ignoreIssns, outDir):
    """ raise an exception if the ISSN of the article has had too many errors 
    in this run (stored in the global issnYearErrorCounts or a previous run (stored in ignoreIssns).
    """
    issnYear = getIssnYear(pubmedMeta)
    if issnYearErrorCounts[issnYear] > MAXISSNERRORCOUNT:
        blacklistIssnYear(outDir, issnYear, pubmedMeta["journal"])
        raise pubGetError("during this run, too many errors for ISSN %s and year %s" % issnYear,
                "issnYearErrorExceed\t%s %s" % issnYear)
    if issnYear in ignoreIssns:
        raise pubGetError("a previous run disabled this issn+year", "issnErrorExceed", \
            "%s %s" % issnYear)

def resolveDoi(doi):
    """ resolve a DOI to the final target url or None on error
    #>>> resolveDoi("10.1073/pnas.1121051109")
    >>> logging.warn("doi test")
    >>> resolveDoi("10.1111/j.1440-1754.2010.01952.x")
    """
    logging.debug("Resolving DOI %s" % doi)
    doiUrl = "http://dx.doi.org/" + urllib.quote(doi.encode("utf8"))
    #resp = maxCommon.retryHttpHeadRequest(doiUrl, repeatCount=2, delaySecs=4, userAgent=userAgent)
    #if resp==None:
        #return None
    page = httpGetDelay(doiUrl)
    trgUrl = page["url"]
    logging.debug("DOI %s redirects to %s" % (doi, trgUrl))
    return trgUrl

def findCrawlers_article(artMeta):
    """
    return those crawlers that return True for canDo_article(artMeta)
    """
    crawlers = []
    for c in allCrawlers:
        if c.canDo_article(artMeta):
            logging.log(5, "Crawler %s is OK to crawl article %s" % (c.name, artMeta["title"]))
            crawlers.append(c)
    return crawlers

def findCrawlers_url(landingUrl):
    """
    return the crawlers that are OK with crawling a URL
    """
    crawlers = []
    for c in allCrawlers:
        if c.canDo_url(landingUrl):
            logging.log(5, "Crawler %s is OK to crawl url %s" % (c.name, landingUrl))
            crawlers.append(c)
    return crawlers

class Crawler():
    """
    a scraper for article webpages.
    """
    name = "empty"
    def canDo_article(self, artMeta):
        return False
    def canDo_url(self, artMeta):
        return False
    def makeLandingUrl(self, artMeta):
        return None
    def getDelay(self):
        return 0
    def crawl(self, url):
        return None

def parseDirectories(outDirs):
    """
    iterates over all directories and collects data from
    docIds.txt, issns.tab, crawler.txt, pmidStatus.tab and issnStatus.tab

    return a three-tuple:
    a list of (docId, outDir) from all outDirs, a set of docIds to skip,
    a set of issns to skip 
    """
    docIds = [] # a list of tuples (docId, outDir)
    ignoreDocIds = []
    ignoreIssns = []
    for srcDir in outDirs:
        # do some basic checks on outDir
        if not isdir(srcDir):
            continue
        srcDocIds = parseDocIds(srcDir)
        if srcDocIds==None:
            continue

        docIds.extend(srcDocIds)
        ignoreDocIds.extend(parseDocIdStatus(srcDir))
        ignoreIssns.extend(parseIssnStatus(srcDir))

    return docIds, ignoreDocIds, ignoreIssns


def findLinksByText(page, searchRe):
    " parse html page and return URLs in links with matches to given compiled re pattern"
    urls = []
    page = parseHtmlLinks(page)
    for linkUrl, linkText in page["links"].iteritems():
        dbgStr = "Checking linkText %s (url %s) against %s" % \
            (unidecode.unidecode(linkText), linkUrl, searchRe.pattern)
        logging.log(5, dbgStr)
        if searchRe.match(linkText):
            urls.append(linkUrl)
            logging.debug("Found link: %s -> %s" % (linkText, linkUrl))
    logging.debug("Found links with %s in label: %s" % (repr(searchRe.pattern), urls))
    return urls

def findLinksWithUrlPart(page, searchText, canBeOffsite=False):
    " parse html page and return URLs in links that contain some text in the href attribute"
    if page==None:
        return []
    urls = []
    page = parseHtmlLinks(page, canBeOffsite=canBeOffsite)
    for linkUrl, linkText in page["links"].iteritems():
        dbgStr = "Checking linkText %s (url %s) against %s" % \
            (unidecode.unidecode(linkText), linkUrl, searchText)
        logging.log(5, dbgStr)
        if searchText in linkUrl:
            urls.append(linkUrl)
            logging.debug("Found link: %s -> %s" % (linkText, linkUrl))
    logging.debug("Found links with %s in URL: %s" % (repr(searchText), urls))
    return urls

def downloadSuppFiles(urls, paperData, delayTime):
    suppIdx = 1
    for url in urls:
        suppFile = httpGetDelay(url, delayTime)
        fileExt = detFileExt(suppFile)
        paperData["S"+str(suppIdx)+"."+fileExt] = suppFile
        suppIdx += 1
        if suppIdx > SUPPFILEMAX:
            raise pubGetError("max suppl count reached", "tooManySupplFiles", str(len(urls)))
    return paperData

def stripOutsideOfTags(htmlStr, startTag, endTag):
    """ retain only part between two lines that include keywords, include the marker lines themselves
    Only look at the first matches of the tags. Bail out if multiple matches found.
    Require at least 10 lines in between start and end
    """
    lines = htmlStr.splitlines()
    start, end = 0,0
    for i, line in enumerate(lines):
        if startTag in line:
            if start != 0:
                logging.debug("could not strip extra html, double start tag")
                return htmlStr
            start = i
        if endTag in line and end!=0:
            if end != 0:
                logging.debug("could not strip extra html, double end tag")
                return htmlStr
            end = i
    if start!=0 and end!=0 and end > start and end-start > 10 and end < len(lines):
        logging.log(5, "stripping some extra html based on tags")
        return "".join(lines[start:end+1])
    else:
        logging.log(5, "could not strip extra html based on tags")
        return htmlStr

# cache of journal data for getHosterIssns
publisherIssns = None
publisherUrls = None

def getHosterIssns(publisherName):
    """
    get the ISSNs of a hoster from our global journal table
    """
    global publisherIssns, publisherUrls
    if publisherIssns is None:
        journalFname = pubConf.journalTable
        if not isfile(journalFname):
            logging.warn("%s does not exist, cannot use ISSNs to assign crawler" % journalFname)
            return {}, []

        # create two dicts: hoster -> issn -> url
        # and hoster -> urls
        publisherIssns = defaultdict(dict)
        publisherUrls = defaultdict(set)
        logging.log(5, "Parsing %s to get highwire ISSNs" % journalFname)

        for row in maxCommon.iterTsvRows(journalFname):
            if row.source in ["HIGHWIRE", "WILEY"]:
                hoster = row.source
                journalUrl = row.urls.strip()
                issn = row.pIssn.strip()
                eIssn = row.eIssn.strip()
                publisherIssns[hoster][issn] = journalUrl
                publisherIssns[hoster][eIssn] = journalUrl

                if journalUrl!="":
                    publisherUrls[hoster].add(journalUrl)

    return publisherIssns[publisherName], publisherUrls[publisherName]

def pageContains(page, strList):
    " check if page contains one of a list of strings "
    for text in strList:
        if text in page["data"]:
            logging.log(5, "Found string %s" % text)
            return True
    return False

def getMetaPdfUrl(page):
    " given a downloaded web page, return the citation_pdf_url meta tag value "
    if "metas" not in page:
        parseHtmlLinks(page)
    htmlMetas = page["metas"]
    if "citation_pdf_url" in htmlMetas:
        pdfUrl = htmlMetas["citation_pdf_url"]
        logging.debug("Found link to PDF in meta tag citation_pdf_url: %s" % pdfUrl)
        if not pdfUrl.startswith("http://"):
            pdfUrl = urlparse.urljoin(page["url"], pdfUrl)
        return pdfUrl
    return None

def findLinksByAttr(fullPage, attrs):
    " parse html page and look for links with a set of attributes specified as a dict "


class PmcCrawler(Crawler):
    """
    a scraper for PMC
    """
    name = "pmc"

    def canDo_article(self, artMeta):
        if ("pmcId" in artMeta and artMeta["pmcId"]!=""):
            return True
        else:
            return False

    def canDo_url(self, url):
        return ("http://www.ncbi.nlm.nih.gov/pmc/" in url)

    def makeLandingUrl(self, artMeta):
        return "http://www.ncbi.nlm.nih.gov/pmc/articles/PMC"+artMeta["pmcId"]

    def crawl(self, url):
        url = url.rstrip("/")
        delayTime = 5
        htmlPage = httpGetDelay(url, delayTime)
        waitText = "This article has a delayed release (embargo) and will be available in PMC on" 
        if waitText in htmlPage["data"]:
            logging.warn("PMC embargo note found")
            return None

        paperData = OrderedDict()

        # strip the navigation elements from the html
        html = htmlPage["data"].replace('xmlns="http://www.w3.org/1999/xhtml"', '')
        root = etree.fromstring(html)
        mainContElList = root.findall(".//div[@id='maincontent']")
        if len(mainContElList)==1:
            htmlPage["data"] = etree.tostring(mainContElList[0])

        paperData["main.html"] = htmlPage

        pdfUrl = url+"/pdf"
        pdfPage = httpGetDelay(pdfUrl, delayTime)
        paperData["main.pdf"] = pdfPage

        suppUrls = findLinksByText(htmlPage, re.compile("Click here for additional data file.*"))
        paperData = downloadSuppFiles(suppUrls, paperData, delayTime)
        return paperData

class NpgCrawler(Crawler):
    """
    a crawler for all Nature Publishing Group journals
    """
    name = "npg"

    # obtained the list of ISSNs by saving the NPG Catalog PDF to text with Acrobat 
    # downloaded from http://www.nature.com/catalog/
    # then ran this command:
    # cat 2015_NPG_Catalog_WEB.txt | tr '\r' '\n' | egrep -o '[0-9]{4}-[0-9]{4}ISSN' | sed -e "s/ISSN/',/" | sed -e "s/^/'/" | tr -d '\n'
    # No need to update this list, the DOI prefix should be enough for newer journals
    issnList = [
    '0028-0836', '0036-8733', '1087-0156', '1465-7392', '1552-4450',
    '1755-4330', '2041-1723', '1061-4036', '1752-0894', '1529-2908',
    '1476-1122', '1078-8956', '1548-7091', '1748-3387', '1097-6256',
    '1749-4885', '1745-2473', '2055-0278', '1754-2189', '1545-9993',
    '1759-5002', '1759-4774', '1474-1776', '1759-5029', '1759-5045',
    '1471-0056', '1474-1733', '1740-1526', '1471-0072', '1759-5061',
    '1759-4758', '1759-4790', '1759-4812', '2056-3973', '2055-5008',
    '2055-1010', '0002-9270', '1671-4083', '0007-0610', '2054-7617',
    '0007-0920', '2044-5385', '2047-6396', '0268-3369', '2095-6231',
    '0929-1903', '1350-9047', '2041-4889', '1748-7838', '1672-7681',
    '2050-0068', '1462-0049', '1672-7681', '0954-3007', '1018-4813',
    '2092-6413', '0969-7128', '1466-4879', '1098-3600', '2052-7276',
    '0916-9636', '0818-9641', '0955-9930', '0307-0565', '1674-2818',
    '1751-7362', '0021-8820', '1559-0631', '1434-5161', '0950-9240',
    '0743-8346', '0085-2538', '0023-6837', '0887-6924', '2047-7538',
    '2055-7434', '0893-3952', '1359-4184', '1525-0016', '2329-0501',
    '2162-2531', '2372-7705', '1933-0219', '1884-4057', '2044-4052',
    '0950-9232', '2157-9024', '0031-3998', '0032-3896', '1365-7852',
    '2052-4463', '2045-2322', '1362-4393', '2158-3188'
    ]

    def canDo_article(self, artMeta):
        # 1038 is the NPG doi prefix
        if "10.1038" in artMeta["doi"]:
            return True
        if artMeta["printIssn"] in self.issnList:
            return True
        return False

    def canDo_url(self, url):
        if "nature.com" in url:
            return True
        return False

    def _npgStripExtra(self, htmlStr):
        " retain only part between first line with <article> and </article> "
        lines = htmlStr.splitlines()
        start, end = 0,0
        for i, line in enumerate(lines):
            if "<article>" in line and start!=0:
                start = i
            if "</article>" in line and end!=0:
                end = i
        if start!=0 and end!=0 and end > start and end-start > 10 and end < len(lines):
            logging.log(5, "stripping some extra html")
            return "".join(lines[start:end+1])
        else:
            return htmlStr

    def crawl(self, url):
        # http://www.nature.com/nature/journal/v463/n7279/suppinfo/nature08696.html
        # http://www.nature.com/pr/journal/v42/n4/abs/pr19972520a.html - has no pdf
        # unusual: PMID 10854325 has a useless splash page
        if "status.nature.com" in url:
            logging.warn("Server outage at NPG, waiting for 5 minutes")
            time.sleep(300)
            pubGetError("NPG Server error page, waited 5 minutes", "errorPage", url)

        paperData = OrderedDict()

        # make sure get the main text page, not the abstract
        url = url.replace("/abs/", "/full/")
        delayTime = 5
        htmlPage = httpGetDelay(url, delayTime)
        if pageContains(htmlPage, ["make a payment", "purchase this article"]):
            return None

        # try to strip the navigation elements from more recent article html
        html = htmlPage["data"]
        htmlPage["data"] = self._npgStripExtra(html)
        paperData["main.html"] = htmlPage

        pdfUrl = url.replace("/full/", "/pdf/").replace(".html", ".pdf")
        pdfPage = httpGetDelay(pdfUrl, delayTime)
        paperData["main.pdf"] = pdfPage

        suppUrls = findLinksWithUrlPart(htmlPage, "/extref/")
        paperData = downloadSuppFiles(suppUrls, paperData, delayTime)
        return paperData

class ElsevierCrawler(Crawler):
    """ sciencedirect.com is Elsevier's hosting platform 
    This crawler is minimalistic, we use ConSyn to get Elsevier text at UCSC.
    """
    name = "elsevier"

    def canDo_url(self, url):
        if "sciencedirect.com" in url:
            return True
        else:
            return False

    def crawl(self, url):
        paperData = OrderedDict()
        delayTime = crawlDelays["elsevier"]

        url = url+"?np=y" # get around javascript requirement
        htmlPage = httpGetDelay(url, delayTime)

        if pageContains(htmlPage, ["Choose an option to locate/access this article:", "purchase this article", "Purchase PDF"]):
            raise pubGetError("noLicense", "no license")
        if pageContains(htmlPage, ["Sorry, the requested document is unavailable."]):
            raise pubGetError("documentUnavail", "document is not available")

        # strip the navigation elements from the html
        html = htmlPage["data"]
        bs = BeautifulSoup(html)
        mainCont = bs.find("div", id='centerInner')
        if mainCont!=None:
            htmlPage["data"] = str(mainCont)
        htmlPage["url"] = htmlPage["url"].replace("?np=y", "")
        paperData["main.html"] = htmlPage

        # main PDF
        pdfEl = bs.find("a", id="pdfLink")
        if pdfEl!=None:
            pdfUrl = pdfEl["href"]
            pdfUrl = urlparse.urljoin(htmlPage["url"], url)
            pdfPage = httpGetDelay(pdfUrl, delayTime)
            paperData["main.pdf"] = pdfPage
            # the PDF link becomes invalid after 10 minutes, so direct users
            # to html instead when they select a PDF
            paperData["main.pdf"]["url"] = htmlPage["url"]
        
        # supp files
        suppEls = bs.findAll("a", attrs={'class':'MMCvLINK'})
        if len(suppEls)!=0:
            suppUrls = [s["href"] for s in suppEls]
            paperData = downloadSuppFiles(suppUrls, paperData, delayTime)

        return paperData
            
        # this choked on invalid HTML, so not using LXML anymore
        #root = etree.fromstring(html)
        #mainContElList = root.findall(".//div[@id='centerInner']")
        #if len(mainContElList)==1:
            #logging.debug("Stripping extra html from sciencedirect page")
            #htmlPage["data"] = etree.tostring(mainContElList[0])
        #linkEl = root.findall(".//div[@title='Download PDF']")
        #if linkEl==None:
            #logging.error("sciencedirect PDF link not found")
        #else:
            #pdfUrl = linkEl.attrib.get("pdfurl")
            #if pdfUrl is None:
                #logging.error("sciencedirect PDF link href/pdfurl not found")
            #else:
                #pdfPage = httpGetDelay(pdfUrl, delayTime)
                #paperData["main.pdf"] = pdfPage

class HighwireCrawler(Crawler):
    " crawler for old-style highwire files. cannot get suppl files out of the new-style pages  "
    # new style files are actually drupal, so 
    # is a redirect to http://www.bloodjournal.org/node/870328, the number is in a meta tag 
    # "shortlink"
    # The suppl data is in http://www.bloodjournal.org/panels_ajax_tab/jnl_bloodjournal_tab_data/node:870328/1?panels_ajax_tab_trigger=figures-only
    # Some new suppl material we can get, e.g. http://emboj.embopress.org/content/34/7/955#DC1
    # because it's linked from the main page
    name = "highwire"

    # little hard coded list of top highwire sites, to avoid some DNS lookups
    highwireHosts = ["asm.org", "rupress.org", "jcb.org", "cshlp.org", \
        "aspetjournals.org", "fasebj.org", "jleukbio.org"]
    # cache of IP lookups, to avoid some DNS lookups which tend to fail in python
    hostCache = {}

    # table with ISSN -> url, obtained from our big journal list
    highwireIssns = None
    # set of highwire hosts
    highwireHosts = set()

    def _highwireDelay(self, url):
        """ return current delay for highwire, depending on current time at east coast
            can be overriden in pubConf per host-keyword
        """
        hostname = urlparse.urlsplit(url)[1]
        for hostKey, delaySec in pubConf.highwireDelayOverride.iteritems():
            if hostKey in hostname:
                logging.debug("Overriding normal Highwire delay with %d secs as specified in conf" % delaySec)
                return delaySec

        os.environ['TZ'] = 'US/Eastern'
        if hasattr(time, "tzset"):
            time.tzset()
        tm = time.localtime()
        # highwire delay is 5seconds on the weekend
        # or 60sec/10secs depending on if they work on the East Coast
        # As instructed by Highwire by email
        if tm.tm_wday in [5,6]:
            delay=5
        else:
            if tm.tm_hour >= 9 and tm.tm_hour <= 17:
                delay = 60
            else:
                delay = 10
        logging.log(5, "current highwire delay time is %d" % (delay))
        return delay

    def canDo_article(self, artMeta):
        " return true if ISSN is known to be hosted by highwire "
        if self.highwireIssns is None:
            self.highwireIssns, self.highwireHosts = getHosterIssns("HIGHWIRE")

        if artMeta["printIssn"] in self.highwireIssns:
            return True
        if artMeta["eIssn"] in self.highwireIssns:
            return True

        return False

    def canDo_url(self, url):
        "return true if a hostname is hosted by highwire at stanford "
        hostname = urlparse.urlsplit(url)[1]
        for hostEnd in self.highwireHosts:
            if hostname.endswith(hostEnd):
                return True
        if hostname in self.hostCache:
            ipAddr = self.hostCache[hostname]
        else:
            logging.debug("Looking up IP for %s" % hostname)
            try:
                ipAddr = socket.gethostbyname(hostname)
                self.hostCache[hostname] = ipAddr
            except socket.gaierror:
                raise pubGetError("Illegal hostname %s in link" % hostname, "invalidHostname", hostname)

        ipParts = ipAddr.split(".")
        ipParts = [int(x) for x in ipParts]
        result = (ipParts[0] == 171 and ipParts[1] in range(64, 68)) # stanford IP range
        if result==True:
            logging.log(5, "hostname %s is highwire host" % hostname)
        return result

    def makeLandingUrl(self, artMeta):
        " given the article meta, construct a landing URL and check that it's valid "
        issn = getIssn(artMeta)
        if issn in self.highwireIssns:
            baseUrl = self.highwireIssns[issn]
            delayTime = self._highwireDelay(baseUrl)

            # try the vol/issue/page, is a lot faster
            vol = artMeta.get("vol", "")
            issue = artMeta.get("issue", "")
            page = artMeta.get("page", "")
            if (vol, issue, page) != ("", "", ""):
                url = "%s/content/%s/%s/%s.long" % (baseUrl, vol, issue, page)
                page = httpGetDelay(url, delayTime)
                if page != None:
                    return url

            if "pmid" in artMeta:
                url = "%s/cgi/pmidlookup?view=long&pmid=%s" % (baseUrl, artMeta["pmid"])
                page = httpGetDelay(url, delayTime)
                if page != None:
                    return url

        return None

    def crawl(self, url):
        " get main html, pdf and supplements for highwire "
        paperData = OrderedDict()

        if url.endswith(".short"):
            # make sure we don't try to crawl the abstract page
            url = url.replace(".short", ".long")
        if not url.endswith(".long") and not "pmidlookup" in url:
            url = url+".long"

        delayTime = self._highwireDelay(url)
        htmlPage = httpGetDelay(url, delayTime)

        if htmlPage["mimeType"] != "application/pdf" and not htmlPage["data"].startswith("%PDF"):
            aaasStr = "The content you requested is not included in your institutional subscription"
            aacrStr = "Purchase Short-Term Access"
            stopWords = [aaasStr, aacrStr]
            if pageContains(htmlPage, stopWords):
                raise pubGetError("noLicense", "no license for this article")

            if pageContains(htmlPage, ["We are currently doing routine maintenance"]):
                time.sleep(600)
                raise pubGetError("siteMaintenance", "site is down, waited for 10 minutes")
            # try to strip the navigation elements from more recent article html
            # highwire has at least two generators: a new one based on drupal and their older
            # in-house one
            if "drupal.org" in htmlPage["data"]:
                logging.debug("Drupal-Highwire detected")
                # trailing space!
                htmlPage["data"] = htmlExtractPart(htmlPage, "div", {"class":"article fulltext-view "})
            else:
                htmlPage["data"] = htmlExtractPart(htmlPage, "div", {"id":"content-block"})

            # also try to strip them via two known tags highwire is leaving for us
            htmlPage["data"] = stripOutsideOfTags(htmlPage["data"], "highwire-journal-article-marker-start", \
                "highwire-journal-article-marker-end")

            paperData["main.html"] = htmlPage

        else:
            logging.warn("Got PDF page where html page was expected, no html available")

        # check if we have a review process file, EMBO journals
        # e.g. http://emboj.embopress.org/content/early/2015/03/31/embj.201490819
        if "Transparent Process" in htmlPage["data"]:
            reviewUrl = url.replace(".long","")+".reviewer-comments.pdf"
            logging.debug("Downloading review process file")
            reviewPage = httpGetDelay(reviewUrl, delayTime)
            paperData["review.pdf"] = reviewPage

        pdfUrl = url.replace(".long", ".full.pdf")
        pdfPage = httpGetDelay(pdfUrl, delayTime)
        paperData["main.pdf"] = pdfPage

        suppListUrl = url.replace(".long", "/suppl/DC1")
        suppListPage = httpGetDelay(suppListUrl, delayTime)
        suppUrls = findLinksWithUrlPart(suppListPage, "/content/suppl/")
        if len(suppUrls)==0:
            suppUrls = findLinksWithUrlPart(suppListPage, "supplementary-material.")
        paperData = downloadSuppFiles(suppUrls, paperData, delayTime)
        return paperData

class NejmCrawler(Crawler):
    " the new england journal of medicine seems to have its own hosting platform "

    name = "nejm"

    def canDo_article(self, artMeta):
        if artMeta["printIssn"]=="0028-4793" or artMeta["eIssn"]=="1533-4406":
            return True
        if artMeta["doi"].startswith("10.1056"):
            return True
        return False

    def canDo_url(self, url):
        if "nejm.org" in url:
            return True
        else:
            return False

    def crawl(self, url):
        paperData = OrderedDict()
        delayTime = crawlDelays["nejm"]

        htmlPage = httpGetDelay(url)

        # suppl files first, as we modify the html afterwards
        suppListUrls = findLinksWithUrlPart(htmlPage, "/showSupplements?")
        if len(suppListUrls)==1:
            suppListPage = httpGetDelay(suppListUrls[0])
            suppUrls = findLinksWithUrlPart(suppListPage, "/suppl_file/")
            paperData = downloadSuppFiles(suppUrls, paperData, delayTime)

        # strip the navigation elements from the html
        html = htmlPage["data"]
        bs = BeautifulSoup(html)
        mainCont = bs.find("div", id='content')
        if mainCont!=None:
            htmlPage["data"] = str(mainCont)
        htmlPage["url"] = htmlPage["url"].replace("?np=y", "")
        paperData["main.html"] = htmlPage

        # PDF 
        pdfUrl = url.replace("/full/", "/pdf/")
        assert(pdfUrl != url)

        return paperData
    
class WileyCrawler(Crawler):
    """
    for wileyonline.com, Wiley's hosting platform
    """
    name = "wiley"

    issnList = None
    urlList = None

    def canDo_article(self, artMeta):
        if self.issnList==None:
            self.issnList, self.urlList = getHosterIssns("WILEY")
        if artMeta["printIssn"] in self.issnList or  \
            artMeta["eIssn"] in self.issnList:
            return True
        # DOI prefixes for wiley and the old blackwell prefix
        if artMeta["doi"].startswith("10.1002") or artMeta["doi"].startswith("10.1111"):
            return True
        return False

    def canDo_url(self, url):
        if "onlinelibrary.wiley.com" in url:
            return True
        else:
            return False

    def makeLandingUrl(self, artMeta):
        ""
        url = "http://onlinelibrary.wiley.com/resolve/openurl?genre=article&sid=genomeBot&issn=%(printIssn)s&volume=%(vol)s&issue=%(issue)s&spage=%(page)s" % artMeta
        return url
        
    def crawl(self, url):
        delayTime = crawlDelays["wiley"]
        paperData = OrderedDict()
        # landing URLs looks like this:
        # http://onlinelibrary.wiley.com/doi/10.1002/ijc.28737/abstract;jsessionid=17141D5DEE13E4C5A32C45C29AFADED8.f04t01
        # the url goes in most cases to the abstract, but it may well be an openurl
        # so we first have to resolve it to the final url before we can continue
        absPage = httpGetDelay(url, delayTime)
        absUrl = absPage["url"]

        # try to get the fulltext html
        mainUrl = absUrl.replace("/abstract", "/full")
        mainPage = httpGetDelay(mainUrl, delayTime)
        if "You can purchase online access" in mainPage["data"] or \
           "Registered Users please login" in mainPage["data"]:
            return None

        # strip the navigation elements from the html
        absHtml = htmlExtractPart(mainPage, "div", {"id":"articleDesc"})
        artHtml = htmlExtractPart(mainPage, "div", {"id":"fulltext"})
        if absHtml!=None and artHtml!=None:
            logging.debug("Stripped extra wiley html")
            mainHtml = absHtml + artHtml
            mainPage["data"] = mainHtml
        paperData["main.html"] = mainPage

        # pdf
        #pdfUrl = getMetaPdfUrl(mainPage)
        pdfUrl = absUrl.replace("/abstract", "/pdf")
        pdfPage = httpGetDelay(pdfUrl, delayTime)
        parseHtmlLinks(pdfPage)
        if "pdfDocument" in pdfPage["iframes"]:
            logging.debug("found framed PDF, requesting inline pdf")
            pdfPage  = httpGetDelay(pdfPage["iframes"]["pdfDocument"], delayTime)
        paperData["main.pdf"] = pdfPage

        # supplements
        # example suppinfo links 20967753 - major type of suppl
        # spurious suppinfo link 8536951 -- doesn't seem to be true in 2015 anymore
        suppListUrl = findLinksWithUrlPart(mainPage, "/suppinfo/")
        if len(suppListUrl)!=1:
            logging.debug("No list to suppl file list page found")
            return paperData

        suppListPage = httpGetDelay(suppListUrl, delayTime)
        suppUrls = findLinksWithUrlPart(suppListPage, "/asset/supinfo/")
        if len(suppUrls)==0:
            # legacy supp info links?
            suppUrls = findLinksWithUrlPart(suppListPage, "_s.pdf")
        paperData = downloadSuppFiles(suppUrls, paperData, delayTime)
        return paperData

class SpringerCrawler(Crawler):
    " crawler for springerlink "

    name = "springer"

    def canDo_article(self, artMeta):
        if artMeta["doi"].startswith("10.1007"):
            return True
        return False

    def canDo_url(self, url):
        if "link.springer.com" in url:
            return True
        else:
            return False

    def crawl(self, url):
        paperData = OrderedDict()
        delayTime = crawlDelays["springer"]

        absPage = httpGetDelay(url, delayTime)
        if pageContains(absPage, ["make a payment", "purchase this article", "Buy now"]):
            return None

        # landing page has only abstract
        fullUrl = url+"/fulltext.html"
        fullPage = httpGetDelay(fullUrl, delayTime)
        fullPage["data"] = htmlExtractPart(fullPage, "div", {"class":"FulltextWrapper"})
        paperData["main.html"] = fullPage

        # PDF 
        pdfUrl = url.replace("/article/", "/content/pdf/")+".pdf"
        pdfPage = httpGetDelay(pdfUrl, delayTime)
        paperData["main.pdf"] = pdfPage

        # suppl files 
        suppUrls = findLinksWithUrlPart(absPage, "/MediaObjects/")
        paperData = downloadSuppFiles(suppUrls, paperData, delayTime)

        return paperData
    
class LwwCrawler(Crawler):
    """ 
    Lippincott-Williams is Wolters-Kluwer's journal branch.
    crawler for the various wolters/kluwer related hosting websites.
    There seem to be two completely different systems.
    """

    name = "lww"

    issnList = None

    def canDo_article(self, artMeta):
        if self.issnList==None:
            self.issnList, _ = getHosterIssns("LWW")

        if artMeta["printIssn"] in self.issnList or artMeta["eIssn"] in self.issnList:
            return True
        if artMeta["doi"].startswith("10.1097"):
            return True
        return False

    def canDo_url(self, url):
        if "wkhealth.com" in url or "lww.com" in url:
            return True
        else:
            return False

    def makeLandingUrl(self, artMeta):
        url =  "http://content.wkhealth.com/linkback/openurl?issn=%(printIssn)s&volume=%(vol)s&issue=%(issue)s&spage=%(page)s" % artMeta
        return url

    def crawl(self, url):
        paperData = OrderedDict()
        delayTime = crawlDelays["lww"]

        if "landingpage.htm" in url and "?" in url:
            # get around the ovid/lww splash page
            logging.debug("Routing around ovid splash page")
            params = url.split("&")[1:]
            # remove type=abstract from params
            # the .js code seems to do this
            params = [s for s in params if s!="type"]
            url = "http://content.wkhealth.com/linkback/openurl?" + "&".join(params)

        #if pageContains(absPage, ["make a payment", "purchase this article", "Buy now"]):
            #return None
        fullPage = httpGetDelay(url, delayTime)
        if fullPage==None:
            return None
        if "type=abstract" in fullPage["url"]:
            url = fullPage["url"].replace("type=abstract", "type=fulltext")
            logging.debug("Regetting page for fulltext with %s" % url)
            fullPage = httpGetDelay(url, delayTime)

        fullPage["data"] = htmlExtractPart(fullPage, "div", {"id":"ej-article-view"})
        paperData["main.html"] = fullPage

        # PDF 
        # lww PDFs are not on the same server => offsite
        pdfUrls = findLinksWithUrlPart(fullPage, "pdfs.journals.lww.com", canBeOffsite=True)
        if len(pdfUrls)==1:
            pdfPage = httpGetDelay(pdfUrls[0], delayTime)
            paperData["main.pdf"] = pdfPage

        # suppl files , also on different server
        suppUrls = findLinksWithUrlPart(fullPage, "links.lww.com", canBeOffsite=True)
        paperData = downloadSuppFiles(suppUrls, paperData, delayTime)

        return paperData
    
class SilverchairCrawler(Crawler):
    " Silverchair is an increasingly popular hoster "
    name = "silverchair"

    def canDo_url(self, url):
        # pretty stupid check, have no better idea
        if "article.aspx" in url or "/cgi/doi/" in url:
            return True
        else:
            return False

    #def makeLandingUrl(self, artMeta):
        # did not work for old
        # pages like http://journal.publications.chestnet.org/article.aspx?articleid=1065945
        #if artMeta["doi"]!="":
            #url =  "http://journal.publications.chestnet.org/article.aspx?doi=%(doi)s" % artMeta
            #return url
        #else:
            #return None

    def crawl(self, url):
        paperData = OrderedDict()
        delayTime = crawlDelays["silverchair"]

        fullPage = httpGetDelay(url, delayTime)
        if pageContains(fullPage, ["Purchase a Subscription"]):
            logging.debug("No license")
            return None
        if fullPage==None:
            logging.debug("Got no page")
            return None

        # pages like http://journal.publications.chestnet.org/article.aspx?articleid=1065945
        # have no html view
        if "First Page Preview" in fullPage["data"]:
            logging.debug("No html view")
        elif "The resource you are looking for might have been removed" in fullPage["data"]:
            raise pubGetError("errorPage", "landing page is error page")
        else:
            fullPage["data"] = htmlExtractPart(fullPage, "div", {"class":"left contentColumn eqColumn"})
            paperData["main.html"] = fullPage

        # PDF 
        pdfUrls = htmlFindLinkUrls(fullPage, {"class" : "linkPDF"})
        if len(pdfUrls)==1:
            pdfPage = httpGetDelay(pdfUrls[0], delayTime)
            paperData["main.pdf"] = pdfPage

        # suppl files 
        suppUrls = htmlFindLinkUrls(fullPage, {"class" : "supplementLink"})
        paperData = downloadSuppFiles(suppUrls, paperData, delayTime)

        return paperData

# the list of all crawlers
# order is important: the most specific crawlers come first
allCrawlers = [
    ElsevierCrawler(), NpgCrawler(), HighwireCrawler(), SpringerCrawler(), \
    WileyCrawler(), SilverchairCrawler(), NejmCrawler(), LwwCrawler(), PmcCrawler()
    ]
allCrawlerNames = [c.name for c in allCrawlers]

def sortCrawlers(crawlers):
    """
    re-order crawlers in the same order that they have in the allCrawlers list
    """
    cByName = {}
    for c in crawlers:
        cByName[c.name] = c
    
    sortedCrawlers = []
    for cName in allCrawlerNames:
        if cName in cByName:
            sortedCrawlers.append(cByName[cName])
    return sortedCrawlers

def selectCrawlers(artMeta, srcDir):
    """
    returns the crawlers to use for an article, by first asking all crawlers
    if they want to handle this paper, based on either article meta
    data like ISSN or pmcId, or - if that fails - on the landing page.
    """
    # if a crawler is specified via a text file, just use it
    if srcDir!=None:
        crawlerSpecFname = join(srcDir, "crawler.txt")
        if isfile(crawlerSpecFname):
            crawlerName = open(crawlerSpecFname).read().strip()
            crawlers = [c for c in allCrawlers if c.name==crawlerName]
            return crawlers

    # find crawlers that agree to crawl, based on the article meta
    okCrawlers = findCrawlers_article(artMeta)
    landingUrl = None
    crawlerNames = [c.name for c in okCrawlers]

    if len(okCrawlers)==0 or crawlerNames==["pmc"]:
        # get the landing URL from a search engine like pubmed or crossref
        # and ask the crawlers again
        logging.debug("No crawler or only PMC accepted paper based on meta data, getting landing URL")
        landingUrl = getLandingUrlSearchEngine(artMeta)
        okCrawlers.extend(findCrawlers_url(landingUrl))

    if len(okCrawlers)==0:
        #logging.info("No crawler found on either article metadata or URL. Using generic crawler")
        #return [genericCrawler]
        logging.info("No crawler found on either article metadata or URL.")
        return [], landingUrl

    okCrawlers = sortCrawlers(okCrawlers)

    logging.debug("List of crawlers for this document, by priority: %s" % [c.name for c in okCrawlers])
    return okCrawlers, landingUrl

def crawlOneDoc(artMeta, srcDir):
    """
    return all data from a paper given the article meta data
    """
    # determine the crawlers to use, this possibly produces a landing url as a side-effect
    crawlers, landingUrl = selectCrawlers(artMeta, srcDir)
    if len(crawlers)==0:
        errMsg = "no crawler for article %s at %s" % (artMeta["title"], landingUrl)
        raise pubGetError(errMsg, "noCrawler", landingUrl)

    artMeta["page"] = artMeta["page"].split("-")[0] # need only first page
    if landingUrl is not None:
        artMeta["landingUrl"] = landingUrl

    for crawler in crawlers:
        logging.info("Trying crawler %s" % crawler.name)
        # first try if the crawler can generate the landing url from the metaData
        url = crawler.makeLandingUrl(artMeta)
        if url==None:
            if landingUrl!=None:
                url = landingUrl
            else:
                url = getLandingUrlSearchEngine(artMeta)

        logging.info("Crawling base URL %s" % url)
        paperData = crawler.crawl(url)

        if paperData!=None:
            paperData["crawlerName"] = crawler.name
            return paperData
        else:
            return None

    logging.warn("No crawler was able to handle paper, giving up")
    raise pubGetError("noCrawlerSuccess", "No crawler was able to handle the paper")

def getArticleMeta(docId):
    " get pubmed article info from local db or ncbi webservice. return as dict. "
    artMeta = None

    haveMedline = pubConf.mayResolveTextDir("medline")

    if haveMedline and not SKIPLOCALMEDLINE:
        artMeta = readLocalMedline(docId)
    if artMeta==None:
        artMeta = downloadPubmedMeta(docId)

    return artMeta

def crawlDocuments(docIds, skipDocIds, skipIssns):
    """
    run crawler on a list of (paperId, sourceDir) tuples
    """
    totalCount = 0
    consecErrorCount = 0
    for docId, srcDir in docIds:
        if docId in skipDocIds:
            logging.log(5, "Skipping docId %s" % docId)
            continue
        logging.info("Crawling document with ID %s" % docId)

        global webCache
        webCache.clear()

        try:
            artMeta = getArticleMeta(docId)
        except pubGetError:
            writeDocIdStatus(srcDir, docId, "no meta", "")
            continue

        logging.info("Got Metadata: %s, %s, %s" % (artMeta["journal"], artMeta["year"], artMeta["title"]))

        try:
            checkIssnErrorCounts(artMeta, skipIssns, srcDir)
            paperData = crawlOneDoc(artMeta, srcDir)
            writePaperData(docId, artMeta, paperData, srcDir)
            consecErrorCount = 0
            totalCount += 1

        except pubGetError, e:
            # track document failure
            consecErrorCount += 1
            docId = artMeta["pmid"]
            logging.error("docId %s, error: %s, code: %s, details: %s" % (docId, e.longMsg, e.logMsg, e.detailMsg))
            writeDocIdStatus(srcDir, docId, e.logMsg, e.detailMsg)

            # track journal failure counts
            issnYear = getIssnYear(artMeta)
            global issnYearErrorCounts
            issnYearErrorCounts[issnYear] += 1

            # some errors require longer waiting times
            if e.logMsg not in ["noOutlinkOrDoi", "unknownHost", "noLicense"]:
                waitSec = ERRWAIT*consecErrorCount
                logging.debug("Sleeping for %d secs after error" % waitSec)
                time.sleep(waitSec)

            # if too many errors in a row, bail out
            if consecErrorCount > MAXCONSECERR:
                logging.error("Too many consecutive errors, stopping crawl")
                e.longMsg = "Crawl stopped after too many consecutive errors / "+e.longMsg
                raise

            if DO_PAUSE:
                raw_input("Press Enter to process next paper...")
        except:
            raise

    logging.info("Downloaded %d articles" % (totalCount))


if __name__=="__main__":
    import doctest
    doctest.testmod()
