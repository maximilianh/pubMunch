# library to crawl pdf and supplemental file from pubmed

# load our own libraries
import pubConf, pubGeneric, maxMysql, pubStore, tabfile, maxCommon, pubPubmed, maxTables, \
    pubCrossRef, html, maxCommon, pubCrawlConf
import chardet # library for guessing encodings
#from bs4 import BeautifulSoup  # the new version of bs crashes too much
from BeautifulSoup import BeautifulSoup, SoupStrainer, BeautifulStoneSoup # parsing of non-wellformed html

import logging, optparse, os, shutil, glob, tempfile, sys, codecs, types, re, \
    traceback, urllib2, re, zipfile, collections, urlparse, time, atexit, socket, signal, \
    sqlite3, doctest, urllib, copy
from os.path import *

# ===== GLOBALS ======

# options for wget 
# (python's http implementation is extremely buggy and tends to hang for minutes)
WGETOPTIONS = " --no-check-certificate --tries=3 --random-wait --waitretry=%d --connect-timeout=%d --dns-timeout=%d --read-timeout=%d --ignore-length " % (pubConf.httpTimeout, pubConf.httpTimeout, pubConf.httpTimeout, pubConf.httpTimeout)

# global variable, http userAgent for all requests
userAgent = None

# name of pmid status file
PMIDSTATNAME = "pmidStatus.tab"

# name of issn status file
ISSNSTATNAME = "issnStatus.tab"

# maximum of suppl files
SUPPFILEMAX = 25

# max number of consecutive errors
# will abort if exceeded
MAXCONSECERR = 50
MAXCONSECERR_TRYHARD = 500

# maximum number of errors per issn and year
# after this number of errors per ISSN, an ISSN will be ignored
MAXISSNERRORCOUNT = 30

# number of seconds to wait after an error
ERRWAIT = 10
ERRWAIT_TRYHARD = 3

# GLOBALS 

# filename of lockfile
lockFname = None

# list of highwire sites, for some reason ip resolution fails too often
highwireHosts = ["asm.org", "rupress.org", "jcb.org", "cshlp.org", "aspetjournals.org", "fasebj.org", "jleukbio.org"] # too many DNS queries fail, so we hardcode some of the work

# if any of these is found in a landing page Url, wait for 15 minutes and retry
# has to be independent of pubsCrawlCfg, NPG at least redirects to a separate server
errorPageUrls = ["http://status.nature.com"]

# wget page cache, to avoid duplicate downloads
wgetCache = {}

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
"fulltextUrl",     # URL to fulltext of article
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
        return repr(self.longMsg+"/"+self.logMsg)

# ===== FUNCTIONS =======

def resolvePmidWithSfx(sfxServer, pmid):
    " return the fulltext url for pmid using the SFX system "
    logging.debug("Resolving pmid %s with SFX" % pmid)
    xmlQuery = '%s/SFX_API/sfx_local?XML=<?xml version="1.0" ?><open-url><object_description><global_identifier_zone><id>pmid:%s</id></global_identifier_zone><object_metadata_zone><__service_type>getFullTxt</__service_type></object_metadata_zone></object_description></open-url>' % (sfxServer, str(pmid))
    sfxResult = delayedWget(xmlQuery, forceDelaySecs=0)
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

def findLandingUrl(articleData, crawlConfig, hostToConfig):
    """ try to find landing URL either by constructing it or by other means:
    - inferred from medline data via landingUrl_templates
    - medlina's DOI
    - a Crossref search with medline data
    - Pubmed Outlink 
    - an SFX search
    >>> findLandingUrl({"pmid":"12515824", "doi":"10.1083/jcb.200210084", "printIssn" : "1234", "page":"8"}, {}, {})
    'http://jcb.rupress.org/content/160/1/53'
    """
    logging.log(5, "Looking for landing page")

    landingUrl = None

    # sometimes, if know the publisher upfront, we can derive the landing page from the medline data
    # e.g. via pmid or doi
    if crawlConfig==None:
        logging.debug("no config, cannot use URL templates")
    else:
        issn = articleData["printIssn"]
        # need to use print issn as older articles in pubmed don't have any eIssn
        logging.debug("Trying URL template to find landing page for *PRINT* issn %s", issn)
        articleData["firstPage"] = articleData["page"].split(".")[0].split("-")[0]
        logging.debug("firstPage %s" % articleData["firstPage"])
        urlTemplates = crawlConfig.get("landingUrl_templates", {})
        urlTemplate = urlTemplates.get(issn, None)
        # if the ISSN is not in, try the "anyIssn" template
        if urlTemplate==None and "anyIssn" in urlTemplates:
            urlTemplate = urlTemplates.get("anyIssn", None)

        if urlTemplate==None:
            logging.debug("No template found for issn %s" % issn)
        else:
            landingUrl = urlTemplate % articleData
            assert(landingUrl != urlTemplate)
            # check if url is OK
            try:
                landingPage  = delayedWget(landingUrl)
                landingUrl = landingPage["url"]
                logging.debug("found landing url %s" % landingUrl)
            except pubGetError:
                logging.debug("Constructed URL %s is not valid, trying other options" % landingUrl)
                landingUrl = None

    # try medline's DOI
    # note that can sometimes differ e.g. 12515824 directs to a different page via DOI
    # than via Pubmed outlink, so we need sometimes to rewrite the doi urls
    if landingUrl==None and articleData["doi"]!="":
        landingUrl, crawlConfig = resolveDoiRewrite(articleData["doi"], crawlConfig, hostToConfig)

    # try crossref's search API to find the DOI
    if landingUrl==None and articleData["doi"]=="":
        xrDoi = pubCrossRef.lookupDoi(articleData)
        if xrDoi != None:
            articleData["doi"] = xrDoi
            landingUrl, crawlConfig = resolveDoiRewrite(xrDoi, crawlConfig, hostToConfig)

    # try pubmed's outlink
    if landingUrl==None:
        outlinks = pubPubmed.getOutlinks(articleData["pmid"])
        if outlinks==None:
            logging.info("pubmed error, waiting for 120 secs")
            time.sleep(120)
            raise pubGetError("pubmed outlinks http error", "PubmedOutlinkHttpError")

        if len(outlinks)!=0:
            landingUrl =  outlinks.values()[0]
            logging.debug("landing page based on first outlink of Pubmed, URL %s" % landingUrl)

    # try SFX
    if landingUrl==None:
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


defaultDelay = 20

def delayedWget(url, forceDelaySecs=None):
    """ download with wget and make sure that delaySecs (global var) secs have passed between two calls
    special cases for highwire hosts and some hosts configured in config file.
    """
    global wgetCache
    if url in wgetCache:
        logging.log(5, "Using cached wget results")
        return wgetCache[url]

    if forceDelaySecs==None:
        host = urlparse.urlsplit(url)[1]
        logging.debug("Looking up delay time for host %s" % host)
        if host in pubCrawlConf.crawlDelays:
            delaySecs = pubCrawlConf.crawlDelays.get(host, defaultDelay)
            logging.debug("Delay time for host %s configured in pubConf as %d seconds" % (host, delaySecs))
        elif isHighwire(host):
            delaySecs = highwireDelay()
        else:
            logging.debug("Delay time for host %s not configured in pubConf" % (host))
            delaySecs = defaultDelay
    else:
        delaySecs = forceDelaySecs
        host = "noHost"

    wait(delaySecs, host)
    page = runWget(url)
    return page

def runWget(url):
    """ download url with wget and return dict with keys url, mimeType, charset, data 
    global variable userAgent is used if possible
    """
    # check if file is already in cache
    global wgetCache
    if url in wgetCache:
        logging.log(5, "Using cached wget results")
        return wgetCache[url]

    logging.debug("Downloading %s" % url)
    url = url.replace("'", "")

    # construct user agent
    global userAgent
    if userAgent==None:
        userAgent = pubConf.httpUserAgent
    userAgent = userAgent.replace("'", "")

    # construct & run wget command
    tmpFile = tempfile.NamedTemporaryFile(dir = pubConf.getTempDir(), \
        prefix="WgetGoogleCrawler", suffix=".data")
    cmd = "wget '%s' -O %s --server-response " % (url, tmpFile.name)
    cmd += WGETOPTIONS
    cmd += "--user-agent='%s'" % userAgent

    logFile = tempfile.NamedTemporaryFile(dir = pubConf.getTempDir(), \
        prefix="pubGetPmid-Wget-", suffix=".log")
    cmd += " -o %s " % logFile.name
    logging.verbose("command: %s" % cmd)
    print cmd
    stdout, stderr, ret = pubGeneric.runCommandTimeout(cmd, timeout=pubConf.httpTransferTimeout)
    if ret!=0:
        #logging.debug("non-null return code from wget, sleeping for 120 seconds")
        #time.sleep(120)
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

    wgetCache[finalUrl] = ret
    wgetCache[url] = ret

    return ret

def storeFilesNoZip(pmid, metaData, fulltextData, outDir):
    """ write files from dict (keys like main.html or main.pdf or s1.pdf, value is binary data) 
    to directory <outDir>/files
    """
    fileDir = join(outDir, "files")
    if not isdir(fileDir):
        os.makedirs(fileDir)

    suppFnames = []
    suppUrls = []
    for suffix, pageDict in fulltextData.iteritems():
        if suffix=="status":
            continue
        if suffix=="landingPage":
            metaData["landingUrl"] = pageDict["url"]
            continue

        filename = pmid+"."+suffix

        if suffix=="main.html":
            metaData["mainHtmlFile"] = filename
            metaData["mainHtmlUrl"] = pageDict["url"]
        elif suffix=="main.pdf":
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

    # "," in urls? this happened 2 times in 1 million files
    suppFnames = [s.replace(",", "") for s in suppFnames]
    suppUrls = [s.replace(",", "") for s in suppUrls]

    metaData["suppFiles"] = ",".join(suppFnames)
    metaData["suppUrls"] = ",".join(suppUrls)
    return metaData

def storeFiles(pmid, metaData, fulltextData, outDir):
    """ write files from dict (keys like main.html or main.pdf or s1.pdf, value is binary data) 
    to target zip file saves all binary data to <issn>.zip in outDir with filename pmid.<key>
    """
    #global dataZipFile
    suppFnames = []
    suppUrls = []
    for suffix, pageDict in fulltextData.iteritems():
        if suffix=="status":
            continue
        if suffix=="landingPage":
            metaData["landingUrl"] = pageDict["url"]
            continue

        filename = pmid+"."+suffix

        if suffix=="main.html":
            metaData["mainHtmlFile"] = filename
            metaData["mainHtmlUrl"] = pageDict["url"]
        elif suffix=="main.pdf":
            metaData["mainPdfFile"] = filename
            metaData["mainPdfUrl"] = pageDict["url"]
        elif suffix.startswith("S"):
            suppFnames.append(filename)
            suppUrls.append(pageDict["url"])
            
        fileData = pageDict["data"]
        zipBase = "%s.zip" % metaData["eIssn"]
        zipName = join(outDir, zipBase)
        logging.debug("Writing %d bytes to %s as %s" % (len(fileData), zipName, filename))
        #logging.debug("Opening zip")
        dataZipFile = zipfile.ZipFile(zipName, "a")
        try:
            dataZipFile.writestr(filename, fileData)
        except zipfile.LargeZipFile:
            dataZipFile.close()
            i = 0
            while True:
                # append _X to zipname to make unique
                zipName2 = splitext(zipName)[0]+"_"+str(i)+".zip"
                if not isfile(zipName2):
                    break
                i += 1
            logging.info("Moving %s to %s due to zip32 limits" % \
                (zipName, zipName2) )
            shutil.move(zipName, zipName2)
            dataZipFile = zipfile.ZipFile(zipName, "w")
            dataZipFile.writestr(filename, fileData)
        #logging.debug("Closing zip")
        dataZipFile.close()

    metaData["suppFiles"] = ",".join(suppFnames)
    metaData["suppUrls"] = ",".join(suppUrls)
    return metaData
        
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

def parseHtml(page, canBeOffsite=False, landingPage_ignoreUrlREs=[]):
    """ return all A-like links andm meta-tag-info from a html string as a dict url => text
    and a second name -> content dict
    
    """

    # use cached results if page has already been parsed before
    if "links" in page:
        #logging.debug("Using cached parsing results")
        return page

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

    linkDict = collections.OrderedDict()
    metaDict = collections.OrderedDict()
    iframeDict = collections.OrderedDict()

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
            linkDict[text] = fullUrlNoFrag
            logging.log(5, "Added link %s for text %s" % (repr(fullUrlNoFrag), repr(text)))

        elif l.name=="meta":
            # parse meta tags
            name = l.get("name")
            if name!=None and \
            (name.startswith("prism") or \
            name.startswith("citation") or \
            name.startswith("DC")):
                content = l.get("content")
                metaDict[name] = content

    logging.log(5, "Meta tags: %s" % metaDict)
    logging.log(5, "Links: %s" % linkDict)
    logging.log(5, "iframes: %s" % iframeDict)

    page["links"] = linkDict
    page["metas"] = metaDict
    page["iframes"] = iframeDict
    logging.debug("HTML parsing finished")
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

def parsePmidStatus(outDir):
    " parse sqlite db AND status file and return a set with pmids that should not be crawled "
    donePmids = set()
    statusFname = join(outDir, PMIDSTATNAME)
    logging.debug("Parsing %s" % statusFname)
    if isfile(statusFname):
        for l in open(statusFname):
            pmid = l.strip().split("\t")[0]
            pmid = int(pmid)
            donePmids.add(pmid)
        logging.info("Found %d PMIDs that have some status" % len(donePmids))

    dbFname = join(outDir, "articles.db")
    if isfile(dbFname):
        logging.info("Reading done PMIDs from db %s" % dbFname)
        con, cur = maxTables.openSqlite(dbFname)
        dbPmids = set([x for (x,) in cur.execute("SELECT pmid from articles")])
        donePmids.update(dbPmids)
        logging.info("Found %d PMIDs that are already done or have status" % len(donePmids))
        logging.log(5, "PMIDs are: %s" % donePmids)
    return donePmids

def parseIssnStatus(outDir):
    " parse outDir/issnStatus.tab and return a set with (issn, year) hat should be ignored "
    statusFname = join(outDir, ISSNSTATNAME)
    logging.debug("Parsing %s" % statusFname)
    ignoreIssns = set()
    if isfile(statusFname):
        for row in maxCommon.iterTsvRows(statusFname):
            ignoreIssns.add((row.issn, row.year))
    return ignoreIssns

lastCallSec = {}

def wait(delaySec, host="default"):
    " make sure that delaySec seconds have passed between two calls, sleep to ensure it"
    global lastCallSec
    delaySec = float(delaySec)
    nowSec = time.time()
    sinceLastCallSec = nowSec - lastCallSec.get(host, 0)
    #print "host", host, "now", nowSec, "lastCallSecs", lastCallSec
    #print "sinceLastCallSec", sinceLastCallSec
    #logging.debug("sinceLastCall %f" % float(sinceLastCallSec))
    if sinceLastCallSec > 0.1 and sinceLastCallSec < delaySec :
        waitSec = delaySec - sinceLastCallSec
        logging.debug("Waiting for %f seconds" % waitSec)
        time.sleep(waitSec)

    lastCallSec[host] = time.time()

def iterateNewPmids(pmids, ignorePmids):
    """ yield all pmids that are not in ignorePmids """
    ignorePmidCount = 0

    for pmid in pmids:
        pmid = pmid.replace("PMID", "")
        pmid = int(pmid)
        if pmid in ignorePmids:
            ignorePmidCount+=1
            continue

        if ignorePmidCount!=0:
            logging.debug("Skipped %d PMIDs" % ignorePmidCount)
            ignorePmidCount=0

        yield str(pmid)

    if ignorePmidCount!=0:
        logging.debug("Skipped %d PMIDs" % ignorePmidCount)

def readLocalMedline(pmid):
    " returns a dict with info we have locally about PMID, None if not found "
    logging.debug("Trying PMID lookup with local medline copy")
    medlineDb = pubStore.getArtDbPath("medline")
    if not isfile(medlineDb):
        logging.warn("%s does not exist, no local medline lookups, need to use eutils" % medlineDb)
        return None

    con, cur = maxTables.openSqlite(medlineDb)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    rows = None
    tryCount = 30

    while rows==None and tryCount>0:
        try:
            rows = list(cur.execute("SELECT * from articles where pmid=?", (pmid,)))
        except sqlite3.OperationalError:
            logging.info("Database is locked, waiting for 60 secs")
            time.sleep(60)
            tryCount -= 1

    if rows==None:
        raise Exception("Medline database was locked for more than 30 minutes")
        
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
    filename = join(outDir, "articleMeta.sqlbak.tab")
    #if testMode!=None:
        #filenames = join(outDir, "testMeta.tab")
    logging.debug("Appending metadata to %s" % filename)

    # overwrite fields with identifers and URLs
    minId = pubConf.identifierStart["crawler"]
    metaData["articleId"] = str(minId+int(metaData["pmid"]))
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
            maxTables.insertSqliteRow(cur, con, "articles", metaHeaders, row)
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
    urlPath = urlparse.urlparse(linkUrl)[2]
    urlExt = os.path.splitext(urlPath)[1].strip(".")
    for searchFileExt in searchFileExts:
        if urlExt==searchFileExt:
            logging.debug("Found matching link for pattern %s: text %s, url %s" % \
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
    assert(len(searchTextRes)!=0 or len(searchUrlRes)!=0)

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
                if urlHasExt(linkUrl, linkText, searchFileExts, searchRe.pattern):
                        yield linkUrl

def getSuppData(fulltextData, suppListPage, crawlConfig, suppExts):
    " given a page with links to supp files, add supplemental files to fulltextData dict "
    suppTextREs = crawlConfig.get("suppListPage_suppFileTextREs", [])
    suppUrlREs = crawlConfig.get("suppListPage_suppFile_urlREs", [])
    ignSuppTextWords = crawlConfig.get("ignoreSuppFileLinkWords", [])

    suppFilesAreOffsite = crawlConfig.get("suppFilesAreOffsite", False)
    landingPage_ignoreUrlREs = crawlConfig.get("landingPage_ignoreUrlREs", [])
    suppListPage = parseHtml(suppListPage, suppFilesAreOffsite, landingPage_ignoreUrlREs=landingPage_ignoreUrlREs)
    suppLinks = suppListPage["links"]
    htmlMetas = suppListPage["metas"]
    suppUrls  = list(findMatchingLinks(suppLinks, suppTextREs, suppUrlREs, suppExts, ignSuppTextWords))

    if len(suppUrls)==0:
        logging.debug("No links to supplementary files found")
    else:
        logging.debug("Found %d links to supplementary files" % len(suppUrls))
        suppIdx = 1
        for url in suppUrls:
            suppFile = delayedWget(url)
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

def replaceUrl(landingUrl, landingUrl_fulltextUrl_replace):
    " try to find link to PDF/suppInfo based on just the landing URL alone "
    replaceCount = 0
    newUrl = landingUrl
    for word, replacement in landingUrl_fulltextUrl_replace.iteritems():
        if word in newUrl:
            replaceCount+=1
            newUrl = newUrl.replace(word, replacement)
    if replaceCount==0:
        logging.debug("Could not replace words in URL")
        return None

    logging.debug("Replacing words in URL %s yields new URL %s" % (landingUrl, newUrl))
    try:
        newPage = delayedWget(newUrl)
        newUrl = newPage["url"]
    except pubGetError:
        logging.debug("replaced URL is not valid / triggers wget error")
        newUrl = None
    return newUrl

def findMainFileUrl(landingPage, crawlConfig):
    " return the url that points to the main pdf file on the landing page "
    pdfUrl = None
    ignoreUrls = crawlConfig.get("landingPage_ignoreUrlREs", [])
    landingPage = parseHtml(landingPage, landingPage_ignoreUrlREs=ignoreUrls)
    links = landingPage["links"]
    htmlMetas = landingPage["metas"]

    # some pages contain meta tags to the pdf
    if "citation_pdf_url" in htmlMetas and not crawlConfig.get("landingPage_ignoreMetaTag", False):
        pdfUrl = htmlMetas["citation_pdf_url"]
        logging.debug("Found link to PDF in meta tag citation_pdf_url: %s" % pdfUrl)
        return pdfUrl

    # some pdf urls are just a variation of the main url by appending something
    if "appendStringForPdfUrl" in crawlConfig:
        pdfUrl = landingPage["url"]+crawlConfig["appendStringForPdfUrl"]
        logging.debug("Appending string to URL yields new URL %s" % (pdfUrl))
        return pdfUrl

    # some others can be derived by replacing strings in the landing url
    if "landingUrl_fulltextUrl_replace" in crawlConfig:
        pdfUrl = replaceUrl(landingPage["url"], crawlConfig["landingUrl_fulltextUrl_replace"])
        return pdfUrl

    # if all of that doesn't work, parse the html and try all <a> links
    if pdfUrl == None:
        pdfLinkNames = crawlConfig["landingPage_mainLinkTextREs"]
        for pdfLinkName in pdfLinkNames:
            for linkText, linkUrl in links.iteritems():
                if pdfLinkName.match(linkText):
                    pdfUrl = linkUrl
                    logging.debug("Found link to main PDF: %s -> %s" % (pdfLinkName, pdfUrl))

    if pdfUrl==None:
        raise pubGetError("main PDF not found", "mainPdfNotFound")

    return pdfUrl

def isErrorPage(landingPage, crawlConfig):
    if not "landingPage_errorKeywords" in crawlConfig:
        return False

    if crawlConfig["landingPage_errorKeywords"] in landingPage["data"]:
        logging.warn("Found error page, waiting for 15 minutes")
        time.sleep(60*15)
        return True
    else:
        return False

    
def crawlForFulltext(landingPage, crawlConfig):
    """ 
    given a landingPage-dict (with url, data, mimeType), return a dict with the
    keys main.html, main.pdf and S<X>.<ext> that contains all (url, data,
    mimeType) pages for an article 
    """
    
    if noLicensePage(landingPage, crawlConfig):
        raise pubGetError("no license for this article", "noLicense")
    if isErrorPage(landingPage, crawlConfig):
        raise pubGetError("hit error page", "errorPage")

    landUrl = landingPage["url"]
    logging.debug("Final landing page after redirects is %s" % landingPage["url"])

    fulltextData = {}

    fulltextData["landingPage"] = landingPage # in case we need the landing url later

    # some landing pages ARE the article PDF
    if landingPage["mimeType"] == "application/pdf":
        logging.debug("Landing page is the PDF, no suppl file downloading possible")
        fulltextData["main.pdf"] = landingPage
        fulltextData["status"] = "LandingOnPdf_NoSuppl"
        return fulltextData

    # some landing pages contain the full article directly as html
    elif crawlConfig.get("landingUrl_isFulltextKeyword", False) and \
           crawlConfig["landingUrl_isFulltextKeyword"] in landUrl:
                logging.debug("URL suggests that landing page is same as article html")
                fulltextData["main.html"] = landingPage

    if "landingPage_ignorePageWords" in crawlConfig and \
        containsAnyWord(landingPage["data"], crawlConfig["landingPage_ignorePageWords"]):
        logging.debug("Found blacklist word, ignoring article")
        raise pubGetError("blacklist word on landing page", "blackListWord")

    # search for main PDF on landing page
    pdfUrl = findMainFileUrl(landingPage, crawlConfig)
    if pdfUrl==None :
        if not crawlConfig.get("landingPage_acceptNoPdf", False):
            logging.debug("Could not find PDF on landing page")
            raise pubGetError("Could not find main PDF", "notFoundMainPdf")
        else:
            logging.debug("No PDF found, but we accept this case for this publisher")
    else:
        pdfPage = delayedWget(pdfUrl)
        if pdfPage["mimeType"] != "application/pdf":
            pdfPage = parseHtml(pdfPage)
            if "pdfDocument" in pdfPage["iframes"]:
                logging.debug("found framed PDF, requesting inline pdf")
                pdfPage2  = delayedWget(pdfPage["iframes"]["pdfDocument"])
                if pdfPage2!=None and pdfPage2["mimeType"]=="application/pdf":
                    pdfPage = pdfPage2
                else:
                    raise pubGetError("inline pdf is invalid", "invalidInlinePdf")
            else:
                raise pubGetError("putative PDF link has not PDF mimetype", "MainPdfWrongMime_InlineNotFound")
        if noLicensePage(pdfPage, crawlConfig):
            raise pubGetError("putative PDF page indicates no license", "MainPdfNoLicense")
        fulltextData["main.pdf"] = pdfPage

    # find suppl list and then get suppl files of specified types
    suppListUrl  = findSuppListUrl(landingPage, crawlConfig)
    if suppListUrl!=None:
        suppListPage = delayedWget(suppListUrl)
        suppExts = pubConf.crawlSuppExts
        suppExts.update(crawlConfig.get("suppListPage_addSuppFileTypes", []))
        fulltextData = getSuppData(fulltextData, suppListPage, crawlConfig, suppExts)

    return fulltextData

def noLicensePage(landingPage, crawlConfig):
    " return True if page looks like a 'purchase this article now' page "
    for stopPhrase in crawlConfig.get("landingPage_stopPhrases", []):
        if stopPhrase in landingPage["data"]:
            logging.debug("Found stop phrase %s" % stopPhrase)
            return True
    return False

def writeIssnStatus(outDir, issnYear):
    " append a line to issnStatus.tab file in outDir "
    issn, year = issnYear
    fname = join(outDir, ISSNSTATNAME)
    if isfile(fname):
        outFh = open(fname, "a")
    else:
        outFh = open(fname, "w")
        outFh.write("issn\tyear\n")
    outFh.write("%s\t%s\n" % (issn, year))

def writePmidStatus(outDir, pmid, msg, detail=None):
    " append a line to pmidStatus.tab file in outDir "
    fname = join(outDir, PMIDSTATNAME)
    if isfile(fname):
        outFh = codecs.open(fname, "a", encoding="utf8")
    else:
        outFh = codecs.open(fname, "w", encoding="utf8")
    if detail==None:
        outFh.write("%s\t%s\n" % (str(pmid), msg))
    else:
        outFh.write("%s\t%s\t%s\n" % (str(pmid), msg, detail))

def removeLock():
    logging.debug("Removing lockfile %s" % lockFname)
    os.remove(lockFname)

def checkCreateLock(outDir):
    " creates lockfile, squeaks if exists, register exit handler to delete "
    global lockFname
    lockFname = join(outDir, "_pubCrawl.lock")
    if isfile(lockFname):
        raise Exception("File %s exists - it seems that a crawl is running now. \
        If you're sure that this is not the case, remove the lockfile and retry again" % lockFname)
    logging.debug("Creating lockfile %s" % lockFname)
    open(lockFname, "w")
    atexit.register(removeLock) # register handler that is executed on program exit

def parsePmids(outDir):
    " parse pmids.txt in outDir and return as list, ignore duplicates "
    pmidFname = join(outDir, "pmids.txt")
    logging.debug("Parsing %s" % pmidFname)
    if not isfile(pmidFname):
        raise Exception("file %s not found. You need to create this manually or "
            " run pubPrepCrawl pmids to create this file." % pmidFname)
    logging.debug("Parsing PMIDS %s" % pmidFname)
    #pmids = [p.strip() for p in open(pmidFname).readlines()]
    pmids = []
    seen = set()
    for line in open(pmidFname):
        if line.startswith("#"):
            continue
        pmid = line.strip().split("#")[0]
        if pmid=="":
            continue
        if pmid in seen:
            continue
        pmids.append(pmid)
        seen.add(pmid)
    logging.debug("Found %d PMIDS" % len(pmids))
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

def findSuppListUrl(landingPage, crawlConfig):
    " given the landing page, find the link to the list of supp files "
    ignoreUrls = crawlConfig.get("landingPage_ignoreUrlREs", [])
    landingPage = parseHtml(landingPage, landingPage_ignoreUrlREs=ignoreUrls)

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

def checkForOngoingMaintenanceUrl(url):
    if url in errorPageUrls:
        logging.debug("page %s looks like error page, waiting for 15 minutes" % url)
        time.sleep(60*15)
        raise pubGetError("Landing page is error page", "errorPage", url)

def getConfig(hostToConfig, url):
    """ based on the url or IP of the landing page, return a crawl configuration dict 
    problem: This is used all the time but is not using a hashmap... could be faster...
    
    """
    hostname = urlparse.urlparse(url).netloc
    thisConfig = None
    logging.debug("Looking for config for url %s, hostname %s" % (url, hostname))
    for cfgHost, config in hostToConfig.iteritems():
        #logging.debug("Cmp %s with %s" % (repr(hostname), repr(cfgHost)))
        if hostname.endswith(cfgHost):
            logging.debug("Found config for host %s: %s" % (hostname, cfgHost))
            thisConfig = config
            break

    # not found -> try default HIGHWIRE config, if highwire host
    if thisConfig==None and isHighwire(hostname):
        thisConfig = getConfig(hostToConfig, "HIGHWIRE")

    if thisConfig==None:
        raise pubGetError("No config for hostname %s" % hostname, "noConfig", hostname)
    else:
        return thisConfig


hostCache = {}

def isHighwire(hostname):
    "return true if a hostname is hosted by highwire at stanford "
    global hostCache
    for hostEnd in highwireHosts:
        if hostname.endswith(hostEnd):
            return True
    if hostname in hostCache:
        ipAddr = hostCache[hostname]
    else:
        logging.debug("Looking up IP for %s" % hostname)
        try:
            ipAddr = socket.gethostbyname(hostname)
            hostCache[hostname] = ipAddr
        except socket.gaierror:
            raise pubGetError("Illegal hostname %s in link" % hostname, "invalidHostname", hostname)

    ipParts = ipAddr.split(".")
    ipParts = [int(x) for x in ipParts]
    result = (ipParts[0] == 171 and ipParts[1] in range(64, 67))
    if result==True:
        logging.log(5, "hostname %s is highwire host" % hostname)
    return result

def highwireDelay():
    """ return current delay for highwire, get current time at east coast
    """
    os.environ['TZ'] = 'US/Eastern'
    time.tzset()
    tm = time.localtime()
    if tm.tm_wday in [5,6]:
        delay=5
    else:
        if tm.tm_hour >= 9 and tm.tm_hour <= 17:
            delay = 60
        else:
            delay = 10
    logging.debug("current highwire delay time is %d" % (delay))
    return delay

def ignoreCtrlc(signum, frame):
    logging.info('Signal handler called with signal %s' % str (signum))

def writePaperData(pmid, pubmedMeta, fulltextData, outDir, crawlConfig, testMode):
    " write all paper data to status and fulltext output files in outDir "
    if testMode:
        for suffix, pageDict in fulltextData.iteritems():
            logging.info("Got file: Suffix %s, url %s, mime %s, content %s" % \
                (suffix, pageDict["url"], pageDict["mimeType"], repr(pageDict["data"][:10])))
        return

    oldHandler = signal.signal(signal.SIGINT, ignoreCtrlc) # deact ctrl-c
    # do we need zipfiles ?
    #pubmedMeta = storeFiles(pmid, pubmedMeta, fulltextData, outDir)
    pubmedMeta = storeFilesNoZip(pmid, pubmedMeta, fulltextData, outDir)
    writeMeta(outDir, pubmedMeta, fulltextData)
    addStatus = ""
        
    if "status" in fulltextData:
        addStatus = fulltextData["status"]
    pmidStatus = "OK\t%s %s, %d files\t%s" % \
        (pubmedMeta["journal"], pubmedMeta["year"], len(fulltextData), addStatus)
    writePmidStatus(outDir, pmid, pmidStatus)
    signal.signal(signal.SIGINT, oldHandler) # react ctrl c handler

def parseIdStatus(fname):
    " parse crawling status file, return as dict status -> count "
    res = {}
    if not os.path.isfile(fname):
        print "%s does not exist" % fname
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

def writeReport(baseDir, htmlFname):
    " parse pmids.txt and pmidStatus.tab and write a html report to htmlFname "
    h = html.htmlWriter(htmlFname)
    h.head("Genocoding crawler status", stylesheet="bootstrap/css/bootstrap.css")
    h.startBody("Crawler status as of %s" % time.asctime())

    publDesc = {}
    for key, value in pubConf.crawlPubIds.iteritems():
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
        h.li("Crawler progress rate: %0.2f %%" % (100*len(statusPmids["OK"])/float(pmidCount)))
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


def checkIssnErrorCounts(pubmedMeta, issnErrorCount, ignoreIssns, outDir):
    issnYear = (pubmedMeta["eIssn"], pubmedMeta["year"])
    issnYearErrorCount = issnErrorCount[issnYear]
    if issnYearErrorCount > MAXISSNERRORCOUNT:
        writeIssnStatus(outDir, issnYear)
        raise pubGetError("too many errors for ISSN %s and year %s" % issnYear,
                "issnYearErrorExceed\t%s %s" % issnYear)
    if issnYear in ignoreIssns:
        raise pubGetError("issn+year blacklisted", "issnErrorExceed", "%s %s" % issnYear)

def noMatches(landingUrl, hostnames):
    " check if landing url contains none of the hostnames "
    for hostname in hostnames:
        if hostname in landingUrl:
            return False
    return True

def stringRewrite(origString, crawlConfig, configKey):
    " lookup a dict with pat, repl combinations and run them over origString through re.sub "
    if crawlConfig==None or configKey not in crawlConfig:
        return origString

    string = origString
    for pat, repl in crawlConfig[configKey].iteritems():
        string = re.sub(pat, repl, string)

    logging.debug("string %s was rewritten to %s" % (origString, string))
    return string



def resolveDoi(doi):
    """ resolve a DOI to the final target url or None on error
    #>>> resolveDoi("10.1073/pnas.1121051109")
    """
    logging.debug("Resolving DOI %s" % doi)
    doiUrl = "http://dx.doi.org/"+urllib.quote(doi.encode("utf8"))
    resp = maxCommon.retryHttpHeadRequest(doiUrl, repeatCount=2, delaySecs=4, userAgent=userAgent)
    if resp==None:
        return None
    trgUrl = resp.geturl()
    logging.debug("DOI %s redirects to %s" % (doi, trgUrl))
    return trgUrl

def resolveDoiRewrite(doi, crawlConfig, hostToConfig):
    """ resolve a DOI to the final target url and rewrite according to crawlConfig rules
        Returns None on error
    #>>> resolveDoiRewrite("10.1073/pnas.1121051109")
    """
    logging.debug("Resolving DOI and rewriting")
    url = resolveDoi(doi)
    if url==None:
        return None, crawlConfig
    if crawlConfig==None:
        crawlConfig  = getConfig(hostToConfig, url)
    if url==None or crawlConfig==None or "doiUrl_replace" not in crawlConfig:
        logging.debug("Nothing to rewrite")
        return url, crawlConfig
    newUrl = stringRewrite(url, crawlConfig, "doiUrl_replace")
    return newUrl, crawlConfig

def crawlFilesViaPubmed(outDir, waitSec, testPmid, pause, tryHarder, restrictPublisher, \
    localMedline, fakeUseragent):
    " download all files for pmids in outDir/pmids.txt to outDir/files "
    checkCreateLock(outDir)

    global userAgent
    if fakeUseragent:
        logging.debug("Setting useragent to Mozilla")
        userAgent = 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20130406 Firefox/23.0'

    if testPmid!=None:
        pmids, ignorePmids, ignoreIssns = [testPmid], [], []
    else:
        pmids = parsePmids(outDir)
        ignorePmids = parsePmidStatus(outDir)
        ignoreIssns = parseIssnStatus(outDir)
    issnErrorCount = collections.defaultdict(int)
    issnYear = (0,0)

    global defaultDelay
    defaultDelay = waitSec

    pubsCrawlCfg, hostToConfig = pubCrawlConf.prepConfigIndexByHost()

    pubId = basename(outDir.rstrip("/"))
    crawlConfig = None
    if restrictPublisher:
        logging.error("pubId is %s" % pubId)
        assert(pubId in pubsCrawlCfg)
        crawlConfig  = pubsCrawlCfg[pubId]

    consecErrorCount = 0

    for pmid in iterateNewPmids(pmids, ignorePmids):
        logging.debug("PMID %s, %s" % (pmid, time.asctime()))

        if tryHarder:
            errorWaitSecs = ERRWAIT_TRYHARD
            maxConSecError = MAXCONSECERR_TRYHARD
        else:
            errorWaitSecs = ERRWAIT * consecErrorCount
            maxConSecError = MAXCONSECERR

        try:
            global wgetCache
            wgetCache.clear()

            # get pubmed info from local db or ncbi webservice
            pubmedMeta = None
            if localMedline:
                pubmedMeta = readLocalMedline(pmid)
            if pubmedMeta==None:
                pubmedMeta = downloadPubmedMeta(pmid)

            checkIssnErrorCounts(pubmedMeta, issnErrorCount, ignoreIssns, outDir)

            landingUrl   = findLandingUrl(pubmedMeta, crawlConfig, hostToConfig)

            # first resolve the url (e.g. doi) to something on a webserver
            landingPage  = delayedWget(landingUrl)

            if crawlConfig==None:
                crawlConfig  = getConfig(hostToConfig, landingPage["url"])
            else:
                if noMatches(landingPage["url"], crawlConfig["hostnames"]):
                    raise pubGetError("Landing page is on an unknown server", "unknownHost", landingPage["url"])

            checkForOngoingMaintenanceUrl(landingPage["url"])

            fulltextData = crawlForFulltext(landingPage, crawlConfig)

            # write results to output files
            writePaperData(pmid, pubmedMeta, fulltextData, outDir, crawlConfig, testPmid)
            #else:
                #logging.info("Test-mode, not saving anything")

            if pause:
                raw_input("Press Enter...")
            consecErrorCount = 0

        except pubGetError, e:
            if e.logMsg!="issnErrorExceed":
                consecErrorCount += 1
            logging.error("PMID %s, error: %s, code: %s, details: %s" % (pmid, e.longMsg, e.logMsg, e.detailMsg))
            writePmidStatus(outDir, pmid, e.logMsg, e.detailMsg)
            issnErrorCount[issnYear] += 1
            if e.logMsg not in ["issnErrorExceed","noOutlinkOrDoi", "unknownHost", "noLicense"]:
                logging.debug("Sleeping for %d secs after error" % errorWaitSecs)
                time.sleep(errorWaitSecs)

            if consecErrorCount > maxConSecError:
                logging.error("Too many consecutive errors, stopping crawl")
                e.longMsg = "Crawl stopped after too many consecutive errors / "+e.longMsg
                raise
            if pause:
                raw_input("Press Enter...")
        except:
            raise

if __name__=="__main__":
    import doctest
    doctest.testmod()
