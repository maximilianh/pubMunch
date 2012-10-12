# library to crawl pdf and supplemental file from pubmed

# load our own libraries
import pubConf, pubGeneric, maxMysql, pubStore, tabfile, maxCommon, pubPubmed, maxTables,\
    pubCrossRef, html
import chardet # library for guessing encodings
#from bs4 import BeautifulSoup  # the new version of bs crashes too much
from BeautifulSoup import BeautifulSoup, SoupStrainer, BeautifulStoneSoup # parsing of non-wellformed html

import logging, optparse, os, shutil, glob, tempfile, sys, codecs, types, re, \
    traceback, urllib2, re, zipfile, collections, urlparse, time, atexit, socket, signal, \
    sqlite3
from os.path import *

# ===== GLOBALS ======

# options for wget 
# (python's http implementation is extremely buggy and tends to hang for minutes)
WGETOPTIONS = " --no-check-certificate --tries=3 --random-wait --waitretry=%d --connect-timeout=%d --dns-timeout=%d --read-timeout=%d --ignore-length --user-agent='%s'" % (pubConf.httpTimeout, pubConf.httpTimeout, pubConf.httpTimeout, pubConf.httpTimeout, pubConf.httpUserAgent)

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
highwireHosts = ["asm.org", "rupress.org"] # too many DNS queries fail, so we hardcode some of the work

# if any of these is found in a landing page Url, wait for 15 minutes and retry
# has to be independent of siteCrawlConfig, NPG at least redirects to a separate server
errorPageUrls = ["http://status.nature.com"]

# crawl configuration: for each website, define how to crawl the pages
siteCrawlConfig = { ("www.nature.com") :
    # http://www.nature.com/nature/journal/v463/n7279/suppinfo/nature08696.html
    # 
    {
        "stopPhrases": ["make a payment", "purchase this article"],
        "replaceUrlWords" : {"full" : "pdf", "html" : "pdf", "abs" : "pdf"},
        "landingPageIsArticleUrlKeyword" : "full",
        "mainPdfLinkREs" : ["Download PDF"],
        "replaceUrlWords_suppList" : {"full" : "suppinfo", "abs" : "suppinfo"},
        "suppListPageREs" : ["Supplementary information index", "[Ss]upplementary [iI]nfo", "[sS]upplementary [iI]nformation"],
        "suppFileTextREs" : ["[Ss]upplementary [dD]ata.*", "[Ss]upplementary [iI]nformation.*", "Supplementary [tT]able.*", "Supplementary [fF]ile.*", "Supplementary [Ff]ig.*", "Supplementary [lL]eg.*", "Download PDF file.*", "Supplementary [tT]ext.*", "Supplementary [mM]ethods.*", "Supplementary [mM]aterials.*", "Review Process File"]
    # Review process file for EMBO, see http://www.nature.com/emboj/journal/v30/n13/suppinfo/emboj2011171as1.html
    },

    # https://www.jstage.jst.go.jp/article/circj/75/4/75_CJ-10-0798/_article
    # suppl file download does NOT work: strange javascript links
    ("www.jstage.jst.go.jp") :
    {
        "replaceUrlWords" : {"_article" : "_pdf" },
        "mainPdfLinkREs" : ["Full Text PDF.*"],
        "suppListPageREs" : ["Supplementary materials.*"]
    },
    # ruppress tests:
    # PMID 12515824 - with integrated suppl files into main PDF
    # PMID 15824131 - with separate suppl files
    # PMID 8636223  - landing page is full (via Pubmed), abstract via DOI
    # cannot do suppl zip files like this one http://jcb.rupress.org/content/169/1/35/suppl/DC1
    # 
    #("rupress.org") :
    ("rupress.org") :
    {
        "landingPageIsArticleUrlKeyword" : ".long",
        #"appendStringForPdfUrl" : ".full.pdf?with-ds=yes",
        "ignoreLandingPageWords" : ["From The Jcb"],
        "ignoreMetaTag" : True,
        "replaceUrlWords" : {"long" : "full.pdf?with-ds=yes", "abstract" : "full.pdf?with-ds=yes" },
        "addSuppFileTypes" : ["html", "htm"], # pubConf does not include htm/html
        "mainPdfLinkREs" : ["Full Text (PDF)"],
        #"suppListPageREs" : ["Supplemental [Mm]aterial [Iindex]", "Supplemental [Mm]aterial"],
        "replaceUrlWords_suppList" : {".long" : "/suppl/DC1", ".abstract" : "/suppl/DC1"},
        "suppFileTextREs" : ["[Ss]upplementary [dD]ata.*", "[Ss]upplementary [iI]nformation.*", "Supplementary [tT]able.*", "Supplementary [fF]ile.*", "Supplementary [Ff]ig.*", "[ ]+Figure S[0-9]+.*", "Supplementary [lL]eg.*", "Download PDF file.*", "Supplementary [tT]ext.*", "Supplementary [mM]aterials and [mM]ethods.*", "Supplementary [mM]aterial \(.*"],
        "ignoreSuppFileLinkWords" : ["Video"],
        "suppFileUrlREs" : [".*/content/suppl/.*"],
        "ignoreSuppFileContentText" : ["Reprint (PDF) Version"]
    },
    # http://jb.asm.org/content/194/16/4161.abstract = PMID 22636775
    ("asm.org") :
    {
        "landingPageIsArticleUrlKeyword" : ".long",
        "ignoreMetaTag" : True,
        "errorPageText" : "We are currently doing routine maintenance", # if found on landing page Url, wait for 15 minutes and retry
        "replaceUrlWords" : {"long" : "full.pdf", "abstract" : "full.pdf" },
        #"replaceUrlWords" : {"long" : "full.pdf?with-ds=yes", "abstract" : "full.pdf?with-ds=yes" },
        #"replaceUrlWords_suppList" : {".long" : "/suppl/DCSupplemental", ".abstract" : "/suppl/DCSupplemental"},
        "suppListUrlREs" : [".*suppl/DCSupplemental"],
        "suppFileUrlREs" : [".*/content/suppl/.*"],
    },
    # 1995 PMID 7816814 
    # 2012 PMID 22847410 has one supplement, has suppl integrated in paper
    ("pnas.org") :
    {
        "landingPageIsArticleUrlKeyword" : ".long",
        "ignoreMetaTag" : True,
        "errorPageText" : "We are currently doing routine maintenance", # if found on landing page Url, wait for 15 minutes and retry
        "replaceUrlWords" : {"long" : "full.pdf", "abstract" : "full.pdf" },
        "suppListUrlREs" : [".*suppl/DCSupplemental"],
        "suppFileUrlREs" : [".*/content/suppl/.*"],
    },
    # example suppinfo links 20967753 (major type of suppl, some also have "legacy" suppinfo
    # example spurious suppinfo link 8536951
    # 
    ("onlinelibrary.wiley.com") :
    {
        "replaceUrlWords" : {"abstract" : "pdf"},
        #"replaceUrlWords_suppList" : {"abstract" : "suppinfo"},
        "suppListPageREs" : ["Supporting Information"],
        "suppFileUrlREs" : [".*/asset/supinfo/.*", ".*_s.pdf"],
        "suppFilesAreOffsite" : True,
        "ignoreUrlREs"  : ["http://onlinelibrary.wiley.com/resolve/openurl.genre=journal&issn=[0-9-X]+/suppmat/"],
        "stopPhrases" : ["You can purchase online access", "Registered Users please login"]
    },
    # http://www.futuremedicine.com/doi/abs/10.2217/epi.12.21
    ("futuremedicine.com", "future-science.com", "expert-reviews.com", "future-drugs.com") :
    {
        "replaceUrlWords" : {"abs" : "pdfplus"},
        "replaceUrlWords_suppList" : {"abs" : "suppl"},
        "suppFileUrlREs" : [".*suppl_file.*"],
        "stopPhrases" : ["single article purchase is required", "The page you have requested is unfortunately unavailable"]
    },

}

# wget page cache, to avoid duplicate downloads
wgetCache = {}

addHeaders = [ # additional headers for fulltext download metaData
"mainHtmlUrl", # the main fulltext HTML URL
"mainPdfUrl", # the main fulltext PDF URL
"suppUrls", # a comma-sep list of supplemental file URLs
"mainHtmlFile", # the main text file on local disk, relative to the metaData file
"mainPdfFile", # the main text pdf file on local disk, relative to the metaData file
"suppFiles" # comma-sep list of supplemental files on local disk
]

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
    print urlEls, type(urlEls), repr(urlEls)
    if len(urlEls)==0 or urlEls==None:
        return None
    urls = [x.string for x in urlEls]
    logging.debug("SFX returned (using only first of these): %s" % urls)
    if urlEls[0]==None:
        return None
    url = urls[0].encode("utf8")
    return url

def findLandingUrl(articleData):
    " try to find landing URL either via DOI or via Pubmed Outlink "
    logging.log(5, "Looking for landing page")

    # first try pubmed outlink, then DOI
    # because they sometimes differ e.g. 12515824 directs to a different page via DOI
    # than via Pubmed
    try:
        outlinks = pubPubmed.getOutlinks(articleData["pmid"])
    except urllib2.HTTPError:
        logging.info("pubmed http error, waiting for 120 secs")
        time.sleep(120)
        raise pubGetError("pubmed outlinks http error", "PubmedOutlinkHttpError")

    fulltextUrl = None

    if len(outlinks)!=0:
        fulltextUrl =  outlinks.values()[0]
        logging.debug("landing page based on first outlink of Pubmed, URL %s" % fulltextUrl)
    elif articleData["doi"]!=None and articleData["doi"]!="":
            doi = articleData["doi"]
            doiUrl = "http://dx.doi.org/"+doi
            fulltextUrl =  doiUrl
            logging.debug("landing page based on DOI, URL %s" % fulltextUrl)
    else:
        fulltextUrl = resolvePmidWithSfx(pubConf.crawlSfxServer, articleData["pmid"])
        if fulltextUrl==None:
            raise pubGetError("No fulltext for this article", "noOutlinkOrDoi") 

    return fulltextUrl

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
        if host in pubConf.crawlDelays:
            delaySecs = pubConf.crawlDelays.get(host, defaultDelay)
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
    " download url with wget and return dict with keys url, mimeType, charset, data "
    # check if file is already in cache
    global wgetCache
    if url in wgetCache:
        logging.log(5, "Using cached wget results")
        return wgetCache[url]

    logging.debug("Downloading %s" % url)
    url = url.replace("'", "")
    #urlParts = urlparse.urlsplit(url)
    #filePath = urlParts[0:5] # get rid of #anchornames
    #if filePath in downloadedUrls:
        #raise pubGetError("url %s has been downloaded before, crawler error?" % url, "doubleDownload\t"+url)
    #downloadedUrls.add(filePath)

    # run wget command
    tmpFile = tempfile.NamedTemporaryFile(dir = pubConf.getTempDir(), prefix="WgetGoogleCrawler", suffix=".data")
    cmd = "wget '%s' -O %s --server-response" % (url, tmpFile.name)
    cmd += WGETOPTIONS
    logFile = tempfile.NamedTemporaryFile(dir = pubConf.getTempDir(), \
        prefix="pubGetPmid-Wget-", suffix=".log")
    cmd += " -o %s " % logFile.name
    stdout, stderr, ret = pubGeneric.runCommandTimeout(cmd, timeout=pubConf.httpTransferTimeout)
    if ret!=0:
        #logging.debug("non-null return code from wget, sleeping 120 seconds")
        #time.sleep(120)
        raise pubGetError("non-null return code from wget", "wgetRetNonNull\t"+url.decode("utf8"))

    # parse wget log
    mimeType, redirectUrl, charset = parseWgetLog(logFile, url)
    if mimeType==None:
        raise pubGetError("No mimetype found in http reply", "noMimeType\t"+url)

    if redirectUrl!=None:
        finalUrl = redirectUrl
    else:
        finalUrl = url
    
    data = tmpFile.read()
    logging.log(5, "Download OK, size %d bytes" % len(data))
    if len(data)==0:
        raise pubGetError("empty http reply from %s" % url, "emptyHttp\t"+url)

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

def storeFiles(pmid, metaData, fulltextData, outDir):
    """ write files from dict (keys like main.html or main.pdf or s1.pdf, value is binary data) 
    to target zip file 
    saves all binary data to <issn>.zip in outDir with filename pmid.<key>
    """
    #global dataZipFile
    suppFnames = []
    suppUrls = []
    for suffix, pageDict in fulltextData.iteritems():
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
        
def soupToText(soup):
    ' convert a tag to a string of the text within it '
    text = soup.string
    if text!=None:
        return text

    # soup has children: need to get them and concat their texts
    texts = []
    allTags = soup.findAll(True)
    for t in allTags:
        if t.string!=None:
            texts.append(t.string)
    return " ".join(texts)

def anyMatch(regexList, queryStr):
    for regex in regexList:
        if regex.match(queryStr):
            logging.debug("url %s ignored due to regex %s" % (queryStr, regex.pattern))
            return True
    return False

def parseHtml(page, canBeOffsite=False, ignoreUrlREs=[]):
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
        if l.name=="iframe":
            src = l.get("src")
            if src==None or "pdf" not in src:
                continue
            id = l.get("id", "pdfDocument")
            iframeDict[id] = src

        elif l.name=="a":
            text = soupToText(l)
            text = text.encode("utf8")
            url = l.get("href")
            if url==None:
                continue
            try:
                linkLoc = urlparse.urlsplit(url)[1]
                linkPath = urlparse.urlsplit(url)[2]
            except ValueError:
                raise pubGetError("Value error on url split %s" % url, "urlSplitError", url)
            # skip links that point to a different server
            if canBeOffsite==False and linkLoc!="" and linkLoc!=baseLoc:
                continue

            # remove #xxxx fragment identifiers from link URL
            fullUrl = urlparse.urljoin(baseUrl, url)
            parts = list(urlparse.urlsplit(fullUrl)[:4])
            if parts[0]=="javascript":
                continue
            parts.append("")
            fullUrlNoFrag = urlparse.urlunsplit(parts)
            #logging.debug("Checking link against %s" % ignoreUrlREs)
            if anyMatch(ignoreUrlREs, fullUrlNoFrag):
                continue
            linkDict[text] = fullUrlNoFrag

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
    " parse outDir/pmidStatus.tab and return a set with pmids that should be ignored "
    statusFname = join(outDir, PMIDSTATNAME)
    logging.debug("Parsing %s" % statusFname)
    donePmids = set()
    if isfile(statusFname):
        for l in open(statusFname):
            pmid = l.strip().split("\t")[0]
            pmid = int(pmid)
            donePmids.add(pmid)
    logging.debug("Found %d PMIDs with status" % len(donePmids))
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

    con, cur = maxTables.openSqliteRo(medlineDb)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    rows = list(cur.execute("SELECT * from articles where pmid=?", (pmid,)))
    if len(rows)==0:
        logging.info("No info in local medline for PMID %s" % pmid)
        return None
    # the last entry should be the newest one
    lastRow = rows[-1]

    # convert sqlite results to dict
    result = {}
    for key, val in zip(lastRow.keys(), lastRow):
        result[key] = unicode(val)
    return result
        

def getMedlineInfo(pmid):
    """ try to get information about pmid first from local medline with NCBI eutils as fallback, 
    add the DOI to fulltext from CrossRef 
    """
    
    metaDict = readLocalMedline(pmid)
    if metaDict==None:
        metaDict = downloadPubmedMeta(pmid)

    if metaDict["doi"]=="":
        xrDoi = pubCrossRef.lookupDoi(metaDict)
        if xrDoi != None:
            metaDict["doi"] = xrDoi

    return metaDict

def downloadPubmedMeta(pmid):
    """ wrapper around pubPubmed that converts exceptions"""
    try:
        wait(3, "eutils.ncbi.nlm.nih.gov")
        ret = pubPubmed.getOnePmid(pmid)
    except urllib2.HTTPError, e:
        raise pubGetError("HTTP error %s on Pubmed" % str(e.code), "pubmedHttpError%s" % str(e.code))
    except pubPubmed.PubmedError, e:
        raise pubGetError(e.longMsg, e.logMsg)
        
    if ret==None:
        raise pubGetError("empty result when requesting metadata from NCBI Eutils for PMID %s" % str(pmid), \
            "pubmedEmpty")
    for h in addHeaders:
        ret[h] = ""
    return ret

def storeMeta(outDir, metaData, fulltextData):
    " append one metadata dict as a tab-sep row to outDir/articleMeta.tab "
    filename = join(outDir, "articleMeta.tab")
    #if testMode!=None:
        #filenames = join(outDir, "testMeta.tab")
    logging.debug("Appending metadata to %s" % filename)

    # save all URLs to metadata object, nice for debugging
    metaData["mainHtmlUrl"] = fulltextData.get("main.htm",{}).get("url", "")
    metaData["mainPdfUrl"] = fulltextData.get("main.pdf",{}).get("url", "")
    suppUrls = []
    for key, page in metaData.iteritems():
        if key.startswith("S"):
            suppUrls.append(page.get("url",""))
    metaData["suppUrls"] = ",".join(suppUrls)
            
    if not isfile(filename):
        headers = pubStore.articleFields
        headers.extend(addHeaders)
        codecs.open(filename, "w", encoding="utf8").write(u"\t".join(headers)+"\n")
    maxCommon.appendTsvOrderedDict(filename, metaData)

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
            logging.debug("Ignoring link text %s, url %s" % (linkText, linkUrl))
            continue

        for searchRe in searchTextRes:
            if searchRe.match(linkText):
                if urlHasExt(linkUrl, linkText, searchFileExts, searchRe.pattern):
                        yield linkUrl
                        continue

        for searchRe in searchUrlRes:
            if searchRe.match(linkUrl):
                if urlHasExt(linkUrl, linkText, searchFileExts, searchRe.pattern):
                        yield linkUrl

def getSuppData(fulltextData, suppListPage, crawlConfig, suppExts):
    " given a page with links to supp files, add supplemental files to fulltextData dict "
    suppTextREs = crawlConfig.get("suppFileTextREs", [])
    suppUrlREs = crawlConfig.get("suppFileUrlREs", [])
    ignSuppTextWords = crawlConfig.get("ignoreSuppFileLinkWords", [])

    suppFilesAreOffsite = crawlConfig.get("suppFilesAreOffsite", False)
    ignoreUrlREs = crawlConfig.get("ignoreUrlREs", [])
    suppListPage = parseHtml(suppListPage, suppFilesAreOffsite, ignoreUrlREs=ignoreUrlREs)
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
                raise pubGetError("max suppl count reached", "tooManySupplFiles")
    return fulltextData

def replaceUrl(landingUrl, replaceUrlWords):
    " try to find link to PDF/suppInfo based on just the landing URL alone "
    replaceCount = 0
    newUrl = landingUrl
    for word, replacement in replaceUrlWords.iteritems():
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
    ignoreUrls = crawlConfig.get("ignoreUrlREs", [])
    landingPage = parseHtml(landingPage, ignoreUrlREs=ignoreUrls)
    links = landingPage["links"]
    htmlMetas = landingPage["metas"]

    if "citation_pdf_url" in htmlMetas and not crawlConfig.get("ignoreMetaTag", False):
        pdfUrl = htmlMetas["citation_pdf_url"]
        logging.debug("Found link to PDF in meta tag citation_pdf_url: %s" % pdfUrl)
        return pdfUrl

    if "appendStringForPdfUrl" in crawlConfig:
        pdfUrl = landingPage["url"]+crawlConfig["appendStringForPdfUrl"]
        logging.debug("Appending string to URL yields new URL %s" % (pdfUrl))
        return pdfUrl

    if "replaceUrlWords" in crawlConfig:
        pdfUrl = replaceUrl(landingPage["url"], crawlConfig["replaceUrlWords"])
        return pdfUrl

    if pdfUrl != None:
        pdfLinkNames = crawlConfig["mainPdfLinkREs"]
        for pdfLinkName in pdfLinkNames:
            for linkText, linkUrl in links.iteritems():
                if pdfLinkName.match(linkText):
                    pdfUrl = linkUrl
                    logging.debug("Found link to main PDF: %s -> %s" % (pdfLinkName, pdfUrl))

    if pdfUrl==None:
        raise pubGetError("main PDF not found", "mainPdfNotFound")

    return pdfUrl

def isErrorPage(landingPage, crawlConfig):
    if not "errorPageText" in crawlConfig:
        return False

    if crawlConfig["errorPageText"] in landingPage["data"]:
        logging.warn("Found error page, waiting for 15 minutes")
        time.sleep(60*15)
        return True
    else:
        return False

    
def crawlForFulltext(landingPage):
    """ 
    given a landingPage-dict (with url, data, mimeType), return a dict with the
    keys main.html, main.pdf and S<X>.<ext> that contains all (url, data,
    mimeType) pages for an article 
    """
    
    checkForOngoingMaintenanceUrl(landingPage["url"])
    crawlConfig  = getConfig(siteCrawlConfig, landingPage["url"])

    if noLicensePage(landingPage, crawlConfig):
        raise pubGetError("no license for this article", "noLicense")
    if isErrorPage(landingPage, crawlConfig):
        raise pubGetError("hit error page", "errorPage")

    landUrl = landingPage["url"]
    logging.debug("Final landing page after redirects is %s" % landingPage["url"])

    fulltextData = {}

    if landingPage["mimeType"] == "application/pdf":
        logging.debug("Landing page is the PDF, no suppl file downloading possible")
        fulltextData["main.pdf"] = landingPage
    else:
        # some landing pages contain the full article
        if crawlConfig.get("landingPageIsArticleUrlKeyword", False) and \
           crawlConfig["landingPageIsArticleUrlKeyword"] in landUrl:
                logging.debug("URL suggests that landing page is same as article html")
                fulltextData["main.html"] = landingPage

        if "ignoreLandingPageWords" in crawlConfig and \
            containsAnyWord(landingPage["data"], crawlConfig["ignoreLandingPageWords"]):
            logging.debug("Found blacklist word, ignoring article")
            raise pubGetError("blacklist word on landing page", "blackListWord")

            
        # search for main PDF on landing page
        pdfUrl = findMainFileUrl(landingPage, crawlConfig)
        if pdfUrl==None:
            logging.debug("Could not find PDF on landing page")
            raise pubGetError("Could not find main PDF", "notFoundMainPdf")

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
            suppExts.update(crawlConfig.get("addSuppFileTypes", []))
            fulltextData = getSuppData(fulltextData, suppListPage, crawlConfig, suppExts)

    return fulltextData

def noLicensePage(landingPage, crawlConfig):
    " return True if page looks like a 'purchase this article now' page "
    for stopPhrase in crawlConfig.get("stopPhrases", []):
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
    " parse pmids.txt in outDir and return as list "
    pmidFname = join(outDir, "pmids.txt")
    if not isfile(pmidFname):
        raise Exception("file %s not found. You need to run pubPrepCrawl pmids to create this file." % pmidFname)
    logging.debug("Parsing PMIDS %s" % pmidFname)
    pmids = [p.strip() for p in open(pmidFname).readlines()]
    logging.debug("Found %d PMIDS" % len(pmids))
    return pmids

def findLinkMatchingReList(links, searchLinkRes, searchUrls=False):
    """ given a list of (text, url) and search strings, return first url where text or url matches 
    one of the search strings. if searchUrls is True, searches URLs of links, otherwise their text.
    """
    for searchLinkRe in searchLinkRes:
        for linkName, linkUrl in links.iteritems():
            if (not searchUrls and searchLinkRe.match(linkName)) or \
                    (searchUrls and searchLinkRe.match(linkUrl)):
                suppListUrl = linkUrl
                logging.debug("Found link: %s, %s" % (repr(linkName), repr(linkUrl)))
                return suppListUrl

def findSuppListUrl(landingPage, crawlConfig):
    " given the landing page, find the link to the list of supp files "
    ignoreUrls = crawlConfig.get("ignoreUrlREs", [])
    landingPage = parseHtml(landingPage, ignoreUrlREs=ignoreUrls)

    links = landingPage["links"]
    htmlMetas = landingPage["metas"]

    suppListUrl = None
    # first try if we can derive suppListUrl from main URL
    if "replaceUrlWords_suppList" in crawlConfig:
        landUrl = landingPage["url"]
        suppListUrlRepl = replaceUrl(landUrl, crawlConfig["replaceUrlWords_suppList"])
        if suppListUrlRepl!=None:
            suppListUrl = suppListUrlRepl

    # then try to find URLs in links
    if suppListUrl==None and "suppListUrlREs" in crawlConfig:
        searchLinkRes = crawlConfig.get("suppListUrlREs", [])
        logging.debug("Searching for links to suppl list on page %s using URLs" % landingPage["url"])
        suppListUrl = findLinkMatchingReList(links, searchLinkRes, searchUrls=True)

    # then try text description in links
    if suppListUrl==None and "suppListPageREs" in crawlConfig:
        # if link url replacement not configured, try to search for links
        searchLinkRes = crawlConfig.get("suppListPageREs", [])
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

def getConfig(siteCrawlConfig, url):
    " based on the url or IP of the landing page, return a crawl configuration dict "
    hostname = urlparse.urlparse(url)[1]
    thisConfig = None
    for configHosts, config in siteCrawlConfig.iteritems():
        if type(configHosts)==types.StringType: # tuples with one element get converted by python
            configHosts = [configHosts]
        #logging.debug("cfhosts %s", configHosts)
        for configHost in configHosts:
            #logging.debug("cfhost %s", configHost)
            if hostname.endswith(configHost):
                logging.debug("Found config for host %s: %s" % (hostname, configHost))
                thisConfig = config
                break

    # not found -> try default HIGHWIRE config, if highwire host
    if thisConfig==None and isHighwire(hostname):
        thisConfig = getConfig(siteCrawlConfig, "HIGHWIRE")

    if thisConfig==None:
        raise pubGetError("No config for hostname %s" % hostname, "noConfig", hostname)
    else:
        return thisConfig

def prepConfig(siteCrawlConfig):
    " compile regexes in siteCrawlConfig "
    ret = {}
    for site, crawlConfig in siteCrawlConfig.iteritems():
        ret[site] = {}
        for key, values in crawlConfig.iteritems():
            if key.endswith("REs"):
                newValues = []
                for regex in values:
                    newValues.append(re.compile(regex))
            else:
                newValues = values
            ret[site][key] = newValues
    return ret

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
    " return current delay for highwire "
    os.environ['TZ'] = 'US/Eastern'
    time.tzset()
    tm = time.localtime()
    #time.struct_time(tm_year=2012, tm_mon=8, tm_mday=2, tm_hour=19, tm_min=22, tm_sec=49, tm_wday=3, tm_yday=215, tm_isdst=1)
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
    print 'Signal handler called with signal', signum

def writePaperData(pmid, pubmedMeta, fulltextData, outDir):
    " write all paper data to status and fulltext output files in outDir "
    oldHandler = signal.signal(signal.SIGINT, ignoreCtrlc) # deact ctrl-c
    pubmedMeta = storeFiles(pmid, pubmedMeta, fulltextData, outDir)
    storeMeta(outDir, pubmedMeta, fulltextData)
    pmidStatus = "OK\t%s %s, %d files" % (pubmedMeta["journal"], pubmedMeta["year"],
        len(fulltextData))
    writePmidStatus(outDir, pmid, pmidStatus)
    signal.signal(signal.SIGINT, oldHandler) # react ctrl c handler

def parseIdStatus(fname):
    " parse crawling status file, return as dict status -> count "
    res = {}
    for line in open(fname):
        pmid, status = line.strip().split()[:2]
        status = status.split("\\")[0]
        status = status.strip('"')
        status = status.strip("'")
        res.setdefault(status, 0)
        res[status]+=1
    return res

def writeReport(baseDir, htmlFname):
    " parse pmids.txt and pmidStatus.tab and write a html report to htmlFname "
    h = html.htmlWriter(htmlFname)
    h.head("Genocoding crawler status", stylesheet="bootstrap/css/bootstrap.css")
    h.startBody("Crawler status as of %s" % time.asctime())

    publDesc = {}
    for key, value in pubConf.crawlPubDirs.iteritems():
        publDesc[value] = key

    totalPmidCount = 0
    totalOkCount = 0

    for name in os.listdir(baseDir):
        dirName = join(baseDir, name)
        if not isdir(dirName) or name.startswith("_"):
            continue
        print dirName
        print "pmidCount"
        pmidCount = len(open(join(dirName, "pmids.txt")).readlines())
        print "status"
        statusCounts = parseIdStatus(join(dirName, "pmidStatus.tab"))
        publisher = basename(dirName)
        isActive = isfile(join(dirName, "_pubCrawl.lock"))
        totalPmidCount += pmidCount
        totalOkCount  += statusCounts["OK"]

        h.h4("Publisher: %s (%s)" % (publDesc[publisher], publisher))
        h.startUl()
        h.li("Crawler is running: %s" % isActive)
        h.li("Total PMIDs scheduled: %d" % pmidCount)
        h.li("Crawl success rate: %0.2f %%" % (100*statusCounts["OK"]/float(pmidCount)))
        h.startUl()
        for status, count in statusCounts.iteritems():
            h.li("Status %s: %d" % (status, count))
        h.endUl()
        h.endUl()

    h.h4("Total PMIDs scheduled to download: %d" % totalPmidCount)
    h.h4("Overall PMIDs downloaded so far: %d" % totalOkCount)
    h.endHtml()


def crawlFilesViaPubmed(outDir, waitSec, testPmid, pause, tryHarder):
    " download all files for pmids in outDir/pmids.txt to zipfiles in outDir "
    checkCreateLock(outDir)
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

    global siteCrawlConfig
    siteCrawlConfig = prepConfig(siteCrawlConfig)

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
            wgetCache = {}
            pubmedMeta = getMedlineInfo(pmid)

            issnYear = (pubmedMeta["eIssn"], pubmedMeta["year"])
            issnYearErrorCount = issnErrorCount[issnYear]
            if issnYearErrorCount > MAXISSNERRORCOUNT:
                writeIssnStatus(outDir, issnYear)
                raise pubGetError("too many errors for ISSN %s and year %s" % issnYear,
                        "issnYearErrorExceed\t%s %s" % issnYear)
            if issnYear in ignoreIssns:
                raise pubGetError("issn+year blacklisted", "issnErrorExceed", "%s %s" % issnYear)

            landingUrl   = findLandingUrl(pubmedMeta)
            landingPage  = delayedWget(landingUrl)
            
            fulltextData = crawlForFulltext(landingPage)

            # write results to output files
            if not testPmid:
                writePaperData(pmid, pubmedMeta, fulltextData, outDir)
            else:
                logging.info("Test-mode, not saving anything")

            if pause:
                raw_input("Press Enter...")
            consecErrorCount = 0

        except pubGetError, e:
            consecErrorCount += 1
            logging.error("PMID %s, error: %s, code: %s, details: %s" % (pmid, e.longMsg, e.logMsg, e.detailMsg))
            writePmidStatus(outDir, pmid, e.logMsg, e.detailMsg)
            issnErrorCount[issnYear] += 1
            if e.logMsg not in ["issnErrorExceed"]:
                logging.debug("Sleeping for %d secs after error" % errorWaitSecs)
                time.sleep(errorWaitSecs)

            if consecErrorCount > maxConSecError:
                logging.error("Too many consecutive errors, stopping crawl")
                raise
            if pause:
                raw_input("Press Enter...")


