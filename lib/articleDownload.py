from __future__ import print_function
from optparse import OptionParser
import util, maxXml
import urllib2, cookielib, urllib, re, time, sgmllib, os, sys, glob, urlparse,\
socket, tempfile, time, subprocess, fcntl, logging
from maxWeb import httpStartsWith

SLEEPSECONDS = 5 # how long to sleep between two http requests

# a python library to download the fulltext of an article from the internet.
# call like this:
#
# browser = FulltextDownloader(statusFilename="/tmp/fulltextDownload")
# ft = browser.downloadFulltext(9808786)
#
#
#
# CHANGES:
# Thu Dec  9 23:09:46 GMT 2010:  Almost complete rewrite, now part of pubtools
# id to test: 9808786
# Wed Apr 16 16:22:31 CEST 2008: handle pubmedcentral outlinks, 2583107 works now
# Wed Apr 16 16:42:03 CEST 2008: follow links to suppl data, handles BMC 17439641
# Thu Apr 17 20:26:23 CEST 2008: was choking on nature oncogene+http404 error messages (10022128)
# Fri Apr 18 18:28:08 CEST 2008: problem with endlines fixed (if input from file)
# Fri Apr 18 18:29:16 CEST 2008: selecting only "publisher" outlinks from pubmed now if multiple (18271954)
# Fri Apr 18 18:28:08 CEST 2008: recognizes pdfs that do not have extension ".pdf" by mime type (17032682)
# Tue Apr 22 20:58:27 CEST 2008: fix &lt and &gt in pubmed outlinks 10022610
# Thu May  1 10:57:37 CEST 2008: don't download files that are not application/pdf mime type
# Thu May  1 10:56:41 CEST 2008: remove \n and \t from urls 15124226
# Thu May  1 10:57:17 CEST 2008: added timeout for all sockets (not checked, problem occurs randomly)
# Fri May  9 14:02:00 CEST 2008: make http downloader timeout, not compat. with windows anymore
#                                due to fork() system calls
# Fri May  9 14:08:46 CEST 2008: interrupt with ctrl+c

def readLines(filename):
    """ return first field of lines as set, if exists """
    if filename!=None and os.path.isfile(filename):
        if filename!="stdin":
            lines = open(filename, "r").readlines()
        else:
            lines = stdin.readlines()
        pmids = set()
        for l in lines:
            if len(l)>1:
                pmids.add(l.split('\t')[0])
        return pmids
    else:
        return set()

def getPubmedOutlinks(pmid, preferPmc=True):
    """ use eutils to get outlinks from pubmed """
    logging.debug("%s: Getting outlink from pubmed" % (pmid))
    url = "http://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&id=%s&retmode=llinks&cmd=llinks" % pmid
    xp = maxXml.XmlParser(url=url)
    #req.add_header('User-Agent', 'User-Agent: Mozilla/5.0 (Macintosh; U; Intel Mac OS X; en-US) PubmedToPdfDownloader_by_maximilianh@gmail.com')
    outlinks = []
    aggregatorOutlinks = []

    for objUrl in xp.getXmlAll("LinkSet/IdUrlList/IdUrlSet/ObjUrl"):
        url = objUrl.getTextFirst("Url")
        SubjType = objUrl.getTextFirst("SubjectType")
        if SubjType != "publishers/providers":
            logging.log(5, "skipping url %s, is not a URL to a provider/publisher" % url)
            if SubjType == "aggregators":
                aggregatorOutlinks.append(url)
            continue

        attrList =  list(objUrl.getTextAll("Attribute"))
        if not "full-text online" in attrList and \
            "full-text PDF" not in attrList:
            logging.log(5, "skipping url %s, does not seem to provide fulltext" % url)
            continue
        else:
            outlinks.append(url)

    logging.debug("Found %d outlinks" % len(outlinks))
    logging.log(5, "Outlinks: %s" % str(outlinks))
    if len(outlinks)==0:
        logging.debug("No Outlinks found, checking for PMC aggregator")
        if len(aggregatorOutlinks)>1 and \
           "ukpmc" in aggregatorOutlinks[0]: # let's get rid of UKPMC
            aggregatorOutlinks.pop(0)
        for outlink in aggregatorOutlinks:
            if preferPmc and httpStartsWith("http://www.ncbi.nlm.nih.gov/pmc", outlink):
                logging.debug("Found PMC outlink")
                return outlink
        logging.debug("No PMC outlink")
        return None
    else:
        if "ukpmc" in outlinks[0] and len(outlinks)>1:
            outlinks.pop(0)
        if "swetswise" in outlinks[0] and len(outlinks)>1:
            outlinks.pop(0)
        outlink = outlinks[0]
        logging.debug("Using outlink: %s" % outlink)
        return outlink

def getPubmedDoi(pmid):
    """ retrieve doi for pmid via http eutils"""
    url = 'http://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&email=maximilianh@gmail.com&retmode=xml&id=%s' % pmid
    xp = maxXml.XmlParser(url=url)
    doi = xp.getTextFirst("PubmedArticle/PubmedData/ArticleIdList/ArticleId", reqAttrDict={'IdType':'doi'}, default=None)
    logging.debug("Found DOI: %s" % doi)
    return doi

class HtmlParser(sgmllib.SGMLParser):
    """ parses href-information from html file and stores some info in its attributes: hyperlinks, metaInfo, title, url """
    def __init__(self, docUrl, verbose=0, htmlData=None):
        "Initialise an object, passing 'verbose' to the superclass."
        sgmllib.SGMLParser.__init__(self, verbose)
        self.hyperlinks = []
        self.iframeLinks = []
        self.element=None
        self.metaInfo = {}
        self.title = ""
        self.completeUrl = docUrl
        self.baseUrl = "/".join(docUrl.split("/")[:-1])+"/"
        self.host = "http://" + urlparse.urlparse(self.baseUrl)[1]
        self.foundAccess=False
        if htmlData:
            self.parseLines(htmlData)


    def titleContains(self, text):
        return self.title.find(text)!=-1

    def getLinksWith(self, textList, type="desc"):
        """ type can be either desc or url, will search link descriptions or their urls for text and return list of urls"""
        """ urls will be lowercased for comparison and normalized, descriptions will be changed (see below)
            all links have their #-parts stripped off
        """
        links = []
        #print self.hyperlinks
        for text in textList:
            for desc, url in self.hyperlinks:
                # correct url
                if url not in links:
                    if type=="desc" and desc.find(text)!=-1:
                        links.append(url)
                    if type=="url" and url.lower().find(text.lower())!=-1:
                        links.append(url)
        return links

    def parseLines(self, data):
        self.feed(data)
        self.close()

    def start_iframe(self, attributes):
        for name, url in attributes:
            if name == "id":
                self.element = "iframeId"
            if name == "src" and self.element=="iframeId":
                self.iframeLinks.append(url)

    def end_iframe(self):
        self.element = None

    def start_a(self, attributes):
        "Process a hyperlink and its 'attributes'."
        for name, url in attributes:
            if name == "href":
                self.element="a"
                url = url.split("#")[0]
                url = url.strip().replace("\t","")
                url = url.strip().replace("\n","")
                url = url.replace("&amp;", "&")
                if url.endswith("+html"): # for oxf journals, e.g. pmid 17032682
                    url = url.replace("+html","")
                if not url.startswith("http"): # adding base url to url
                    url = urlparse.urljoin(self.baseUrl, url)
                if url==self.baseUrl or not urlparse.urlparse(url)[1]==urlparse.urlparse(self.baseUrl)[1]: # has to be on same server
                    logging.log(5, "Ignoring link: %s" % url)
                    self.element=None
                    continue
                else:
                    logging.log(5, "Found link: %s" % url)
                    self.hyperlinks.append(["", url])

    def end_a(self):
        self.element=None

    def start_title(self, attributes):
        self.element="title"

    def end_title(self):
        self.element=None

    def start_meta(self, attributes):
        name = None
        httpEquivRefresh=False
        for key, value in attributes:
                if key=="name":
                        name=value.strip()
                if key=="content":
                            if not httpEquivRefresh:
                                self.metaInfo[name]=value.strip()
                            else:
                                val= value.strip()
                                val = "=".join(val.split("=")[1:]) # split off part before =
                                url = urlparse.urljoin(self.baseUrl, val)
                                self.metaInfo["httpRefresh"]=url
                                logging.debug("htmlParser: found content %s for httpRefresh attribute" % url)
                if key=="http-equiv" and value=="refresh":
                        httpEquivRefresh=True
                        logging.debug("htmlParser: found attribute http-equipv and value refresh in meta-tag")

    def handle_data(self, data):
        lowerData = data.lower()
        if lowerData.find("access") or lowerData.find("purchase"):
            self.foundAccess=True
        if self.element=="a":
            self.hyperlinks[-1][0] += data.strip()
        if self.element=="title":
            self.title = data.strip()

class HTTPSpecialErrorHandler(urllib2.HTTPDefaultErrorHandler):
    """ for urrlib2 error catching """
    def http_error_403(self, req, fp, code, msg, headers):
        logging.debug("HTTP ERROR 403: MaryAnnLiebert-anti-crawler technique")

class FulltextLinkTable:
    """ stores all fulltext link data for a single article located at baseUrl

    contains URLs and information about them and the content of the URLs, retrieved by http-get requests """
    def __init__(self, pmid, baseUrl=""):
        self.urlInfo = {} # format: url -> (fileType, isSuppData)
        self.pmid = pmid
        self.baseUrl = baseUrl
        self.notDownloadReason = None
        self.httpData = {} # the binary data of pdf/doc/xsl files, for each url in urlInfo

    def notDownloadable(self, reason):
        logging.debug("Marking PMID %s as undownloadable, reason: %s"  % (self.pmid,  reason))
        self.notDownloadReason = reason

    def toString(self):
        lines = []
        # write error message if downloading was not successfull
        if self.notDownloadReason:
            lines.append("%s\t%s" % (self.pmid, self.notDownloadReason))
        else:
            # write one line per URL
            for url, extSuppData in self.urlInfo.iteritems():
                fileType, isSuppData = extSuppData
                fields = [self.pmid, self.baseUrl, url, fileType, isSuppData]
                fields = [str(x) for x in fields]
                line  = "\t".join(fields)
                lines.append(line)
        return "\n".join(lines)

    def getData(self, onlyFileType=None):
        """ retrieve table as a generator in format url, fileType, isSuppData, httpData """
        for url, extSuppData in self.urlInfo.iteritems():
            fileType, isSuppData = extSuppData
            if onlyFileType and onlyFileType!=fileType:
                continue
            yield url, fileType, isSuppData, self.httpData[url]

    def hasPdfData(self):
        hasPdf =  len(list(self.getData(onlyFileType="pdf")))>0
        return hasPdf

    def contains(self, url):
        """ check if url is part of table"""
        return url in self.urlInfo

    def addAll(self, urls, isSupp, httpRequester, fileType = None):
        """ add all urls to table """
        for url in urls:
            self.add(url, isSupp, httpRequester, fileType)

    def add(self, url, isSupp, httpRequester, fileType = None):
        """ add url to table and download contents via httpRequester"""
        if fileType==None:
            fileType = os.path.splitext(url)[1].strip(".")
        if "?" in fileType:
            fileType = fileType.split("?")[0]
        logging.debug("Saving link %s, fileType %s, isSuppData %s" % (url, fileType, isSupp))
        self.urlInfo[url]          = (fileType, isSupp)
        realUrl, contentType, data = httpRequester.get(url)
        self.httpData[url]         = data

    def appendToFile(self, filename):
        """ write table to file """
        logging.debug("Writing all links for PMID %s to %s" % (self.pmid, filename))
        fh = open(filename, "a")
        fh.write(self.toString()+"\n")

class HttpRequester:
    """ class to send requests via http and potentially save cookies
        caches requests dictionary "httpData"

    """

    def __init__(self):
        self.clearCache()

    def clearCache(self):
        self.httpData = {}

    def loginInist(self, user, passw):
        """ french national research service fulltext access system """
        logging.debug("Login into inist.fr")
        url="http://gate1.inist.fr/login"
        data = {"user" : user, "pass" : passw}
        httpRequest(url, data)

    def get(self, url, data=None):
        """
            http get/post request with mozilla headers(get if data==None, post if data!=None)
            returns tuple (url, contentType, data)
        """
        if url in self.httpData:
            logging.debug("Getting data from cache for %s" % url)
            contentType, data = self.httpData[url]
            return url, contentType, data
        logging.debug("Sleeping for %d seconds" % SLEEPSECONDS)
        time.sleep(SLEEPSECONDS)
        logging.debug("Retrieving %s" % url)
        if data!=None:
            data = urllib.urlencode(data)
        request = urllib2.Request(url, data)
        request.add_header('User-Agent', 'User-Agent: Mozilla/5.0 (Macintosh; U; Intel Mac OS X; en-US; rv:1.8.1.13) Gecko/20080311 Firefox/2.0.0.13')
        try:
            resp = urllib2.urlopen(request)
            info = {}
            url = resp.geturl()
            contentType = resp.info()["Content-type"].strip().split(";")[0]
            httpData = resp.read()
            self.httpData[url] = (contentType, httpData)
            return url, contentType, httpData
        except urllib2.HTTPError:
            return None, None, None

class FulltextDownloader:
    """ "browser" to download pdfs given their pubmedId, using sgmlParser and PubmedParser """
    def __init__(self, authMode=None, user=None, passw=None, statusFilename=None, force=False):
        # read status file into memory, contains pmids to ignore
        self.statusFilename = statusFilename
        self.ignorePmids    = readLines(statusFilename)
        self.force          = force
        self.httpRequester  = HttpRequester()

        if authMode=="inist":
            self.httpRequester.loginInist(user, passw)

    def pmidToUrl(self, pmid):
        """ for a PMID: try to find first outlink or alternatively DOI as referenced from pubmed
        """
        outlink = getPubmedOutlinks(pmid)

        if outlink:
            return outlink
        else:
            logging.debug("%s: could not find outlink, trying DOI" % (pmid))
            doi = getPubmedDoi(pmid)
            if doi==None:
                logging.debug("%s: could not find DOI, giving up" % (pmid))
                #self.markUndownloadable(pmid, "noOutlink")
                return None
            else:
                logging.debug("%s: Using DOI" % (pmid))
                url = "http://dx.doi.org/"+doi
                return url

    def searchFileLinks(self, parser, fulltextLinkTable, isSupp):
        """ uses the parser object to find links to pdf/xls/doc files and adds them to fulltextLinkTable, returns fulltextLinkTable """

        # check if html meta info contains link to pdf
        if "citation_pdf_url" in parser.metaInfo:
            logging.debug("Found citation_pdf_url meta information")
            pdfurl= parser.metaInfo["citation_pdf_url"]
            # HANDLE WILEY: for PDF Iframe
            if httpStartsWith("http://onlinelibrary.wiley.com", pdfurl):
                logging.debug("Detected Wiley IFrame")
                url, contentType, htmlData = self.httpRequester.get(pdfurl)
                parser = HtmlParser(pdfurl, htmlData=htmlData)
                if len(parser.iframeLinks)>0:
                      pdfurl = parser.iframeLinks[0]
                      logging.debug("Pulled PDF link %s from Wiley IFrame" % pdfurl)
                      fulltextLinkTable.add(pdfurl, isSupp, self.httpRequester, fileType="pdf")
                else:
                      logging.debug("Could not find PDF link")

            else:
                pdfurl = urlparse.urljoin(parser.baseUrl, pdfurl)
                fulltextLinkTable.add(pdfurl, isSupp, self.httpRequester)


        # get first link to a .pdf file
        urls = parser.getLinksWith([".pdf"], "url")
        # HANDLE SCIENCEDIRECT: keep only first link to pdf (remove references' links)
        #if parser.baseUrl.startswith("http://www.sciencedirect.com"):
        if len(urls)>0:
            fulltextLinkTable.add(urls[0], False, self.httpRequester)
            #logging.debug("keeping only first .pdf link")
            #newUrls = []
            #foundPdf = False
            #for url in urls:
                #if url.endswith("pdf"):
                    #if not foundPdf:
                        #newUrls.append(url)
                        #foundPdf=True
                #else:
                    #newUrls.append(url)
            #urls = newUrls

        # add all office files
        urls = parser.getLinksWith([".doc", ".xls"], "url")
        fulltextLinkTable.addAll(urls, True, self.httpRequester)

        for url in urls:
            logging.debug("Found link to pdf/doc/xls file: %s" % url)
            if url.endswith(".doc") or url.endswith(".xls"):
                isSupp=True
            fulltextLinkTable.add(url, isSupp, self.httpRequester)

        # Only if still no pdf: check all link DESCRIPTIONS of html
        # keep only first one
        if not fulltextLinkTable.hasPdfData():
            urls = parser.getLinksWith(["PDF", "pdf"], "desc")
            logging.debug("Found %d links with description PDF/pdf, keeping first one" % (len(urls)))
            if len(urls)>0:
                url = urls[0]
                fulltextLinkTable.add(url, isSupp, self.httpRequester, fileType="pdf")

        return fulltextLinkTable

    def getSupplementalListFiles(self, url, fulltextLinkTable):
        """ get all files from supplemental file list page """
        logging.debug("Getting files from supplemental data page at %s" % url)
        parser = HtmlParser(url)
        url, contentType, data = self.httpRequester.get(url)
        if data==None:
            logging.info("Received NO data from http request")
            return fulltextLinkTable
        parser.parseLines(data)
        logging.debug("Content type of supplementary file is %s" % contentType)
        if not contentType.startswith("text/html"):
            logging.info("Supplementary Link does not lead to HTML page, stopping search")
            return fulltextLinkTable

        fileTypes = [".pdf", ".doc", ".xls"]
        urls = parser.getLinksWith(fileTypes, type="url")
        logging.debug("Found %d files on supplemental data page" % len(urls))
        for url in urls:
            fulltextLinkTable.add(url, True, self.httpRequester)
        return fulltextLinkTable

    def crawl(self, url, fulltextLinkTable, depth, isSupp=False):
        """ recursively crawl pdf/doc/xls files and "Supplemental"-like links
        from url with maximum depth returns, a dict with 'pdf' -> url, 'html' ->
        url, suppFiles -> list of urls
        """
        logging.debug("Crawling %s" % url)
        url, contentType, data = self.httpRequester.get(url)

        if depth==0:
            logging.debug("Maximum depth reached, stop crawling")
            return fulltextLinkTable

        if url==None:
            logging.debug("HTTP error while retrieving outlink target %s" % (url))
            return fulltextLinkTable

        if contentType=="application/pdf":
            logging.debug("%s: Crawling: URL %s is a pdf file" % pmid)
            fulltextLinkTable.add(url, isSupp, self.httpRequester)
            return fulltextLinkTable

        logging.debug("Collecting links from %s" % (url))
        parser = HtmlParser(url)
        parser.parseLines(data)

        # handle special cases:

        # Elsevier chooser:
        if parser.titleContains("Elsevier Article Locator"):
            logging.debug("Getting across the Elsevier chooser")
            scDirectLinks = parser.getLinksWith(["sciencedirect.com"], type="url")
            if len(scDirectLinks)==0:
                logging.debug("Could not find ScienceDirect link after Elsevier Chooser, giving up.")
                return fulltextLinkTable
            fulltextLinkTable = self.crawl(scDirectLinks[0], fulltextLinkTable, depth)
            return fulltextLinkTable

        if parser.titleContains("Blackwell Synergy"):
            logging.debug("Getting around blackwell synergy javascripts")
            url = req.geturl()
            url = url.replace("/abs/", "/pdf/")
            fulltextLinkTable.add(url, self.httpRequester, fileType="pdf")
            return fulltextLinkTable

        fulltextLinkTable = self.searchFileLinks(parser, fulltextLinkTable, isSupp)

        suppFileTriggerwords = ["Additional files", "Supplemental", "Supplementary", "Supporting Information"]
        urls = parser.getLinksWith(suppFileTriggerwords)

        if len(urls)!=0:
            logging.debug("Found %d links to supplemental file list pages" % (len(urls)))
            logging.log(5, "URLs are: %s" % str(urls))
            for url in urls:
                if fulltextLinkTable.contains(url):
                    continue
                else:
                    fulltextLinkTable = self.getSupplementalListFiles(url, fulltextLinkTable)

        return fulltextLinkTable

    def getFulltextFileUrls(self, pmid):
        """ get links from Pubmed and recursively search them for links to
        pdf/doc/xls files, returns a dictionary with keys:
        'pdf' -> pdfUrl
        'html' -> htmlUrl
        <anyOtherDesc> -> suppUrl

        """
        outlink = self.pmidToUrl(pmid)
        fulltextLinksTable = FulltextLinkTable(pmid, outlink)
        if not outlink:
            fulltextLinksTable.notDownloadable("noOutlink")
            return fulltextLinksTable

        logging.debug("Collecting links from publisher's website")
        fulltextLinksTable = self.crawl(outlink, fulltextLinksTable, 1)

        if not fulltextLinksTable.hasPdfData():
            fulltextLinksTable.notDownloadable("noPdf")
        return fulltextLinksTable

    def markUndownloadable(self, pmid, reason):
        """ add pmid to notDownloadableFile aka pmid Cache and memory cache """
        self.ignorePmids.add(pmid)
        filename = self.statusFilename
        logging.debug("Marking PMID %s as undownloadable in %s, reason: %s"  % (pmid, filename, reason))
        f = open(file, "a")
        f.write(pmid+"\n")
        f.close()

    def downloadFulltext(self, pmid):
         """ force = download even if in cache

         """
         self.httpRequester.clearCache()
         pmid = str(pmid)
         logging.debug("%s: Crawler start" % pmid)

         if pmid in self.ignorePmids and not self.force:
             logging.info ("Pmid %s is marked as not being downloadable by %s" % (pmid, self.statusFilename))
             return None

         try:
            fulltextLinkTable = self.getFulltextFileUrls(pmid)
            fulltextLinkTable.appendToFile(self.statusFilename)
            return fulltextLinkTable
         except:
            excType, excVal, excTraceback = sys.exc_info()
            logging.error("Got exception %s, with value %s, and traceback %s" % (str(excType), str(excVal), str(excTraceback)))
            return None

if __name__ == "__main__":
    #import doctest
    #doctest.testmod()
    logging.basicConfig(level=logging.DEBUG)
    #logging.basicConfig(level=5)
    browser = FulltextDownloader(statusFilename="/tmp/fulltextDownload", force=True)
    pmids = [2583107, 20419150, 17439641, 10022128, 18271954, 17032682, 15124226, 9808786]
    for pmid in pmids:
        ft = browser.downloadFulltext(pmid)
        print(ft.toString())
