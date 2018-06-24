# Python bytecode 2.7 (decompiled from Python 2.7)
# Embedded file name: /cluster/home/max/projects/pubs/tools/lib/pubCrawlLib.py
# Compiled at: 2016-07-12 00:58:53
import logging, os, shutil, tempfile, codecs, re, types, urllib2, re, zipfile, collections, urlparse, time, atexit, socket, signal, sqlite3, doctest, urllib, hashlib, string, copy, cStringIO, mimetypes, httplib
from os.path import *
from collections import defaultdict, OrderedDict
from distutils.spawn import find_executable
from socket import timeout
import pubConf, pubGeneric, pubStore, pubCrossRef, pubPubmed
import maxTables, htmlPrint, maxCommon
from scihub import scihub
import chardet
import unidecode
from incapsula import crack
import requests
logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARNING)
requestsLoaded = True
try:
    from selenium import webdriver
    from selenium.webdriver.common.proxy import *
    from selenium.webdriver.common.by import By
    from pyvirtualdisplay import Display
    seleniumLoaded = True
except:
    seleniumLoaded = False

from BeautifulSoup import BeautifulSoup, SoupStrainer, BeautifulStoneSoup
import xml.etree.ElementTree as etree
PUBLOCKFNAME = '_pubCrawl.lock'
issnYearErrorCounts = defaultdict(int)
forceUserAgent = None
PMIDSTATNAME = 'docStatus.tab'
ISSNSTATNAME = 'issnStatus.tab'
SUPPFILEMAX = 50
SUPPFILEMAXSIZE = 30000000
MAXCONSECERR = 50
MAXISSNERRORCOUNT = 30
ERRWAIT = 5
DO_PAUSE = False
TEST_OUTPUT = False
SKIPLOCALMEDLINE = False
crawlDelays = {'onlinelibrary.wiley.com': 1,
 'dx.doi.org': 1,
 'ucelinks.cdlib.org': 10,
 'eutils.ncbi.nlm.nih.gov': 3,
 'www.ncbi.nlm.nih.gov': 10,
 'lww': 10,
 'npg': 10,
 'nejm': 10,
 'elsevier': 10,
 'wiley': 10,
 'springer': 10,
 'tandf': 10,
 'karger': 10,
 'generic': 5,
 'silverchair': 10}
crawlDelays.update(pubConf.crawlDelays)
defaultDelay = 10
globalForceDelay = None
lockFnames = []
webCache = {}
addHeaders = ['mainHtmlUrl',
 'mainPdfUrl',
 'suppUrls',
 'mainHtmlFile',
 'mainPdfFile',
 'suppFiles',
 'landingUrl']
articleFields = ['articleId',
 'externalId',
 'source',
 'origFile',
 'journal',
 'printIssn',
 'eIssn',
 'journalUniqueId',
 'year',
 'articleType',
 'articleSection',
 'authors',
 'authorEmails',
 'authorAffiliations',
 'keywords',
 'title',
 'abstract',
 'vol',
 'issue',
 'page',
 'pmid',
 'pmcId',
 'doi',
 'fulltextUrl',
 'time']
metaHeaders = articleFields
metaHeaders.extend(addHeaders)

class pubGetError(Exception):

    def __init__(self, longMsg, logMsg, detailMsg=None):
        self.longMsg = longMsg
        self.logMsg = logMsg
        self.detailMsg = detailMsg

    def __str__(self):
        return unidecode.unidecode(self.longMsg + '/' + self.logMsg + '/' + self.detailMsg)


def resolveDoiWithSfx(sfxServer, doi):
    """ return the fulltext url for doi using the SFX system """
    logging.debug('Resolving doi %s with SFX' % doi)
    xmlQuery = '%s/SFX_API/sfx_local?XML=<?xml version="1.0" ?><open-url><object_description><global_identifier_zone><id>doi:%s</id></global_identifier_zone><object_metadata_zone><__service_type>getFullTxt</__service_type></object_metadata_zone></object_description></open-url>' % (sfxServer, str(doi))
    return resolveWithSfx(sfxServer, xmlQuery)


def resolvePmidWithSfx(sfxServer, pmid):
    """ return the fulltext url for pmid using the SFX system """
    logging.debug('Resolving pmid %s with SFX' % pmid)
    xmlQuery = '%s/SFX_API/sfx_local?XML=<?xml version="1.0" ?><open-url><object_description><global_identifier_zone><id>pmid:%s</id></global_identifier_zone><object_metadata_zone><__service_type>getFullTxt</__service_type></object_metadata_zone></object_description></open-url>' % (sfxServer, str(pmid))
    return resolveWithSfx(sfxServer, xmlQuery)


def resolveWithSfx(sfxServer, xmlQuery):
    sfxResult = httpGetDelay(xmlQuery, forceDelaySecs=0)
    xmlResult = sfxResult['data']
    soup = BeautifulStoneSoup(xmlResult, convertEntities=BeautifulSoup.HTML_ENTITIES, smartQuotesTo=None)
    urlEls = soup.findAll('url')
    if len(urlEls) == 0 or urlEls == None:
        return
    elif urlEls[0] == None:
        return
    else:
        urls = [ x.string for x in urlEls ]
        logging.debug('SFX returned (using only first of these): %s' % urls)
        if urls[0] == None:
            return
        url = urls[0].encode('utf8')
        return url


def getLandingUrlSearchEngine(articleData):
    """ given article meta data, try to find landing URL via a search engine:
    - medline's DOI
    - a Crossref search with medline data
    - Pubmed Outlink
    - an SFX search
    #>>> findLandingUrl({"pmid":"12515824", "doi":"10.1083/jcb.200210084", "printIssn" : "1234", "page":"8"})
    'http://jcb.rupress.org/content/160/1/53'
    """
    landingUrl = None
    assert articleData['doi'] is not None
    if articleData['doi'] != '':
        landingUrl = resolveDoi(articleData['doi'])
        if landingUrl != None:
            return landingUrl
    if articleData['doi'] == '':
        xrDoi = pubCrossRef.lookupDoi(articleData)
        if xrDoi != None:
            articleData['doi'] = re.sub('https?://(dx\.)?doi.org/', '', xrDoi)
            landingUrl = resolveDoi(xrDoi)
            if landingUrl != None:
                return landingUrl
    if articleData['pmid'] != '':
        outlinks = pubPubmed.getOutlinks(articleData['pmid'])
        if outlinks == None:
            logging.info('pubmed error, waiting for 120 secs')
            time.sleep(120)
            raise pubGetError('pubmed outlinks http error', 'PubmedOutlinkHttpError')
        if len(outlinks) != 0:
            landingUrl = outlinks.values()[0]
            logging.debug('landing page based on first outlink of Pubmed, URL %s' % landingUrl)
            return landingUrl
    if pubConf.crawlSfxServer == None:
        logging.warn('You have not defined an SFX server in pubConf.py, cannot search for fulltext')
    else:
        landingUrl = resolvePmidWithSfx(pubConf.crawlSfxServer, articleData['pmid'])
    if landingUrl == None:
        raise pubGetError('No fulltext for this article', 'noOutlinkOrDoi'), landingUrl
    return landingUrl

def getDelaySecs(host, forceDelaySecs):
    """ return the number of seconds to pause. Either globally forced from command line,
    or set by-host or some global default if everything fails
    returns number of secs
    """
    if globalForceDelay != None:
        logging.log(5, 'delay time is set globally to %d seconds' % globalForceDelay)
        return globalForceDelay
    elif forceDelaySecs != None:
        logging.log(5, 'delay time is set for this download to %d seconds' % forceDelaySecs)
        return forceDelaySecs
    else:
        logging.debug('Looking up delay time for host %s' % host)
        if host in crawlDelays:
            delaySecs = crawlDelays.get(host, defaultDelay)
            logging.info('Delay time for host %s set to %d seconds' % (host, delaySecs))
            return delaySecs
        logging.debug('Delay time for host %s not known' % host)
        return defaultDelay


browser = None
display = None

def httpGetSelenium(url, delaySecs, mustGet=False):
    """ use selenium to download a page """
    global display
    global browser
    logging.info('Downloading %s using Selenium/Firefox' % url)
    host = urlparse.urlsplit(url)[1]
    delaySecs = getDelaySecs(host, delaySecs)
    wait(delaySecs, host)
    if not seleniumLoaded:
        raise pubGetError('Cannot get page, selenium is not installed on this machine', 'noSelenium')
    page = {}
    if not display:
        logging.info('Starting pseudo-display')
        display = Display(visible=0, size=(1024, 768))
        display.start()
    proxy = None
    if pubConf.httpProxy:
        proxy = Proxy({'proxyType': ProxyType.MANUAL,
         'httpProxy': pubConf.httpProxy,
         'ftpProxy': pubConf.httpProxy,
         'sslProxy': pubConf.httpProxy})
    if not browser:
        logging.info('Starting firefox on pseudo-display')
        browser = webdriver.Firefox(proxy=proxy)
    count = 0
    while count < 5:
        try:
            browser.get(url)
            break
        except (requests.Timeout, timeout):
            logging.warn('timeout from selenium')
            count += 1
        except httplib.CannotSendRequest:
            logging.warn("selenium's firefox died, restarting")
            count += 1
            browser = webdriver.Firefox(proxy=proxy)

    if count >= 5:
        logging.warn('too many timeouts/errors from selenium')
        if mustGet:
            raise pubGetError('too many timeouts', 'httpTimeout')
        return
    else:
        page['seleniumDriver'] = browser
        page['url'] = browser.current_url
        page['data'] = browser.page_source
        page['mimeType'] = 'unknown'
        if type(page['data']) == types.UnicodeType:
            page['data'] = page['data'].encode('utf8')
        return page


def httpGetDelay(url, forceDelaySecs=None, mustGet=False, blockFlash=False, cookies=None, userAgent=None, referer=None):
    """ download with http and make sure that delaySecs (global var)
    secs have passed between two calls special cases for highwire hosts and
    some hosts configured in config file.
    
    returns dict with these keys: url, mimeType, charset, data
    Follows redirects, "url" is really the final URL.
    """
    global webCache
    if url in webCache:
        logging.log(5, 'Using cached http results')
        return webCache[url]

    logging.info('Downloading %s' % url)
    host = urlparse.urlsplit(url)[1]
    delaySecs = getDelaySecs(host, forceDelaySecs)
    wait(delaySecs, host)
    if userAgent == None:
        if forceUserAgent == None:
            userAgent = pubConf.httpUserAgent
        else:
            userAgent = forceUserAgent
    if blockFlash:
        userAgent = 'Mozilla/5.0(iPad; U; CPU iPhone OS 3_2 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4 Mobile/7B314 Safari/531.21.10'
    page = httpGetRequest(url, userAgent, cookies, referer=referer)
    if mustGet and page == None:
        raise pubGetError('Could not get URL %s' % url, 'illegalUrl')
    return page

session = None

def httpGetRequest(url, userAgent, cookies, referer=None):
    """
    download a url with the requests module, return a dict with the keys
    url, mimeType, charset and data
    """
    global session
    logging.debug('HTTP request to %s. useragent: %s' % (url, userAgent))
    headers = {'user-agent': userAgent}
    if referer is not None:
        headers['referer'] = referer
    if session is None:
        session = requests.Session()
        if pubConf.httpProxy != None:
            proxies = {'http': pubConf.httpProxy,
             'https': pubConf.httpProxy}
            session.proxies.update(proxies)
    tryCount = 0
    r = None
    while tryCount < 5:
        try:
            r = session.get(url, headers=headers, cookies=cookies, allow_redirects=True)
            break
        except (requests.exceptions.ConnectionError,
         requests.exceptions.TooManyRedirects,
         requests.exceptions.Timeout,
         requests.exceptions.RequestException):
            tryCount += 1
            logging.info('HTTP error, retry number %d' % tryCount)
            time.sleep(3)

    if r == None:
        raise pubGetError('HTTP error on %s' % url, 'httpError', url)
    page = {}
    page['url'] = r.url
    page['data'] = r.content
    page['mimeType'] = r.headers['content-type'].split(';')[0]
    page['encoding'] = r.encoding
    logging.log(5, 'Got page, url=%(url)s, mimeType=%(mimeType)s, encoding=%(encoding)s' % page)
    webCache[r.url] = page
    webCache[url] = page
    return page


def anyMatch(regexList, queryStr):
    for regex in regexList:
        if regex.match(queryStr):
            logging.debug('url %s ignored due to regex %s' % (queryStr, regex.pattern))
            return True

    return False


def removeThreeByteUtf(html):
    """ 
    the normal narrow python build cannot do 3-byte utf8. 
    But html can include them e.g. with &#x1D6C4;
    Beautiful soup then throws an error. 
    This function simply replaces all entities with more than 4 hex digits
    """
    entRe = re.compile('&#x[0-9ABCDEabcde]{5,9}')
    return entRe.sub('<WideUnicodeChar>', html)

def htmlParsePage(page):
    """ parse the html page with beautifulsoup 3 """
    if 'parsedHtml' not in page:
        logging.debug('Parsing HTML')
        html = page['data']
        html = html.replace(' xmlns="http://www.w3.org/1999/xhtml"', '')
        html = removeThreeByteUtf(html)
        page['parsedHtml'] = BeautifulSoup(html)

def htmlExtractPart(page, tag, attrs):
    """
    return a part of an html page as a string given a tag and the required attribute values
    If not found, return the full html text string. Parsing result is cached in the page.
    """
    try:
        htmlParsePage(page)
    except UnicodeEncodeError:
        logging.warn('could not parse html')
        return page['data']

    bs = page['parsedHtml']
    el = bs.find(tag, attrs=attrs)
    if el != None:
        logging.debug('Successfully stripped html')
        return str(el)
    else:
        logging.debug('Could not strip html')
        return page['data']
        return


def htmlFindLinkUrls(page, attrs={}):
    """ parses the whole page and finds links with certain attributes, returns the href URLs 
    This is really slow.
    """
    htmlParsePage(page)
    bs = page['parsedHtml']
    elList = bs.findAll('a', attrs=attrs)
    urls = []
    for el in elList:
        if not el.has_key('href'):
            continue
        url = el['href']
        url = urlparse.urljoin(page['url'], url)
        urls.append(url)

    return urls


def parseLinksSelenium(page):
    """
    like parseHtmlLinks but for a page in selenium. 
    Queries DOM to find links
    """
    logging.debug('Parsing links with Selenium')
    driver = page['seleniumDriver']
    links = OrderedDict()
    for el in driver.find_elements(By.TAG_NAME, 'a'):
        if el.get_attribute('href') is not None:
            links[el.get_attribute('href')] = el.text

    page['links'] = links
    metas = OrderedDict()
    for el in driver.find_elements(By.TAG_NAME, 'meta'):
        if el.get_attribute('name') is not None:
            links[el.get_attribute('name')] = el.get_attribute('content')

    page['metas'] = metas
    frames = OrderedDict()
    for el in driver.find_elements(By.TAG_NAME, 'frame'):
        if el.get_attribute('src') is not None:
            links[el.get_attribute('id', 'pdfDocument')] = el.get_attribute('src')

    page['frames'] = frames
    iframes = OrderedDict()
    for el in driver.find_elements(By.TAG_NAME, 'iframe'):
        if el.get_attribute('src') is not None:
            idName = el.get_attribute('id')
            if idName == None:
                idName = 'pdfDocument'
            links[idName] = el.get_attribute('src')

    page['iframes'] = iframes
    return page


def parseHtmlLinks(page, canBeOffsite=False, landingPage_ignoreUrlREs=[]):
    """ 
    find all A-like links and meta-tag-info from a html string and add 
    to page dictionary as keys "links", "metas" and "iframes"
    """
    if 'links' in page:
        return page
    elif 'seleniumDriver' in page:
        return parseLinksSelenium(page)
    else:
        logging.debug('Parsing HTML links')
        htmlString = page['data']
        baseUrl = page['url']
        urlParts = urlparse.urlsplit(baseUrl)
        basePath = urlParts[2]
        baseLoc = urlParts[1]
        logging.log(5, 'Parsing %s with bs3' % page['url'])
        linkStrainer = SoupStrainer(['a',
         'meta',
         'iframe',
         'frame'])
        try:
            fulltextLinks = BeautifulSoup(htmlString, smartQuotesTo=None, convertEntities=BeautifulSoup.ALL_ENTITIES, parseOnlyThese=linkStrainer)
        except ValueError as e:
            raise pubGetError('Exception during bs html parse', 'htmlParseException', e.message)

        logging.log(5, 'bs parsing finished')
        linkDict = OrderedDict()
        metaDict = OrderedDict()
        iframeDict = OrderedDict()
        frameDict = OrderedDict()
        for l in fulltextLinks:
            logging.log(5, 'got link %s' % l)
            if l.name == 'iframe':
                src = l.get('src')
                if src == None or 'pdf' not in src:
                    continue
                id = l.get('id', 'pdfDocument')
                iframeDict[id] = src
            if l.name == 'frame':
                src = l.get('src')
                if src == None or 'pdf' not in src:
                    continue
                id = l.get('id', 'pdfDocument')
                frameDict[id] = src
            elif l.name == 'a':
                text = l.getText()
                text = text.encode('utf8')
                url = l.get('href')
                if url == None:
                    logging.log(5, 'url is None')
                    continue
                try:
                    linkLoc = urlparse.urlsplit(url)[1]
                    linkPath = urlparse.urlsplit(url)[2]
                except ValueError:
                    raise pubGetError('Value error on url split %s' % url, 'urlSplitError', url)

                if canBeOffsite == False and linkLoc != '' and linkLoc != baseLoc:
                    logging.log(5, 'skipping link %s, is offsite' % url)
                    continue
                fullUrl = urlparse.urljoin(baseUrl, url)
                parts = list(urlparse.urlsplit(fullUrl)[:4])
                if parts[0] == 'javascript':
                    logging.log(5, 'skipping link %s, is javascript' % url)
                    continue
                parts.append('')
                fullUrlNoFrag = urlparse.urlunsplit(parts)
                if anyMatch(landingPage_ignoreUrlREs, fullUrlNoFrag):
                    logging.log(5, 'skipping link %s, because of ignore REs' % url)
                    continue
                linkDict[fullUrlNoFrag] = text
                logging.log(5, 'Added link %s for text %s' % (repr(fullUrlNoFrag), repr(text)))
            elif l.name == 'meta':
                name = l.get('name')
                if name != None:
                    content = l.get('content')
                    metaDict[name] = content
                if str(l.get('http-equiv')).lower() == 'refresh':
                    content = l.get('content')
                    logging.log('found meta refresh tag: %s' % str(content))
                    if content != None:
                        url = string.split(content, '=', 1)[1]
                        url = urlparse.urljoin(baseUrl, url)
                        metaDict['refresh'] = url

        logging.log(5, 'Meta tags: %s' % metaDict)
        logging.log(5, 'Links: %s' % linkDict)
        logging.log(5, 'iframes: %s' % iframeDict)
        logging.log(5, 'frames: %s' % frameDict)

        page['links'] = linkDict
        page['metas'] = metaDict
        page['iframes'] = iframeDict
        page['frames'] = frameDict
        logging.log(5, 'HTML parsing finished')
        return page

def recodeToUtf8(data):
    """ use chardet to find out codepage and recode to utf8"""
    try:
        data = data.decode('utf8').encode('utf8')
        return data
    except UnicodeDecodeError:
        encoding = chardet.detect(data)['encoding']
        logging.log(5, 'encoding should be %s' % encoding)
        if encoding == None:
            encoding = 'latin1'
        try:
            data = data.decode(encoding).encode('utf8')
        except UnicodeDecodeError:
            logging.warn('Error when decoding as %s' % encoding)
            data = data
        except LookupError:
            logging.warn('Unknown encoding when decoding as %s' % encoding)
            data = data

        return data

    return


def parseDocIdStatus(outDir):
    """ parse status file and return a set with pmids that should not be crawled """
    donePmids = set()
    statusFname = join(outDir, PMIDSTATNAME)
    logging.info('Parsing %s' % statusFname)
    statusFname2 = join(outDir, 'pmidStatus.tab')
    if isfile(statusFname2):
        for l in open(statusFname2):
            docId = l.strip().split('#')[0].split('\t')[0]
            if docId == '':
                continue
            donePmids.add(docId)

        logging.info('Found %d PMIDs that have some status in %s' % (len(donePmids), statusFname2))
    if isfile(statusFname):
        for l in open(statusFname):
            docId = l.strip().split('#')[0].split('\t')[0]
            if docId == '':
                continue
            donePmids.add(docId)

        logging.info('Found %d PMIDs that have some status' % len(donePmids))
    return donePmids


def parseIssnStatus(outDir):
    """ parse outDir/issnStatus.tab and return a set with (issn, year) hat should be ignored """
    statusFname = join(outDir, ISSNSTATNAME)
    logging.info('Parsing %s' % statusFname)
    ignoreIssns = set()
    if isfile(statusFname):
        for row in maxCommon.iterTsvRows(statusFname):
            ignoreIssns.add((row.issn, row.year))

    return ignoreIssns


lastCallSec = {}

def wait(delaySec, host='default'):
    """ make sure that delaySec seconds have passed between two calls with the same value of 'host' """
    global lastCallSec
    delaySec = float(delaySec)
    nowSec = time.time()
    sinceLastCallSec = nowSec - lastCallSec.get(host, nowSec)
    if sinceLastCallSec > 0.1 and sinceLastCallSec < delaySec:
        waitSec = max(0.0, delaySec - sinceLastCallSec)
        logging.info('Waiting for %f seconds before downloading from host %s' % (waitSec, host))
        time.sleep(waitSec)
    lastCallSec[host] = time.time()


def iterateNewPmids(pmids, ignorePmids):
    """ yield all pmids that are not in ignorePmids """
    ignorePmidCount = 0
    ignorePmids = set([ int(p) for p in ignorePmids ])
    pmids = set([ int(p) for p in pmids ])
    todoPmids = pmids - ignorePmids
    logging.debug('Skipped %d PMIDs' % (len(pmids) - len(todoPmids)))
    for pmidPos, pmid in enumerate(todoPmids):
        logging.debug('%d more PMIDs to go' % (len(todoPmids) - pmidPos))
        yield str(pmid)


def readLocalMedline(pmid):
    """ returns a dict with info we have locally about PMID, None if not found """
    logging.debug('Trying PMID lookup with local medline copy')
    medlineDb = pubStore.getArtDbPath('medline')
    if not isfile(medlineDb):
        logging.debug('%s does not exist, no local medline lookups' % medlineDb)
        return
    else:
        con, cur = maxTables.openSqlite(medlineDb)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        rows = None
        tryCount = 60
        while rows == None and tryCount > 0:
            try:
                rows = list(cur.execute('SELECT * from articles where pmid=?', (pmid,)))
            except sqlite3.OperationalError:
                logging.info('Database is locked, waiting for 60 secs')
                time.sleep(60)
                tryCount -= 1

        if rows == None:
            raise Exception('Medline database was locked for more than 60 minutes')
        if len(rows) == 0:
            logging.info('No info in local medline for PMID %s' % pmid)
            return
        lastRow = rows[-1]
        result = {}
        for key, val in zip(lastRow.keys(), lastRow):
            result[key] = unicode(val)

        result['source'] = ''
        result['origFile'] = ''
        return result


def downloadPubmedMeta(pmid):
    """ wrapper around pubPubmed that converts exceptions"""
    try:
        wait(3, 'eutils.ncbi.nlm.nih.gov')
        ret = pubPubmed.getOnePmid(pmid)
    except urllib2.HTTPError as e:
        raise pubGetError('HTTP error %s on Pubmed' % str(e.code), 'pubmedHttpError', str(e.code))
    except pubPubmed.PubmedError as e:
        raise pubGetError(e.longMsg, e.logMsg)

    if ret == None:
        raise pubGetError('empty result when requesting metadata from NCBI Eutils for PMID %s' % str(pmid), 'pubmedEmpty')
    for h in addHeaders:
        ret[h] = ''

    return ret


def writeMeta(outDir, metaData, fulltextData):
    """ append one metadata dict as a tab-sep row to outDir/articleMeta.tab and articles.db """
    filename = join(outDir, 'articleMeta.tab')
    logging.debug('Appending metadata to %s' % filename)
    minId = pubConf.identifierStart['crawler']
    metaData['articleId'] = str(minId + int(metaData['pmid']))
    if 'main.html' in metaData:
        metaData['fulltextUrl'] = metaData['main.html']['url']
    elif 'landingUrl' in metaData:
        metaData['fulltextUrl'] = metaData['landingUrl']
    if not isfile(filename):
        codecs.open(filename, 'w', encoding='utf8').write(u'\t'.join(metaHeaders) + '\n')
    maxCommon.appendTsvDict(filename, metaData, metaHeaders)
    row = []
    for h in metaHeaders:
        row.append(metaData.get(h, ''))

    dbFname = join(outDir, 'articles.db')
    con, cur = maxTables.openSqliteCreateTable(dbFname, 'articles', metaHeaders, idxFields=['pmid', 'pmcId', 'doi'], intFields=['pmid', 'articleId', 'pmcId'], primKey='pmid', retries=100)
    writeOk = False
    tryCount = 100
    logging.log(5, '%s' % row)
    while not writeOk and tryCount > 0:
        try:
            try:
                maxTables.insertSqliteRow(cur, con, 'articles', metaHeaders, row)
            except sqlite3.IntegrityError:
                logging.warn('Already present in meta info db')

            writeOk = True
        except sqlite3.OperationalError:
            logging.info('sqlite db is locked, waiting for 60 secs')
            time.sleep(60)
            tryCount -= 1

    if not writeOk:
        raise Exception('Could not write to sqlite db')


def detFileExt(page):
    """ determine file extension based on either mimetype or url file extension """
    fileExt = pubConf.MIMEMAP.get(page['mimeType'], None)
    linkUrl = page['url']
    if fileExt != None:
        logging.debug('Determined filetype as %s based on mime type' % fileExt)
    else:
        urlPath = urlparse.urlparse(linkUrl)[2]
        fileExt = os.path.splitext(urlPath)[1]
        logging.debug('Using extension %s based on URL %s' % (fileExt, linkUrl))
    fileExt = fileExt.strip('.')
    return fileExt


def containsAnyWord(text, ignWords):
    for word in ignWords:
        if word in text:
            logging.debug('blacklist word %s found' % word)
            return True

    return False


def blacklistIssnYear(outDir, issnYear, journal):
    """ append a line to issnStatus.tab file in outDir """
    issn, year = issnYear
    fname = join(outDir, ISSNSTATNAME)
    if isfile(fname):
        outFh = open(fname, 'a')
        colCount = len(open(fname).next().split('\t'))
    else:
        outFh = open(fname, 'w')
        outFh.write('issn\tyear\tjournal\n')
        colCount = 3
    if colCount == 3:
        outFh.write('%s\t%s\t%s\n' % (issn, year, journal))
    else:
        outFh.write('%s\t%s\n' % (issn, year))
    outFh.close()


def writeDocIdStatus(outDir, pmid, msg, longMsg, details=None):
    """ append a line to doc status file in outDir """
    fname = join(outDir, PMIDSTATNAME)
    if isfile(fname):
        outFh = open(fname, 'a')
    else:
        outFh = open(fname, 'w')
    row = [str(pmid), unidecode.unidecode(msg)]
    if longMsg is not None:
        row.append(unidecode.unidecode(longMsg))
    if details is not None:
        row.append(unidecode.unidecode(details))
    row = [ x.encode('utf8') for x in row ]
    logging.info('Document status (pmid, logType, desc, details): %s' % ','.join(row))
    outFh.write('\t'.join(row))
    outFh.write('\n')
    return


def removeLocks():
    """ remove all lock files """
    global lockFnames
    for lockFname in lockFnames:
        if isfile(lockFname):
            logging.debug('Removing lockfile %s' % lockFname)
            os.remove(lockFname)

    lockFnames = []


def getLockFname(outDir):
    return join(outDir, PUBLOCKFNAME)


def checkCreateLock(outDir):
    """ creates lockfile, squeaks if exists, register exit handler to delete """
    lockFname = join(outDir, PUBLOCKFNAME)
    if isfile(lockFname):
        raise Exception("File %s exists - it seems that a crawl is running now.         If you're sure that this is not the case, remove the lockfile and retry again" % lockFname)
    logging.debug('Creating lockfile %s' % lockFname)
    open(lockFname, 'w')
    lockFnames.append(lockFname)
    atexit.register(removeLocks)


def parseDocIds(outDir):
    """ parse docIds.txt in outDir and return as list, ignore duplicates """
    docIdFname1 = join(outDir, 'docIds.txt')
    docIdsFname = docIdFname1
    if not isfile(docIdFname1):
        docIdFname2 = join(outDir, 'pmids.txt')
        if not isfile(docIdFname2):
            logging.info('neither %s nor %s exist, skipping dir %s' % (docIdFname1, docIdFname2, outDir))
            return None
        docIdsFname = docIdFname2
    logging.debug('Parsing %s' % docIdsFname)
    logging.debug('Parsing document IDs / PMIDs %s' % docIdsFname)
    pmids = []
    seen = set()
    for line in open(docIdsFname):
        if line.startswith('#'):
            continue
        pmid = line.strip().split('#')[0].strip()
        if pmid == '':
            continue
        if pmid in seen:
            continue
        pmids.append(pmid)
        seen.add(pmid)

    logging.debug('Found %d documentIds/PMIDS' % len(pmids))
    return pmids


def ignoreCtrlc(signum, frame):
    logging.info('Signal handler called with signal %s' % str(signum))


def printPaperData(paperData, artMeta):
    """ output summary info of paper data obtained, for testing """
    if paperData == None:
        logging.info('No data received from crawler')
        return
    else:
        printFileHash(paperData, artMeta)
        return


def isPdf(page):
    """ true if page is really a PDF file """
    return page['data'][:4] == '%PDF'


def checkIfPdf(pageDict, metaData):
    """ bail out if pageDict is not a PDF file """
    if isPdf(pageDict):
        pageDict['mimeType'] = 'application/pdf'
    if pageDict['mimeType'] != 'application/pdf':
        raise pubGetError('not a PDF, docId %s, title %s, url %s, mimeType %s' % (metaData['pmid'],
         metaData['title'],
         pageDict['url'],
         pageDict['mimeType']), 'invalidPdf', pageDict['url'])


def storeFilesNoZip(pmid, metaData, fulltextData, outDir):
    """ write files from dict (keys like main.html or main.pdf or s1.pdf, value is binary data) 
    to directory <outDir>/files
    """
    warnMsgs = []
    fileDir = join(outDir, 'files')
    if not isdir(fileDir):
        os.makedirs(fileDir)
    suppFnames = []
    suppUrls = []
    pdfFound = False
    for suffix, pageDict in fulltextData.iteritems():
        if suffix in ('status', 'crawlerName'):
            continue
        if suffix == 'landingPage':
            metaData['landingUrl'] = pageDict['url']
            continue
        filename = pmid + '.' + suffix
        warnMinSize = 5000
        if len(pageDict['data']) < warnMinSize:
            warnMsgs.append('%s is smaller than %d bytes' % (suffix, warnMinSize))
        if suffix == 'main.html':
            if '<html' in pageDict['data']:
                warnMsgs.append('main.html contains html tag')
            metaData['mainHtmlFile'] = filename
            metaData['mainHtmlUrl'] = pageDict['url']
        elif suffix == 'main.pdf':
            pdfFound = True
            checkIfPdf(pageDict, metaData)
            metaData['mainPdfFile'] = filename
            metaData['mainPdfUrl'] = pageDict['url']
        elif suffix.startswith('S'):
            suppFnames.append(filename)
            suppUrls.append(pageDict['url'])
        fileData = pageDict['data']
        filePath = join(fileDir, filename)
        logging.debug('Writing file %s' % filePath)
        fh = open(filePath, 'wb')
        fh.write(fileData)
        fh.close()

    if not pdfFound:
        warnMsgs.append('No PDF file')
    suppFnames = [ s.replace(',', '') for s in suppFnames ]
    suppFnames = [ s.replace('\t', '') for s in suppFnames ]
    suppUrls = [ s.replace(',', '') for s in suppUrls ]
    suppUrls = [ s.replace('\t', '') for s in suppUrls ]
    metaData['suppFiles'] = ','.join(suppFnames)
    metaData['suppUrls'] = ','.join(suppUrls)
    return (metaData, warnMsgs)


def printFileHash(fulltextData, artMeta):
    """ output a table with file extension and SHA1 of all files """
    crawlerName = fulltextData['crawlerName']
    for ext, page in fulltextData.iteritems():
        if ext in ('crawlerName', 'status'):
            continue
        if ext == 'main.pdf':
            checkIfPdf(page, artMeta)
        sha1 = hashlib.sha1(page['data']).hexdigest()
        row = [crawlerName,
         ext,
         page['url'],
         str(len(page['data'])),
         sha1]
        print '\t'.join(row)


def writePaperData(docId, pubmedMeta, fulltextData, outDir):
    """ write all paper data to status and fulltext output files in outDir """
    if TEST_OUTPUT:
        printFileHash(fulltextData, pubmedMeta)
        return
    pubmedMeta, warnMsgs = storeFilesNoZip(docId, pubmedMeta, fulltextData, outDir)
    oldHandler = signal.signal(signal.SIGINT, ignoreCtrlc)
    writeMeta(outDir, pubmedMeta, fulltextData)
    addStatus = ''
    if 'status' in fulltextData:
        addStatus = fulltextData['status']
    crawlerName = fulltextData['crawlerName']
    docIdStatus = 'OK\t%s\t%s %s, %d files\t%s' % (crawlerName,
     pubmedMeta['journal'],
     pubmedMeta['year'],
     len(fulltextData),
     addStatus)
    writeDocIdStatus(outDir, docId, docIdStatus, ';'.join(warnMsgs))
    signal.signal(signal.SIGINT, oldHandler)


def parseIdStatus(fname):
    """ parse crawling status file, return as dict status -> count """
    res = {}
    if not os.path.isfile(fname):
        logging.info('%s does not exist' % fname)
        res['OK'] = []
        return res
    for line in open(fname):
        pmid, status = line.strip().split()[:2]
        status = status.split('\\')[0]
        status = status.strip('"')
        status = status.strip("'")
        res.setdefault(status, [])
        pmid = int(pmid)
        res[status].append(pmid)

    return res


crawlPubIds = {'LWW lww': 'lww',
 'WILEY Wiley': 'wiley',
 'NLM Nature Publishing Group': 'npg',
 'NLM American College of Chest Physicians': 'chest',
 'NLM American Association for Cancer Research': 'aacr',
 'NLM Mary Ann Liebert': 'mal',
 'NLM Oxford University Press': 'oup',
 'NLM Future Science': 'futureScience',
 'NLM National Academy of Sciences': 'pnas',
 'NLM American Association of Immunologists': 'aai',
 'NLM Karger': 'karger',
 'HIGHWIRE Rockefeller University Press': 'rupress',
 'HIGHWIRE American Society for Microbiology': 'asm',
 'HIGHWIRE Cold Spring Harbor Laboratory': 'cshlp',
 'HIGHWIRE The American Society for Pharmacology and Experimental Therapeutics': 'aspet',
 'HIGHWIRE American Society for Biochemistry and Molecular Biology': 'asbmb',
 'HIGHWIRE Federation of American Societies for Experimental Biology': 'faseb',
 'HIGHWIRE Society for Leukocyte Biology': 'slb',
 'HIGHWIRE The Company of Biologists': 'cob',
 'HIGHWIRE Genetics Society of America': 'genetics',
 'HIGHWIRE Society for General Microbiology': 'sgm',
 'NLM Informa Healthcare': 'informa'}

def writeReport(baseDir, htmlFname):
    """ parse pmids.txt and pmidStatus.tab and write a html report to htmlFname """
    h = htmlPrint.htmlWriter(htmlFname)
    h.head('Genocoding crawler status', stylesheet='bootstrap/css/bootstrap.css')
    h.startBody('Crawler status as of %s' % time.asctime())
    publDesc = {}
    for key, value in crawlPubIds.iteritems():
        publDesc[value] = key

    totalPmidCount = 0
    totalOkCount = 0
    totalDownCount = 0
    for name in os.listdir(baseDir):
        dirName = join(baseDir, name)
        if not isdir(dirName) or name.startswith('_'):
            continue
        logging.info('processing dir %s' % name)
        pmidFile = join(dirName, 'pmids.txt')
        if not isfile(pmidFile):
            continue
        pmidCount = len(open(pmidFile).readlines())
        issnCount = len(open(join(dirName, 'issns.tab')).readlines())
        statusPmids = parseIdStatus(join(dirName, 'pmidStatus.tab'))
        publisher = basename(dirName)
        isActive = isfile(join(dirName, PUBLOCKFNAME))
        totalPmidCount += pmidCount
        totalOkCount += len(statusPmids.get('OK', []))
        totalDownCount += len(statusPmids.values())
        if publisher in publDesc:
            h.h4('Publisher: %s (%s)' % (publDesc[publisher], publisher))
        else:
            h.h4('Publisher: %s (%s)' % (publisher, publisher))
        h.startUl()
        h.li('Crawler is currently running: %s' % isActive)
        h.li('Number of journals: %d' % issnCount)
        h.li('Total PMIDs scheduled: %d' % pmidCount)
        h.li('Crawler progress rate: %0.2f %%' % (100 * len(statusPmids.get('OK', '')) / float(pmidCount)))
        h.startUl()
        for status, pmidList in statusPmids.iteritems():
            exampleLinks = [ htmlPrint.pubmedLink(pmid) for pmid in pmidList[:10] ]
            exampleLinkStr = '&nbsp;&nbsp; (examples: %s, ...)' % ','.join(exampleLinks)
            h.li('Status %s: %d %s' % (status, len(pmidList), exampleLinkStr))

        h.endUl()
        h.endUl()

    h.h4('Total PMIDs scheduled to download: %d' % totalPmidCount)
    h.h4('Overall PMID download tried: %d' % totalDownCount)
    h.h4('Overall PMIDs downloaded successfully: %d' % totalOkCount)
    h.endHtml()


def getIssn(artMeta):
    """ get the eIssn or the pIssn, prefer eIssn """
    issn = artMeta['eIssn']
    if issn == '':
        issn = artMeta['printIssn']
    return issn


def getIssnYear(artMeta):
    """ return a tuple (issn, year). fallback to journal name if we have no ISSN. """
    if artMeta == None:
        return
    else:
        issn = getIssn(artMeta)
        if issn == '':
            issn = artMeta['journal']
        if issn == '':
            return ('noJournal', artMeta['year'])
        issnYear = (issn, artMeta['year'])
        return issnYear


def checkIssnErrorCounts(pubmedMeta, ignoreIssns, outDir):
    """ raise an exception if the ISSN of the article has had too many errors 
    in this run (stored in the global issnYearErrorCounts or a previous run (stored in ignoreIssns).
    """
    global issnYearErrorCounts
    issnYear = getIssnYear(pubmedMeta)
    if issnYearErrorCounts[issnYear] > MAXISSNERRORCOUNT:
        blacklistIssnYear(outDir, issnYear, pubmedMeta['journal'])
        raise pubGetError('during this run, too many errors for ISSN %s and year %s' % issnYear, 'issnYearErrorExceed-new', str(issnYear))
    if issnYear in ignoreIssns:
        raise pubGetError('a previous run disabled this issn+year', 'issnYearErrorExceed-old', '%s %s' % issnYear)


def resolveDoi(doi):
    """ resolve a DOI to the final target url or None on error
    #>>> resolveDoi("10.1073/pnas.1121051109")
    >>> logging.warn("doi test")
    >>> resolveDoi("10.1111/j.1440-1754.2010.01952.x")
    """
    logging.debug('Resolving DOI %s' % doi)
    doiUrl = 'https://doi.org/' + urllib.quote(doi.encode('utf8'))
    page = httpGetDelay(doiUrl)
    trgUrl = page['url']
    logging.debug('DOI %s redirects to %s' % (doi, trgUrl))
    return trgUrl


def findCrawlers_article(artMeta):
    """
    return those crawlers that return True for canDo_article(artMeta)
    """
    global allCrawlers
    crawlers = []
    for c in allCrawlers:
        if c.canDo_article(artMeta):
            logging.log(5, 'Based on meta data: Crawler %s is OK to crawl article %s' % (c.name, artMeta['title']))
            crawlers.append(c)

    return crawlers


def findCrawlers_url(landingUrl):
    """
    return the crawlers that are OK with crawling a URL
    """
    crawlers = []
    for c in allCrawlers:
        if c.canDo_url(landingUrl):
            logging.log(5, 'Based on URL: Crawler %s is OK to crawl url %s' % (c.name, landingUrl))
            crawlers.append(c)

    return crawlers


class Crawler():
    """
    the base class of all article crawlers. 
    Some of these are only for certain journals or publishers.
    Others are more generic.
    """
    name = 'empty'

    def canDo_article(self, artMeta):
        """ some crawlers can decide based on ISSN or other meta data fields like DOI 
        Returns True/False
        """
        return False

    def canDo_url(self, artMeta):
        """ some crawlers can only decide if they apply based on the URL, returns True/False """
        return False

    def makeLandingUrl(self, artMeta):
        """ try to avoid DOI or NCBI queries by building the URL to the paper from the meta data 
        Especially useful for Highwire, which has a very slow DOI resolver.
        Returns a string, the URL.
        """
        return None

    def crawl(self, url):
        """ now get the paper, return a paperData dict with 'main.pdf', 'main.html', "S1.pdf" etc 
        """
        return None


def parseDirectories(srcDirs):
    """
    iterates over all directories and collects data from
    docIds.txt, issns.tab, crawler.txt, pmidStatus.tab and issnStatus.tab
    
    return a two-tuple:
    a list of (docId, outDir) from all outDirs
    a set of issns to skip 
    """
    allDocIds = []
    ignoreIssns = []
    seenIds = set()
    for srcDir in srcDirs:
        if not isdir(srcDir):
            continue
        if isfile(getLockFname(srcDir)):
            logging.warn('%s exists. Looks like a crawl is going on in %s. Skipping.' % (getLockFname(srcDir), srcDir))
            continue
        dirDocIds = parseDocIds(srcDir)
        if dirDocIds == None:
            logging.info('Found no document IDs in directory %s' % srcDir)
            continue
        doneIds = set(parseDocIdStatus(srcDir))
        count = 0
        for docId in dirDocIds:
            if docId in seenIds or docId in doneIds:
                continue
            seenIds.add(docId)
            allDocIds.append((docId, srcDir))
            count += 1

        ignoreIssns.extend(parseIssnStatus(srcDir))
        logging.info('Directory %s: %d docIds to crawl' % (srcDir, count))

    logging.info('Found %d docIds to crawl, %d ISSNs to ignore in %d directories' % (len(allDocIds), len(ignoreIssns), len(srcDirs)))
    return (allDocIds, ignoreIssns)


def findLinksByText(page, searchRe):
    """ parse html page and return URLs in links with matches to given compiled re pattern"""
    urls = []
    page = parseHtmlLinks(page)
    for linkUrl, linkText in page['links'].iteritems():
        dbgStr = 'Checking linkText %s (url %s) against %s' % (repr(unidecode.unidecode(linkText)), linkUrl, searchRe.pattern)
        logging.log(5, dbgStr)
        if searchRe.match(linkText):
            urls.append(linkUrl)
            logging.debug('Found link: %s -> %s' % (linkText, linkUrl))

    logging.debug('Found links with %s in label: %s' % (repr(searchRe.pattern), urls))
    return urls


def findLinksWithUrlPart(page, searchText, canBeOffsite=False):
    """ parse html page and return URLs in links that contain some text in the href attribute
    This is pretty fast, as it uses the already extracted links.
    """
    if page == None:
        return []
    else:
        urls = []
        page = parseHtmlLinks(page, canBeOffsite=canBeOffsite)
        for linkUrl, linkText in page['links'].iteritems():
            dbgStr = "Checking link Url '%s', url %s, against %s" % (unidecode.unidecode(linkText), linkUrl, searchText)
            logging.log(5, dbgStr)
            if searchText in linkUrl:
                urls.append(linkUrl)
                logging.debug(u'Found link: %s -> %s' % (repr(linkText.decode('utf8')), repr(linkUrl.decode('utf8'))))

        if len(urls) != 0:
            logging.debug('Found links with %s in URL: %s' % (repr(searchText), urls))
        else:
            logging.debug('Found no links with %s in URL' % searchText)
        return urls


def findLinksWithUrlRe(page, searchRe):
    """ Find links where the target URL matches a regular expression object
    This is pretty fast, as it uses the already extracted links.
    """
    urls = []
    page = parseHtmlLinks(page)
    for linkUrl, linkText in page['links'].iteritems():
        dbgStr = 'Checking link: %s (%s), against %s' % (linkUrl, unidecode.unidecode(linkText), searchRe.pattern)
        logging.log(5, dbgStr)
        if searchRe.match(linkUrl):
            urls.append(linkUrl)
            logging.debug(u'Found link: %s -> %s' % (repr(linkText.decode('utf8')), repr(linkUrl.decode('utf8'))))

    if len(urls) != 0:
        logging.debug('Found links with %s in URL: %s' % (repr(searchRe.pattern), urls))
    else:
        logging.debug('Found no links with %s in URL' % searchRe.pattern)
    return urls


def downloadSuppFiles(urls, paperData, delayTime, httpGetFunc=httpGetDelay):
    suppIdx = 1
    for url in urls:
        suppFile = httpGetFunc(url, delayTime)
        fileExt = detFileExt(suppFile)
        if len(suppFile['data']) > SUPPFILEMAXSIZE:
            logging.warn('supp file %s is too big' % suppFile['url'])
            continue
        paperData['S' + str(suppIdx) + '.' + fileExt] = suppFile
        suppIdx += 1
        if suppIdx > SUPPFILEMAX:
            raise pubGetError('max suppl count reached', 'tooManySupplFiles', str(len(urls)))

    return paperData


def stripOutsideOfTags(htmlStr, startTag, endTag):
    """ retain only part between two lines that include keywords, include the marker lines themselves
    Only look at the first matches of the tags. Bail out if multiple matches found.
    Require at least 10 lines in between start and end
    """
    lines = htmlStr.splitlines()
    start, end = (0, 0)
    for i, line in enumerate(lines):
        if startTag in line:
            if start != 0:
                logging.debug('could not strip extra html, double start tag')
                return htmlStr
            start = i
        if endTag in line and end != 0:
            if end != 0:
                logging.debug('could not strip extra html, double end tag')
                return htmlStr
            end = i

    if start != 0 and end != 0 and end > start and end - start > 10 and end < len(lines):
        logging.log(5, 'stripping some extra html based on tags')
        return ''.join(lines[start:end + 1])
    else:
        logging.log(5, 'could not strip extra html based on tags')
        return htmlStr


publisherIssns = None
publisherUrls = None

def getScopusIssns(publisherName):
    """ return set of scopus ISSNs for a given publisher """
    journalFname = pubConf.journalTable
    if not isfile(journalFname):
        logging.warn('%s does not exist, cannot use ISSNs to assign crawler' % journalFname)
        return ({}, [])
    issns = set()
    for row in maxCommon.iterTsvRows(journalFname):
        if row.source != 'SCOPUS':
            continue
        if row.correctPublisher != publisherName:
            continue
        if row.pIssn != '':
            issns.add(row.pIssn.strip())
        if row.eIssn != '':
            issns.add(row.eIssn.strip())

    logging.debug('Read %d issns from %s' % (len(issns), journalFname))
    return issns


def getHosterIssns(publisherName):
    """
    get the ISSNs of a hoster from our global journal table
    """
    global publisherIssns
    global publisherUrls
    if publisherIssns is None:
        journalFname = pubConf.journalTable
        if not isfile(journalFname):
            logging.warn('%s does not exist, cannot ISSN-assign highwire crawler' % journalFname)
            return ({}, set([]))
        publisherIssns = defaultdict(dict)
        publisherUrls = defaultdict(set)
        logging.log(5, 'Parsing %s to get highwire ISSNs' % journalFname)
        for row in maxCommon.iterTsvRows(journalFname):
            if row.source in ('HIGHWIRE', 'WILEY'):
                hoster = row.source
                journalUrl = 'http://' + row.urls.strip().replace('http://', '')
                issn = row.pIssn.strip()
                eIssn = row.eIssn.strip()
                publisherIssns[hoster][issn] = journalUrl
                publisherIssns[hoster][eIssn] = journalUrl
                if journalUrl != '':
                    publisherUrls[hoster].add(journalUrl)

    return (publisherIssns[publisherName], publisherUrls[publisherName])


def pageContains(page, strList):
    """ check if page contains one of a list of strings """
    for text in strList:
        if text in page['data']:
            logging.log(5, 'Found string %s' % text)
            return True

    return False


def getMetaPdfUrl(page):
    """ given a downloaded web page, return the citation_pdf_url meta tag value """
    if 'metas' not in page:
        parseHtmlLinks(page)
    htmlMetas = page['metas']
    if 'citation_pdf_url' in htmlMetas:
        pdfUrl = htmlMetas['citation_pdf_url']
        logging.debug('Found link to PDF in meta tag citation_pdf_url: %s' % pdfUrl)
        if not pdfUrl.startswith('http://'):
            pdfUrl = urlparse.urljoin(page['url'], pdfUrl)
        return pdfUrl
    else:
        return None


def makeOpenUrl(baseUrl, artMeta):
    """ return the openUrl url to find the article """
    if artMeta['printIssn'] != '' and artMeta['vol'] != '' and artMeta['issue'] != '' and artMeta['page'] != '':
        query = '?genre=article&sid=genomeBot&issn=%(printIssn)s&volume=%(vol)s&issue=%(issue)s&spage=%(page)s' % artMeta
    elif artMeta['eIssn'] != '' and artMeta['vol'] != '' and artMeta['issue'] != '' and artMeta['page'] != '':
        query = '?genre=article&sid=genomeBot&issn=%(eIssn)s&volume=%(vol)s&issue=%(issue)s&spage=%(page)s' % artMeta
    else:
        logging.debug('Cannot make openUrl')
        return None
    url = baseUrl + query
    logging.debug('Got Open URL %s' % url)
    return url


def addSuppZipFiles(suppZipUrl, paperData, delayTime):
    """ add all files from zipfile to paper data dict """
    zipPage = httpGetDelay(suppZipUrl, delayTime, mustGet=True)
    zipFile = cStringIO.StringIO(zipPage['data'])
    try:
        zfp = zipfile.ZipFile(zipFile, 'r')
    except (zipfile.BadZipfile, zipfile.LargeZipFile) as e:
        logging.warn('Bad zipfile, url %s' % suppZipUrl)
        return paperData

    for suppIdx, fname in enumerate(zfp.namelist()):
        data = zfp.read(fname)
        page = {}
        page['url'] = suppZipUrl + '/' + fname
        page['mimeType'] = mimetypes.guess_type(fname)
        page['encoding'] = 'None'
        page['data'] = data
        if len(data) > SUPPFILEMAXSIZE:
            logging.warn('supp file %s is too big' % page['url'])
            continue
        if suppIdx > SUPPFILEMAX:
            logging.warn('too many suppl files')
            continue
        fileExt = splitext(fname)[1]
        paperData['S' + str(suppIdx + 1) + fileExt] = page


class PmcCrawler(Crawler):
    """
    a scraper for PMC
    """
    name = 'pmc'

    def canDo_article(self, artMeta):
        if 'pmcId' in artMeta and artMeta['pmcId'] != '':
            return True
        else:
            return False

    def canDo_url(self, url):
        return 'http://www.ncbi.nlm.nih.gov/pmc/' in url

    def makeLandingUrl(self, artMeta):
        return 'http://www.ncbi.nlm.nih.gov/pmc/articles/PMC' + artMeta['pmcId']

    def crawl(self, url):
        url = url.rstrip('/')
        delayTime = 5
        htmlPage = httpGetDelay(url, delayTime)
        waitText = 'This article has a delayed release (embargo) and will be available in PMC on'
        if waitText in htmlPage['data']:
            logging.warn('PMC embargo note found')
            return None
        else:
            paperData = OrderedDict()
            html = htmlPage['data'].replace('xmlns="http://www.w3.org/1999/xhtml"', '')
            root = etree.fromstring(html)
            mainContElList = root.findall(".//div[@id='maincontent']")
            if len(mainContElList) == 1:
                htmlPage['data'] = etree.tostring(mainContElList[0])
            paperData['main.html'] = htmlPage
            pdfUrl = url + '/pdf'
            pdfPage = httpGetDelay(pdfUrl, delayTime)
            if 'The web page address (URL) that you used may be incorrect' in pdfPage['data']:
                logging.debug('PMC article has no PDF')
            else:
                paperData['main.pdf'] = pdfPage
            suppUrls = findLinksByText(htmlPage, re.compile('Click here for additional data file.*'))
            paperData = downloadSuppFiles(suppUrls, paperData, delayTime)
            return paperData


class NpgCrawler(Crawler):
    """
    a crawler for all Nature Publishing Group journals
    """
    name = 'npg'
    issnList = ['0028-0836',
     '0036-8733',
     '1087-0156',
     '1465-7392',
     '1552-4450',
     '1755-4330',
     '2041-1723',
     '1061-4036',
     '1752-0894',
     '1529-2908',
     '1476-1122',
     '1078-8956',
     '1548-7091',
     '1748-3387',
     '1097-6256',
     '1749-4885',
     '1745-2473',
     '2055-0278',
     '1754-2189',
     '1545-9993',
     '1759-5002',
     '1759-4774',
     '1474-1776',
     '1759-5029',
     '1759-5045',
     '1471-0056',
     '1474-1733',
     '1740-1526',
     '1471-0072',
     '1759-5061',
     '1759-4758',
     '1759-4790',
     '1759-4812',
     '2056-3973',
     '2055-5008',
     '2055-1010',
     '0002-9270',
     '1671-4083',
     '0007-0610',
     '2054-7617',
     '0007-0920',
     '2044-5385',
     '2047-6396',
     '0268-3369',
     '2095-6231',
     '0929-1903',
     '1350-9047',
     '2041-4889',
     '1748-7838',
     '1672-7681',
     '2050-0068',
     '1462-0049',
     '1672-7681',
     '0954-3007',
     '1018-4813',
     '2092-6413',
     '0969-7128',
     '1466-4879',
     '1098-3600',
     '2052-7276',
     '0916-9636',
     '0818-9641',
     '0955-9930',
     '0307-0565',
     '1674-2818',
     '1751-7362',
     '0021-8820',
     '1559-0631',
     '1434-5161',
     '0950-9240',
     '0743-8346',
     '0085-2538',
     '0023-6837',
     '0887-6924',
     '2047-7538',
     '2055-7434',
     '0893-3952',
     '1359-4184',
     '1525-0016',
     '2329-0501',
     '2162-2531',
     '2372-7705',
     '1933-0219',
     '1884-4057',
     '2044-4052',
     '0950-9232',
     '2157-9024',
     '0031-3998',
     '0032-3896',
     '1365-7852',
     '2052-4463',
     '2045-2322',
     '1362-4393',
     '2158-3188']

    def canDo_article(self, artMeta):
        if '10.1038' in artMeta['doi']:
            return True
        if artMeta['printIssn'] in self.issnList:
            return True
        return False

    def canDo_url(self, url):
        if 'nature.com' in url:
            return True
        return False

    def _npgStripExtra(self, htmlStr):
        """ retain only part between first line with <article> and </article> """
        lines = htmlStr.splitlines()
        start, end = (0, 0)
        for i, line in enumerate(lines):
            if '<article>' in line and start != 0:
                start = i
            if '</article>' in line and end != 0:
                end = i

        if start != 0 and end != 0 and end > start and end - start > 10 and end < len(lines):
            logging.log(5, 'stripping some extra html')
            return ''.join(lines[start:end + 1])
        else:
            return htmlStr

    def makeLandingUrl(self, artMeta):
        if artMeta['doi'] != '':
            return 'http://www.nature.com/doifinder/' + artMeta['doi']

    def crawl(self, url):
        if 'status.nature.com' in url:
            logging.warn('Server outage at NPG, waiting for 5 minutes')
            time.sleep(300)
            pubGetError('NPG Server error page, waited 5 minutes', 'NPGErrorPage', url)
        paperData = OrderedDict()
        url = url.replace('/abs/', '/full/')
        delayTime = 5
        htmlPage = httpGetDelay(url, delayTime)
        if pageContains(htmlPage, ['make a payment', 'purchase this article']):
            return None
        else:
            html = htmlPage['data']
            htmlPage['data'] = self._npgStripExtra(html)
            paperData['main.html'] = htmlPage
            pdfUrl = url.replace('/full/', '/pdf/').replace('.html', '.pdf')
            pdfPage = httpGetDelay(pdfUrl, delayTime)
            paperData['main.pdf'] = pdfPage
            suppUrls = findLinksWithUrlPart(htmlPage, '/extref/')
            paperData = downloadSuppFiles(suppUrls, paperData, delayTime)
            return paperData


class ElsevierCrawler(Crawler):
    """ sciencedirect.com is Elsevier's hosting platform 
    This crawler is minimalistic, we use ConSyn to get Elsevier text at UCSC.
    """
    name = 'elsevier'

    def canDo_url(self, url):
        if 'sciencedirect.com' in url:
            return True
        else:
            return False

    def crawl(self, url):
        paperData = OrderedDict()
        delayTime = crawlDelays['elsevier']
        agent = 'Googlebot/2.1 (+http://www.googlebot.com/bot.html)'
        htmlPage = httpGetDelay(url, delayTime, userAgent=agent)
        if pageContains(htmlPage, ['Choose an option to locate/access this article:',
         'purchase this article',
         'Purchase PDF',
         'First page preview</h2>']):
            raise pubGetError('no License', 'noLicense')
        if pageContains(htmlPage, ['Sorry, the requested document is unavailable.']):
            raise pubGetError('document is not available', 'documentUnavail')
        pdfUrls = findLinksWithUrlPart(htmlPage, '-main.pdf')
        htmlExtractPart(htmlPage, 'div', {'div': 'centerInner'})
        paperData['main.html'] = htmlPage
        if len(pdfUrls) > 0:
            pdfPage = httpGetDelay(pdfUrls[0], delayTime)
            pdfPage['url'] = htmlPage['url']
            paperData['main.pdf'] = pdfPage
        else:
            logging.debug('Could not find PDF link')
        suppEls = htmlFindLinkUrls(htmlPage, {'class': 'MMCvLINK'})
        if len(suppEls) != 0:
            suppUrls = [ s['href'] for s in suppEls ]
            paperData = downloadSuppFiles(suppUrls, paperData, delayTime)
        return paperData


class HighwireCrawler(Crawler):
    """ crawler for old-style highwire files. cannot get suppl files out of the new-style pages  
    test with new drupal page: 17726441, 16818525
    sagepub with old highwire pub system: 19435752
    """
    name = 'highwire'
    manualHosts = set(['asm.org',
     'rupress.org',
     'jcb.org',
     'cshlp.org',
     'aspetjournals.org',
     'fasebj.org',
     'jleukbio.org',
     'oxfordjournals.org'])
    hostCache = {}
    highwireIssns = None

    def _highwireDelay(self, url):
        """ return current delay for highwire, depending on current time at east coast
            can be overriden in pubConf per host-keyword
        """
        hostname = urlparse.urlsplit(url)[1]
        for hostKey, delaySec in pubConf.highwireDelayOverride.iteritems():
            if hostKey in hostname:
                logging.debug('Overriding normal Highwire delay with %d secs as specified in conf' % delaySec)
                return delaySec

        os.environ['TZ'] = 'US/Eastern'
        if hasattr(time, 'tzset'):
            time.tzset()
        tm = time.localtime()
        if tm.tm_wday in (5, 6):
            delay = 5
        elif tm.tm_hour >= 9 and tm.tm_hour <= 17:
            delay = 60
        else:
            delay = 10
        logging.log(5, 'current highwire delay time is %d' % delay)
        return delay

    def _readIssns(self):
        """ parse the ISSNs that this crawler can do """
        if self.highwireIssns is None:
            self.highwireIssns, self.highwireHosts = getHosterIssns('HIGHWIRE')
        self.highwireHosts.update(self.manualHosts)
        return

    def canDo_article(self, artMeta):
        """ return true if ISSN is known to be hosted by highwire """
        self._readIssns()
        if artMeta['printIssn'] in self.highwireIssns:
            return True
        elif artMeta['eIssn'] in self.highwireIssns:
            return True
        else:
            return None

    def canDo_url(self, url):
        """return true if a hostname is hosted by highwire at stanford """
        hostname = urlparse.urlsplit(url)[1]
        for hostEnd in self.highwireHosts:
            if hostname.endswith(hostEnd):
                logging.log(5, 'url hostname %s ends with %s -> highwire' % (hostname, hostEnd))
                return True

        if hostname in self.hostCache:
            ipAddr = self.hostCache[hostname]
        else:
            logging.debug('Looking up IP for %s' % hostname)
            try:
                ipAddr = socket.gethostbyname(hostname)
                self.hostCache[hostname] = ipAddr
            except socket.gaierror:
                raise pubGetError('Illegal hostname %s in link' % hostname, 'invalidHostname', hostname)

        ipParts = ipAddr.split('.')
        ipParts = [ int(x) for x in ipParts ]
        result = ipParts[0] == 171 and ipParts[1] in range(64, 68)
        if result == True:
            logging.log(5, 'hostname %s is highwire host' % hostname)
        return result

    def makeLandingUrl(self, artMeta):
        """ given the article meta, construct a landing URL and check that it's valid """
        self._readIssns()
        issn = getIssn(artMeta)
        if issn in self.highwireIssns:
            baseUrl = self.highwireIssns[issn]
            delayTime = self._highwireDelay(baseUrl)
            vol = artMeta.get('vol', '')
            issue = artMeta.get('issue', '')
            page = artMeta.get('page', '')
            if (vol, issue, page) != ('', '', '') and ' ' not in vol + issue + page:
                url = '%s/content/%s/%s/%s.long' % (baseUrl,
                 vol,
                 issue,
                 page)
                page = httpGetDelay(url, delayTime)
                if page != None:
                    return url
            if 'pmid' in artMeta:
                url = '%s/cgi/pmidlookup?view=long&pmid=%s' % (baseUrl, artMeta['pmid'])
                page = httpGetDelay(url, delayTime)
                if page != None:
                    return url
        return

    def crawl(self, url):
        """ get main html, pdf and supplements for highwire """
        paperData = OrderedDict()
        if url.endswith('.short'):
            url = url.replace('.short', '.long')
        if not url.endswith('.long') and 'pmidlookup' not in url:
            url = url + '.long'
        delayTime = self._highwireDelay(url)
        htmlPage = httpGetDelay(url, delayTime)
        url = htmlPage['url']
        if 'install.php.' in url:
            raise pubGetError('Highwire invalid DOI', 'highwireInvalidUrl')
        isDrupal = False
        if htmlPage['mimeType'] != 'application/pdf' and not htmlPage['data'].startswith('%PDF'):
            aaasStr = 'The content you requested is not included in your institutional subscription'
            aacrStr = 'Purchase Short-Term Access'
            stopWords = [aaasStr, aacrStr]
            if pageContains(htmlPage, stopWords):
                raise pubGetError('no license for this article', 'noLicense')
            if pageContains(htmlPage, ['We are currently doing routine maintenance']):
                time.sleep(600)
                raise pubGetError('site is down, waited for 10 minutes', 'siteMaintenance')
            isDrupal = False
            if 'drupal.org' in htmlPage['data']:
                logging.debug('Drupal-Highwire detected')
                isDrupal = True
            paperData['main.html'] = htmlPage
        else:
            logging.warn('Got PDF page where html page was expected, no html available')

        if 'Transparent Process' in htmlPage['data']:
            reviewUrl = url.replace('.long', '') + '.reviewer-comments.pdf'
            logging.debug('Downloading review process file')
            reviewPage = httpGetDelay(reviewUrl, delayTime)
            paperData['review.pdf'] = reviewPage

        url = htmlPage['url']
        if '.long' in url:
            pdfUrl = url.replace('.long', '.full.pdf')
        else:
            pdfUrl = url + '.full.pdf'
        pdfPage = httpGetDelay(pdfUrl, delayTime)
        if not isPdf(pdfPage):
            raise pubGetError('predicted PDF page is not PDF. Is this really highwire?', 'HighwirePdfNotValid', pdfUrl)
        paperData['main.pdf'] = pdfPage
        if isDrupal:
            htmlPage['data'] = htmlExtractPart(htmlPage, 'div', {'class': 'article fulltext-view '})
        else:
            htmlPage['data'] = htmlExtractPart(htmlPage, 'div', {'id': 'content-block'})
        htmlPage['data'] = stripOutsideOfTags(htmlPage['data'], 'highwire-journal-article-marker-start', 'highwire-journal-article-marker-end')
        suppListUrl = url.replace('.long', '/suppl/DC1')
        suppListPage = httpGetDelay(suppListUrl, delayTime)
        suppUrls = findLinksWithUrlPart(suppListPage, '/content/suppl/')
        if len(suppUrls) == 0:
            suppUrls = findLinksWithUrlPart(suppListPage, 'supplementary-material.')
        paperData = downloadSuppFiles(suppUrls, paperData, delayTime)
        return paperData


class NejmCrawler(Crawler):
    """ the new england journal of medicine seems to have its own hosting platform. test:27355532, 3 suppl """
    name = 'nejm'

    def canDo_article(self, artMeta):
        if artMeta['printIssn'] == '0028-4793' or artMeta['eIssn'] == '1533-4406':
            return True
        if artMeta['doi'].startswith('10.1056'):
            return True
        return False

    def canDo_url(self, url):
        if 'nejm.org' in url:
            return True
        else:
            return False

    def makeLandingUrl(self, artMeta):
        if artMeta['doi'] != '':
            return 'http://www.nejm.org/doi/%s' % artMeta['doi']
        else:
            return None

    def crawl(self, url):
        paperData = OrderedDict()
        delayTime = crawlDelays['nejm']
        if 'doi' in url and 'doi/full' not in url:
            url = url.replace('/doi/', '/doi/full/')
        url = url.replace('/abs/', '/full/')
        htmlPage = httpGetDelay(url, delayTime)
        suppListUrls = findLinksWithUrlPart(htmlPage, '/showSupplements?')
        if len(suppListUrls) == 1:
            suppListPage = httpGetDelay(suppListUrls[0])
            suppUrls = findLinksWithUrlPart(suppListPage, '/suppl_file/')
            paperData = downloadSuppFiles(suppUrls, paperData, delayTime)
        html = htmlPage['data']
        bs = BeautifulSoup(html)
        mainCont = bs.find('div', id='content')
        if mainCont != None:
            htmlPage['data'] = str(mainCont)
        htmlPage['url'] = htmlPage['url'].replace('?np=y', '')
        paperData['main.html'] = htmlPage
        pdfUrl = url.replace('/full/', '/pdf/')
        assert pdfUrl != url
        pdfPage = httpGetDelay(pdfUrl)
        paperData['main.pdf'] = pdfPage
        return paperData


class WileyCrawler(Crawler):
    """
    for wileyonline.com, Wiley's hosting platform
    """
    name = 'wiley'
    issnList = None
    urlList = None

    def canDo_article(self, artMeta):
        if self.issnList == None:
            self.issnList, self.urlList = getHosterIssns('WILEY')
        if artMeta['printIssn'] in self.issnList or artMeta['eIssn'] in self.issnList:
            return True
        elif artMeta['doi'].startswith('10.1002') or artMeta['doi'].startswith('10.1111'):
            return True
        else:
            return

    def canDo_url(self, url):
        if 'onlinelibrary.wiley.com' in url:
            return True
        else:
            return False

    def makeLandingUrl(self, artMeta):
        """"""
        url = makeOpenUrl('http://onlinelibrary.wiley.com/resolve/openurl', artMeta)
        if url == None and artMeta['doi'] == '':
            newDoi = pubCrossRef.lookupDoi(artMeta)
            if newDoi == None:
                newDoi = ''
            artMeta['doi'] = newDoi
        if url == None and artMeta['doi'] != '':
            url = 'http://onlinelibrary.wiley.com/doi/%s/full' % artMeta['doi']
        return url

    def crawl(self, url):
        delayTime = crawlDelays['wiley']
        paperData = OrderedDict()
        absPage = httpGetDelay(url, delayTime)
        absUrl = absPage['url']
        mainUrl = absUrl.replace('/abstract', '/full')
        mainPage = httpGetDelay(mainUrl, delayTime)
        if 'You can purchase online access' in mainPage['data'] or 'Registered Users please login' in mainPage['data']:
            logging.info('No license for this article via Wiley')
            return
        elif 'temporarily unavailable' in mainPage['data']:
            logging.info('Article page not found')
            return
        else:
            absHtml = htmlExtractPart(mainPage, 'div', {'id': 'articleDesc'})
            artHtml = htmlExtractPart(mainPage, 'div', {'id': 'fulltext'})
            noStripMain = copy.copy(mainPage)
            if absHtml != None and artHtml != None:
                logging.debug('Stripped extra wiley html')
                mainHtml = absHtml + artHtml
                mainPage['data'] = mainHtml
            paperData['main.html'] = mainPage
            pdfUrl = absUrl.replace('/abstract', '/pdf').replace('/full', '/pdf')
            pdfPage = httpGetDelay(pdfUrl, delayTime)
            if not isPdf(pdfPage):
                parseHtmlLinks(pdfPage)
                if 'pdfDocument' in pdfPage['iframes']:
                    logging.debug('found framed PDF, requesting inline pdf')
                    pdfPage = httpGetDelay(pdfPage['iframes']['pdfDocument'], delayTime)
            if isPdf(pdfPage):
                paperData['main.pdf'] = pdfPage
            suppListUrls = findLinksWithUrlPart(noStripMain, '/suppinfo')
            if len(suppListUrls) == 0:
                logging.debug('No list to suppl file list page found')
                return paperData
            if len(suppListUrls) > 1:
                logging.debug('Too many supp links found')
                raise Exception('Too many suppl. links found in wiley paper')
            suppListPage = httpGetDelay(suppListUrls[0], delayTime)
            suppUrls = findLinksWithUrlPart(suppListPage, '/asset/supinfo/')
            if len(suppUrls) == 0:
                suppUrls = findLinksWithUrlPart(suppListPage, '_s.pdf')
            paperData = downloadSuppFiles(suppUrls, paperData, delayTime)
            return paperData


class SpringerCrawler(Crawler):
    """ crawler for springerlink. Not usually needed, see pubGetSpringer and pubConvSpringer. """
    name = 'springer'

    def canDo_article(self, artMeta):
        if artMeta['doi'].split('/')[0] in ('10.1007', '10.1023', '10.1134'):
            return True
        else:
            return None

    def canDo_url(self, url):
        if 'link.springer.com' in url:
            return True
        else:
            return False

    def crawl(self, url):
        paperData = OrderedDict()
        delayTime = crawlDelays['springer']
        absPage = httpGetDelay(url, delayTime)
        if pageContains(absPage, ['make a payment', 'purchase this article', 'Buy now']):
            raise pubGetError('no springer license', 'noSpringerLicense')
        fullUrl = url + '/fulltext.html'
        fullPage = httpGetDelay(fullUrl, delayTime)
        fullPage['data'] = htmlExtractPart(fullPage, 'div', {'class': 'FulltextWrapper'})
        paperData['main.html'] = fullPage
        pdfUrl = url.replace('/article/', '/content/pdf/') + '.pdf'
        pdfPage = httpGetDelay(pdfUrl, delayTime)
        paperData['main.pdf'] = pdfPage
        suppUrls = findLinksWithUrlPart(absPage, '/MediaObjects/')
        paperData = downloadSuppFiles(suppUrls, paperData, delayTime)
        return paperData


class LwwCrawler(Crawler):
    """ 
    Lippincott-Williams is Wolters-Kluwer's journal branch.
    crawler for the various wolters/kluwer related hosting websites.
    There seem to be two completely different systems.
    """
    name = 'lww'
    issnList = None

    def canDo_article(self, artMeta):
        if self.issnList == None:
            self.issnList, _ = getHosterIssns('LWW')
        if artMeta['printIssn'] in self.issnList or artMeta['eIssn'] in self.issnList:
            return True
        elif artMeta['doi'].startswith('10.1097'):
            return True
        else:
            return

    def canDo_url(self, url):
        if 'wkhealth.com' in url or 'lww.com' in url:
            return True
        else:
            return False

    def makeLandingUrl(self, artMeta):
        url = 'http://content.wkhealth.com/linkback/openurl?issn=%(printIssn)s&volume=%(vol)s&issue=%(issue)s&spage=%(page)s' % artMeta
        return url

    def crawl(self, url):
        paperData = OrderedDict()
        delayTime = crawlDelays['lww']
        if 'landingpage.htm' in url and '?' in url:
            logging.debug('Routing around ovid splash page')
            params = url.split('&')[1:]
            params = [ s for s in params if s != 'type' ]
            url = 'http://content.wkhealth.com/linkback/openurl?' + '&'.join(params)
        fullPage = httpGetDelay(url, delayTime)
        if fullPage == None:
            return
        else:
            if 'type=abstract' in fullPage['url']:
                url = fullPage['url'].replace('type=abstract', 'type=fulltext')
                logging.debug('Regetting page for fulltext with %s' % url)
                fullPage = httpGetDelay(url, delayTime)
            fullPage['data'] = htmlExtractPart(fullPage, 'div', {'id': 'ej-article-view'})
            paperData['main.html'] = fullPage
            pdfUrls = findLinksWithUrlPart(fullPage, 'pdfs.journals.lww.com', canBeOffsite=True)
            if len(pdfUrls) == 1:
                pdfPage = httpGetDelay(pdfUrls[0], delayTime)
                paperData['main.pdf'] = pdfPage
            suppUrls = findLinksWithUrlPart(fullPage, 'links.lww.com', canBeOffsite=True)
            paperData = downloadSuppFiles(suppUrls, paperData, delayTime)
            return paperData


class SilverchairCrawler(Crawler):
    """ Silverchair is an increasingly popular hoster """
    name = 'silverchair'

    def canDo_url(self, url):
        if 'article.aspx' in url or '/cgi/doi/' in url:
            return True
        else:
            return False

    def crawl(self, url):
        paperData = OrderedDict()
        delayTime = crawlDelays['silverchair']
        fullPage = httpGetDelay(url, delayTime)
        if pageContains(fullPage, ['Purchase a Subscription']):
            logging.info('No license')
            return
        elif fullPage == None:
            logging.debug('Got no page')
            return
        else:
            if 'First Page Preview' in fullPage['data']:
                logging.debug('No html view')
            elif 'The resource you are looking for might have been removed' in fullPage['data']:
                raise pubGetError('landing page is error page', 'errorPage')
            else:
                fullPage['data'] = htmlExtractPart(fullPage, 'div', {'class': 'left contentColumn eqColumn'})
                paperData['main.html'] = fullPage
            pdfUrls = htmlFindLinkUrls(fullPage, {'class': 'linkPDF'})
            if len(pdfUrls) == 1:
                pdfPage = httpGetDelay(pdfUrls[0], delayTime)
                paperData['main.pdf'] = pdfPage
            suppUrls = htmlFindLinkUrls(fullPage, {'class': 'supplementLink'})
            paperData = downloadSuppFiles(suppUrls, paperData, delayTime)
            return paperData


class TandfCrawler(Crawler):
    """ tandfonline.com is Taylor&Francis' hosting platform. Test: 26643588 """
    name = 'tandf'
    canDoIssns = None

    def canDo_article(self, artMeta):
        doi = artMeta['doi']
        if artMeta['doi'] != '' and (doi.startswith('10.1080/') or doi.startswith('10.3109/')):
            logging.log(5, 'TandF: DOI prefix OK')
            return True
        if self.canDoIssns == None:
            logging.debug('Reading T & F ISSNs')
            issnPath = join(pubConf.journalInfoDir, 'tandfIssns.txt')
            if not isfile(issnPath):
                logging.warn('Cannot find %s' % issnPath)
                self.canDoIssns = set()
            else:
                logging.debug('Reading %s' % issnPath)
                self.canDoIssns = set(open(issnPath).read().splitlines())
            self.canDoIssns.update(getScopusIssns('Informa'))
        if artMeta['printIssn'] != '' and artMeta['printIssn'] in self.canDoIssns:
            logging.log(5, 'TandF: ISSN match')
            return True
        elif artMeta['eIssn'] != '' and artMeta['eIssn'] in self.canDoIssns:
            logging.log(5, 'TandF: ISSN match')
            return True
        else:
            return

    def canDo_url(self, url):
        if 'tandfonline.com' in url:
            return True
        else:
            return False

    def makeLandingUrl(self, artMeta):
        url = None
        if artMeta['doi'] != '':
            url = 'http://www.tandfonline.com/doi/full/%s' % artMeta['doi']
        else:
            url = makeOpenUrl('http://www.tandfonline.com/openurl', artMeta)
        return url

    def crawl(self, url):
        paperData = OrderedDict()
        delayTime = crawlDelays['tandf']
        fullPage = httpGetDelay(url, delayTime)
        if '/abs/' in fullPage['url']:
            logging.debug('Navigating to full text page')
            url = fullPage['url'].replace('/abs/', '/full/')
            fullPage = httpGetDelay(url, delayTime)
        if fullPage == None:
            logging.debug('Got no page')
            return
        else:
            noAccTags = ['Access options</div>', 'Sorry, you do not have access to this article.']
            if pageContains(fullPage, noAccTags):
                logging.info('No license for this Taylor and Francis journal')
                raise pubGetError('No license', 'noLicense_TandF')
            fullPage['data'] = htmlExtractPart(fullPage, 'div', {'id': 'fulltextPanel'})
            paperData['main.html'] = fullPage
            pdfUrl = url.replace('/full/', '/pdf/')
            pdfPage = httpGetDelay(pdfUrl, delayTime)
            paperData['main.pdf'] = pdfPage
            suppUrl = url.replace('/full/', '/suppl/')
            supplPage = httpGetDelay(suppUrl, delayTime)
            suppPageUrls = htmlFindLinkUrls(supplPage)
            suppZipUrl = None
            for suppPageUrl in suppPageUrls:
                if suppPageUrl.endswith('.zip'):
                    suppZipUrl = suppPageUrl

            if suppZipUrl is not None:
                addSuppZipFiles(suppZipUrl, paperData, delayTime)
            return paperData


class KargerCrawler(Crawler):
    """ 
    Karger developed its own publishing platform. 
    It's protected by the Incapsula CDN bot detection.
    
    Test with karger-hosted supplements: 26347487
    """
    name = 'karger'
    issns = None
    session = None
    useSelenium = False

    def canDo_article(self, artMeta):
        if self.issns == None:
            self.issns, _ = getHosterIssns('KARGER')
        if artMeta['printIssn'] in self.issns or artMeta['eIssn'] in self.issns:
            return True
        else:
            return

    def canDo_url(self, url):
        if 'karger.com' in url:
            return True
        else:
            return False

    def makeLandingUrl(self, artMeta):
        openUrl = makeOpenUrl('http://www.karger.com/OpenUrl', artMeta)
        return openUrl

    def _httpGetKarger(self, url, delaySecs):
        """ work around annoying incapsula javascript """
        wait(delaySecs, 'karger.com')
        if self.useSelenium:
            page = httpGetSelenium(url, delaySecs)
            if 'Incapsula incident' in page['data']:
                raise pubGetError('Got blocked by Incapsula', 'incapsulaBlock')
            return page
        else:
            count = 0
            if self.session is None:
                if not requestsLoaded:
                    raise pubGetError("Karger.com requires the requests python module. Install it with 'pip install requests' and retry", 'noRequestsKarger')
                self.session = requests.Session()
                while count < 5:
                    try:
                        response = self.session.get(url)
                        response = crack(self.session, response)
                        break
                    except requests.exceptions.ConnectionError:
                        count += 1
                        logging.warn('Got connection error when trying to get Karger page, retrying...')

            else:
                while count < 5:
                    try:
                        response = self.session.get(url)
                        break
                    except requests.exceptions.ConnectionError:
                        count += 1
                        logging.warn('Got connection error when trying to get Karger page, retrying...')

            if count >= 5:
                raise pubGetError('Too many Karger connection errors', 'KargerConnErrors')
            page = {}
            page['data'] = response.content
            page['url'] = response.url
            page['mimeType'] = response.headers['content-type'].split(';')[0]
            if 'Incapsula incident' in page['data']:
                self.useSelenium = True
                page = httpGetSelenium(url, delaySecs)
            if 'Incapsula incident' in page['data']:
                raise pubGetError('Got blocked by Incapsula even with selenium', 'incapsulaBlockFirefox')
            return page

    def crawl(self, url):
        if 'karger.com' not in url:
            raise pubGetError('not a karger URL', 'notKarger')
        paperData = OrderedDict()
        delayTime = getDelaySecs('karger', None)
        url = url.replace('/Abstract/', '/FullText/')
        webCache.clear()
        fullPage = self._httpGetKarger(url, delayTime)
        noAccTags = ['wpurchase',
         'Unrestricted printing',
         'Purchase</strong>',
         'Request unsuccessful.']
        if pageContains(fullPage, noAccTags):
            logging.info('No license for this Karger journal')
            return
        else:
            fullPage['data'] = htmlExtractPart(fullPage, 'div', {'class': 'inhalt'})
            paperData['main.html'] = fullPage
            pdfUrl = fullPage['url'].replace('/FullText/', '/Fulltext/')
            pdfUrl = pdfUrl.replace('/Abstract/', '/Fulltext/').replace('/Fulltext/', '/Pdf/')
            pdfPage = self._httpGetKarger(pdfUrl, delayTime)
            if pdfPage['data'][:4] == '%PDF':
                paperData['main.pdf'] = pdfPage
            else:
                logging.warn('Could not get PDF from Karger')
            suppListUrls = findLinksWithUrlPart(fullPage, '/ProdukteDB/miscArchiv/')
            if len(suppListUrls) == 1:
                if suppListUrls[0].lower().endswith('.pdf'):
                    suppFile = self._httpGetKarger(suppListUrls[0], delayTime)
                    paperData['S1.pdf'] = suppFile
                else:
                    suppListPage = self._httpGetKarger(suppListUrls[0], delayTime)
                    suppUrls = findLinksWithUrlPart(suppListPage, '/ProdukteDB/miscArchiv')
                    if len(suppUrls) == 0:
                        logging.warn('Karger suppl page %s has no links on it' % ','.join(suppUrls))
                    else:
                        downloadSuppFiles(suppUrls, paperData, delayTime, httpGetFunc=self._httpGetKarger)
            elif len(suppListUrls) > 1:
                downloadSuppFiles(suppListUrls, paperData, delayTime, httpGetFunc=self._httpGetKarger)
            else:
                logging.debug('Karger: no suppl files found')
            suppListUrls = findLinksWithUrlPart(fullPage, 'figshare.com')
            assert len(suppListUrls) <= 1
            if len(suppListUrls) == 1:
                figShareId = suppListUrls[0].split('/')[-1]
                zipUrl = 'https://ndownloader.figshare.com/articles/%s/versions/1' % figShareId
                addSuppZipFiles(zipUrl, paperData, delayTime)
            return paperData

class ScihubCrawler(Crawler):
    """
    Can only get the PDF, nothing else.
    using a scihub scraper found on github. Will get blocked after a while
    by captchas. Not legal to use, except in Russia. Certainly not used at UCSC,
    just an experiment and as a sample how to write crawlers.
    Captchas seem to be from http://www.flogocloud.com/pricing/contact-files/captcha/words/words.txt
    The captcha URL is http://sci-hub.cc/captcha/securimage_show.php?<randomNumer>
    """
    name = 'scihub'
    scihub = None
    artMeta = None

    def canDo_article(self, artMeta):
        logging.debug('scihub crawler got meta data')
        self.artMeta = artMeta
        return True

    def crawl(self, url):
        if self.scihub == None:
            self.scihub = scihub.SciHub()
        if self.artMeta['doi'] != '':
            logging.info('Using SciHub API with DOI')
            data = self.scihub.fetch(self.artMeta['doi'])
        elif self.artMeta['pmid'] != '':
            logging.info('Using SciHub API with PMID')
            data = self.scihub.fetch(self.artMeta['pmid'])
        else:
            logging.info('Using SciHub API with URL')
            data = self.scihub.fetch(url)
        if 'pdf' not in data:
            logging.info('Cannot get paper from scihub: %s' % data['err'])
            return
        else:
            if 'captcha' in data['pdf']:
                raise pubGetError('got captcha page from scihub', 'scihubCaptcha')
            pdfPage = {}
            pdfPage['url'] = url
            pdfPage['mimeType'] = 'application/pdf'
            pdfPage['data'] = data['pdf']
            paperData = {}
            paperData['main.pdf'] = pdfPage
            return paperData

class GenericCrawler(Crawler):
    """
    Crawler that tries to get the files with rules. Not specific to any publisher.
    Uses a few rules to find links to the PDF.
    Will usually only the get main PDF, sometimes the supplement.
    
    Test: 
    - ACS: 25867541
    - RCS: 24577138 (with suppl.)
    - IEEE: 25861092
    - Jstage: 26567999, 26369595
    - yiigle: 24685044 (selenium)
    - scitation: 25096102
    - osapublishing: 24281500
    - koreascience: 26625779
    - thieme: 26397852, should work, uses meta url, but UCSC has no access
    - mary ann liebert: 26789706
    - IOP: 26020697 (requires requests module for cookies)
    - journals.cambridge.org: 22717054
    - aps.org: no way, has captcha 22463183
    - psycnet: no, we have no license, 25419911
    - ebsco: no, too much .js, 20136062
    - ingentaconnect/ingenta.com: no, requires onclick, 23975508
    - koreamed.org: 26907485
    - bmj: 23657193
    - impactjournals: 26431498
    - eurekaselect: no, links not valid? DOIs wrong? 20446905
    - spandidos: 19639195
    - zootaxa: 26250189
    - jstor: no, 16999195 redirects to splash page and click
    - mitpressjournals: 24479543
    - medicalletter.org: 22869291 (selenium. PDF download requires the cookies)
    - funpecrp.com: 25867385
    - degruyter: 25867385 (selenium)
    - wangfangdata.com.cn: no, requires login, 27095733
    - healio.com: 26821222 (selenium, actually incapsulate)
    - minervamedica: no, captcha, 26771917
    - thejns.org: 26828890
    - magonlinelibrary: don't no, UCSC has no license, 26926349
    - dovepress.com: 27330308
    - mdpi.com: 26867192
    - futuremedicine: no, UCSC has no license, 26638726
    - bioone.org: 27010308
    - iucr.org: 26894534 (meta tag)
    - humankinetics: no, 26218309, UCSC has no license
    - hogrefe: yes, 27167488, but UCSC has no license
    - annualreviews: 26768245
    - pieronline: no, UCSC has no license, 27306822
    - turkjournalgastroenterol: 27210792
    - painphysicianjournal: 20859316
    - iospress.nl: 17102353
    - aapPublications.org: 16818525
    - sciencesocieties.org: 20176825
    - endocrine press: 19628582 (now atypon, originally highwire)
    - ojs.kardiologiapolska.pl: 26832813 (viamedica hoster?)
    - jbjs.org: 21084575, this is highwire, but not on our Highwire ISSN list? -> generic
    - tjtes.org:16456754
    - spandidos: 27109139 (no access)
    - frontiersin: 27242737 (requires correct "referer" for PDF download)
    - cancerjournal.net: 26458637 (only html is free, PDF is not, so accept html only)
    - indianjcancer.com: 26458637 (two level page)
    - phmd.pl: 21918253
    - jsava.co.za: 18678190 (two level page, via meta refresh)
    
    """
    name = 'generic'
    useSelenium = False
    urlPatterns = ['.*/pdf/.*',
     '.*current/pdf\\?article.*',
     '.*/articlepdf/.*',
     '.*/_pdf.*',
     '.*mimeType=pdf.*',
     '.*viewmedia\\.cfm.*',
     '.*/PDFData/.*',
     '.*attachment\\.jspx\\?.*',
     '.*/stable/pdf/.*',
     '.*/doi/pdf/.*',
     '.*/pdfplus/.*',
     '.*:pdfeventlink.*',
     '.*getfile\\.php\\?fileID.*',
     '.*full-text\\.pdf$',
     '.*fulltxt.php.ICID.*',
     '.*/viewPDFInterstitial.*',
     '.*download_fulltext\\.asp.*',
     '.*\\.full\\.pdf$',
     '.*/TML-article.*',
     '.*/_pdf$',
     '.*type=2$',
     '.*/submission/.*\\.pdf$',
     '.*/article/download/.*']
    urlREs = [ re.compile(r) for r in urlPatterns ]

    def canDo_article(self, artMeta):
        return True

    def _httpGetDelay(self, url, waitSecs, mustGet=False, noFlash=False, useSelCookies=False, referer=None):
        """ generic http get request, uses selenium if needed """
        page = None
        if self.useSelenium:
            page = httpGetSelenium(url, waitSecs, mustGet=mustGet)
            time.sleep(5)
        else:
            cookies = None
            if useSelCookies:
                logging.debug('Importing cookies from selenium')
                all_cookies = browser.get_cookies()
                cookies = {}
                for s_cookie in all_cookies:
                    cookies[s_cookie['name']] = s_cookie['value']

            page = httpGetDelay(url, waitSecs, mustGet=mustGet, blockFlash=noFlash, cookies=cookies, referer=referer)
        return page

    def _findRedirect(self, page):
        """ search for URLs on page and reload the page """
        htmlRes = [('koreascience', re.compile("<script>location.href='(.+)'</script>"))]
        for domainTag, htmlRe in htmlRes:
            if domainTag not in page['url']:
                continue
            logging.debug('redirect: domain match')
            match = htmlRe.search(page['data'])
            if match != None:
                url = match.group(1)
                url = urlparse.urljoin(page['url'], url)
                logging.debug('redirect: found URL %s' % url)
                page = self._httpGetDelay(url, 1)
                page = parseHtmlLinks(page)

        return page

    def _findPdfLink(self, landPage):
        """ return first link to PDF on landing page """
        logging.debug('Looking for link to PDF on landing page')
        landPage = parseHtmlLinks(landPage)
        metaUrl = getMetaPdfUrl(landPage)
        if metaUrl is not None:
            isInvalidMeta = False
            ignoreMetaHosts = ['cambridge.org', 'degruyter.com', 'frontiersin.org']
            for ignoreHost in ignoreMetaHosts:
                if ignoreHost in metaUrl:
                    isInvalidMeta = True

            if not isInvalidMeta:
                return metaUrl
        for urlRe in self.urlREs:
            pdfUrls = findLinksWithUrlRe(landPage, urlRe)
            if len(pdfUrls) > 0:
                logging.debug('Found pattern %s in link' % urlRe.pattern)
                return pdfUrls[0]

        classNames = ['typePDF',
         'download-files-pdf action-link',
         'full_text_pdf',
         'pdf']
        for className in classNames:
            pdfUrls = htmlFindLinkUrls(landPage, {'class': className})
            if len(pdfUrls) > 0:
                logging.debug('Found className %s in link' % className)
                return pdfUrls[0]

        textTags = [re.compile('^Full Text \\(PDF\\)$'), re.compile('^.Full Text \\(PDF\\)$'), re.compile('^PDF/A \\([.0-9]+ .B\\)$')]
        for textRe in textTags:
            pdfUrls = findLinksByText(landPage, textRe)
            if len(pdfUrls) > 0:
                logging.debug('Found text pattern %s in link text' % textRe.pattern)
                return pdfUrls[0]

        return

    def _findSupplUrls(self, landPage):
        """ return list of supp data URLs """
        urlParts = ['/suppdata/']
        for urlPart in urlParts:
            suppUrls = findLinksWithUrlPart(landPage, urlPart)
            if len(suppUrls) > 0:
                return suppUrls

        return []

    def _wrapPdfUrl(self, url):
        """ wrap a url to a PDF into a paperdata dict and return it """
        pdfPage = httpGetDelay(url)
        paperData = {}
        paperData['main.pdf'] = pdfPage
        return paperData

    def _checkErrors(self, landPage):
        """ check page for errors or "no license" messages """
        noLicenseTags = ['Purchase a Subscription',
         'Purchase This Content',
         'to gain access to this content',
         'purchaseItem',
         'Purchase Full Text',
         'Purchase access',
         'Purchase PDF',
         'Pay Per Article',
         'Purchase this article.',
         'Online access to the content you have requested requires one of the following',
         'To view this item, select one of the options below',
         'PAY PER VIEW',
         'This article requires a subscription.',
         'leaf-pricing-buy-now',
         'To access this article, please choose from the options below',
         'Buy this article',
         'Your current credentials do not allow retrieval of the full text.',
         'Access to the content you have requested requires one of the following:',
         'Online access to the content you have requested requires one of the following']
        if pageContains(landPage, noLicenseTags):
            logging.info("generic crawler found 'No license' on " + landPage['url'])
            raise pubGetError('No License', 'noLicense', landPage['url'])
        errTags = ['This may be the result of a broken link',
         'please verify that the link is correct',
         'Sorry, we could not find the page you were looking for',
         'We are now performing maintenance',
         'DOI cannot be found in the DOI System']
        if pageContains(landPage, errTags):
            raise pubGetError('Error Message', 'errorMessage', landPage['url'])

    def crawl(self, url):
        if url.endswith('.pdf'):
            logging.debug('Landing URL is already a PDF')
            return self._wrapPdfUrl(url)
        else:
            paperData = OrderedDict()
            delayTime = crawlDelays['generic']
            useSeleniumHosts = ['yiigle',
             'medicalletter.org',
             'degruyter.com',
             'healio.com']
            self.useSelenium = False
            for tag in useSeleniumHosts:
                if tag in url:
                    self.useSelenium = True
                    break

            landPage = self._httpGetDelay(url, delayTime, mustGet=True)
            self._checkErrors(landPage)
            landPage = self._findRedirect(landPage)
            if isPdf(landPage):
                logging.debug('Landing page is already a PDF')
                return self._wrapPdfUrl(url)
            paperData['main.html'] = landPage
            pdfUrl = self._findPdfLink(landPage)
            if pdfUrl == None:
                logging.info('generic: could not find link to PDF')
                return
            logging.debug('Found link to PDF %s' % pdfUrl)
            self.useSelenium = False
            useSelCookies = False
            if 'seleniumDriver' in landPage:
                useSelCookies = True
            pdfPage = self._httpGetDelay(pdfUrl, delayTime, noFlash=True, useSelCookies=useSelCookies, referer=landPage['url'])
            self._checkErrors(pdfPage)
            if 'Verification Required' in pdfPage['data']:
                raise pubGetError('APScaptcha page prevents us from downloading', 'APScaptcha')
            requirePdf = True
            if 'cancerjournal.net' in landPage['url']:
                requirePdf = False
            if requirePdf and not isPdf(pdfPage):
                logging.debug('PDF may be in a frame, trying to resolve it to final PDF')
                pdfPage = parseHtmlLinks(pdfPage)
                pdfUrlRe = re.compile('.*temp.*\\.pdf$')
                pdfUrls = findLinksWithUrlRe(pdfPage, pdfUrlRe)
                if 'pdfDocument' in pdfPage['frames']:
                    logging.debug('found frame')
                    pdfUrl = pdfPage['frames']['pdfDocument']
                    pdfPage = self._httpGetDelay(pdfUrl, delayTime, noFlash=True)
                elif 'refresh' in pdfPage['metas']:
                    logging.debug('found refresh')
                    pdfUrl = pdfPage['metas']['refresh']
                    pdfPage = self._httpGetDelay(pdfUrl, delayTime, noFlash=True)
                elif len(pdfUrls) == 1:
                    logging.debug('found PDF link')
                    pdfPage = self._httpGetDelay(pdfUrls[0], delayTime)
                else:
                    logging.warn('PDF-link is not a PDF and not framed')
                    return
            if requirePdf and not isPdf(pdfPage):
                logging.warn('PDF-link is not a PDF')
                return
            if isPdf(pdfPage):
                paperData['main.pdf'] = pdfPage
            suppUrls = self._findSupplUrls(landPage)
            paperData = downloadSuppFiles(suppUrls, paperData, delayTime, httpGetFunc=self._httpGetDelay)
            return paperData


allCrawlers = [ElsevierCrawler(),
 NpgCrawler(),
 HighwireCrawler(),
 SpringerCrawler(),
 WileyCrawler(),
 SilverchairCrawler(),
 NejmCrawler(),
 LwwCrawler(),
 TandfCrawler(),
 KargerCrawler(),
 PmcCrawler(),
 GenericCrawler()]
allCrawlerNames = [ c.name for c in allCrawlers ]

def addCrawler(name):
    """ add an custom crawler to global list """
    global allCrawlerNames
    if name == 'scihub':
        allCrawlers.append(ScihubCrawler())
        allCrawlerNames = [ c.name for c in allCrawlers ]


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
    if srcDir != None:
        crawlerSpecFname = join(srcDir, 'crawler.txt')
        logging.debug('Checking %s to see if a crawler has been configured' % crawlerSpecFname)
        if isfile(crawlerSpecFname):
            crawlerName = open(crawlerSpecFname).read().strip()
            crawlers = [ c for c in allCrawlers if c.name == crawlerName ]
            cNames = []
            for c in crawlers:
                c.canDo_article(artMeta)
                cNames.append(c.name)

            logging.debug('Keeping only these crawlers: %s' % ','.join(cNames))
            return (crawlers, None)
    okCrawlers = findCrawlers_article(artMeta)
    landingUrl = None
    crawlerNames = [ c.name for c in okCrawlers ]
    customCrawlers = set(crawlerNames) - set(['pmc', 'generic'])
    if len(customCrawlers) == 0:
        logging.debug('No custom crawler accepted paper based on meta data, getting landing URL')
        landingUrl = getLandingUrlSearchEngine(artMeta)
        okCrawlers.extend(findCrawlers_url(landingUrl))
    if len(okCrawlers) == 0:
        logging.info('No crawler found on either article metadata or URL.')
        return ([], landingUrl)
    else:
        okCrawlers = sortCrawlers(okCrawlers)
        logging.debug('List of crawlers for this document, by priority: %s' % [ c.name for c in okCrawlers ])
        return (okCrawlers, landingUrl)


def crawlOneDoc(artMeta, srcDir, forceCrawlers=None):
    """
    return all data from a paper given the article meta data
    
    forceCrawlers is a list of crawlers that have to be used, e.g. ["npg", "pmc"]
    """
    if forceCrawlers == None:
        crawlers, landingUrl = selectCrawlers(artMeta, srcDir)
    else:
        logging.debug('Crawlers were fixed externally: %s' % ','.join(forceCrawlers))
        cByName = {}
        for c in allCrawlers:
            cByName[c.name] = c

        crawlers = []
        for fc in forceCrawlers:
            crawlers.append(cByName[fc])

        landingUrl = None
    if len(crawlers) == 0:
        errMsg = 'no crawler for article %s at %s' % (artMeta['title'], landingUrl)
        raise pubGetError(errMsg, 'noCrawler', landingUrl)
    artMeta['page'] = artMeta['page'].split('-')[0]
    if landingUrl is not None:
        artMeta['landingUrl'] = landingUrl
    lastException = None
    for crawler in crawlers:
        logging.info('Trying crawler %s' % crawler.name)
        crawler.canDo_article(artMeta)
        url = crawler.makeLandingUrl(artMeta)
        if url == None:
            if landingUrl != None:
                url = landingUrl
            else:
                url = getLandingUrlSearchEngine(artMeta)
        logging.info('Crawling base URL %s' % str(url))
        paperData = None
        try:
            paperData = crawler.crawl(url)
        except pubGetError as ex:
            lastException = ex

        if paperData is None:
            if lastException is not None:
                logging.warn('Crawler failed, Error: %s, %s, %s' % (lastException.logMsg, lastException.longMsg, lastException.detailMsg))
            else:
                logging.warn('Crawler failed')
            continue
        paperData['crawlerName'] = crawler.name
        return paperData

    logging.warn('No crawler was able to handle the paper, giving up')
    if lastException is None:
        raise pubGetError('No crawler was able to handle the paper', 'noCrawlerSuccess', landingUrl)
    else:
        raise lastException
    return


def getArticleMeta(docId):
    """ get pubmed article info from local db or ncbi webservice. return as dict. """
    artMeta = None
    haveMedline = pubConf.mayResolveTextDir('medline')
    if haveMedline and not SKIPLOCALMEDLINE:
        artMeta = readLocalMedline(docId)
    if artMeta == None:
        artMeta = downloadPubmedMeta(docId)
    return artMeta


def crawlDocuments(docIds, skipIssns):
    """
    run crawler on a list of (paperId, sourceDir) tuples
    """
    rootLog = logging.getLogger('')
    successCount = 0
    consecErrorCount = 0
    fileLogHandler = None
    for i, docIdTuple in enumerate(docIds):
        docId, srcDir = docIdTuple
        removeLocks()
        checkCreateLock(srcDir)
        if fileLogHandler is not None:
            rootLog.handlers.remove(fileLogHandler)
        fileLogHandler = pubGeneric.logToFile(join(srcDir, 'crawler.log'))
        todoCount = len(docIds) - i
        logging.info('--- Crawling document with ID %s, dir %s (%d IDs left)' % (docId, srcDir, todoCount))
        webCache.clear()
        try:
            artMeta = getArticleMeta(docId)
        except pubGetError:
            writeDocIdStatus(srcDir, docId, 'no meta', '')
            continue

        logging.info('Got Metadata: %s, %s, %s' % (artMeta['journal'], artMeta['year'], artMeta['title']))
        try:
            checkIssnErrorCounts(artMeta, skipIssns, srcDir)
            paperData = crawlOneDoc(artMeta, srcDir)
            writePaperData(docId, artMeta, paperData, srcDir)
            consecErrorCount = 0
            successCount += 1
        except pubGetError as e:
            consecErrorCount += 1
            docId = artMeta['pmid']
            writeDocIdStatus(srcDir, docId, e.logMsg, e.longMsg, e.detailMsg)
            issnYear = getIssnYear(artMeta)
            issnYearErrorCounts[issnYear] += 1
            if e.logMsg not in ('noOutlinkOrDoi', 'unknownHost', 'noLicense'):
                waitSec = ERRWAIT * consecErrorCount
                logging.debug('Sleeping for %d secs after error' % waitSec)
                time.sleep(waitSec)
            if consecErrorCount > MAXCONSECERR:
                logging.error('Too many consecutive errors, stopping crawl')
                e.longMsg = 'Crawl stopped after too many consecutive errors / ' + e.longMsg
                raise
            if DO_PAUSE:
                raw_input('Press Enter to process next paper...')
        except:
            raise

    logging.info('Downloaded %d articles' % successCount)
    removeLocks()
    if fileLogHandler != None:
        rootLog.handlers.remove(fileLogHandler)
    return


if __name__ == '__main__':
    import doctest
    doctest.testmod()
# okay decompiling /tmp/pubCrawlLib.pyc
