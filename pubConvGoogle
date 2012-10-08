#!/usr/bin/env python

# required option, supplied by google when you register a search project
# Google also checks IP address
PROJECTID = "annotatepublicgenomedata-soe.ucsc.edu"

# maximum filesize to convert (binary)
MAXFILESIZE = 50000000
# maximum filesize to store (ASCII)
MAXASCIISIZE = 30000000

# how many chunks to split the index file in
CHUNKCOUNT=1000

# do not download any files from domains with a keyword in here
BLACKLIST = ["sciencedirect", "ncbi.nlm.nih.gov", "springerlink",\
    "pnas", "nature.com", "genome.cshlp.org", "jstor.org", "sciencemag", \
    "oxfordjournals", "blackwell", "wiley.com", "genecards", \
    "biomedcentral", "jbc.org", "ukpmc", "plosone.org", "www.plos"]

WGETOPTIONS = " --tries=3 --dns-timeout=10 --read-timeout=10 --ignore-length --user-agent='Googlebot/2.1 (compatible; GenomeBot0.1-crawling-for-DNA; +http://hgwdev.soe.ucsc.edu/~max)'"

import optparse, os, sys, urllib, urllib2, xml.dom.minidom, logging, urlparse, time, codecs, glob, tempfile, traceback
from os.path import *

# add <scriptDir>/lib/ to package search path
progFile = os.path.abspath(sys.argv[0])
progDir  = os.path.dirname(progFile)
pubToolsLibDir = os.path.join(progDir, "lib")
sys.path.insert(0, pubToolsLibDir)
import pubGeneric, pubStore, util, maxCommon, maxRun, pubConf
import robotexclusionrulesparser as rp

# === COMMAND LINE INTERFACE, OPTIONS AND HELP ===
parser = optparse.OptionParser("""usage: %prog [options] index|download targetDir ["search terms"] - search for hits with google and write to output file 

command "index":
    need to specify search terms
    generates index.tab by querying Google for random nucleotides

command "download":
    submits cluster jobs to download, no need to specify search terms again
    
""")

parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages") 
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="show more debug messages") 
parser.add_option("-s", "--startSeqLen", dest="startSeqLen", action="store", type="int", help="minimum sequence length to try, default %default", default=5) 
parser.add_option("-e", "--endSeqLen", dest="endSeqLen", action="store", type="int", help="maximum sequence length to try, default %default", default=5) 
parser.add_option("-f", "--seqFile", dest="seqFile", action="store", help="file with sequences to try, instead of generating DNA sequences exhaustively", default=None) 
parser.add_option("-l", "--maxDownload", dest="maxDownload", action="store", type="int", \
    help="stop downloading after x files", default=1000) 
(options, args) = parser.parse_args()

# --- PART COPIED FROM GOOGLE EXAMPLE ---
# Copyright 2007 Google Inc.
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
class Response(object):
  '''A wrapper around the XML of a search response'''

  def __init__(self, node):
    '''Construct a wrapper around an XML search response.

    Exposes the following properties:

      terms : the requested search terms
      size: the requested number of search results
      start: the requested start index (offset 0)
      first: the index (offset 1) of the first result in the response
      last: the index (offset 1) of the last result in the response
      total: the total number of search results
      results: a sequence of Result instances

    Args:
      node: An xml.dom.Node instance containing the search response
    '''
    # Parse the response for information about the request
    self.terms = GetText(node.getElementsByTagName('Q')[0])
    params = node.getElementsByTagName('PARAM')
    for param in params:
      name = param.getAttribute('name')
      if name == 'num':
        self.size = param.getAttribute('value')
      elif name == 'start':
        self.start = param.getAttribute('value')
    # Parse the response for metadata about the results
    elemList = node.getElementsByTagName('RES')
    if len(elemList)==0:
        logging.info("No result found")
        self.total=0
    else:
        res = elemList[0]
        self.total = GetText(res.getElementsByTagName('M')[0])
        self.first = res.getAttribute('SN')
        self.last = res.getAttribute('EN')
        self.results = []
        # Parse the individual results
        [self.results.append(Result(r)) for r in res.getElementsByTagName('R')]

  def __str__(self):
    '''Return a representation of this instance as a unicode string'''
    s = 'terms: %s\n' % self.terms
    s += 'size: %s\n' % self.size
    s += 'start: %s\n' % self.start
    s += 'first: %s\n' % self.first
    s += 'last: %s\n' % self.last
    s += 'total: %s\n' % self.total
    s += 'results: \n'
    for result in self.results:
      s += unicode(result)
    return s

class Result(object):
  '''A wrapper around the XML of an individual result'''

  def __init__(self, node):
    '''Construct a wrapper around an XML search result.

    Exposes the following properties:

      index: the index of the result (offset 1)
      url: the address of the page matching the request
      encoded_url: the url-encoded address of the page matching the request
      title: the title of the page matching the request, includes <b> tags
      title_no_bold: the title of the page matching the request, no <b> tags

    Args:
      node: An xml.dom.Node instance containing a search result
    '''
    self.index = node.getAttribute('N')
    self.url = GetText(node.getElementsByTagName('U')[0])
    self.encoded_url = GetText(node.getElementsByTagName('UE')[0])
    titles = node.getElementsByTagName('T')
    if len(titles)>0:
        self.title = GetText(titles[0])
        self.title_no_bold = GetText(node.getElementsByTagName('TNB')[0])
    else:
        self.title=""
        self.title_no_bold = ""
    #print self.url, self.title

  def __str__(self):
    '''Return a representation of this instance as a unicode string'''
    s = '  index: %s\n' % self.index
    s += '    url: %s\n' % self.url
    s += '    encoded_url: %s\n' % self.encoded_url
    s += '    title: %s\n' % self.title
    s += '    title_no_bold: %s\n' % self.title_no_bold
    return s

def parseWgetLog(logFile):
    " parse a wget logfile and return mimetype "
    #   Content-Type: text/html; charset=utf-8 
    lines = logFile.readlines()
    logging.log(5, "Wget logfile: %s" % " / ".join(lines))
    for l in lines:
        if l.strip().lower().startswith("content-type:"):
            logging.debug("wget mime type line: %s" % l.strip("\n"))
            mimeParts = l.strip("\n").split()
            if len(mimeParts)>1:
                mimeType = mimeParts[1]
                mimeType = mimeType.split(";")[0]
                return mimeType
    logging.warn("No mimetype found, returning text/html")
    return "text/html"

def wget(url, getMime=True):
    " download url with wget and return (content, mimeType) tuple "
    url = url.replace("'", "")
    tmpFile = tempfile.NamedTemporaryFile(dir = pubConf.getTempDir(), prefix="WgetGoogleCrawler", suffix=".data")
    cmd = "wget '%s' -O %s --server-response" % (url, tmpFile.name)
    cmd += WGETOPTIONS
    if getMime:
        logFile = tempfile.NamedTemporaryFile(dir = pubConf.getTempDir(), \
            prefix="WgetGoogleCrawler", suffix=".log")
        cmd += " -o %s " % logFile.name
    pubGeneric.runCommandTimeout(cmd, timeout=10)

    if getMime:
        mimeType = parseWgetLog(logFile)
    else:
        mimeType = None
    data = tmpFile.read()
    logging.debug("Downloaded raw data size: %d" % len(data))
    if len(data)==0:
        data = None
    return data, mimeType

def GetText(node):
  '''Extract the contents of a xml.dom.Nodelist as a string.

  Args:
    nodelist: An xml.dom.Node instance
  Returns:
    a string containing the contents of all node.TEXT_NODE instances
  '''
  text = []
  for child in node.childNodes:
    if child.nodeType == xml.dom.Node.TEXT_NODE:
      text.append(child.data)
  return ''.join(text)


def Search(id, size, start, terms):
  '''Perform a search and print the results to standard out.

  Args:
    id: the assigned service id
    size: the desired size of the search response ('small' or 'large')
    start: the index of the first search result
    terms: the terms to search for

  Returns:
    A Response instance representing the search results
  '''
  logging.debug("Running search for %s with start %d" % (terms, start))
  BASE_URL = 'http://research.google.com/university/search/service'
  values = {'clid': id, 'rsz': size, 'start': start, 'q': terms}
  url = '?'.join([BASE_URL, urllib.urlencode(values)])
  logging.debug("Getting %s" % url)
  request = urllib2.Request(url)
  response = urllib2.urlopen(request)
  #print response.read()
  document = xml.dom.minidom.parse(response)
  return Response(document)
# END PART COPIED FROM GOOGLE

class WgetDownloader:
    def __init__(self):
        self.robotParsers = {} # cache for parsers: url -> robotParser
        pass

    def _blacklisted(self, url):
        " True if URL is not blacklisted"
        for blackListWord in BLACKLIST:
            if blackListWord in url:
                logging.debug("Ignoring URL, contains blacklist word %s" % blackListWord)
                return True
        logging.debug("URL is not blacklisted")
        return False

    def _robotOk(self, url):
        " True if url is OK according to its robots.txt"
        urlParts = urlparse.urlparse(url)
        if urlParts.scheme!="http":
            logging.debug("url is not http, skipping robots parsing")
            return True

        baseUrl = "http://"+urlParts.netloc
        if not baseUrl in self.robotParsers:
            rerp = rp.RobotExclusionRulesParser()
            robUrl = urlparse.urljoin(baseUrl, "robots.txt")
            logging.debug("Loading robots.txt from %s" % robUrl)
            robContent, dummy = wget(robUrl, getMime=False)
            if robContent!=None:
                rerp.parse(robContent)
                self.robotParsers[baseUrl] = rerp
            else:
                logging.debug("Could not load robots.txt, allowing download")
                self.robotParsers[baseUrl] = None
                return True
        else:
            rerp = self.robotParsers[baseUrl]

        if rerp!=None:
            isOk = rerp.is_allowed("*", url)
            logging.debug("robots.txt allowed: %s" % str(isOk))
            return isOk
        else:
            logging.debug("Cached robots.txt is None, allowing download")
            return True


    def get(self, url):
        if not self._blacklisted(url) and self._robotOk(url):
            return wget(url)
        else:
            return None, None

class PythonDownloader:
    " NOT USED  TOO MANY CRASHES / TIMEOUTS "
    def __init__(self):
        self.robotParsers = {} # cache for parsers: url -> robotParser

    def _robotOk(self, url):
        " True if url is OK according to its robots.txt"
        urlParts = urlparse.urlparse(url)
        if urlParts.scheme!="http":
            logging.debug("url is not http, skipping")
            return True

        baseUrl = "http://"+urlParts.netloc
        if not baseUrl in self.robotParsers:
            rerp = rp.RobotExclusionRulesParser()
            robUrl = urlparse.urljoin(baseUrl, "robots.txt")
            logging.debug("Loading robots.txt from %s" % robUrl)
            try:
                rerp.fetch(robUrl)
            except urllib2.URLError:
                logging.info("Unusual error while opening robots.txt, skipping")
                return False # better stay away from hosts that trigger strange errors
            self.robotParsers[baseUrl] = rerp
        else:
            rerp = self.robotParsers[baseUrl]

        if rerp!=None:
            isOk = rerp.is_allowed("*", url)
            logging.debug("robots.txt allowed: %s" % str(isOk))
            return isOk
        else:
            logging.debug("robots.txt could not be opened, assuming that it's ok to download")
            return True
            

    def _blackListed(self, url):
        " True if url is not blacklisted"
        for blackListWord in BLACKLIST:
            if blackListWord in url:
                logging.debug("Ignoring URL, contains blacklist word %s" % blackListWord)
                return True
        return False

    def get(self, url):
        if self._blackListed(url):
            return None, None
        if not self._robotOk(url):
            return None, None
        try:
            logging.debug("Starting download of %s" % url)
            contentFile = util.httpGet(url)
        except urllib2.HTTPError, ex:
            logging.debug("Download error (HttpError): %s" % str(ex))
            return None, None
        except urllib2.URLError, ex:
            logging.debug("Download error (UrlError): %s" % str(ex))
            return None, None
        contentData = contentFile.read()
        mimeType = contentFile.info().gettype()
        return contentData, mimeType

def downloadFileData(downloader, url, title):
    """ given a list of (url, title)-objects, download them via http and convert 
    to ASCII and return as a fileData-dictioary (defined in pubStore.py)
    """
    logging.debug("-- Starting download of url %s" % url)

    contentData, mimeType = downloader.get(url)
    if contentData==None:
        logging.debug("Could not download")
        return None

    logging.debug("mimeType is %s" % mimeType)
    # construct dict
    fileData = {}
    fileData["url"] = url
    fileData["desc"] = title
    fileData["content"] = contentData
    fileData["mimeType"] = mimeType
    fileData["articleId"] = "0"
    fileData["fileId"] = "0"
    fileData["fileType"] = ""
    fileData["time"] = ""
    fileData["url"] = url
    fileData = pubGeneric.toAsciiEscape(fileData, mimeType=mimeType, \
        maxTxtFileSize=MAXASCIISIZE, maxBinFileSize=MAXFILESIZE)
    if fileData==None:
        return None
    logging.debug("Downloaded data ASCII-format size: %d" % len(fileData["content"]))
    logging.log(5, "Data is"+fileData["content"])
    return fileData

def responseToList(response):
    " given a response object, return a list of (url, title) "
    urlTitleList = []
    for res in response.results:
        urlTitleList.append((res.url, res.title_no_bold))
    return urlTitleList

def googleIter(termString, maxResults=1000, logFh=None):
    " search google for termString and return result as a (url, title)-tuple"
    id = PROJECTID
    size = "large"

    start = 0
    total = 1000
    logWritten = False
    while start + 20 <= maxResults and start + 20 < total:
        logging.debug("start=%d, total=%d, maxResults=%d" % (start, total, maxResults))

        while True:
            try:
                response = Search(id, size, start, termString)
                total = int(response.total)
                break
            except Exception, err:
                logging.info("Exception during Search(): %s" % err)
                open("errorlog.txt","a").write(str(err)+"\n")
                traceback.print_exc()
                time.sleep(10)

        if logFh and not logWritten:
            logFh.write("\t".join([termString, str(total)])+"\n")
            logWritten = True

        if total==0:
            yield None, None
            time.sleep(1)
            break
        for url, title in responseToList(response):
            yield url, title
        time.sleep(1)
        start += 20


def dnaMotifGenerator(minLen, maxLen, symbols="ACTG"): 
    " generate random ACTG strings of length from minLen to maxLen "
    for mLen in range(minLen, maxLen+1):
        noToNucl = {}
        for i in range(0, len(symbols)):
            noToNucl[i]=symbols[i]

        motifList = []
        for x in xrange(0, len(symbols)**mLen):
            motif = []
            remainder = x
            for i in reversed(range(0, mLen)):
                    digit = remainder / len(symbols)**i
                    remainder = remainder % len(symbols)**i
                    nucl = noToNucl[digit]
                    motif.append(nucl)
            motif = "".join(motif)
            yield motif

def seqFileIterator(fname):
    " yield seqs from file, one per line "
    for line in open(fname):
        line = line.strip()
        if len(line)==0:
            continue
        else:
            yield line

def writeIndex(baseTerms, indexFname, seqIterator, maxDownload=1000):
    """ search google for base terms (string) + sequences from seqIterator
    and write resulting urls to indexFname 
    """
    logFname = join(dirname(indexFname), "log.tab")
    if isfile(indexFname):
        mode = "a"
        logging.info("Reading old sequences, already crawled")
        oldSeqs = set([row.searchTerm.split()[-1] for row in maxCommon.iterTsvRows(logFname)])
        logFh  = open(logFname, mode)
    else:
        mode = "w"
        oldSeqs = set()
        logFh  = open(logFname, mode)
        logFh.write("searchTerm\tcount\n")

    logging.info("Writing index, base terms %s, file %s" % (baseTerms, indexFname))
    outFh = codecs.open(indexFname, mode, encoding="utf8")

    headers = ["urlId", "chunkId", "seq", "url", "title"]
    outFh.write("\t".join(headers)+"\n")

    urlCount = 0
    #for seqLen in range(startLen, endLen+1):
    for seq in seqIterator:
        if seq in oldSeqs:
            logging.info("Skipping %s, already done" % seq)
            continue
        logging.info("Sequence is %s" % seq)
        if baseTerms!="":
            terms = baseTerms+" "+seq
        else:
            terms = seq
        for url, title in googleIter(terms, logFh=logFh, maxResults=maxDownload):
            if url!=None:
                chunkId = urlCount % CHUNKCOUNT
                data = [str(urlCount), str(chunkId), seq, url, title]
                outFh.write("\t".join(data)+"\n")
                urlCount += 1

def downloadFiles(inFile, outFile):
    " download all urls from inFile, convert to ASCII and write to outFile """
    pw = pubStore.PubWriterFile(outFile)
    #downloader = PythonDownloader()
    downloader = WgetDownloader()
    for row in maxCommon.iterTsvRows(inFile):
        url, title = row.url, row.title
        urlId = row.urlId
        fileData = downloadFileData(downloader, url, title)
        if fileData!=None:
            pw.writeFile(0, urlId, fileData)
    pw.close()

if __name__ == "__main__":
    if args==[]:
        parser.print_help()
        exit(1)

    command = args[0]
    targetDir = args[1]
    indexFname = join(targetDir, "index.tab")
    splitIndexDir = join(targetDir, "index.split")
    pubGeneric.setupLogging(progFile, options)

    if command == "index":
        terms = args[2]
        #terms = "pcr primer"
        targetDir = args[0]
        maxCommon.mustBeEmptyDir(targetDir, makeDir=True)
        if options.seqFile==None:
            seqIterator = dnaMotifGenerator(options.startSeqLen, options.endSeqLen)
        else:
            seqIterator = seqFileIterator(options.seqFile)

        writeIndex(terms, indexFname, seqIterator, maxDownload=options.maxDownload)

    elif command=="download":
        #maxCommon.mustBeEmptyDir(splitIndexDir, makeDir=True)
        logging.info("Splitting index file %s for jobs" % indexFname)
        pubStore.splitTabFileOnChunkId(indexFname, splitIndexDir) 
        runner = maxRun.Runner()
        chunkFiles = glob.glob(join(splitIndexDir, "*.tab"))
        for chunkFname in chunkFiles:
            chunkId = splitext(basename(chunkFname))[0]
            outFname = join(targetDir, chunkId+".zip")
            cmd = "job:download {check in line %s} {check out exists %s}" % (chunkFname, outFname)
            pubGeneric.recursiveSubmit(runner, cmd)
        runner.finish()

    elif command=="job:download":
        inFile, outFile = args[1:]
        pubGeneric.setupLogging(progFile, options, logFileName=splitext(outFile)[0]+".log")
        downloadFiles(inFile, outFile)

    else:
        logging.info("Illegal command %s" % command)
