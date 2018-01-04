# general routines to make http connections

from os.path import *
import os, logging, telnetlib, urlparse, time, tempfile, urllib, random

import chardet
import pubGeneric

httpTimeout = 30
httpUserAgent = 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:7.0.1) Gecko/20100101 Firefox/7.0.120'
TEMPDIR = "/scratch/tmp"

WGETOPTIONS = " --no-check-certificate --tries=3 --random-wait --waitretry=%d --connect-timeout=%d --dns-timeout=%d --read-timeout=%d --ignore-length --user-agent='%s'" % (httpTimeout, httpTimeout, httpTimeout, httpTimeout, httpUserAgent)

def recodeToUtf8(data):
    " use chardet to find out codepage and recode to utf8"
    encoding = chardet.detect(data)['encoding']
    logging.log(5, "encoding should be %s" % encoding)
    try:
        data = data.decode(encoding).encode("utf8")
    except UnicodeDecodeError:
        logging.warn("Error when decoding as %s" % encoding)
        data = data
    return data

lastCallSec = {}

def wait(delaySec, host="default"):
    " make sure that delaySec seconds have passed between two requests to host "
    global lastCallSec
    delaySec = float(delaySec)
    nowSec = time.time()
    sinceLastCallSec = nowSec - lastCallSec.get(host, 0)
    #logging.debug("sinceLastCall %f" % float(sinceLastCallSec))
    if sinceLastCallSec > 0.1 and sinceLastCallSec < delaySec :
        waitSec = delaySec - sinceLastCallSec
        logging.debug("Waiting for %f seconds" % waitSec)
        time.sleep(waitSec)

    lastCallSec[host] = time.time()

defaultDelay = 20
wgetCache = {}

crawlDelays = {}

def delayedWget(url, forceDelaySecs=None):
    " download with wget and make sure that delaySecs (global var) secs have passed between two calls "
    global wgetCache
    if url in wgetCache:
        logging.log(5, "Using cached wget results")
        return wgetCache[url]

    if forceDelaySecs==None:
        host = urlparse.urlsplit(url)[1]
        logging.debug("Looking up delay time for host %s" % host)
        if host in crawlDelays:
            delaySecs = crawlDelays.get(host, defaultDelay)
            logging.debug("Delay time for host %s configured as %d seconds" % (host, delaySecs))
        else:
            logging.debug("Delay time for host %s not configured" % (host))
            delaySecs = defaultDelay
    else:
        delaySecs = forceDelaySecs
        host = "noHost"

    wait(delaySecs, host)
    page = runWget(url)
    return page

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

def setTorExitNode(host):
    " establish a control connection to tor on localhost:9051 and set the current exit node "
    logging.debug("Setting tor exit node to %s" % host)
    tn = telnetlib.Telnet("127.0.0.1", 9051)

    tn.write('authenticate ""\n')
    reIdx, match, text = tn.expect(["250 OK"], 3)
    #logging.debug("Received %s from tor" % text)
    text = tn.read_eager()
    #logging.debug("Received %s from tor" % text)
    assert(reIdx==0)

    tn.write('setconf ExitNodes=%s\n' % host)
    reIdx, match, text = tn.expect(["250 OK"], 3)
    #logging.debug("Received %s from tor" % text)
    assert(reIdx==0)
    tn.read_eager()
    tn.close()

torNodes = None
currentTorNode = 0


def runWget(url, useTor=None, tmpDir='/tmp'):
    """ download url with wget and return dict with keys url, mimeType, charset, data

    tor support requires a running tor on localhost with an activated control connection
    port and polipo listening on port 8118 and connected to tor.

    """

    global torNodes
    global currentTorNode

    if useTor:
        # download the current exit node list every 60 minutes
        torFname = join(tmpDir, "Tor_ip_list_EXIT.csv")
        if (isfile(torFname) and os.path.getmtime(torFname)-time.time() > 3600) or \
            not isfile(torFname):
            exitNodeUrl = "http://torstatus.blutmagie.de/ip_list_exit.php/Tor_ip_list_EXIT.csv"
            logging.info("Downloading current tor exit node lists from %s" % exitNodeUrl)
            exitNodeData = urllib.urlopen(exitNodeUrl).read()
            open(torFname, "w").write(exitNodeData)
            logging.info("wrote new tor list to %s" % torFname)

        if torNodes==None:
            torNodes = []
            for line in open(torFname):
                ip = line.strip()
                torNodes.append(ip)
            random.shuffle(torNodes)

        if currentTorNode > len(torNodes):
            currentTorNode = 0

        os.environ["http_proxy"] = "http://127.0.0.1:8118"
        setTorExitNode(torNodes[currentTorNode])
        currentTorNode += 1

    # check if file is already in cache
    global wgetCache
    if url in wgetCache:
        logging.log(5, "Using cached wget results")
        return wgetCache[url]

    logging.debug("Downloading %s" % url)
    url = url.replace("'", "")

    # run wget command
    tmpFile = tempfile.NamedTemporaryFile(dir = TEMPDIR, prefix="wgetTemp", suffix=".data")
    cmd = "wget '%s' -O %s --server-response" % (url, tmpFile.name)
    cmd += WGETOPTIONS
    logFile = tempfile.NamedTemporaryFile(dir = TEMPDIR, \
        prefix="pubGetPmid-Wget-", suffix=".log")
    cmd += " -o %s " % logFile.name
    stdout, stderr, ret = pubGeneric.runCommandTimeout(cmd, timeout=httpTimeout)
    if ret!=0:
        raise Exception("wgetRetNonNull\t"+url.decode("utf8"))

    # parse wget log
    mimeType, redirectUrl, charset = parseWgetLog(logFile, url)
    if mimeType==None:
        raise Exception("No mimetype found in http reply\t"+url)

    if redirectUrl!=None:
        finalUrl = redirectUrl
    else:
        finalUrl = url

    data = tmpFile.read()
    logging.log(5, "Download OK, size %d bytes" % len(data))
    if len(data)==0:
        raise Exception("empty http reply from %s" % url)

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

    if useTor:
        del os.environ["http_proxy"]

    return ret

def httpStartsWith(urlPrefix, url):
    """check if url starts with urlPrefix, which should be an http: prefix.  This will also check
    with https:"""
    if url.startswith(urlPrefix):
        return True
    return url.startswith(urlPrefix.replace("http:", "https:", 1))
