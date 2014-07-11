#!/usr/bin/env python

# first load the standard libraries from python
# we require at least python 2.5
#from sys import *
import sys
import logging, optparse, os, glob, urllib2, tempfile, subprocess, shutil
from os.path import *

# add <scriptDir>/lib/ to package search path
progFile = os.path.abspath(sys.argv[0])
progDir  = os.path.dirname(progFile)
pubToolsLibDir = os.path.join(progDir, "lib")
sys.path.insert(0, pubToolsLibDir)

import pubGeneric, pubConf, maxXml, maxCommon

# load lxml parser, with fallback to default python parser
try:
    from lxml import etree # you can install this. Debian/Redhat package: python-lxml, see also: codespeak.net/lxml/installation.html
    import lxml
except ImportError:
    import xml.etree.cElementTree as etree # this is the slower, python2.5 default package

# === COMMAND LINE INTERFACE, OPTIONS AND HELP ===
parser = optparse.OptionParser("""usage: %prog [options] <outDir> - download newest update from Elsevier Consyn and place into outDir

NB: The initial download has to be uploaded by Elsevier into your own ftp server
    or are shipped on blue ray discs. This script only downloads the updates

""")

parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages") 
#parser.add_option("", "--parse", dest="parse", action="store_true", help="for debugging, just parse one single xml file", default=None) 
parser.add_option("", "--auto", dest="auto", action="store_true", \
    help="automatically set the output directory based on pubConf.extDir")
(options, args) = parser.parse_args()

# ==== FUNCTIONs =====

def downloadConsyn(rssUrl, outDir):
    " parse RSS feed and download file to outDir "
    # parse xml
    logging.debug("Downloading RSS from %s" % rssUrl)
    xmlString = urllib2.urlopen(rssUrl).read()
    xml = maxXml.XmlParser(string=xmlString, removeNamespaces=True)
    entriesXml = list(xml.getXmlAll("entry"))

    logFname = join(outDir, "download.log")
    logFh = open(logFname, "a")
    # for each entry, download to temp file, then move to final dest
    entriesXml.reverse()

    downloadCount = 0
    logging.info("Downloading...")
    for entryXml in entriesXml:
        fileUrl = entryXml.getXmlFirst("link").getAttr("href")
        fileName = entryXml.getTextFirst("title")

        outFilename = join(outDir, fileName)
        if isfile(outFilename):
            logging.debug("Not downloading %s, found %s" % (fileUrl, fileName))
        else:
            tmpFile = tempfile.NamedTemporaryFile(dir=pubConf.getTempDir(), prefix="tempDownload.pubGetElsevier", suffix=".zip")
            tmpName = tmpFile.name
            #logging.debug("Downloading %s" % (fileUrl))
            logFh.write("%s -> %s\n" % (fileUrl, outFilename))
            logging.debug("Downloading %s to %s" % (fileUrl, tmpName))
            subprocess.call(["wget", fileUrl, "-O", tmpName, "-q", "--no-check-certificate"])
            #subprocess.call(["wget", fileUrl, "-O", tmpName, "-q"])

            assert(os.path.getsize(tmpName)!=0)
            logging.debug("Moving %s to %s" % (tmpName, outFilename))
            shutil.copy(tmpName, outFilename)
            # tmpFile is running out of scope here -> will get deleted automatically
            downloadCount += 1

    logging.info("Downloaded %d files" % downloadCount)

# ----------- MAIN --------------
# only for debugging
if args==[] and not options.auto:
    parser.print_help()
    exit(1)

# normal operation
#outDir = args[0]
#maxCommon.mustExist(outDir)
pubGeneric.setupLogging(progFile, options)

#outDir = pubConf.consynDownloadDir
if options.auto:
    outDir = join(pubConf.extDir, "elsevier")
else:
    outDir = args[0]
rssUrl = pubConf.consynRssUrl
downloadConsyn(rssUrl, outDir)


