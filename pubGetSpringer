#!/usr/bin/env python

# first load the standard libraries from python
# we require at least python 2.5
#from sys import *
import sys
import logging, optparse, os, glob, urllib2, tempfile, subprocess, shutil
import ftplib
from datetime import datetime
from os.path import *

# add <scriptDir>/lib/ to package search path
progFile = os.path.abspath(sys.argv[0])
progDir  = os.path.dirname(progFile)
pubToolsLibDir = os.path.join(progDir, "lib")
sys.path.insert(0, pubToolsLibDir)

import pubGeneric, pubConf, maxXml, maxCommon

# === COMMAND LINE INTERFACE, OPTIONS AND HELP ===
parser = optparse.OptionParser("""usage: %prog [options] <outDir> - download newest updates from Springer FTP

The first "download" is shipped on harddisk. See pubConvSpringer.
""")

parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages") 
#parser.add_option("", "--parse", dest="parse", action="store_true", help="for debugging, just parse one single xml file", default=None) 
parser.add_option("", "--auto", dest="auto", action="store_true", \
    help="automatically set the output directory based on pubConf.textBaseDir")
(options, args) = parser.parse_args()

# ==== FUNCTIONs =====
def downloadSpringer(user, password, outDir):
    ftpUrl, ftpDir = "ftp.springer-dds.com", "data/in"
    logging.debug("Connecting to %s, dir %s, user %s, password %s" % (ftpUrl, ftpDir, user, password))
    ftp = ftplib.FTP("ftp.springer-dds.com", user, password)
    ftp.cwd("/data/in")
    fileNames = ftp.nlst()
    logging.debug("Files on server: %s" % ",".join(fileNames))
    
    i = 0
    size = 0
    startTime = datetime.now()
    for fileName in fileNames:
        locFileName = join(outDir, fileName)
        locTmpFile, locTmpFname = pubGeneric.makeTempFile("pubGetSpringer", ".zip")
        maxCommon.delOnExit(locTmpFname)

        logging.debug("Retrieving file %s to %s" % (fileName, locTmpFname))
        ftp.retrbinary('RETR %s' % fileName, locTmpFile.write)
        locTmpFile.flush()
        logging.debug("Moving file %s to %s" % (locTmpFname, locFileName))
        shutil.copy(locTmpFname, locFileName)
        locTmpFile.close()
        logging.debug("Deleting file %s on server" % (fileName))
        ftp.delete(fileName)

        i+=1
        size += getsize(locFileName)

    runtime = datetime.now() - startTime
    runSecs = runtime.seconds
    if i!=0:
        logging.info("Downloaded/removed %d files, total size %d, time %d seconds" % (i, size, runSecs))
    else:
        logging.info("No new springer updates to download.")

# ----------- MAIN --------------
# only for debugging
if args==[] and not options.auto:
    parser.print_help()
    exit(1)

pubGeneric.setupLogging(progFile, options)

#outDir = pubConf.consynDownloadDir
if options.auto:
    outDir = join(pubConf.extDir, "springer")
else:
    outDir = args[0]
user = pubConf.springerUser
password = pubConf.springerPass
downloadSpringer(user, password, outDir)

