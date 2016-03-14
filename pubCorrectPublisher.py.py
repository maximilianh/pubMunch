#!/usr/bin/env python

# first load the standard libraries from python
# we require at least python 2.7
#from sys import *
import sys
if sys.version_info[0]==2 and not sys.version_info[1]>=7:
    print "Sorry, this program requires at least python 2.7"
    print "You can download a more current python version from python.org and compile it"
    print "into your homedir (or anywhere) with 'configure --prefix ~/python27'; make;"
    print "then run this program by specifying your own python executable like this: "
    print "   ~/python27/bin/python <scriptFile>"
    print "or add ~/python27/bin to your PATH before /usr/bin"
    exit(1)

# load default python packages
import logging, optparse, os, collections, tarfile, mimetypes, tempfile, \
    copy, shutil, glob, time
from os.path import *

# add <scriptDir>/lib/ to package search path
progFile = os.path.abspath(sys.argv[0])
progDir  = os.path.dirname(progFile)
pubToolsLibDir = os.path.join(progDir, "lib")
sys.path.insert(0, pubToolsLibDir)

# now load our own libraries
import maxRun, pubStore, pubConf, pubGeneric, pubAlg, maxCommon
from maxCommon import *

# === COMMAND LINE INTERFACE, OPTIONS AND HELP ===
parser = optparse.OptionParser("""usage: %prog [options] publisher dir - add publisher field to article tables

""")

parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages") 
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="show more debug messages") 
(options, args) = parser.parse_args()

# ==== FUNCTIONs =====
# ----------- MAIN --------------
if args==[]: 
    parser.print_help()
    exit(1)

pubGeneric.setupLogging(progFile, options)

publisher, inDir= args[:3]

if isfile(inDir):
    inFnames = [inDir]
else:
    inFnames = glob.glob(join(inDir, "*.articles.gz"))

for inFname in inFnames:
    logging.info("Reading %s" % inFname)
    headerLine = gzip.open(inFname).readline()
    if "publisher" in headerLine:
        logging.info("%s is OK" % inFname)
        continue

    bakFname = inFname+".bak"
    if isfile(bakFname):
        logging.info("%s exists" % bakFname)
        sys.exit(1)
    logging.info("Renaming %s to %s" % (inFname, bakFname))
    shutil.move(inFname, bakFname)
    headers = headerLine.strip().split("\t")
    headers.insert(3, "publisher")

    outFname = inFname
    inFname = bakFname

    logging.info("Writing %s" % outFname)
    ofh = gzip.open(outFname, "w")
    ofh.write("\t".join(headers)+"\n")
    for row in maxCommon.iterTsvRows(inFname, isGzip=True):
        row = list(row)
        row.insert(3, publisher)
        row = [r.encode("utf8") for r in row]
        line = "\t".join(row)+"\n"
        ofh.write(line)



