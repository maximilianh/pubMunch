#!/usr/bin/env python

# load default python packages
import sys, logging, optparse, os, collections, tarfile, mimetypes
from os.path import *

# add <scriptDir>/lib/ to package search path
progFile = os.path.abspath(sys.argv[0])
progDir  = os.path.dirname(progFile)
pubToolsLibDir = os.path.join(progDir, "lib")
sys.path.insert(0, pubToolsLibDir)

# now load our own libraries
import pubGeneric, pubStore, pubConf, maxbio
from pubXml import *

# === COMMAND LINE INTERFACE, OPTIONS AND HELP ===
parser = optparse.OptionParser("""usage: %prog [options] <inFileOr"stdin"> - convert a list of articleIds to metadata by looking them up in the sqlite databases and write to stdout.

It's a lot faster to do this on sorted files, as disk access is more linear then.

example:
%d pmids.txt -k pmid
""")

parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages")
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="show more debug messages")
parser.add_option("-p", "--parse", dest="parseFile", action="store", help="only parse a single file (for debugging)") 
parser.add_option("-k", "--key", dest="key", action="store", default="articleId", help="key to lookup, default %default") 
parser.add_option("-c", "--chars", dest="chars", action="store", type="int", default=10, help="number of characters to use from key, default 10")
(options, args) = parser.parse_args()

# ==== FUNCTIONs =====

    
# ----------- MAIN --------------
if args==[]:
    parser.print_help()
    exit(1)

inFname = args[0]
pubGeneric.setupLogging(progFile, options)
lookupKey = options.key

headerWritten = False
for line in maxbio.openFile(inFname):
    artId = line.strip().split("\t")[0]
    artData = pubStore.lookupArticleData(artId, lookupKey=lookupKey)
    #row = [artId, str(artData["pmid"])]
    if artData==None:
        continue
    if not headerWritten:
        #print "\t".join(artData._fields)
        print "\t".join(artData.keys())
        headerWritten = True
    row = ["\t".join([unicode(x).encode("utf8") for x in artData])]
    print "\t".join(row)
