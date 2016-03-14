#!/usr/bin/env python

# load default python packages
import sys, logging, optparse, os, glob, zipfile, types, re, tempfile, shutil, codecs
from os.path import *

# add <scriptDir>/lib/ to package search path
progFile = os.path.abspath(sys.argv[0])
progDir  = os.path.dirname(progFile)
pubToolsLibDir = os.path.join(progDir, "lib")
sys.path.insert(0, pubToolsLibDir)

# now load our own libraries
import pubGeneric, pubStore, pubConf, maxCommon, pubXml, pubConvSpringer, maxRun, pubKeyVal

# === COMMAND LINE INTERFACE, OPTIONS AND HELP ===
parser = optparse.OptionParser("""usage: %prog [options] <in> - index key/value tab-separated file  with some database system like gdbm, leveldb, sqlite or redis
""")
parser.add_option("-p", "--prefer", dest="prefer", action="store", help="prefer a certain type of system, e.g. server (redis) or disk (sqlite)")
parser.add_option("-n", "--newDb", dest="newDb", action="store_true", help="delete the old db before writing data to it")
pubGeneric.addGeneralOptions(parser)
(options, args) = parser.parse_args()

if len(args)==0:
    parser.print_help()
    sys.exit(0)
# ----------- MAIN --------------
# only for debugging
pubGeneric.setupLogging(progFile, options)
fname = args[0]
if args==[]:
    parser.print_help()
    exit(1)

logging.info("Indexing file %s" % fname)
pubKeyVal.indexKvFile(fname, prefer=options.prefer, newDb=options.newDb)
