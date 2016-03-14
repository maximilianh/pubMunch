#!/usr/bin/env python

# load default python packages
import logging, optparse, os, shutil, glob, tempfile, sys, sqlite3
from os.path import *

# add <scriptDir>/lib/ to package search path
sys.path.insert(0, join(dirname(abspath(__file__)),"lib"))

# load our own libraries
import pubConf, pubGeneric, pubStore, maxTables
from maxCommon import *

# ===== FUNCTIONS =======

def main(args, options):
    dbFname, tableName, dataDir = args
    pubGeneric.setupLoggingOptions(options)
    dataDir = pubConf.resolveTextDir(dataDir)

    if isdir(dataDir):
        logging.debug("Reading dir %s" % dataDir)
        tsvFnames = glob.glob(join(dataDir,"*.articles.gz"))
    elif isfile(dataDir):
        tsvFnames = [dataDir]
    else:
        assert(False)

    if len(tsvFnames)==0:
        raise Exception("No input files found in %s" % dataDir)

    pubStore.loadNewTsvFilesSqlite(dbFname, tableName, tsvFnames)

# === COMMAND LINE INTERFACE, OPTIONS AND HELP ===
parser = optparse.OptionParser("""usage: %prog [options] <db> <tableName> <directoryOrFile> - create sqllite db and load pubTools *.article files into database and tableName. """)

parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages") 
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="show more debug messages") 
(options, args) = parser.parse_args()

if args==[]:
    parser.print_help()
    exit(1)

main(args, options)
