#!/usr/bin/env python

# first load the standard libraries from python
# we require at least python 2.7
#from sys import *
import sys
if sys.version_info[0]==2 and not sys.version_info[1]>=5:
    print "Sorry, this program requires at least python 2.5"
    print "You can download a more current python version from python.org and compile it"
    print "into your homedir (or anywhere else) with 'configure --prefix ~/python27'; make;"
    print "then run this program again by specifying your own python executable like this: "
    print "   ~/python27/bin/python <%s>" % sys.argv[0]
    print "or add ~/python27/bin to your PATH before /usr/bin"
    exit(1)

# load default python packages
import logging, optparse, os, collections, tarfile, mimetypes, tempfile, \
    copy, shutil, glob, cPickle
from os.path import *

# add <scriptDir>/lib/ to package search path
progFile = os.path.abspath(sys.argv[0])
progDir  = os.path.dirname(progFile)
pubToolsLibDir = os.path.join(progDir, "lib")
sys.path.insert(0, pubToolsLibDir)

# now load our own libraries
import maxRun, pubStore, pubConf, pubGeneric, pubAlg
from maxCommon import *

# === COMMAND LINE INTERFACE, OPTIONS AND HELP ===
parser = optparse.OptionParser("""usage: %prog [options] <algorithmName> <inDir> <outFile> - run the "reduce" part of an algorithm onto a directory

    Iterate over .pickle files in directory, run reduce function on them
    Write result to outfile

    The pickle files contain python dictionaries that are all merged
    into one big dictionary which is run through the reduce() function
    of the algorithm.

    For debugging, you can also supply just a single .pickle file to see if the algorithm works.
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

algName, inDir, outFilename = args[:3]
parameters = args[3:]
#paramDict = cPickle.load(open("mapReduceParam.pickle"))
paramList = args[3:]
paramDict = pubGeneric.stringListToDict(paramList)

#alg = pubAlg.getAlg(algName) 

pubAlg.runReduce(algName, paramDict, inDir, outFilename)
