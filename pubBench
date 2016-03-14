#!/usr/bin/python

from sys import *
from optparse import OptionParser
import tabfile

# add <scriptDir>/lib/ to package search path
progFile = os.path.abspath(sys.argv[0])
progDir  = os.path.dirname(progFile)
pubToolsLibDir = os.path.join(progDir, "lib")
sys.path.insert(0, pubToolsLibDir)

import maxCommon

# === COMMAND LINE INTERFACE, OPTIONS AND HELP ===
parser = OptionParser("usage: %prog [options] refFile outTable - compare a table with (pmid,string) against a .out table from pubRunAnnot")

# ==== FUNCTIONs =====
def parseRef(refFname):
    refDict = {}
    for row in maxCommon.iterTsvRows(refFname):
        refDict[row.extId] = (row, id)
    return refDict
        
def parsePred(predFname):
    predDict = {}
    for row in maxCommon.iterTsvRows(predFname):
        predDict[row.extId] = (row, id, snippet)
    return predDict
        
def printBench(refDict, predDict):
    for
def main(args, options):
    refFname, predFname = args
    refDict = parseRef(refFname)
    predDict = parsePred(predFname)
    printBench(refDict, predDict)

# ----------- MAIN --------------
if args==[]: 
    parser.print_help()
    exit(1)

(options, args) = parser.parse_args()
main(args, options)
