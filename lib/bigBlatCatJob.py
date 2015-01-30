#!/usr/bin/env python
# job script for bigBlat.py cat step
# concats and optionally sorts/filters all psl files of a query
# args: queryPslDirectory outputPslPath pslOptions (or string "None")
from sys import argv
from os import system
from bigBlatJob import splitAddDashes
args = argv[1:]
queryPath, pslCatName, pslOptions = args

sortCmd = ""
if pslOptions!="None":
    filtOpt = splitAddDashes(pslOptions)
    sortCmd = " | sort -k10,10 | pslCDnaFilter %s stdin stdout " % (filtOpt)

filtOptStr = splitAddDashes(pslOptions)
cmd = "cat %(queryPath)s/* %(sortCmd)s > %(pslCatName)s" % locals()
ret = system(cmd)
assert(ret==0)
