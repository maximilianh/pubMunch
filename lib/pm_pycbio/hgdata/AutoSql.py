# Copyright 2006-2012 Mark Diekhans
"""support classes for parsing autoSql generated objects"""

def strArraySplit(commaStr):
     "parser for comma-separated string list into a list"
     strs = commaStr.split(",")
     if commaStr.endswith(","):
          strs = strs[0:-1]
     return strs

def strArrayJoin(strs):
     "formatter for a list into a comma seperated string"
     if strs != None:
          return ",".join(strs) + ","
     else:
          return ","

# TSV typeMap tuple for str arrays
strArrayType = (strArraySplit, strArrayJoin)

def intArraySplit(commaStr):
     "parser for comma-separated string list into a list of ints"
     ints = []
     for s in strArraySplit(commaStr):
          ints.append(int(s))
     return ints

def intArrayJoin(ints):
     "formatter for a list of ints into a comma seperated string"
     if ints != None:
          strs = []
          for i in ints:
               strs.append(str(i))
          return ",".join(strs) + ","
     else:
          return ","

# TSV typeMap tuple for str arrays
intArrayType = (intArraySplit, intArrayJoin)
