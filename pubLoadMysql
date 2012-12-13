#!/usr/bin/env python

# load default python packages
import logging, optparse, os, shutil, glob, tempfile, sys
from os.path import *

# add <scriptDir>/lib/ to package search path
sys.path.insert(0, join(dirname(abspath(__file__)),"lib"))

# load our own libraries
import pubConf, pubGeneric, maxMysql, pubStore, maxTables, maxCommon
from maxCommon import *

schemaTmpFh = None # to make sure this tmp file stays around until the program ends
tableTmpFh = None # see above

# ===== FUNCTIONS =======

def readFields(dataDir):
    " read first line from first file in dataDir or from dataDir if dataDir happens to be a file "
    if isfile(dataDir):
        line1 = open(dataDir).readline()
    elif isdir(dataDir):
        line1 = gzip.open(glob.glob(join(dataDir, "*.tab.gz"))[0]).readline()
    else:
        raise Exception("No dir or file %s" % dataDir)
    fields = line1.strip("\n").strip("#").split("\t")
    if "articleId" not in fields:
        fields.insert(0, "articleId")
        fields.insert(1, "fileId")
    return fields

def createAnnotSchema(tableName, fields, idxFields):
    #idxFields=["type","id"], \
    fieldTypes = {"articleId" : "bigInt", "snippet":"VARCHAR(16000)"}
    tblSql, idxSql= maxTables.makeTableCreateStatement(tableName, fields, type="mysql", \
        primKey="autoId", \
        intFields=["autoId", "articleId", "fileId", "annotId"], \
        idxFields=idxFields, \
        fieldTypes = fieldTypes, \
        inlineIndex=True)

    logging.debug("Got sql statement %s" % tblSql)
    tblSql = tblSql.replace("IF NOT EXISTS ", "") # jim's tool doesn't understand this
    global tmpFh
    tmpName = join(pubConf.getTempDir(), "tmp.sql")
    #schemaTmpFh = tempfile.NamedTemporaryFile(dir=pubConf.getTempDir(), prefix="pubLoadMysql", suffix=".temp.sql")
    #tmpName = schemaTmpFh.name
    open(tmpName, "w").write(tblSql)
    return tmpName

def annotIter(dataDirs, fields, typeFilter=None):
    " return selected fields of annotation rows from dataDir and split annotId into its parts "
    fnames = []
    for dataDir in dataDirs:
        if dataDir=="map":
            dirFnames = []
            for pub in pubConf.loadPublishers:
                for annotDir in glob.glob(join(pubConf.pubMapBaseDir, pub, "batches", "*", "annots", "markers")):
                    logging.info("checking dir %s for .tab.gz files" % annotDir)
                    annotNames = glob.glob(join(annotDir, "*.tab.gz"))
                    dirFnames.extend(annotNames)
        elif isfile(dataDir):
            dirFnames = [dataDir]
        else:
            dirFnames = glob.glob(join(dataDir, "*.tab.gz"))
        fnames.extend(dirFnames)

    logging.info("Found %d input files from %d input directories" % (len(fnames), len(dataDirs)))
    AnnotRec = collections.namedtuple("AnnotRec", fields)
    pm = maxCommon.ProgressMeter(len(fnames), stepCount=100)
    autoId = 0
    for fname in fnames:
        for row in maxCommon.iterTsvRows(fname):
            rowDict = row._asdict()
            if "type" in rowDict and typeFilter!=None and \
                    row.type not in typeFilter:
                continue
            if "annotId" in row._fields:
                articleId, fileId, annotId = pubGeneric.splitAnnotIdString(row.annotId)
                rowDict["articleId"] = str(articleId)
                rowDict["fileId"] = str(fileId)
                rowDict["annotId"] = str(annotId)
            newRow = []
            for field in fields:
                if field=="autoId":
                    val = str(autoId)
                else:
                    val = rowDict[field]
                newRow.append(val)
            newTuple = AnnotRec(*newRow)
            autoId += 1
            yield newTuple
        pm.taskCompleted()
                
def main(args, options):
    fileType, db, tableName = args[:3]
    dataDirs = args[3:]
    assert(len(dataDirs)>=1)
    pubGeneric.setupLoggingOptions(options)

    if fileType=="files":
        sqlFname = join(pubConf.sqlDir, "file.sql")
        dataIter = pubStore.iterArticleDataDirs(dataDirs, type=fileType)
    elif fileType=="articles":
        sqlFname = join(pubConf.sqlDir, "article.sql")
        dataIter = pubStore.iterArticleDataDirs(dataDirs, type=fileType)
    elif fileType=="markers":
        annotFields = ["autoId", "articleId", "fileId", "annotId", "type", "id", "snippet"]
        idxFields = ["articleId", "type", "id", "markerId"]
        annotFields = readFields(dataDirs[0])
        sqlFname = createAnnotSchema(tableName, annotFields, idxFields)
        dataIter = annotIter(dataDirs, annotFields, typeFilter=options.markerTypes)
    elif fileType=="fusions":
        annotFields = readFields(dataDir)
        idxFields = ["articleId", "sym1", "sym2", "symPair"]
        sqlFname = createAnnotSchema(tableName, annotFields, idxFields)
        dataIter  = annotIter(dataDirs, annotFields, typeFilter=options.markerTypes)
    else:
        assert(False) # illegal file type

    # create table
    tempName = join(pubConf.TEMPDIR, "pubLoad.%s.sqlTable.tmp" % tableName)
    if not options.reuseTable:
        #tableTmpFh = tempfile.NamedTemporaryFile(dir=pubConf.getTempDir(), prefix="pubLoadMysql", suffix="tmp.tab")
        #tempName = tableTmpFh.name
        if isfile(tempName):
            logging.error("Found an already existing file %s" % tempName)
            logging.error("Please make sure that no concurrent pubLoad is running and remove this file first.")
            sys.exit(1)
        else:
            tempFile = open(tempName, "w")
            logging.info("Concatting tables to %s" % tempFile.name)
            for row in dataIter:
                line = "\t".join(row)+"\n"
                line = line.replace("\\", "\\\\") # mysql treats \ as escape char on LOAD DATA
                line = line.replace("\a", "\\n") # mysql treats \ as escape char on LOAD DATA
                tempFile.write(line.encode("utf8"))
            tempName = tempFile.name
            tempFile.close()

    logging.info("Loading table")
    maxMysql.hgLoadSqlTab(db, tableName, sqlFname, tempName, optString="-warn")
    #os.remove(tempName)

# === COMMAND LINE INTERFACE, OPTIONS AND HELP ===
parser = optparse.OptionParser("""usage: %prog [options] <articles|files|markers|fusions> <db> <tableName> <inDirOrFiles> - create sql table and load pubTools files into database.tableName. Uses pubConf.sqlDir to find sql file

inDirOrFiles can be the special keyword "map" which will load all markers from the pubMap-batch-annotation directories
""")

parser.add_option("-d", "--debug", dest="debug", action="store_true", help="show debug messages") 
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="show more debug messages") 
parser.add_option("-t", "--markerType", dest="markerTypes", action="append", help="for markers: only load rows with this type (e.g. snp, symbol or pdb). Can be specified multiple times") 
parser.add_option("", "--reuseTable", dest="reuseTable", action="store_true", help="do not recreate big table, use the existing temporary one, for debugging") 
#parser.add_option("-f", "--files", dest="files", action="store_true", help="do not load article but files into db, uses a different schema") 
(options, args) = parser.parse_args()

if args==[]:
    parser.print_help()
    exit(1)

main(args, options)
