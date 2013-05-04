# dumb wrappers around the mysql command line 
# So we don't depend on python mysql packages

import tempfile, maxCommon, logging, re, subprocess, sys
from os.path import *

def runSql(binName, db, cmd):
    " run a cmd through any mysql client "
    cmdList = [binName,db,"-NB","-e", cmd] 
    logging.debug("Running hgsql with %s" % cmdList)
    ret = subprocess.call(cmdList)
    if ret!=0:
        logging.error("Could not run hgSql with this command: %s" % cmdList)
        sys.exit(1)

def runHgSql(db, cmd):
    " run a cmd through hgSql "
    runSql("hgsql", db, cmd)

def runMySql(db, cmd):
    " run a cmd through mysql "
    runSql("mysql", db, cmd)

def listTables(db, expr):
    " return list of table names that match mysql expr "
    tmpFile = tempfile.NamedTemporaryFile(prefix="pubBlat.dropTables")
    tmpName = tmpFile.name
    cmd = """hgsql %s -NB -e 'show tables like "%s"' > %s """ % (db, expr, tmpName)
    maxCommon.runCommand(cmd)

    lines = open(tmpName).readlines()
    lines = [l.strip() for l in lines]
    return lines

def tableExists(db, expr):
    " return True if table exists in db "
    if "." in expr:
        db, expr = expr.split(".")
    tableNames = listTables(db, expr)
    return len(tableNames) > 0
    
def dropTable(db, table):
    logging.debug("Dropping table %s" % table)
    cmd = """hgsql %s -NB -e 'drop table if exists %s'""" % (db, table)
    maxCommon.runCommand(cmd, verbose=False)

def truncateTable(db, table):
    logging.debug("Truncating table %s" % table)
    cmd = """hgsql %s -NB -e 'truncate table %s'""" % (db, table)
    maxCommon.runCommand(cmd, verbose=False)

def dropTablesExpr(db, expr):
    " drop all tables in db that match expr"
    logging.debug("Dropping tables for %s with pattern %s" % (db, expr))

    for table in listTables(db, expr):
        dropTable(db, table)

def dropTables(db, tableList):
    " drop all tables of a list"
    logging.debug("Dropping tables %s, %s" % (db, tableList))

    for table in tableList:
        if "." in table:
            db, table = table.split(".")
        dropTable(db, table)

def renameTablesRegex(db, exprOrList, fromStr, toStr):
    " rename tables that match mysql expr or are given as a list from regex fromStr to toStr "
    if isinstance(exprOrList, str):
        tables = listTables(db, exprOrList)
    else:
        tables = exprOrList

    reFrom = re.compile(fromStr)
    renameDesc = []
    for oldTable in tables:
        newTable = reFrom.sub(toStr, oldTable)
        existTables = listTables(db, oldTable)
        if len(existTables)!=0:
            renameDesc.append( [oldTable, newTable] )
            logging.debug("Renaming table %s -> %s" % (oldTable, newTable))

    parts = []
    for oldName, newName in renameDesc:
        parts.append("%s TO %s" % (oldName, newName))
    sqlCmd = "RENAME TABLE "+", ".join(parts)

    cmd = """hgsql %s -NB -e '%s'""" % (db, sqlCmd)
    maxCommon.runCommand(cmd, verbose=False)

def renameTables(db, fromList, toList, checkExists=False):
    " rename tables from old to new, fromToList is a list of 2-tuples "
    assert(len(fromList)==len(toList))
    logging.debug("Renaming mysql tables %s to %s" % (fromList, toList))
    parts = []
    for oldName, newName in zip(fromList, toList):
        if (not checkExists) or (checkExists and tableExists(db, oldName)):
            parts.append("%s TO %s" % (oldName, newName))
        else:
            logging.debug("Could not find table %s, %s" % (db, oldName))
    if len(parts)==0:
        logging.debug("No table found, not renaming anything")
        return
    sqlCmd = "RENAME TABLE "+", ".join(parts)

    cmd = """hgsql %s -NB -e '%s'""" % (db, sqlCmd)
    maxCommon.runCommand(cmd, verbose=False)

def execSqlCreateTableFromFile(db, sqlFname, tableName):
    " exec sql from file, replace table name "
    lines = []
    for line in open(sqlFname):
        line = line.strip()
        if line.startswith("#"):
            continue
        else:
            lines.append(line)
    sql = " ".join(lines)
    execSqlCreateTable(db, sql, tableName)

def execSqlCreateTable(db, sql, tableName):
    " execute sql, replace table name in sql file with tableName"
    # replace table name
    sqlWords = sql.split()
    newWords = []
    doReplace = False
    for word in sqlWords:
        if doReplace:
            word = tableName
            doReplace=False
        if word.lower()=="table":
            doReplace = True
        newWords.append(word)
    newSql = " ".join(newWords)

    runHgSql(db, newSql) 

def hgLoadSqlTab(db, tableName, sqlName, tabFname, optString=""):
    if isfile(tabFname):
        cmd = "hgLoadSqlTab %s %s %s %s %s" % (db, tableName, sqlName, tabFname, optString)
        maxCommon.runCommand(cmd, verbose=False)
    else:
        logging.warn("file %s not found" % tabFname)

def hgGetAllRows(db, tableName, tempDir):
    " return all rows of table as a list of tuples "
    query = "SELECT * from %s" % tableName
    tempFile = tempfile.NamedTemporaryFile(prefix="maxMysql_hgGetAllRows", dir=tempDir)
    cmd = 'hgsql %s -NB -e "%s" > %s' % (db, query, tempFile.name)
    maxCommon.runCommand(cmd)

    data = []
    for line in open(tempFile.name, "r"):
        row = line.strip("\n").split("\t")
        data.append(row)

    return data

def insertInto(db, tableName, colList, valList):
    " append values from valList to tableName with columns colList "
    valListQuote = ['"'+str(val)+'"' for val in valList]
    sql = "INSERT INTO %s (%s) VALUES (%s);" % (tableName, ",".join(colList), ",".join(valListQuote))
    runHgSql(db, sql)
    
