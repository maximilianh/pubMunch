# Routines for reading/writing tables from/to textfiles or databases

import sys, textwrap, operator, types, logging, re, os, collections, codecs, time, gzip, shutil

# jython doesn't have sqlite3
try:
    import sqlite3
except ImportError:
    pass

from types import *

import maxCommon, pubGeneric

# Routines for handling fasta sequences and tab sep files
try:
    import MySQLdb
except:
    #logging.warn("MySQLdb is not installed on this machine, tables cannot be read from mysql")
    pass

def openFileRead(fname):
    return openFile(fname)

def openFile(fname, mode="r"):
    """ open and return filehandle, open stdin if fname=="stdin", do nothing if none """
    if fname=="stdin":
        return sys.stdin
    elif fname.endswith(".gz"):
        return gzip.open(fname)
    elif fname=="stdout":
        return sys.stdout
    elif fname=="none" or fname==None:
        return None

    if mode=="r":
        if not os.path.isfile(fname):
            logging.error("File %s does not exist" % fname)
        else:
            if os.path.getsize(fname)==0:
                logging.warn("File %s is empty" % fname)

    return open(fname, mode)

###
class BlockReader:
    """ a parser for sorted, tables: Splits a table 'blocks' where values in
    one field are identical. Returns one block at a time. As such, it can
    process GB of tables, without having to load them into memory. 
    
    if mustBeSorted==True: Tables MUST BE sorted with sort: remember to use the
    -n option, do not pipe into sort
    to save memory. """

    def __init__(self, fileObj, idField, mustBeSorted=True):
        """ initialize blockReader to read from file fname, the idField is the field which contains the id that the file is sorted on """
        #self.fh = open(fname, "r")
        if type(fileObj)==FileType:
            self.fh = fileObj
        else:
            self.fh = open(fileObj)
        self.block = []
        self.lineCount=0
        self.blockIds=set()
        self.lastId = None
        self.idField = int(idField)
        self.mustBeSorted=mustBeSorted
        self.headers = self.fh.readline().strip("\n").strip("#").split("\t")

    def readNext(self):
        """ read next block and return as list of tuples """
        blockId=None
        for l in self.fh:
            self.lineCount+=1
            tuple = l.strip("\n").split("\t")
            blockId = tuple[self.idField]
            if self.mustBeSorted:
                assert(self.lastId==None or self.lastId=='None' or (int(blockId) >= int(self.lastId))) # infile for this script MUST BE sorted with sort (remember to use the -n option, do not pipe into sort) 
            
            if blockId==self.lastId or self.lastId==None:
                # no change or first block: just add line
                self.block.append(tuple)
                self.lastId=blockId
            else:
                # change of blockId: clear block and update blockId
                self.blockIds.add(blockId)
                oldBlock=self.block
                oldId=self.lastId

                self.block=[tuple]
                self.lastId = blockId
                yield oldId, oldBlock

        # for last line
        if blockId!=None:
            self.blockIds.add(blockId)
        yield self.lastId, self.block


###
class TableParser:
    """ Class to read tables from (tab-sep) textfiles. Fieldnames can be read from textfile headers or are provided, fields are converted to specified data types (int/string/float). 

    >>> docStr="#id\\tfirstname\\tlastname\\n#test\\n1\\tmax\\thaussler\\n"
    >>> import StringIO
    >>> f = StringIO.StringIO(docStr)
    >>> tp = TableParser(fileObj=f)
    >>> print list(tp.lines())
    [tuple(id='1', firstname='max', lastname='haussler')]
    >>> tp2 = TableParser(None, headers=['field1', 'field2'])
    >>> print tp2.parseTuple(['test1', 'test2'])
    tuple(field1='test1', field2='test2')
    """

    def __init__(self, fileObj, headers=None, fileType=None, types=None, colCount=None, encoding="utf8"):
        """ 
        Parse headers from file (read only first line from file)
        or parse headers from headers parameter
        or use predefined headers according to fileType
        fileType can be: numColumns, blastm8, psl, blastConvert, bed3, bed4, sam 

        specify numColumns if file doesn't have a header line
        """

        self.types=types

        if isinstance(fileObj, str):
            fileObj = codecs.open(fileObj, encoding=encoding)

        if fileObj:
            self.fileObj = fileObj

        self.commentChar="#"
        self.line1=None

        if fileType==None and headers==None:
            # parse headers from file and set types 
            # all to String
            line1 = fileObj.readline().strip("\n")
            line1 = line1.strip(self.commentChar)
            self.headers = line1.split("\t")
        elif headers:
            self.headers = headers
        else:
            # predefined file formats, set your editor to nowrap lines to read them better
            if fileType=="numbered" or fileType=="numColumns":
                if colCount==None:
                    self.line1 = fileObj.readline()
                    colCount = len(self.line1.split("\t"))
                self.headers = ["col"+str(i) for i in range(0, int(colCount))]
                self.types   = [StringType] * len(self.headers)
            elif fileType=="psl":
                self.headers = ["score", "misMatches", "repMatches", "nCount", "qNumInsert", "qBaseInsert", "tNumInsert", "tBaseInsert", "strand",    "qName",    "qSize", "qStart", "qEnd", "tName",    "tSize", "tStart", "tEnd", "blockCount", "blockSizes", "qStarts", "tStarts"]
                self.types =   [IntType, IntType,       IntType,      IntType,  IntType,      IntType,       IntType,       IntType,      StringType, StringType, IntType, IntType,  IntType, StringType, IntType, IntType,  IntType, IntType ,   StringType,      StringType,   StringType]
            elif fileType=="blastm8":
                self.headers = ["qName",    "tName",    "percIdentity", "alnLength", "misMatches", "gapOpenCount", "qStart", "qEnd", "tStart", "tEnd",  "eVal",    "score"]
                self.types =   [StringType, StringType, FloatType,      IntType,      IntType,      IntType,        IntType, IntType, IntType, IntType, FloatType, IntType,]
            elif fileType=="intmap":
                self.headers = ["int", "string"]
                self.types = [IntType, StringType]
            elif fileType=="blastConvert":
                self.headers = ["pmcId", "genomeId", "seqId", "chrom", "tStart", "tEnd", "score"]
                self.types =   [IntType, IntType,    IntType, StringType, IntType, IntType, FloatType]
            elif fileType=="bed4":
                self.headers = ["chrom", "start", "end", "name"]
                self.types =   [StringType, IntType, IntType, StringType]
            elif fileType=="bed3":
                self.headers = ["chrom", "start", "end"]
                self.types =   [StringType, IntType, IntType]
            elif fileType == "sam":
                self.headers = ["qname"    , "flag"  , "rname"    , "pos"   , "mapq"  , "cigar"    , "nrnm"     , "mpos"  , "isize" , "seq"      , "qual"     , "tags"]
                self.types   = [StringType , IntType , StringType , IntType , IntType , StringType , StringType , IntType , IntType , StringType , StringType , StringType]
                self.commentChar="@"
                
            else:
                logging.error("maxTables.py: illegal fileType\n")
                sys.exit(1)

        if headers:
            self.headers = headers
        if not self.types:
            self.types = [StringType] * len(self.headers)

        logging.debug("Headers are: %s" % str(self.headers))
        self.Record = collections.namedtuple("tuple", self.headers) # this is a backport from python2.6

    def parseTuple(self, tuple):
        """
        >>> TableParser(None, fileType="intmap").parseTuple(["1", "hallo"])
        tuple(int=1, string='hallo')

        """
        # convert fields to correct data type
        if self.types: 
            tuple = [f(x) for f, x in zip(self.types, tuple)]
        # convert tuple to record with named fields
        return self.Record(*tuple)    

    def parseBlock(self, block):
        """ convert list of tuples to list of records """
        newBlock= []
        for tuple in block:
            newBlock.append(self.parseTuple(tuple))
        return newBlock

    def parseLine(self, line):
        tuple = line.strip("\n").split("\t")    
        return self.parseTuple(tuple)
    
    def generateRows(self):
        """ just another name for lines() """
        return self.lines()

    def rows(self):
        """ and another name for lines() """
        return self.lines()

    def lines(self):
        """ Generator: return next tuple, will skip over empty lines and comments (=lines that start with #). """
        while True:
            line = '\n'
            if self.line1!=None:
                line = self.line1
                self.line1=None
                yield self.parseLine(line)

            while line=='\n' or line.startswith(self.commentChar) or line =="\r" or line =='\r\n':
                line = self.fileObj.readline()
            if line=='':
                return 
            yield self.parseLine(line)

    def column(self, columnName, dataType=types.StringType):
        """ Generator: return values of a given column (name) """
        for row in self.lines():
            yield dataType(row._asdict()[columnName])

    def columns(self, columnNames):
        """ Generator: return values of given columns (name) """
        for row in self.lines():
            result = []
            rowDict = row._asdict()
            for columnName in columnNames:
                result.append(rowDict[columnName])
            yield result
# ---- parse rows from mysql table ------

def hgSqlConnect(db, config=None, **kwargs):
    " connect using information parsed from ~/.hg.conf "
    if config==None:
        config = maxCommon.parseConfig("~/.hg.conf")
    conn = MySQLdb.connect(host=config["db.host"], user=config["db.user"], \
        passwd=config["db.password"], db=db, **kwargs)
    return conn

def sqlConnection(connString=None, **kwargs):
    """ connectionString format: hostWithPort,user,password,db 
    > conn = sqlConnection(connString="localhost,max,test,temp")
    """
    if connString:
        host,user,passwd,db = connString.split(",")

        port = 0
        fs = host.split(":")
        host = fs[0]
        if len(fs)>1:
            port=int(fs[1])

        db = MySQLdb.connect(host, user, passwd, db, port=port)
    else:
        try:
            db = MySQLdb.connect(**kwargs)
        except:
            kwargs["read_default_file"]="~/.my.cnf"
            db = MySQLdb.connect(**kwargs)
    return db

def sqlGetRows(conn, sqlString, *args):
    """ execute sqlString with placeholders %s replaced by args
    caveat: if there is only one arg, you need to send it as (arg,) ! see mysqldb docs
    returns a dictionary fieldname -> value
    """
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(sqlString, *args)
    rows = cursor.fetchall()

    # check if any column is an array (bug in mysqldb 1.2.1 but not in 1.2.2)
    #if len(rows)>0:
        #arrayType = type(array.array('c', "a"))
        #needConvert = False
        #row1 = rows[0]
        #for d in row1:
            #if type(d)==arrayType:
                #needConvert=True
                #break
        #if needConvert:
            #newRows = []
            #for row in rows:
                #newCols = []
                #for col in row:
                    #if type(col)==arrayType:
                       #newCols.append(col.tostring()) 
                    #else:
                        #newCols.append(col)
                #newRows.append(newCols)
            #rows = newRows
        # end bugfix
    cursor.close ()
    conn.commit()
    return rows

def writeRow(db, table, columnData, keys=None):
    """ write row to table, columData can be a dict or a namedtuple, keys are the keys of values that are to be written from the dict/namedtuple """
    if keys==None:
        if type(columnData)==type({}):
            keys=columnData.keys()
        else:
            keys=columnData._fields
            columnData = columnData._asdict()
    
    values = [columnData[key] for key in keys]

    keyString = ", ".join(keys)
    placeHolders = ["%s"]*len(keys)
    placeHolderString = ", ".join(placeHolders)
    sql = "INSERT INTO %s (%s) VALUES (%s);" % (table, keyString, placeHolderString)
    cursor = db.cursor()
    cursor.execute(sql, values)
    
class SqlTableReaderGeneric:
    def _defineRowType(self):
        colNames = [desc[0] for desc in self.cursor.description]
        self.RowType = collections.namedtuple("MysqlRow", colNames)

class SqlTableReader_BigTable(SqlTableReaderGeneric):
    """ Read rows from mysql table, keep table on server and fetches row-by-row"
    >>> p = SqlTableReader_BigTable("show variables like '%protocol_version%';")
    >>> list(p.generateRows())
    [MysqlRow(Variable_name='protocol_version', Value='10')]
    """
    def __init__(self, query, conn=None, **kwargs):
        if conn==None:
            conn = sqlConnection(**kwargs)
            self.connArgs = kwargs
        self.query = query
        self.cursor = conn.cursor(MySQLdb.cursors.SSCursor)
        self.rows = self.cursor.execute(query)
        self._defineRowType()
        self.kwargs=kwargs

    def generateRows(self):
        """ fetch row from mysql server, try to re-establish connection if it times out """
        errCount=0
        while True:
            try:
                rowRaw = self.cursor.fetchone()
                if rowRaw==None:
                    break
                else:
                    rowTuple = self.RowType(*rowRaw)
                    yield rowTuple
                    
            except _mysql_exceptions.OperationalError:
                    errCount+=1
                    if errCount==100:
                        raise Exception, "mysql connection error, even after retrials"
                        break
                    else:
                        logging.info("Mysql connection problems, will reconnect in 10 secs")
                        time.sleep(10)
                        self.__init__(self.query, conn=None, **self.kwargs)

class SqlTableReader(SqlTableReaderGeneric):
    """ Read rows from mysql table, will keep whole table in memory "
    >>> conn = MySQLdb.connect(read_default_file="~/.my.cnf")
    >>> p = SqlTableReader("show variables like '%protocol_version%';", conn)
    >>> list(p.generateRows())
    [MysqlRow(Variable_name='protocol_version', Value='10')]
    """
    def __init__(self, query, conn=None, **kwargs):
        if conn==None:
            conn = sqlConnection(**kwargs)
        self.cursor = conn.cursor()
        self.rows = self.cursor.execute(query)
        self.data = self.cursor.fetchall()
        self._defineRowType()

    def generateRows(self):
        for i in range(0, len(self.data)):
            rowRaw = self.data[i]
            rowTuple = self.RowType(*rowRaw)
            yield rowTuple

    def asDict(self, keyIdx=0, valueIdx=1):
        result = {}
        for row in self.generateRows():
            key = row[keyIdx]
            value = row[valueIdx]
            result[key] = value
        return result

    def writeToFile(self, fileObject):
        for row in self.generateRows():
            row = [str(d) for d in row]
            tabline = "\t".join(row)+"\n"
            fileObject.write(tabline)

def concatHeaderTabFiles(filenames, outFilename, keyColumn=None, progressDots=None):
    """ concats files and writes output to outFile, will output headers (marked with #!) only once """
    outF = openFile(outFilename, "w")
    fno = 0
    allKeys = set()
    count = 0
    for fname in filenames:
        lno = 0
        for l in open(fname):
            l = l.strip("\n")
            if keyColumn!=None:
                key = l.split("\t")[keyColumn]
                if key in allKeys:
                    logging.error("Key %s appears twice" % key)
                    exit(1)
            if lno!=0 or (lno==0 and fno==0 and l.startswith("#")):
                outF.write(l)
                outF.write("\n")
            lno+=1
            count+=1
            if progressDots!=None and count% progressDots==0:
                print ".",
                sys.stdout.flush()
        fno+=1

splitReOp = re.compile("[MIDNSHP]")
splitReNumbers = re.compile("[0-9]+")
def samToBed(row):
    strand = (row.flag & 16) == 16
    if strand==1:
        strand="-"
    else:
        strand="+"

    if row.pos==0:
        return None

    featStart = row.pos -1

    cigarLengths = [int(x) for x in splitReOp.split(row.cigar) if x!=""]
    cigarOps     = [x for x in splitReNumbers.split(row.cigar) if x!=""]
    cigarLine    = zip(cigarOps, cigarLengths)
    assert(len(cigarLengths)==len(cigarOps)==len(cigarLine))

    featLength = 0
    matches = 0
    for op, length in cigarLine:
        length= int(length)
        if op in "MDNP":
            featLength+=length
        if op=="M":
            matches += length
    featEnd = featStart+featLength

    return row.rname, featStart, featEnd, row.qname, matches, strand
    
def parseSam(fileObj):
    tp = TableParser(fileObj=fileObj, fileType="sam")
    for row in tp.generateRows():
        yield samToBed(row)

def parseDict(fname, comments=False, valField=1, doNotCheckLen=False, otherFields=False, headers=False, keyType=None, valueType=None, errorSummary=False, inverse=False):
    """ parse file with key -> value pair on each line, key/value has 1:1 relationship"""
    """ last field: set valField==-1, return as a dictionary key -> value """
    if fname==None or fname=="":
        return {}
    dict = {}
    f = openFile(fname)
    errors = 0
    if not f:
        return dict

    if headers:
        f.readline()
    for l in f:
        #print l
        fs = l.strip().split("\t")
        if comments and l.startswith("#"):
            continue
        if not len(fs)>1:
            if not doNotCheckLen:
                sys.stderr.write("info: not enough fields, ignoring line %s\n" % l)
                continue
            else:
                key = fs[0]
                val = None
        else:
            key = fs[0]

            if keyType:
                try:
                    key = keyType(key)
                except:
                    logging.warn("Line %s: could not cast key to correct type" % l)

            if not otherFields:
                val = fs[valField]
            else:
                val = fs[1:]
            
            if valueType:
                try:
                    val = valueType(val)
                except:
                    logging.warn("Line %s: could not cast value to correct type" % l)

        if inverse:
            key, val = val, key

        if key not in dict:
            dict[key] = val
        else:
            if errorSummary:
                errors+=1
            else:
                sys.stderr.write("info: file %s, hit key %s two times: %s -> %s\n" %(fname, key, key, val))
    if errorSummary:
        logging.warn("found %d lines with non-unique key-value associations!" % errors)
    return dict

def openBed(fname, fileType="bed3"):
    "return iterator for bed files "
    fh = openFile(fname)
    return TableParser(fh, fileType=fileType).lines()

def makeTableCreateStatement(tableName, fields, type="sqlite", intFields=[], primKey=None, idxFields=[], inlineIndex=False, primKeyIsAuto=False, fieldTypes={}):
    """
    return a tuple with a create table statement and a list of create index statements.
    returns the index statements separately for additional speed.
    if inlineIndex is true, index create is part of the table create statement.

    >>> makeTableCreateStatement("testTbl", ["test", "hi", "col3"], intFields=["hi"], primKey="test", idxFields=["col3"])
    ('CREATE TABLE IF NOT EXISTS testTbl (test TEXT PRIMARY KEY, hi INTEGER, col3 TEXT); ', 'CREATE INDEX testTbl_col3_idx ON testTbl (col3);')
    """
    intFields = set(intFields)
    idxFields = set(idxFields)
    idxFieldNames = [] 
    parts = []
    idxSqls = []
    for field in fields:
        if type=="sqlite":
            ftype = "TEXT"
        else:
            ftype = "VARCHAR(255)"

        if field in fieldTypes:
            ftype = fieldTypes[field]
        elif field in intFields:
            ftype = "INTEGER"

        if field in idxFields:
            idxSqls.append("CREATE INDEX IF NOT EXISTS %s_%s_idx ON %s (%s);" % \
                (tableName, field, tableName, field))
            idxFieldNames.append(field)

        statement = field+" "+ftype
        if field == primKey:
            statement += " PRIMARY KEY"
            if primKeyIsAuto:
                statement += " AUTO_INCREMENT"
                
        parts.append(statement)

    idxStr = ""
    if inlineIndex:
        for idxFieldName in idxFieldNames:
            idxStr += ", INDEX (%s)" % idxFieldName

    tableSql = "CREATE TABLE IF NOT EXISTS %s (%s %s); " % (tableName, ", ".join(parts), idxStr)
    return tableSql, idxSqls

def loadTsvSqlite(dbFname, tableName, tsvFnames, headers=None, intFields=[], primKey=None, \
        idxFields=[], dropTable=True):
    " load tabsep file into sqlLite db table "
    # if first parameter is string, make it to a list
    if len(tsvFnames)==0:
        logging.debug("No filenames to load")
        return
    if isinstance(tsvFnames, basestring):
        tsvFnames = [tsvFnames]
    if os.path.isfile(dbFname):
        lockDb = False
        finalDbFname = None
    else:
        lockDb = True
        finalDbFname = dbFname
        dbFname = pubGeneric.getFastUniqueTempFname()
        logging.info("writing first to db on ramdisk %s" % dbFname)
    con, cur = openSqlite(dbFname, lockDb=lockDb)

    # drop old table 
    if dropTable:
        logging.debug("dropping old sqlite table")
        cur.execute('DROP TABLE IF EXISTS %s;'% tableName)
        con.commit()

    # create table
    createSql, idxSqls = makeTableCreateStatement(tableName, headers, \
        intFields=intFields, idxFields=idxFields, primKey=primKey)
    logging.log(5, "creating table with %s" % createSql)
    cur.execute(createSql)
    con.commit()

    logging.info("Loading data into table")
    tp = maxCommon.ProgressMeter(len(tsvFnames))
    sql = "INSERT INTO %s (%s) VALUES (%s)" % (tableName, ", ".join(headers), ", ".join(["?"]*len(headers)))
    for tsvName in tsvFnames:
        logging.debug("Importing %s" % tsvName)
        rows = list(maxCommon.iterTsvRows(tsvName))
        logging.log(5, "Running Sql %s against %d rows" % (sql, len(rows)))
        cur.executemany(sql, rows)
        con.commit()
        tp.taskCompleted()

    logging.info("Adding indexes to table")
    for idxSql in idxSqls:
        cur.execute(idxSql)
        con.commit()

    con.close()

    if finalDbFname!=None:
        logging.info("moving over ramdisk db to %s" % dbFname)
        shutil.move(dbFname, finalDbFname)

def insertSqliteRow(cur, con, tableName, headers, row):
    " append a row to an sqlite cursor "
    sql = "INSERT INTO %s (%s) VALUES (%s)" % (tableName, ", ".join(headers), ", ".join(["?"]*len(headers)))
    logging.log(5, "SQL: %s" % sql)
    cur.execute(sql, row)
    con.commit()

def openSqliteCreateTable(db, tableName, fields, intFields=None, idxFields=None, primKey=None, retries=1):
    " create the sqlite db and create a table if necessary "
    con, cur = openSqlite(db, retries=retries)
    if primKey==None:
        primKey = fields[0]
    createSql, idxSqls = makeTableCreateStatement(tableName, fields, \
        intFields=intFields, idxFields=idxFields, primKey=primKey)
    logging.debug("creating table with %s" % createSql)
    cur.execute(createSql)
    con.commit()
    for idxSql in idxSqls:
        cur.execute(idxSql)
    con.commit()
    return con, cur

def namedtuple_factory(cursor, row):
    """
    Usage:
    con.row_factory = namedtuple_factory
    """
    fields = [col[0] for col in cursor.description]
    Row = collections.namedtuple("Row", fields)
    return Row(*row)

def openSqlite(dbName, asNamedTuples=False, lockDb=False, timeOut=10, retries=1, asDict=False):
    " opens sqlite con and cursor for quick reading "
    logging.debug("Opening sqlite db %s" % dbName)
    tryCount = retries
    con = None
    while tryCount>0 and con==None:
        try:
            con = sqlite3.connect(dbName, timeout=timeOut)
        except sqlite3.OperationalError:
            logging.info("Database is locked, waiting for 60 secs")
            time.sleep(60)
            tryCount -= 1

    if asNamedTuples:
        con.row_factory = namedtuple_factory
    elif asDict:
        con.row_factory = sqlite3.Row

    cur = con.cursor()
    #cur.execute("PRAGMA read_uncommited=true;") # has only effect in shared-cache mode
    con.commit()
    if lockDb:
        cur.execute("PRAGMA synchronous=OFF") # recommended by
        cur.execute("PRAGMA count_changes=OFF") # http://blog.quibb.org/2010/08/fast-bulk-inserts-into-sqlite/
        cur.execute("PRAGMA cache_size=800000") # http://web.utk.edu/~jplyon/sqlite/SQLite_optimization_FAQ.html
        cur.execute("PRAGMA journal_mode=OFF") # http://www.sqlite.org/pragma.html#pragma_journal_mode
        cur.execute("PRAGMA temp_store=memory") 
        con.commit()
    return con, cur

def iterSqliteRows(db, tableName):
    """
    yield rows from sqlite db
    """
    con, cur = openSqlite(db, asNamedTuples=True)
    for row in cur.execute("SELECT * FROM %s" % tableName):
        yield row

def iterSqliteRowNames(cur, tableName):
    for row in cur.execute("PRAGMA table_info(%s);" % tableName):
        yield row[1]

if __name__ == "__main__":
    import doctest
    doctest.testmod()

