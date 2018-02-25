# Copyright 2006-2012 Mark Diekhans
"""TSV reading classes"""
import sys,csv
from pm_pycbio.sys import fileOps
from pm_pycbio.tsv.TSVRow import TSVRow
from pm_pycbio.tsv.TSVError import TSVError

# FIXME:  pass owndership of row to Row instead of having Row inherit from list
# FIXME: create error with file name/line number
# FIMXE: carefully consider naming of functions in Row, as they could
#  conflict with fields.  Maybe put in a base-class??
# FIXME: put in same module as TSV
# FIXME: TSVError and preserving the traceback is a pain
# FIXME: is colMap needed any more???
# FIXME: need to add write stuff. (see GeneCheck), where str() is called on
#        alfor all column ty
# FIXME: change TSVRow to be based on collections.namedtuple **** (thanks Max)
#
# rename  typeMap -> colTypes
# FIXME: make a column object.
# FIXME: save original column names before mapping for output

# typeMap converter for str types were empty represents None
strOrNoneType = (lambda v: None if (v == "") else v,
                 lambda v: "" if (v == None) else v)

# typeMap converter for int types were empty represents None
intOrNoneType = (lambda v: None if (v == "") else int(v),
                 lambda v: "" if (v == None) else str(v))

class TSVReader(object):
    """Class for reading TSV files.  Reads header and builds column name to
    column index map.  After a next, object contains a row and each column
    becomes a field name.  It is also indexable by column name or int index.
    Columns can be automatically type converted by column name.  This can also
    read from a dbapi cursor object (must set allowEmpty to true)
    """

    def __readRow(self):
        "read the next row, returning None on EOF"
        if self.reader == None:
            return None
        try:
            row = self.reader.next()
        except Exception, e:
            self.close()
            if isinstance(e, StopIteration):
                return None
            else:
                raise
        self.lineNum = self.reader.line_num
        return row

    def __readHeader(self, allowEmpty):
        row = self.__readRow()
        if row == None:
            if not allowEmpty:
                raise TSVError("empty TSV file", reader=self), None, sys.exc_info()[2]
        else:
            if self.isRdb:
                self.__readRow() # skip format line
            if (len(row) > 0) and row[0].startswith('#'):
                row[0] = row[0][1:]
            self.__setupColumns(row)

    def __setupColumns(self, columns):
        # n.b. columns could be passed in from client, must copy
        i = 0
        for col in columns:
            if self.columnNameMapper != None:
                col = self.columnNameMapper(col)
            col = intern(col)
            self.columns.append(col)
            if col in self.colMap:
                raise TSVError("Duplicate column name: " + col), None, sys.exc_info()[2]
            self.colMap[col] = i
            i += 1

    def __initColTypes(self, typeMap, defaultColType):
        "save col types as column indexed list"
        if typeMap != None:
            # build from type map
            self.colTypes = []
            for col in self.columns:
                self.colTypes.append(typeMap.get(col, defaultColType))
        elif defaultColType != None:
            # fill in colTypes from default
            self.colTypes = []
            for i in xrange(len(self.columns)):
                self.colTypes.append(defaultColType)

    def __init__(self, fileName, rowClass=None, typeMap=None, defaultColType=None, columns=None, columnNameMapper=None,
                 ignoreExtraCols=False, isRdb=False, inFh=None, allowEmpty=False, dialect=csv.excel_tab):
        """Open TSV file and read header into object.  Removes leading # from
        UCSC header.

        fileName - name of file, opened unless inFh is specified
        rowClass - class or class factory function to use for a row. Must take
            TSVReader and list of string values of columns.
        typeMap - if specified, it maps column names to the type objects to
            use to convert the column.  Unspecified columns will not be
            converted. Key is the column name, value can be either a type
            or a tuple of (parseFunc, formatFunc).  If a type is use,
            str() is used to convert to a printable value.
        defaultColType - if specified, type of unspecified columns
        columns - if specified, the column names to use.  The header
            should not be in the file.
        columnNameMapper - function to map column names to the internal name.
        ignoreExtraCols - should extra columns be ignored?
        isRdb - file is an RDB file, ignore second row (type map still needed).
        inFh - If not None, this is used as the open file, rather than
          opening it.  Closed when the end of file is reached.
        allowEmpty - an empty input results in an EOF rather than an error.
          Should specify this if reading from a database query.
        dialect - a csv dialect object or name.
        """
        self.columns = []
        self.colMap = {}
        self.fileName = fileName
        self.lineNum = 0
        self.rowClass = rowClass
        if rowClass == None:
            self.rowClass = TSVRow
        self.columnNameMapper = columnNameMapper
        self.isRdb = isRdb
        self.colTypes = None
        self.ignoreExtraCols = ignoreExtraCols
        if inFh != None:
            self.inFh = inFh
        else:
            self.inFh = fileOps.opengz(fileName, "r")
        try:
            self.reader = csv.reader(self.inFh, dialect=dialect)
            if columns:
                self.__setupColumns(columns)
            else:
                self.__readHeader(allowEmpty)
            self.__initColTypes(typeMap, defaultColType)
        except Exception, e:
            self.close()
            raise

    def close(self):
        """close file, keeping column map around.  Called automatically
        by iter on EOF."""
        if self.inFh != None:
            self.inFh.close()
            self.inFh = None
            self.reader = None

    def __iter__(self):
        return self

    def next(self):
        try:
            row = self.__readRow()
        except Exception,ex:
            raise TSVError("Error reading TSV row", self, ex), None, sys.exc_info()[2]
        if row == None:
            raise StopIteration
        if ((self.ignoreExtraCols and (len(row) < len(self.columns)))
            or ((not self.ignoreExtraCols) and (len(row) != len(self.columns)))):
            # FIXME: will hang: self.close()
            raise TSVError("row has %d columns, expected %d" %
                           (len(row), len(self.columns)),
                           reader=self), None, sys.exc_info()[2]
        try:
            return self.rowClass(self, row)
        except Exception,ex:
            raise TSVError("Error converting TSV row to object", self, ex), None, sys.exc_info()[2]


