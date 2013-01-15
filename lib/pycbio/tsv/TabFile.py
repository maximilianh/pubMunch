# Copyright 2006-2012 Mark Diekhans

## FIXME: needed for faster readings, but needs cleaned up, need reader/writer
## classes
# FIXME add try and error msg with file/line num, move to row reader class; see fileOps.iterRows
from pycbio.sys import fileOps
import csv

class TabFile(list):
    """Class for reading tab-separated files.  Can be used standalone
       or read into rows of a specific type.
    """

    def __init__(self, fileName, rowClass=None, hashAreComments=False):
        """Read tab file into the object
        """
        self.fileName = fileName
        self.rowClass = rowClass
        for row in TabFileReader(self.fileName, rowClass=rowClass, hashAreComments=hashAreComments):
            self.append(row)

    @staticmethod
    def write(fh, row):
        """print a row (list or tuple) to a tab file."""
        cnt = 0;
        for col in row:
            if cnt > 0:
                fh.write("\t")
            fh.write(str(col))
            cnt += 1
        fh.write("\n")

class TabFileReader(object):
    def __init__(self, tabFile, rowClass=None, hashAreComments=False, skipBlankLines=False):
        self.inFh = fileOps.opengz(tabFile)
        self.csvRdr = csv.reader(self.inFh, dialect=csv.excel_tab)
        self.rowClass = rowClass
        self.hashAreComments = hashAreComments
        self.skipBlankLines = skipBlankLines
        self.lineNum = 0

    def __readRow(self):
        "read the next row, returning None on EOF"
        if self.csvRdr == None:
            return None
        try:
            row = self.csvRdr.next()
        except Exception, e:
            self.close()
            if isinstance(e, StopIteration):
                return None
            else:
                raise
        self.lineNum = self.csvRdr.line_num
        return row

    def __iter__(self):
        return self

    def __keepRow(self, row):
        if self.hashAreComments and (len(row) > 0) and row[0].startswith("#"):
            return False
        elif self.skipBlankLines and (len(row) == 0):
            return False
        else:
            return True

    def next(self):
        while True:
            row = self.__readRow()
            if row == None:
                raise StopIteration
            if self.__keepRow(row):
                if self.rowClass != None:
                    return self.rowClass(row)
                else:
                    return row

    def close(self):
        "close file, called automatically on EOF"
        if self.inFh != None:
            self.inFh.close()
            self.inFh = None
            self.csvRdr = None
