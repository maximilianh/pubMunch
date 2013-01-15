# Copyright 2006-2012 Mark Diekhans

# FIXME: danger of bdump, etc, methods conflicting with columns.  maybe
# a better convention to avoid collisions
# FIXME: need accessor functions for columns

class TSVRow(object):
    "Row of a TSV where columns are fields."
    # n.b.: doesn't inherit from list, as this results in columns in two
    # places when they are stored as fields

    def __init__(self, reader, row):
        # FIXME: stupid names
        self._columns_ = reader.columns
        self._colTypes_ = reader.colTypes
        self._colMap_ = reader.colMap
        if self._colTypes_:
            self.__parse(row)
        else:
            for i in xrange(len(self._columns_)):
                self.__dict__[self._columns_[i]] = row[i]

    def __parse(self, row):
        for i in xrange(len(self._columns_)):
            col = row[i]
            ct = self._colTypes_[i]
            if type(ct) == tuple:
                col = ct[0](col)
            elif ct:
                col = ct(col)
            self.__dict__[self._columns_[i]] = col

    def __getitem__(self, key):
        "access a column by string key or numeric index"
        if isinstance(key, int):
            return self.__dict__[self._columns_[key]]
        else:
            return self.__dict__[key]

    def __setitem__(self, key, val):
        "set a column by string key or numeric index"
        if isinstance(key, int):
            self.__dict__[self._columns_[key]] = val
        else:
            self.__dict__[key] = val

    def __len__(self):
        return len(self._columns_)

    def __iter__(self):
        for col in self._columns_:
            yield self.__dict__[col]

    def __contains__(self, key):
        return key in self._colMap_

    def __fmtWithTypes(self):
        row = []
        for i in xrange(len(self._columns_)):
            col = self.__dict__[self._columns_[i]]
            ct = self._colTypes_[i]
            if col == None:
                col = ""
            elif type(ct) == tuple:
                col = ct[1](col)
            else:
                col = str(col)
            row.append(col)
        return row

    def __fmtNoTypes(self):
        row = []
        for cn in self._columns_:
            col = self.__dict__[cn]
            if col == None:
                row.append("")
            else:
                row.append(str(col))
        return row

    def getRow(self):
        if self._colTypes_:
            return self.__fmtWithTypes()
        else:
            return self.__fmtNoTypes()
    
    def __str__(self):
        return "\t".join(self.getRow())

    def getColumns(self, colNames):
        """get a subset of the columns in the row as a list"""
        subRow = []
        for col in colNames:
            subRow.append(self[col])
        return subRow

    def write(self, fh):
        fh.write(str(self))
        fh.write("\n")

    def dump(self, fh):
        i = 0;
        for col in self:
            if i > 0:
                fh.write("\t")
            fh.write(self._columns_[i])
            fh.write(": ")
            fh.write(str(col))
            i += 1
        fh.write("\n")

