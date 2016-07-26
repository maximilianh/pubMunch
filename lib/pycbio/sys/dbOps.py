# Copyright 2006-2012 Mark Diekhans
"""Operations on dbapi objects"""

def cursorColIdxMap(cur):
    """generate a hash of column name to row index given a cursor that has had
    a select executed"""
    m = {}
    for i in range(len(cur.description)):
        m[cur.description[i][0]] = i
    return m
