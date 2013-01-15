# Copyright 2006-2012 Mark Diekhans
# Copyright sebsauvage.net
"""
Code from:
http://sebsauvage.net/python/snyppets/index.html#dbdict

A dictionnary-like object for LARGE datasets

Python dictionnaries are very efficient objects for fast data access. But when data is too large to fit in memory, you're in trouble.
Here's a dictionnary-like object which uses a SQLite database and behaves like a dictionnary object:

   - You can work on datasets which to not fit in memory. Size is not limited
     by memory, but by disk. Can hold up to several tera-bytes of data (thanks
     to SQLite).
   - Behaves like a dictionnary (can be used in place of a dictionnary object
     in most cases.)
   - Data persists between program runs.
   - ACID (data integrity): Storage file integrity is assured. No half-written
     data. It's really hard to mess up data.
   - Efficient: You do not have to re-write a whole 500 Gb file when changing
     only one item. Only the relevant parts of the file are changed.
   - You can mix several key types (you can do d["foo"]=bar and d[7]=5468)
     (You can't to this with a standard dictionnary.)
   - You can share this dictionnary with other languages and systems (SQLite
     databases are portable, and the SQlite library is available on a wide
     range of systems/languages, from mainframes to PDA/iPhone, from Python to
     Java/C++/C#/perl...)

Modified by markd:
  - renamed dbdict -> DbDict
  - include key name in KeyError exceptions
  - specify name of file, not the dictName in imported code that didn't allow
    specifying the directory.
  - added table option to allow storing multiple dictionaries in table
  - add truncate constructor option
"""

import os, UserDict
from sqlite3 import dbapi2 as sqlite

class DbDict(UserDict.DictMixin):
    ''' DbDict, a dictionnary-like object for large datasets (several
    Tera-bytes) backed by an SQLite database'''
    
    def __init__(self, db_filename, table="data", truncate=False):
        self.db_filename = db_filename
        self.table = table
        self.con = sqlite.connect(self.db_filename)
        if truncate:
            self.con.execute("drop table if exists " + self.table)
        self.con.execute("create table if not exists " + self.table + " (key PRIMARY KEY,value)")
    
    def __getitem__(self, key):
        row = self.con.execute("select value from " + self.table + " where key=?",(key,)).fetchone()
        if not row:
            raise KeyError(str(key))
        return row[0]
    
    def __setitem__(self, key, item):
        if self.con.execute("select key from " + self.table + " where key=?",(key,)).fetchone():
            self.con.execute("update " + self.table + " set value=? where key=?",(item,key))
        else:
            self.con.execute("insert into " + self.table + " (key,value) values (?,?)",(key, item))
        self.con.commit()
               
    def __delitem__(self, key):
        if self.con.execute("select key from " + self.table + " where key=?",(key,)).fetchone():
            self.con.execute("delete from " + self.table + " where key=?",(key,))
            self.con.commit()
        else:
             raise KeyError(str(key))
             
    def keys(self):
        return [row[0] for row in self.con.execute("select key from " + self.table).fetchall()]
