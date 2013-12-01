from os.path import join, dirname, basename, isdir, isfile, abspath
import logging, random, socket, atexit, sqlite3, os, time, shutil

import maxCommon, maxTables

try:
    import redis
except ImportError:
    pass

try:
    import leveldb
except ImportError:
    pass

def openDb(dbName, newDb=False, singleProcess=False, prefer=None):
    " factory function: returns the right db object given a filename "
    #return LevelDb(dbName, newDb=newDb)
    # possible other candidates: mdb, cdb, hamsterdb
    logging.info("key-value store preference for: %s" % prefer)
    logging.info("newDb %s" % str(newDb))
    if prefer=="server":
        return RedisDb(dbName, newDb=newDb, singleProcess=singleProcess)
    else:
        return SqliteKvDb(dbName, newDb=newDb, singleProcess=singleProcess)

def findFreePort(port=None):
    """ 
    returns free port on local host or None if specified port is taken
   
    >>> findFreePort(8080)
    """
    portFound = False
    while not portFound:
        if port == None:
            port = random.randint(1025, 65535)
        s = socket.socket()
        try:
            s.bind(("127.0.0.1", port))
        except Exception:
            logging.info("Port %d is in use" % port)
            return None
        else:
            portFound = True
    logging.debug("Found free port: %d" % port)
    return port

# global for shutdownRedisServers
redisPorts = []

def shutdownRedisServers():
    """ shutdown all redis servers that we started """
    for p in redisPorts:
        logging.info("Shutting down redis server on port %d" % p)
        r = redis.StrictRedis(port=p)
        r.shutdown()
        #r.quit()

def startRedis(dbFname):
    """ starts redis on current server as daemon. 
    Creates status files with filename dbName".pid" and dbName".host". Returns the port.

    >>> import pubGeneric
    >>> pubGeneric.setupLogging(__file__, None)
    >>> h, p = startRedis("/tmp/test.tab.gz")
    >>> r = redis.Redis(port=p)
    >>> r.set("hello", "world")
    True
    >>> r.get("hello")
    'world'
    >>> r.get("world")
    >>> r.shutdown()
    """
    dbFname = abspath(dbFname)
    pidFname  = dbFname+".pid"
    port      = findFreePort()
    dirName   = dirname(dbFname)
    baseName  = basename(dbFname)+".rdb"

    hostFname = dbFname+".host"
    hostname  = socket.gethostbyname("localhost")
    hostDesc  = hostname+":"+str(port)
    open(hostFname, "w").write(hostDesc)
    logging.info("Wrote redis host info %s to %s" % (hostDesc, hostFname))
    maxCommon.delOnExit(hostFname)
    maxCommon.delOnExit(pidFname)
    atexit.register(shutdownRedisServers)
    global redisPorts
    redisPorts.append(port)

    cmd = ["redis-server", "--daemonize", "yes", "--pidfile", pidFname, \
        "--port", str(port), "--rdbchecksum", "no", "--dir", dirName,
        "--dbfilename", baseName, "--maxmemory", "200gb"]
    logging.info("Starting up redis server on localhost")
    maxCommon.runCommand(cmd)

    # wait until startup is complete
    redisStart = True
    while redisStart:
        try:
            r = redis.Redis(port=port)
            dbSize = r.dbsize()
            redisStart=False
        except redis.ConnectionError:
            logging.info("Waiting for 1 sec for redis startup completion")
            time.sleep(1)
            pass
    logging.info("Redis startup completed, dbSize=%d" % dbSize)

    return "localhost", port

class RedisDb(object):
    """ wrapper around redis. Will startup a redis server on localhost and write a dbFname.host 
        file so that clients can find this server. if dbFname.host already exists, we just connect
        to the redis server.
        This can load ~60000 genbank ids/sec on hgwdev
    """
    def __init__(self, fname, singleProcess=False, newDb=False):
        self.dbName = fname+".rdb"
        self.singleProcess = singleProcess
        if newDb:
            if isfile(self.dbName):
                logging.info("Deleting old %s" % self.dbName)
                os.remove(self.dbName)

        statusFname = fname+".host"
        if isfile(statusFname):
            host, port = open(statusFname).read().strip().split(":")
        else:
            host, port = startRedis(fname)
        self.redis = redis.Redis(host=host, port=port)
        logging.info("Connected to redis server at %s:%s, dbSize %d" % (host, str(port), self.redis.dbsize()))

    def get(self, key, default=None):
        val = self.redis.get(key)
        if val==None:
            return default
        else:
            return val

    def __contains__(self, key):
        return bool(self.redis.exists(key))

    def __getitem__(self, key):
        val = self.get(key)
        if val==None:
            raise KeyError()
        else:
            return val

    def __setitem__(self, key, val):
        self.redis.set(key, val)
               
    def __delitem__(self, key):
        self.redis.delete(key)
             
    def update(self, keyValPairs):
        d = dict(keyValPairs)
        self.redis.mset(d)
        
    def keys(self):
        " not advised with redis "
        return self.redis.keys("*")

    def close(self):
        if self.singleProcess:
            logging.info("Telling redis to save data to disk")
            self.redis.save()

    #def shutdown(self):
        #self.redis.shutdown()

class SqliteKvDb(object):
    """ wrapper around sqlite to create an on-disk key/value database
        On ramdisk, this can write 40k pairs / sec, tested on 40M uniprot pairs  
    """
    def __init__(self, fname, singleProcess=False, newDb=False, tmpDir=None):
        self.finalDbName = None
        self.dbName = "%s.sqlite" % fname
        if newDb and isfile(self.dbName):
            os.remove(self.dbName)
        isolLevel = None
        self.singleProcess = singleProcess
        if singleProcess:
            isolLevel = "exclusive"
        if not os.path.isfile(self.dbName) and tmpDir!=None:
            # create a new temp db on ramdisk
            self.finalDbName = self.dbName
            #self.dbName = join(pubConf.getFastTempDir(), basename(self.dbName))
            self.dbName = join(tmpDir, basename(self.dbName))
            if isfile(self.dbName):
                os.remove(self.dbName)
            maxCommon.delOnExit(self.dbName)
            self.con = sqlite3.connect(self.dbName, isolation_level=isolLevel)
            self.con.execute("create table IF NOT EXISTS data (key PRIMARY KEY,value)")
        else:
            self.con = sqlite3.connect(self.dbName)

        self.cur = self.con
        if singleProcess:
            self.cur.execute("PRAGMA synchronous=OFF") # recommended by
            self.cur.execute("PRAGMA count_changes=OFF") # http://blog.quibb.org/2010/08/fast-bulk-inserts-into-sqlite/
            self.cur.execute("PRAGMA cache_size=800000") # http://web.utk.edu/~jplyon/sqlite/SQLite_optimization_FAQ.html
            self.cur.execute("PRAGMA journal_mode=OFF") # http://www.sqlite.org/pragma.html#pragma_journal_mode
            self.cur.execute("PRAGMA temp_store=memory") 
            self.con.commit()
    
    def get(self, key, default=None):
        try:
            val = self[key]
        except KeyError:
            val = default
        return val

    def __contains__(self, key):
        row = self.con.execute("select key from data where key=?",(key,)).fetchone()
        return row!=None
    
    def __getitem__(self, key):
        row = self.con.execute("select value from data where key=?",(key,)).fetchone()
        if not row: raise KeyError
        return row[0]
    
    def __setitem__(self, key, item):
        logging.debug("Writing %s, %s" % (key, item))
        if self.con.execute("select key from data where key=?",(key,)).fetchone():
            self.con.execute("update data set value=? where key=?",(item,key))
        else:
            self.con.execute("INSERT OR REPLACE INTO data (key,value) VALUES (?,?)",(key, item))

        self.con.commit()
               
    def __delitem__(self, key):
        if self.con.execute("select key from data where key=?",(key,)).fetchone():
            self.con.execute("delete from data where key=?",(key,))
            self.con.commit()
        else:
             raise KeyError
             
    def update(self, keyValPairs):
        sql = "INSERT OR REPLACE INTO data (key, value) VALUES (?,?)"
        self.cur.executemany(sql, keyValPairs)
        self.con.commit()
        
    def keys(self):
        return [row[0] for row in self.con.execute("select key from data").fetchall()]

    def close(self):
        if self.finalDbName!=None:
            logging.info("Copying %s to %s" % (self.dbName, self.finalDbName))
            shutil.copy(self.dbName, self.finalDbName)
            os.remove(self.dbName)

class LevelDb(object):
    """ wrapper around leveldb, store and query key/val pais.
    Not very useful, as it always locks the database, only one process can read!!
    """
    def __init__(self, fname, singleProcess=False, newDb=False):
        self.dbName = fname+".levelDb"
        if newDb and isdir(self.dbName):
            logging.debug("Removing %s" % self.dbName)
            shutil.rmtree(self.dbName)
        logging.debug("Opening %s with leveldb" % self.dbName)
        self.db = leveldb.LevelDB(self.dbName)
        self.sync = not singleProcess

    def put(self, key, val):
        return self.db.Put(key, val, sync=self.sync)
        
    def get(self, key, default=None):
        try:
            val = self.db.Get(key)
        except KeyError:
            val = default
        return val
    
    def has_key(self, key):
        # old python way
        self.__contains__(key)

    def __contains__(self, key):
        try:
            self.db.Get(key)
            return True
        except KeyError:
            return False

    def __getitem__(self, key):
        val = self.db.Get(key)
        return val

    def __setitem__(self, key, val):
        if val==None:
            val=""
        return self.db.Put(key, val, sync=self.sync)

    def close(self):
        pass

def indexKvFile(fname, startOffset=0, prefer=None, newDb=False):
    " load a key-value tab-sep file with two fields into a key-value DB "
    db  = openDb(fname, singleProcess=True, prefer=prefer, newDb=newDb)
    ifh = maxTables.openFile(fname)
    ifh.seek(startOffset)
    i = 0
    logging.info("Indexing %s to db %s" % (fname, db.dbName))
    if startOffset!=0:
        logging.info("file offset is %d" % startOffset)
    chunkSize = 500000
            
    pairs = []
    for line in ifh:
        if line.startswith("#"):
            continue
        fields = line.rstrip("\n").split("\t")
        fCount = len(fields)
        if fCount==2:
            key, val = fields
        elif fCount==1:
            key = fields[0]
            val = None
        else:
            raise Exception("cannot load more than two fields or empty line")

        pairs.append ( (key, val) )

        if i%chunkSize==0:
            db.update(pairs)
            logging.info("Wrote %d records..." % i)
            pairs = []
        #db[key] = val
        i+=1
    if len(pairs)!=0:
        db.update(pairs)
    db.close()
    logging.info("Wrote %d records to db %s" % (i, db.dbName))

if __name__=="__main__":
    import doctest
    doctest.testmod()
