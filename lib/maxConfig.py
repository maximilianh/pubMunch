# module to parse config files

import sys, os, ConfigParser, logging

logger = logging.getLogger("maxConfig")

config = None
section = None
configname = None

def parse(filename):
    """ parse config file 'filename' from current dir into memory, 
        then try to read it from ~/.filename """

    global config
    global configname
    configname=filename
    config = ConfigParser.ConfigParser()
    config.read([filename, os.path.expanduser('~/.'+filename)])

def initFromString(section, strings):
    """ converts string like 'bla=test test=2 bla=4' to a dict and initilizes
    config from it. Used to update config from command line parameters """
    for s in strings:
        key, value = s.split("=")
        config.set(section, key, value)

def setSection(sec):
    global section
    section = sec

def mustGet(section, key):
    """ read value from ini file, bail out if not found """
    global configname
    val = get(section, key, None)
    if val==None:
        logging.error("error: key %s not defined in config file %s, section %s."% (key, configname, section))
        sys.exit(1)
    else:
        return val

def mustGetPath(section, key):
    """ like must get, but expand ~ and $(xx)-style env variables """
    path = mustGet(section, key)
    path = os.path.expanduser(path)
    path = os.path.expandvars(path)
    #path = os.path.relpath(path)
    return path

def mustGetInt(section, key):
    return int(mustGet(section, key))

def mustGetBool(section, key):
    return bool(mustGetInt(section, key))

def getSectionValue(key, default):
    return get(section, key, default)

def mustGetSectionValue(key):
    return mustGet(section, key)

def getValue(key, default):
    """ alternative name for getSectionValue """
    return get(section, key, default)

def getSectionPath(key, default):
    value = getValue(key, default)
    if value!=None:
        value = os.path.expanduser(value)
    return value

def get(section, key, default):
    """ retrieve value for key in config file, if not found return default """

    if section in config.sections():
        try:
            val = config.get(section, key)
            logger.debug("Config %s/%s=%s" %(section, key, val))
            return val
        except ConfigParser.NoOptionError:
            logger.debug("Config value %s/%s not found: returning %s" %(section, key, default))
            return default
    else:
        logger.info("Config %s/%s: section not found, returning %s" %(section, key, default))
        return default

def getBool(section, key, default):
    return bool(int(get(section, key, default)))

def getInt(section, key, default):
    return int(get(section, key, default))

def getFloat(section, key, default):
    return float(get(section, key, default))

def getAllPrefix(section, prefix):
    """ get all values in section that start with prefix """
    prefix=prefix.lower()
    values = []
    for name, value in config.items(section):
        if name.startswith(prefix):
            key = name.replace(prefix+".", "")
            values.append((key, value))
    logger.info("Config %s/prefix %s: %s" %(section, prefix, str(values)))
    return values

def sqlConnStringToDict(connString):
    """ convert a string in format host:port,username,passwd,db to a dictionary that can be passed directly to mysqldb.connect """
    if connString==None:
        return {}

    host,user,passwd,db = connString.split(",")

    port = None
    fs = host.split(":")
    host = fs[0]
    if len(fs)>1:
        port=int(fs[1])

    dict = {"host":host, "user":user, "passwd":passwd, "db":db, "read_default_file":"/etc/my.cnf"}
    if port:
        dict["port"]=port
    
    return dict

#
mysqlHost = "127.0.0.1"
mysqlPort = 3306
mysqlUser = "max"
mysqlPassword = "testqwe"
mysqlDb = "text2genome"
menu = [ 
                ("Text2Genome" , [ 
                    ("About","about.cgi"), 
                    ("Search","search.cgi"), 
                    ("Browse","browserOverlay.cgi"),
                    ("Download","download.cgi"),
                    ("API","api.cgi")
                    ] ) ,
                ("About us" , [ ("Bergman Lab", "http://www.bioinf.manchester.ac.uk/bergman/") ] )
            ]

host = "http://max.smith.man.ac.uk"
baseDir = host+"/t2g"
# We need the FULL URL of the inspector.cgi script on your webserver
detailsUrl= baseDir+'/inspector.cgi'
# We need the FULL URL of the DAS root on your webserver
dasUrl = baseDir+'/das'
# We need the FULL URL of the ucsc tracks on your webserver
bedDir= baseDir+'/ucsc/'

